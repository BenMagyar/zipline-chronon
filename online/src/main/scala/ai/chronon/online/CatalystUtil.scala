/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import ai.chronon.api.{DataType, StructType}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.serde._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionAlreadyExistsException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions, types}
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.util.concurrent.{CompletableFuture, CompletionException, ConcurrentHashMap}
import java.util.function

object CatalystUtil {
  lazy val session: SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(s"catalyst_test_${Thread.currentThread().toString}")
      .master("local[*]")
      // This serving path only uses Spark for Catalyst planning/codegen, not for long-lived RDD or shuffle cleanup.
      // Disabling reference tracking avoids the ContextCleaner thread's periodic System.gc() calls in the JVM.
      .config("spark.cleaner.referenceTracking", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.ui.enabled", "false")
      // the default column reader batch size is 4096 - spark reads that many rows into memory buffer at once.
      // that causes ooms on large columns.
      // for derivations we only need to read one row at a time.
      // for interactive we set the limit to 16.
      .config("spark.sql.parquet.columnarReaderBatchSize", "16")
      // The default doesn't seem to be set properly in the scala 2.13 version of spark
      // running into this issue https://github.com/dotnet/spark/issues/435
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config(SQLConf.DATETIME_JAVA8API_ENABLED.key, true)
      .config(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key, false)
      // required for Hive UDF support
      .enableHiveSupport()
      .getOrCreate()
    // Allow callers to override the S3A credential provider via AWS_CREDENTIALS_PROVIDER.
    // core-default.xml (bundled in hadoop-client-runtime) defaults to a narrow chain that
    // excludes file-based credentials (~/.aws/credentials). Setting this on hadoopConf before
    // any s3a:// filesystem is accessed (e.g. ADD JAR setup statements) overrides that default.
    // Example: AWS_CREDENTIALS_PROVIDER=software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
    // for local fetcher runs with ~/.aws/credentials.
    sys.env.get("AWS_CREDENTIALS_PROVIDER").foreach { provider =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", provider)
    }
    // Set fs.s3a.endpoint.region from the standard AWS env vars so ADD JAR s3a:// paths resolve
    // to the correct region. S3A defaults to us-east-1 and returns a redirect for other regions.
    val awsRegion = sys.env.getOrElse("AWS_DEFAULT_REGION", sys.env.getOrElse("AWS_REGION", ""))
    if (awsRegion.nonEmpty && spark.sparkContext.hadoopConfiguration.get("fs.s3a.endpoint.region", "").isEmpty) {
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint.region", awsRegion)
    }
    assert(spark.sessionState.conf.wholeStageEnabled)
    spark
  }

  case class BlueprintKey(inputSchema: StructType,
                          selects: Seq[(String, String)],
                          wheres: Seq[String],
                          setups: Seq[String],
                          timestampMillisOutputColumns: Set[String])

  // the memo holder keeps planning outside of computeIfAbsent - otherwise the map bin stays locked for the
  // ~100ms to 1s that a cold plan takes, blocking unrelated keys that hash to the same bin
  private class Memo[T](thunk: => T) { lazy val get: T = thunk }

  private val blueprints: ConcurrentHashMap[BlueprintKey, Memo[CatalystUtilBlueprint]] =
    new ConcurrentHashMap[BlueprintKey, Memo[CatalystUtilBlueprint]]()

  // s3:// and s3a:// identify the same source for setup purposes. Normalize before building the key so they share
  // both the compiled blueprint and the localized jar.
  private def normalizeSetups(setups: Seq[String]): Seq[String] =
    setups.map(_.replace("s3://", "s3a://"))

  // A setup is part of the blueprint key, so the same UDF jar can otherwise be copied to a new temp file for
  // every distinct query that uses it. Spark's session and jar classloader live for the process lifetime, so cache
  // the complete successful localization and registration operation by source URI.
  private val localizedJars = new ConcurrentHashMap[URI, CompletableFuture[File]]()

  private[online] def normalizeJarUri(jarPath: String): URI = {
    val uri = new URI(jarPath)
    // Preserve existing setup contracts while routing legacy S3 URIs through Hadoop's supported filesystem.
    if (Option(uri.getScheme).exists(_.equalsIgnoreCase("s3"))) {
      new URI("s3a", uri.getRawSchemeSpecificPart, uri.getRawFragment)
    } else {
      uri
    }
  }

  private[online] def localizeJar(jarPath: String): File = {
    val normalizedUri = normalizeJarUri(jarPath)
    // Loading a changed implementation at the same URI cannot safely replace classes already defined by Spark's
    // process-wide classloader. Publishers must use immutable, versioned paths. Keep remote I/O outside the
    // ConcurrentHashMap mutation: callers for the same URI share the promise, while an unrelated hash-bin collision
    // cannot hold up a different jar copy.
    val pending = new CompletableFuture[File]()
    val existing = localizedJars.putIfAbsent(normalizedUri, pending)
    if (existing == null) {
      try {
        val localized = copyAndRegisterJar(normalizedUri)
        if (!pending.complete(localized)) {
          // Startup may have expired this promise so request threads fail fast rather than waiting on a stuck Hadoop
          // copy. If the original copy eventually succeeds, publish that successful file atomically; never start a
          // competing copy or register the same jar concurrently.
          localizedJars.replace(normalizedUri, pending, CompletableFuture.completedFuture(localized))
        }
        localized
      } catch {
        case exception: Throwable =>
          pending.completeExceptionally(exception)
          localizedJars.remove(normalizedUri, pending)
          throw exception
      }
    } else {
      try existing.join()
      catch {
        case wrapper: CompletionException if wrapper.getCause != null => throw wrapper.getCause
      }
    }
  }

  /** Localize and register an immutable derivation jar before request traffic begins.
    *
    * This is intentionally jar-only: callers do not need a live Join configuration, representative keys, or a
    * feature-store read. The same process-wide cache is used by `ADD JAR`, so request-time setup reuses the
    * completed startup copy.
    */
  def prefetchJar(jarPath: String): File = localizeJar(jarPath)

  /** Expire an unresolved startup prefetch without removing its single-flight owner.
    *
    * Existing/request-time callers fail promptly with `error`. If the original copy later succeeds it replaces this
    * exceptional promise with its completed file; if it fails, the owner removes the entry so a later request can retry.
    */
  def expirePrefetchJar(jarPath: String, error: Throwable): Boolean =
    Option(localizedJars.get(normalizeJarUri(jarPath))).exists(_.completeExceptionally(error))

  private[online] def localizedJarCount: Int = localizedJars.size()

  private[online] def isJarLocalized(jarPath: String): Boolean =
    Option(localizedJars.get(normalizeJarUri(jarPath))).exists { pending =>
      pending.isDone && !pending.isCompletedExceptionally && !pending.isCancelled
    }

  private def copyAndRegisterJar(uri: URI): File = {
    val sparkContext = session.sparkContext
    val sourcePath = new org.apache.hadoop.fs.Path(uri)
    val fileSystem = sourcePath.getFileSystem(sparkContext.hadoopConfiguration)
    val tempDir = Files.createTempDirectory("chronon-udf-").toFile
    tempDir.deleteOnExit()
    // SparkContext indexes added jars by basename, so two different URIs ending in the same common build name
    // (for example, out.jar) must still receive distinct local names.
    val sourceName = Option(sourcePath.getName).filter(_.nonEmpty).getOrElse("udf.jar")
    val localFile = new File(tempDir, s"${tempDir.getName}-$sourceName")
    localFile.deleteOnExit()

    try {
      fileSystem.copyToLocalFile(sourcePath, new org.apache.hadoop.fs.Path(localFile.toURI))
      sparkContext.addJar(localFile.getAbsolutePath)
      session.sharedState.jarClassLoader.addURL(localFile.toURI.toURL)
      localFile
    } catch {
      case exception: Exception =>
        Option(tempDir.listFiles()).getOrElse(Array.empty[File]).foreach { child =>
          try Files.deleteIfExists(child.toPath)
          catch {
            case cleanupError: Exception => exception.addSuppressed(cleanupError)
          }
        }
        try Files.deleteIfExists(tempDir.toPath)
        catch {
          case cleanupError: Exception => exception.addSuppressed(cleanupError)
        }
        throw exception
    }
  }

  private[online] def evictLocalizedJarForTest(jarPath: String): Unit = {
    val localizedFile = Option(localizedJars.remove(normalizeJarUri(jarPath))).flatMap { pending =>
      if (pending.isDone && !pending.isCompletedExceptionally && !pending.isCancelled)
        Option(pending.getNow(null))
      else None
    }
    localizedFile.foreach { file =>
      val directory = file.getParentFile
      // LocalFileSystem may put a hidden checksum beside the copied jar.
      Option(directory.listFiles()).getOrElse(Array.empty[File]).foreach(child => Files.deleteIfExists(child.toPath))
      Files.deleteIfExists(directory.toPath)
    }
  }

  /** Spark planning (parse -> analyze -> optimize -> physical plan -> java source generation) dominates the
    * cost of constructing a CatalystUtil and depends only on the query and the input schema. Cache it so that
    * every CatalystUtil for a given query after the first is close to free.
    */
  def blueprintOf(inputSchema: StructType,
                  selects: Seq[(String, String)],
                  wheres: Seq[String],
                  setups: Seq[String],
                  timestampMillisOutputColumns: Set[String]): CatalystUtilBlueprint =
    blueprints
      .computeIfAbsent(
        BlueprintKey(inputSchema, selects, wheres, normalizeSetups(setups), timestampMillisOutputColumns),
        new function.Function[BlueprintKey, Memo[CatalystUtilBlueprint]] {
          override def apply(k: BlueprintKey): Memo[CatalystUtilBlueprint] =
            new Memo(new CatalystUtilBlueprint(k))
        }
      )
      .get
}

/** One CatalystUtil per thread. Instances are cheap now that the plan and the compiled class live on the
  * blueprint, so a thread local beats a shared pool - it drops the single lock that every call used to
  * serialize on. The tradeoff is that a thread which never ran this query before builds its own instance.
  */
class PooledCatalystUtil(expressions: Seq[(String, String)], inputSchema: StructType, setups: Seq[String] = Seq.empty) {
  private val blueprint = CatalystUtil.blueprintOf(inputSchema, expressions, Seq.empty, setups, Set.empty)
  private val threadLocalUtil: ThreadLocal[CatalystUtil] = ThreadLocal.withInitial(() => blueprint.newInstance())

  def performSql(values: Map[String, Any]): Seq[Map[String, Any]] = threadLocalUtil.get().performSql(values)
  def outputChrononSchema: Array[(String, DataType)] = blueprint.outputChrononSchema
}

/** Everything about a (schema, query) pair that is immutable and therefore shareable across threads: the
  * physical plan, the compiled whole stage class, the schemas and the row conversion functions. The only
  * per-instance state is what `transformFactory()` allocates.
  */
class CatalystUtilBlueprint(key: CatalystUtil.BlueprintKey) {

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  val selectClauses: Seq[String] = key.selects.map { case (name, expr) => s"$expr as $name" }

  val whereClauseOpt: Option[String] = Option(key.wheres)
    .filter(_.nonEmpty)
    .map { w =>
      // wrap each clause in parens
      w.map(c => s"( $c )").mkString(" AND ")
    }

  val inputSparkSchema: types.StructType = SparkConversions.fromChrononSchema(key.inputSchema)

  private val sessionTable =
    s"q${math.abs(selectClauses.mkString(", ").hashCode)}_f${math.abs(inputSparkSchema.pretty.hashCode)}"

  val inputEncoder: Any => Any = SparkInternalRowConversions.to(inputSparkSchema)
  val inputArrEncoder: Any => Any = SparkInternalRowConversions.to(inputSparkSchema, false)

  val (transformFactory: CatalystTransformBuilder.TransformFactory, outputSparkSchema: types.StructType) =
    initialize()

  val outputDecoder: Any => Any = SparkInternalRowConversions.from(outputSparkSchema)
  val outputArrDecoder: Any => Any = SparkInternalRowConversions.from(outputSparkSchema, false)
  val outputChrononSchema: Array[(String, DataType)] = SparkConversions.toChrononSchema(outputSparkSchema)

  val hasSetups: Boolean = key.setups.nonEmpty

  def newInstance(): CatalystUtil = new CatalystUtil(this)

  private[chronon] def normalizeTimestampOutputs(df: DataFrame): DataFrame =
    df.schema.fields
      .filter(field => key.timestampMillisOutputColumns.contains(field.name) && field.dataType == types.TimestampType)
      .foldLeft(df) { case (currentDf, field) =>
        val quotedColumn = s"`${field.name.replace("`", "``")}`"
        currentDf.withColumn(field.name, functions.expr(s"unix_millis($quotedColumn)"))
      }

  private def initialize(): (CatalystTransformBuilder.TransformFactory, types.StructType) = {
    val session = CatalystUtil.session
    executeSetups()

    // create dummy df with sql query and schema
    val emptyRowRdd = session.emptyDataFrame.rdd
    val emptyDf = session.createDataFrame(emptyRowRdd, inputSparkSchema)
    emptyDf.createOrReplaceTempView(sessionTable)
    val projectedDf = session.sqlContext.table(sessionTable).selectExpr(selectClauses.toSeq: _*)
    val normalizedDf = normalizeTimestampOutputs(projectedDf)
    val df = whereClauseOpt.map(normalizedDf.where(_)).getOrElse(normalizedDf)

    // extract transform function from the df spark plan
    val execPlan = df.queryExecution.executedPlan
    logger.info(s"Catalyst Execution Plan - ${execPlan}")

    (CatalystTransformBuilder.buildTransformFactory(execPlan), df.schema)
  }

  // SessionCatalog resolves UDF classes through the thread context classloader. Keep setup
  // execution on the cached blueprint, but retain the Hive-aware classloader and remote-jar
  // localization behavior used by the fetcher and Flink paths.
  private def executeSetups(): Unit = {
    if (key.setups.isEmpty) return
    val session = CatalystUtil.session
    val addJarPattern = """(?i)^\s*ADD\s+JAR\s+['"]?([^\s'"]+)['"]?\s*;?\s*$""".r
    val originalClassLoader = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(session.sharedState.jarClassLoader)
    try {
      key.setups.foreach { statement =>
        try {
          statement.trim match {
            case addJarPattern(jarPath) =>
              val localFile = CatalystUtil.localizeJar(jarPath)
              logger.info(s"Executed setup statement via localizeJar (${localFile.length()} bytes): $statement")
            case _ =>
              session.sql(statement)
              logger.info(s"Executed setup statement: $statement")
          }
        } catch {
          case _: FunctionAlreadyExistsException =>
          case e: Exception =>
            logger.warn(s"Failed to execute setup statement: $statement", e)
            throw new RuntimeException(s"Error executing setup statement: $statement", e)
        }
      }
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader)
    }
  }

}

class CatalystUtil(blueprint: CatalystUtilBlueprint) {

  // Hive resolves externally loaded UDF classes through the calling thread's context classloader. Fetcher worker
  // threads are reused and their context loader can change between requests, so scope Spark's jar loader to every
  // instance creation/evaluation and restore the worker's loader afterwards.
  private def withUdfClassLoader[T](operation: => T): T = {
    if (!blueprint.hasSetups) return operation

    val thread = Thread.currentThread()
    val originalClassLoader = thread.getContextClassLoader
    val udfClassLoader = CatalystUtil.session.sharedState.jarClassLoader
    if (originalClassLoader eq udfClassLoader) {
      operation
    } else {
      thread.setContextClassLoader(udfClassLoader)
      try operation
      finally thread.setContextClassLoader(originalClassLoader)
    }
  }

  def this(inputSchema: StructType,
           selects: Seq[(String, String)],
           wheres: Seq[String] = Seq.empty,
           setups: Seq[String] = Seq.empty,
           timestampMillisOutputColumns: Set[String] = Set.empty) =
    this(CatalystUtil.blueprintOf(inputSchema, selects, wheres, setups, timestampMillisOutputColumns))

  val selectClauses: Seq[String] = blueprint.selectClauses
  val whereClauseOpt: Option[String] = blueprint.whereClauseOpt
  def inputSparkSchema: types.StructType = blueprint.inputSparkSchema

  private val inputEncoder = blueprint.inputEncoder
  val inputArrEncoder: Any => Any = blueprint.inputArrEncoder

  // the only per-instance state - generated iterators and projections carry mutable row buffers
  private val transformFunc: InternalRow => Seq[InternalRow] = withUdfClassLoader(blueprint.transformFactory())

  private val outputSparkSchema: types.StructType = blueprint.outputSparkSchema
  private val outputArrDecoder = blueprint.outputArrDecoder
  private val outputDecoder = blueprint.outputDecoder
  def outputChrononSchema: Array[(String, DataType)] = blueprint.outputChrononSchema

  def performSql(values: Array[Any]): Seq[Array[Any]] = withUdfClassLoader {
    val internalRow = inputArrEncoder(values).asInstanceOf[InternalRow]
    val resultRowSeq = transformFunc(internalRow)
    val outputVal = resultRowSeq.map(resultRow => outputArrDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Array[Any]])
  }

  def performSql(values: Map[String, Any]): Seq[Map[String, Any]] = withUdfClassLoader {
    val internalRow = inputEncoder(values).asInstanceOf[InternalRow]
    performSqlInternal(internalRow)
  }

  def performSql(row: InternalRow): Seq[Map[String, Any]] = withUdfClassLoader(performSqlInternal(row))

  private def performSqlInternal(row: InternalRow): Seq[Map[String, Any]] = {
    val resultRowMaybe = transformFunc(row)
    val outputVal = resultRowMaybe.map(resultRow => outputDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Map[String, Any]])
  }

  def getOutputSparkSchema: types.StructType = outputSparkSchema

  private[chronon] def normalizeTimestampOutputs(df: DataFrame): DataFrame = blueprint.normalizeTimestampOutputs(df)
}
