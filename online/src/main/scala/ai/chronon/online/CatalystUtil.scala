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

import java.util.concurrent.ConcurrentHashMap
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
    // to the correct region. S3A defaults to us-east-1 and returns a 301 redirect for other regions,
    // which causes copyToLocalFile to write the redirect XML body instead of the jar.
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

  /** Spark planning (parse -> analyze -> optimize -> physical plan -> java source generation) dominates the
    * cost of constructing a CatalystUtil and depends only on the query and the input schema. Cache it so that
    * every CatalystUtil for a given query after the first is close to free.
    */
  // Normalize s3:// → s3a:// in setup strings so that callers using either scheme produce the
  // same BlueprintKey and share one blueprint (and therefore one jar download).
  private def normalizeSetups(setups: Seq[String]): Seq[String] =
    setups.map(_.replace("s3://", "s3a://"))

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

  // ADD JAR via session.sql() routes through HiveSessionResourceLoader, which lazily instantiates
  // HiveExternalCatalog and the Derby metastore (DataNucleus). These aren't available in all
  // environments. Intercept ADD JAR, download the jar locally, and register it directly so that
  // CREATE FUNCTION calls that follow can resolve UDF classes via Utils.classForName.
  // The thread context classloader must be set to jarClassLoader for SessionCatalog.makeFunctionBuilder.
  private def executeSetups(session: SparkSession): Unit = {
    if (key.setups.isEmpty) return
    val addJarPattern = """(?i)^\s*ADD\s+JAR\s+['"]?([^\s'"]+)['"]?\s*;?\s*$""".r
    val originalCL = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(session.sharedState.jarClassLoader)
    try {
      key.setups.foreach { statement =>
        try {
          statement.trim match {
            case addJarPattern(jarPath) =>
              val localFile = localizeJar(jarPath, session)
              logger.info(s"Executed setup statement via localizeJar (${localFile.length()} bytes): $statement")
            case _ =>
              session.sql(statement)
              logger.info(s"Executed setup statement: $statement")
          }
        } catch {
          case _: FunctionAlreadyExistsException =>
          // ignore - crops up in unit tests when blueprint is reused across test cases
          case e: Exception =>
            logger.warn(s"Failed to execute setup statement: $statement", e)
            throw new RuntimeException(s"Error executing setup statement: $statement", e)
        }
      }
    } finally {
      Thread.currentThread().setContextClassLoader(originalCL)
    }
  }

  // Downloads a remote jar (s3a://, gs://, or local path) to a temp file, registers it with
  // SparkContext for executor distribution, and adds it to jarClassLoader so the driver can
  // resolve UDF classes loaded from it.
  //
  // We bypass session.sql("ADD JAR") because that routes through HiveSessionResourceLoader, which
  // lazily initializes HiveExternalCatalog → Derby metastore → DataNucleus. DataNucleus requires
  // its three jars (core, api-jdo, rdbms) as separate classpath entries so each jar's plugin.xml
  // is discoverable; in an uber-jar only one plugin.xml survives the merge and the JDO adapter
  // registration is lost, causing a fatal startup error.
  private def localizeJar(jarPath: String, session: SparkSession): java.io.File = {
    val sc = session.sparkContext
    // s3:// has no registered Hadoop FileSystem; rewrite to s3a:// which does.
    val normalizedPath = if (jarPath.startsWith("s3://")) "s3a://" + jarPath.stripPrefix("s3://") else jarPath
    val srcPath = new org.apache.hadoop.fs.Path(new java.net.URI(normalizedPath))
    val fs = srcPath.getFileSystem(sc.hadoopConfiguration)
    val localFile = java.io.File.createTempFile(srcPath.getName.stripSuffix(".jar") + "-", ".jar")
    localFile.deleteOnExit()
    fs.copyToLocalFile(srcPath, new org.apache.hadoop.fs.Path(localFile.toURI))
    sc.addJar(localFile.getAbsolutePath)
    session.sharedState.jarClassLoader.addURL(localFile.toURI.toURL)
    localFile
  }

  private[chronon] def normalizeTimestampOutputs(df: DataFrame): DataFrame =
    df.schema.fields
      .filter(field => key.timestampMillisOutputColumns.contains(field.name) && field.dataType == types.TimestampType)
      .foldLeft(df) { case (currentDf, field) =>
        val quotedColumn = s"`${field.name.replace("`", "``")}`"
        currentDf.withColumn(field.name, functions.expr(s"unix_millis($quotedColumn)"))
      }

  private def initialize(): (CatalystTransformBuilder.TransformFactory, types.StructType) = {
    val session = CatalystUtil.session

    executeSetups(session)

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
}

class CatalystUtil(blueprint: CatalystUtilBlueprint) {

  // If UDF jars were registered into jarClassLoader during blueprint setup, this thread must use
  // jarClassLoader as its context CL so HiveShim.createFunction can find UDF classes at eval time.
  if (blueprint.hasSetups) {
    Thread.currentThread().setContextClassLoader(CatalystUtil.session.sharedState.jarClassLoader)
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
  private val transformFunc: InternalRow => Seq[InternalRow] = blueprint.transformFactory()

  private val outputSparkSchema: types.StructType = blueprint.outputSparkSchema
  private val outputArrDecoder = blueprint.outputArrDecoder
  private val outputDecoder = blueprint.outputDecoder
  def outputChrononSchema: Array[(String, DataType)] = blueprint.outputChrononSchema

  def performSql(values: Array[Any]): Seq[Array[Any]] = {
    val internalRow = inputArrEncoder(values).asInstanceOf[InternalRow]
    val resultRowSeq = transformFunc(internalRow)
    val outputVal = resultRowSeq.map(resultRow => outputArrDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Array[Any]])
  }

  def performSql(values: Map[String, Any]): Seq[Map[String, Any]] = {
    val internalRow = inputEncoder(values).asInstanceOf[InternalRow]
    performSql(internalRow)
  }

  def performSql(row: InternalRow): Seq[Map[String, Any]] = {
    val resultRowMaybe = transformFunc(row)
    val outputVal = resultRowMaybe.map(resultRow => outputDecoder(resultRow))
    outputVal.map(_.asInstanceOf[Map[String, Any]])
  }

  def getOutputSparkSchema: types.StructType = outputSparkSchema

  private[chronon] def normalizeTimestampOutputs(df: DataFrame): DataFrame = blueprint.normalizeTimestampOutputs(df)
}
