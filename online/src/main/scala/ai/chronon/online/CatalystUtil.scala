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
import ai.chronon.online.CatalystUtil.{PoolKey, poolMap}
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.online.serde._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionAlreadyExistsException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions, types}
import org.slf4j.LoggerFactory

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}
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
    assert(spark.sessionState.conf.wholeStageEnabled)
    spark
  }

  case class PoolKey(expressions: Seq[(String, String)], inputSchema: StructType, setups: Seq[String])
  val poolMap: PoolMap[PoolKey, CatalystUtil] = new PoolMap[PoolKey, CatalystUtil](pi =>
    new CatalystUtil(pi.inputSchema, pi.expressions, setups = pi.setups))
}

class PoolMap[Key, Value](createFunc: Key => Value, maxSize: Int = 100, initialSize: Int = 2) {
  val map: ConcurrentHashMap[Key, ArrayBlockingQueue[Value]] = new ConcurrentHashMap[Key, ArrayBlockingQueue[Value]]()
  def getPool(input: Key): ArrayBlockingQueue[Value] =
    map.computeIfAbsent(
      input,
      new function.Function[Key, ArrayBlockingQueue[Value]] {
        override def apply(t: Key): ArrayBlockingQueue[Value] = {
          val result = new ArrayBlockingQueue[Value](maxSize)
          var i = 0
          while (i < initialSize) {
            result.add(createFunc(t))
            i += 1
          }
          result
        }
      }
    )

  def performWithValue[Output](key: Key, pool: ArrayBlockingQueue[Value])(func: Value => Output): Output = {
    var value = pool.poll()
    if (value == null) {
      value = createFunc(key)
    }
    try {
      func(value)
    } catch {
      case e: Exception => throw e
    } finally {
      pool.offer(value)
    }
  }
}

class PooledCatalystUtil(expressions: Seq[(String, String)], inputSchema: StructType, setups: Seq[String] = Seq.empty) {
  private val poolKey = PoolKey(expressions, inputSchema, setups)
  private val cuPool = poolMap.getPool(poolKey)
  def performSql(values: Map[String, Any]): Seq[Map[String, Any]] =
    poolMap.performWithValue(poolKey, cuPool) { _.performSql(values) }
  def outputChrononSchema: Array[(String, DataType)] =
    poolMap.performWithValue(poolKey, cuPool) { _.outputChrononSchema }
}

class CatalystUtil(inputSchema: StructType,
                   selects: Seq[(String, String)],
                   wheres: Seq[String] = Seq.empty,
                   setups: Seq[String] = Seq.empty,
                   timestampMillisOutputColumns: Set[String] = Set.empty) {

  def this(inputSchema: StructType, selects: Seq[(String, String)], wheres: Seq[String], setups: Seq[String]) =
    this(inputSchema, selects, wheres, setups, Set.empty)

  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)

  val selectClauses: Seq[String] = selects.map { case (name, expr) => s"$expr as $name" }
  private val sessionTable =
    s"q${math.abs(selectClauses.mkString(", ").hashCode)}_f${math.abs(inputSparkSchema.pretty.hashCode)}"
  val whereClauseOpt: Option[String] = Option(wheres)
    .filter(_.nonEmpty)
    .map { w =>
      // wrap each clause in parens
      w.map(c => s"( $c )").mkString(" AND ")
    }

  @transient lazy val inputSparkSchema: types.StructType = SparkConversions.fromChrononSchema(inputSchema)
  private val inputEncoder = SparkInternalRowConversions.to(inputSparkSchema)
  val inputArrEncoder: Any => Any = SparkInternalRowConversions.to(inputSparkSchema, false)

  private val (transformFunc: (InternalRow => Seq[InternalRow]), outputSparkSchema: types.StructType) = initialize()

  private lazy val outputArrDecoder = SparkInternalRowConversions.from(outputSparkSchema, false)
  @transient lazy val outputChrononSchema: Array[(String, DataType)] =
    SparkConversions.toChrononSchema(outputSparkSchema)
  private val outputDecoder = SparkInternalRowConversions.from(outputSparkSchema)

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

  private[chronon] def normalizeTimestampOutputs(df: DataFrame): DataFrame =
    df.schema.fields
      .filter(field => timestampMillisOutputColumns.contains(field.name) && field.dataType == types.TimestampType)
      .foldLeft(df) { case (currentDf, field) =>
        val quotedColumn = s"`${field.name.replace("`", "``")}`"
        currentDf.withColumn(field.name, functions.expr(s"unix_millis($quotedColumn)"))
      }

  private def initialize(): (InternalRow => Seq[InternalRow], types.StructType) = {
    val session = CatalystUtil.session
    executeSetups()

    // create dummy df with sql query and schema
    val emptyRowRdd = session.emptyDataFrame.rdd
    val inputSparkSchema = SparkConversions.fromChrononSchema(inputSchema)
    val emptyDf = session.createDataFrame(emptyRowRdd, inputSparkSchema)
    emptyDf.createOrReplaceTempView(sessionTable)
    val projectedDf = session.sqlContext.table(sessionTable).selectExpr(selectClauses.toSeq: _*)
    val normalizedDf = normalizeTimestampOutputs(projectedDf)
    val df = whereClauseOpt.map(normalizedDf.where(_)).getOrElse(normalizedDf)

    // extract transform function from the df spark plan
    val execPlan = df.queryExecution.executedPlan
    logger.info(s"Catalyst Execution Plan - ${execPlan}")

    // Use the new recursive approach to build a transformation chain
    val transformer = CatalystTransformBuilder.buildTransformChain(execPlan)

    (transformer, df.schema)
  }

  // Executes setup statements (ADD JAR and CREATE FUNCTION). The thread context classloader is
  // switched to jarClassLoader so that SessionCatalog.makeFunctionBuilder can resolve UDF classes
  // via Utils.classForName (which reads the thread context CL). jarClassLoader delegates to
  // Spark's driver classloader which has the full application classpath, so CREATE FUNCTION can
  // find UDF classes without needing an ADD JAR.
  private def executeSetups(): Unit = {
    if (setups.isEmpty) return
    val session = CatalystUtil.session
    val addJarPattern = """(?i)^\s*ADD\s+JAR\s+['"]?([^\s'"]+)['"]?\s*;?\s*$""".r
    val originalCL = Thread.currentThread().getContextClassLoader
    Thread.currentThread().setContextClassLoader(session.sharedState.jarClassLoader)
    try {
      setups.foreach { statement =>
        try {
          statement.trim match {
            case addJarPattern(jarPath) =>
              val localFile = localizeJar(jarPath)
              logger.info(s"Executed setup statement via localizeJar (${localFile.length()} bytes): $statement")
            case _ =>
              session.sql(statement)
              logger.info(s"Executed setup statement: $statement")
          }
        } catch {
          case _: FunctionAlreadyExistsException =>
          // ignore - this crops up in unit tests on occasion
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
  // resolve UDF classes loaded from it. Returns the local temp file.
  //
  // We bypass session.sql("ADD JAR") because that routes through HiveSessionResourceLoader, which
  // lazily initializes HiveExternalCatalog → Derby metastore → DataNucleus. DataNucleus requires
  // its three jars (core, api-jdo, rdbms) as separate classpath entries so each jar's plugin.xml
  // is discoverable; in an uber-jar only one plugin.xml survives the merge and the JDO adapter
  // registration is lost, causing a fatal startup error.
  private def localizeJar(jarPath: String): java.io.File = {
    val sc = CatalystUtil.session.sparkContext
    // s3:// has no registered Hadoop FileSystem; rewrite to s3a:// which does.
    val normalizedPath = if (jarPath.startsWith("s3://")) "s3a://" + jarPath.stripPrefix("s3://") else jarPath
    val srcPath = new org.apache.hadoop.fs.Path(new java.net.URI(normalizedPath))
    val fs = srcPath.getFileSystem(sc.hadoopConfiguration)
    val localFile = java.io.File.createTempFile(srcPath.getName.stripSuffix(".jar") + "-", ".jar")
    localFile.deleteOnExit()
    fs.copyToLocalFile(srcPath, new org.apache.hadoop.fs.Path(localFile.toURI))
    sc.addJar(localFile.getAbsolutePath)
    CatalystUtil.session.sharedState.jarClassLoader.addURL(localFile.toURI.toURL)
    localFile
  }
}
