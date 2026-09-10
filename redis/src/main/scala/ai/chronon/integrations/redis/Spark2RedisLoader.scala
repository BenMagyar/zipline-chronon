package ai.chronon.integrations.redis

import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisCluster

import java.nio.charset.StandardCharsets
import scala.util.Try

/** Writes every row from one source snapshot without incremental status, tombstones, or deletion of omitted keys. */
object Spark2RedisLoader {
  private val logger = LoggerFactory.getLogger(getClass)
  private val KeyColumn = "key_bytes"
  private val ValueColumn = "value_bytes"
  private val BulkConnectionTimeoutMs = 10000
  private val BulkSocketTimeoutMs = 30000

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    val tableName: ScallopOption[String] =
      opt[String](name = "table-name", descr = "Catalog table containing key_bytes and value_bytes", required = true)
    val dataset: ScallopOption[String] =
      opt[String](name = "dataset", descr = "Chronon GroupBy dataset", required = true)
    val endDs: ScallopOption[String] =
      opt[String](name = "end-ds", descr = "Source partition to upload", required = true)
    val redisClusterNodes: ScallopOption[String] =
      opt[String](name = "redis-cluster-nodes", descr = "Comma-separated Redis cluster nodes", required = true)
    val keyPrefix: ScallopOption[String] =
      opt[String](name = "key-prefix", descr = "Redis key prefix", default = Some("chronon"))
    val ttl: ScallopOption[Int] =
      opt[Int](name = "ttl", descr = "TTL in seconds", default = Some(RedisKVStoreConstants.DataTTLSeconds))
    val batchSize: ScallopOption[Int] =
      opt[Int](name = "batch-size", descr = "Commands per pipeline", default = Some(RedisBatchUploadDefaults.BatchSize))
    val useSsl: ScallopOption[Boolean] =
      opt[Boolean](name = "use-ssl", descr = "Enable TLS", default = Some(false))
    verify()
  }

  private[redis] final case class Settings(job: BatchUploadJob,
                                           target: RedisKVStoreFactory.StoreSettings,
                                           options: FullSnapshotOptions = FullSnapshotOptions()) {
    def sourceTable: String = job.sourceTable
    def sourcePartition: String = job.sourcePartition
    def partitionSpec = job.partitionSpec
    def batchDataset: String = job.batchDataset
    def batchTimestamp: Long = job.batchTimestamp
    def keyPrefix: String = target.keyPrefix
    def clientSettings: RedisKVStoreFactory.ClientSettings = target.client
    def batchSize: Int = options.batchSize
    def ttlSeconds: Int = options.ttlSeconds
  }

  /** Standalone compatibility entry point for explicitly requested full-snapshot uploads. */
  def main(args: Array[String]): Unit = {
    val config = new Conf(args)
    val redisConf = Map(
      RedisKVStoreConstants.PropRedisClusterNodes -> config.redisClusterNodes(),
      RedisKVStoreConstants.PropRedisSSL -> config.useSsl().toString
    )
    val target = RedisKVStoreFactory.StoreSettings(
      RedisKVStoreFactory.settings(redisConf, env = Map.empty),
      config.keyPrefix()
    )
    val settings = Settings(
      BatchUploadJob.from(config.tableName(), config.dataset(), config.endDs(), Map.empty),
      target,
      FullSnapshotOptions(config.batchSize(), config.ttl())
    )
    val spark = SparkSessionBuilder.build(s"Spark2RedisLoader-${settings.sourceTable}")
    val controlClient = RedisKVStoreFactory.createClient(target.client)
    try run(settings, spark, controlClient)
    finally {
      Try(controlClient.close())
      spark.stop()
    }
  }

  def run(settings: Settings, spark: SparkSession, controlClient: JedisCluster): Long = {
    requireNoIncrementalStatus(settings, controlClient)
    val tableUtils = TableUtils(spark, settings.partitionSpec)
    require(
      tableUtils.tableReachable(settings.sourceTable),
      s"Redis full-snapshot upload requires a catalog-backed upload table. ${settings.sourceTable} is not reachable"
    )

    val source = tableUtils
      .loadTable(settings.sourceTable)
      .where(col(settings.partitionSpec.column) === lit(settings.sourcePartition))
    val missing = Seq(KeyColumn, ValueColumn).filterNot(source.columns.contains)
    require(missing.isEmpty,
            s"Redis upload source ${settings.sourceTable} is missing required columns: ${missing.mkString(", ")}")

    val uploadRows = source
      .select(col(KeyColumn), col(ValueColumn))
      .withColumn("dataset", lit(settings.batchDataset))
    val recordCount = uploadRows.count()
    requireNoIncrementalStatus(settings, controlClient)
    RedisBatchUpload.claimPublicationMode(
      controlClient,
      settings.batchDataset,
      settings.keyPrefix,
      RedisBatchModeSelection.FullSnapshot
    )
    if (recordCount == 0L) {
      logger.warn(
        s"No records found in ${settings.sourceTable} for ${settings.partitionSpec.column}=${settings.sourcePartition}")
      return 0L
    }

    val transformed = buildTransformedDataFrame(uploadRows, settings.keyPrefix, settings.batchTimestamp)
    writeToRedis(transformed, settings)
    logger.info(
      s"Full-snapshot Redis bulk load wrote $recordCount records to ${settings.batchDataset} from ${settings.sourceTable}")
    recordCount
  }

  def buildTransformedDataFrame(df: DataFrame, keyPrefix: String, batchTimestamp: Long): DataFrame = {
    val buildRedisKeyUDF = udf((keyBytes: Array[Byte], dataset: String) =>
      RedisKVStore.buildRedisKey(keyBytes.toSeq, dataset, keyPrefix = keyPrefix))
    val prependTimestampUDF = udf((valueBytes: Array[Byte], timestamp: Long) =>
      java.nio.ByteBuffer.allocate(8 + valueBytes.length).putLong(timestamp).put(valueBytes).array())

    df.withColumn("redis_key", buildRedisKeyUDF(col(KeyColumn), col("dataset")))
      .withColumn("redis_value", prependTimestampUDF(col(ValueColumn), lit(batchTimestamp)))
      .select("redis_key", "redis_value")
  }

  /** Source-compatible overload retained for callers compiled against the original standalone loader. */
  def buildTransformedDataFrame(df: DataFrame,
                                keyPrefix: String,
                                batchTimestamp: Long,
                                spark: SparkSession): DataFrame =
    buildTransformedDataFrame(df, keyPrefix, batchTimestamp)

  private def writeToRedis(df: DataFrame, settings: Settings): Unit = {
    val clientSettings = settings.clientSettings
      .copy(
        connectionTimeoutMs = math.max(settings.clientSettings.connectionTimeoutMs, BulkConnectionTimeoutMs),
        soTimeoutMs = math.max(settings.clientSettings.soTimeoutMs, BulkSocketTimeoutMs)
      )
      .bulkWriter
    val batchSize = settings.batchSize
    val ttlSeconds = settings.ttlSeconds

    df.foreachPartition { rows: Iterator[Row] =>
      if (rows.hasNext) {
        val client = RedisKVStoreFactory.createClient(clientSettings)
        try writeRows(rows, client, ttlSeconds, batchSize)
        finally Try(client.close())
      }
    }
  }

  private def writeRows(rows: Iterator[Row], client: JedisCluster, ttlSeconds: Int, batchSize: Int): Unit =
    rows.grouped(batchSize).foreach { batch =>
      val pipeline = client.pipelined()
      val responses =
        try {
          batch.map { row =>
            pipeline.setex(
              row.getAs[String]("redis_key").getBytes(StandardCharsets.UTF_8),
              ttlSeconds.toLong,
              row.getAs[Array[Byte]]("redis_value")
            )
          }
        } finally pipeline.close()
      responses.foreach(_.get())
    }

  def writeWithExistingConnection(df: DataFrame,
                                  client: JedisCluster,
                                  ttlSeconds: Int,
                                  batchSize: Int = RedisBatchUploadDefaults.BatchSize): Unit =
    writeRows(df.collect().iterator, client, ttlSeconds, batchSize)

  private def requireNoIncrementalStatus(settings: Settings, controlClient: JedisCluster): Unit = {
    val statusKey = RedisBatchUpload.buildStatusKey(settings.batchDataset, settings.keyPrefix)
    require(
      RedisBatchUpload.readStatus(controlClient, statusKey).isEmpty,
      s"Redis full-snapshot upload cannot write incremental dataset ${settings.batchDataset}; use a new key prefix"
    )
  }
}
