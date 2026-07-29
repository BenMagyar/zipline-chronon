package ai.chronon.integrations.redis

import ai.chronon.api.Extensions.{GroupByOps, WindowOps, WindowUtils}
import ai.chronon.api.{GroupBy, MetaData, PartitionSpec}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.LoggerFactory

/** This Spark app handles loading Batch IR data into Redis Cluster.
  * Similar to Spark2BigTableLoader, this uses DataFrame transformations to prepare data,
  * but writes directly using Jedis API via foreachPartition.
  *
  * Why not use a Spark-Redis connector?
  * - RedisLabs' spark-redis (org.apache.spark.sql.redis) only supports Jedis 3.x (unmaintained since 2021)
  * - No actively maintained connector exists for Jedis 5.x + Spark 3.5.x
  * - Manual foreachPartition approach gives us full control and works with modern Jedis
  */
object Spark2RedisLoader {
  private val logger = LoggerFactory.getLogger(getClass)

  class Conf(args: Seq[String]) extends ScallopConf(args) {
    val tableName: ScallopOption[String] = opt[String](
      name = "table-name",
      descr = "Name of the Hive/Iceberg table containing the data to upload",
      required = true
    )

    val dataset: ScallopOption[String] = opt[String](
      name = "dataset",
      descr = "Name of the GroupBy dataset (e.g., my_groupby_v1)",
      required = true
    )

    val endDs: ScallopOption[String] = opt[String](
      name = "end-ds",
      descr = "Partition date to upload (e.g., 2024-01-15)",
      required = true
    )

    val redisClusterNodes: ScallopOption[String] = opt[String](
      name = "redis-cluster-nodes",
      descr = "Comma-separated list of Redis cluster nodes (e.g., host1:7000,host2:7001)",
      required = true
    )

    val keyPrefix: ScallopOption[String] = opt[String](
      name = "key-prefix",
      descr = "Optional key prefix (default: chronon)",
      default = Some("chronon")
    )

    val ttl: ScallopOption[Int] = opt[Int](
      name = "ttl",
      descr = "TTL in seconds for Redis keys (default: 432000 = 5 days)",
      default = Some(432000)
    )

    val batchSize: ScallopOption[Int] = opt[Int](
      name = "batch-size",
      descr = "Number of records to batch before syncing pipeline (default: 1000)",
      default = Some(1000)
    )

    val useSsl: ScallopOption[Boolean] = opt[Boolean](
      name = "use-ssl",
      descr = "Enable TLS/SSL for Redis connections (required for ElastiCache in-transit encryption)",
      default = Some(false)
    )

    verify()
  }

  def main(args: Array[String]): Unit = {
    val config = new Conf(args)

    val tableName = config.tableName()
    val dataset = config.dataset()
    val endDate = config.endDs()
    val clusterNodes = config.redisClusterNodes()
    val keyPrefix = config.keyPrefix()
    val ttl = config.ttl()
    val batchSize = config.batchSize()
    val useSsl = config.useSsl()

    logger.info(
      s"Starting Redis bulk load: table=$tableName, dataset=$dataset, partition=$endDate, batchSize=$batchSize")

    val spark = SparkSessionBuilder.build(s"Spark2RedisLoader-$tableName")
    val tableUtils = TableUtils(spark)

    // Get the batch dataset name (with _batch suffix)
    val groupBy = new GroupBy().setMetaData(new MetaData().setName(dataset))
    val batchDataset = groupBy.batchDataset
    // Or validate endDate format before use:
    require(endDate.matches("""\d{4}-\d{2}-\d{2}"""), s"Invalid date format: $endDate")

    val dataDf = tableUtils.sql(s"""
       |SELECT key_bytes, value_bytes, '$batchDataset' as dataset
       |FROM $tableName
       |WHERE ds = '$endDate'
       |""".stripMargin)

    val recordCount = dataDf.count()
    logger.info(s"Loaded $recordCount records from $tableName for partition $endDate")

    if (recordCount == 0) {
      logger.warn(s"No records found in $tableName for partition $endDate")
      return
    }

    // Calculate timestamp for this batch (endDs + 1 Day)
    // This ensures deterministic timestamps: same partition = same timestamp
    val partitionSpec = PartitionSpec("ds", "yyyy-MM-dd", WindowUtils.Day.millis)
    val endDsPlusOne = partitionSpec.epochMillis(endDate) + partitionSpec.spanMillis

    // Transform DataFrame: Build Redis keys and prepend timestamp to values
    val transformedDf = buildTransformedDataFrame(dataDf, keyPrefix, endDsPlusOne, spark)

    // Write to Redis using foreachPartition with direct Jedis API
    writeToRedis(transformedDf, clusterNodes, ttl, batchSize, useSsl)

    logger.info(s"Successfully bulk loaded $recordCount records to Redis dataset $batchDataset")
  }

  /** Test-friendly method that accepts an existing JedisCluster connection.
    * This avoids executor connection issues in Testcontainers scenarios.
    *
    * Why is this needed for tests?
    * - Testcontainers uses Docker port mapping (internal 7000 -> external 32xxx)
    * - Even with cluster-announce-ip/port reconfiguration, Spark executors in local mode
    *   sometimes have timing/networking issues connecting to the mapped ports
    * - This method runs everything from the driver using the test's pre-configured JedisCluster
    *   which has proper HostAndPortMapper to handle the port mapping
    * - In production, foreachPartition works perfectly (no Docker, real routable IPs)
    */
  def writeWithExistingConnection(df: DataFrame,
                                  jedisCluster: redis.clients.jedis.JedisCluster,
                                  ttl: Int,
                                  batchSize: Int = 1000): Unit = {
    import java.nio.charset.StandardCharsets

    logger.info(s"Writing ${df.count()} records to Redis using provided JedisCluster connection (batchSize=$batchSize)")

    // Collect to driver and write directly (avoids executor connection issues in tests)
    val rows = df.collect()

    val pipeline = jedisCluster.pipelined()
    var batchCount = 0

    rows.foreach { row =>
      val key = row.getAs[String]("redis_key")
      val value = row.getAs[Array[Byte]]("redis_value")

      pipeline.setex(key.getBytes(StandardCharsets.UTF_8), ttl.toLong, value)
      batchCount += 1

      if (batchCount >= batchSize) {
        pipeline.sync()
        batchCount = 0
      }
    }

    if (batchCount > 0) {
      pipeline.sync()
    }

    logger.info("Successfully wrote data to Redis using provided connection")
  }

  /** Build Redis key with hash tags and prepend timestamp to value.
    * This matches the format used by multiPut/multiGet.
    */
  def buildTransformedDataFrame(df: DataFrame,
                                keyPrefix: String,
                                batchTimestamp: Long,
                                spark: SparkSession): DataFrame = {
    import spark.implicits._

    // UDF to build Redis key with hash tags for cluster co-location
    val buildRedisKeyUDF = udf((keyBytes: Array[Byte], dataset: String) => {
      val base64Key = java.util.Base64.getEncoder.encodeToString(keyBytes)
      val prefix = if (keyPrefix.isEmpty) "" else s"$keyPrefix${RedisKVStoreConstants.KeySeparator}"
      s"$prefix{$dataset${RedisKVStoreConstants.KeySeparator}$base64Key}"
    })

    // UDF to prepend 8-byte timestamp to value bytes
    // This matches the format expected by multiGet
    val prependTimestampUDF = udf((valueBytes: Array[Byte], ts: Long) => {
      val timestampBytes = java.nio.ByteBuffer.allocate(8).putLong(ts).array()
      timestampBytes ++ valueBytes
    })

    df.withColumn("redis_key", buildRedisKeyUDF(col("key_bytes"), col("dataset")))
      .withColumn("redis_value", prependTimestampUDF(col("value_bytes"), lit(batchTimestamp)))
      .select("redis_key", "redis_value")
  }

  /** Write DataFrame to Redis using foreachPartition with direct Jedis API.
    * Each partition creates its own JedisCluster connection for efficient batch writes.
    *
    * Note: Redis cluster topology must be properly configured to announce
    * externally accessible IPs/ports for Spark executors to connect.
    */
  private def writeToRedis(df: DataFrame,
                           clusterNodes: String,
                           ttl: Int,
                           batchSize: Int,
                           useSsl: Boolean = false): Unit = {
    import redis.clients.jedis.{DefaultJedisClientConfig, HostAndPort, JedisCluster}
    import scala.jdk.CollectionConverters._
    import java.nio.charset.StandardCharsets

    val clusterNodesBroadcast = df.sparkSession.sparkContext.broadcast(clusterNodes)
    val ttlBroadcast = df.sparkSession.sparkContext.broadcast(ttl)
    val useSslBroadcast = df.sparkSession.sparkContext.broadcast(useSsl)

    logger.info(s"Writing to Redis using foreachPartition: nodes=${clusterNodes}, ssl=$useSsl, batchSize=$batchSize")

    df.foreachPartition { rows: Iterator[org.apache.spark.sql.Row] =>
      if (rows.hasNext) {
        val nodes = clusterNodesBroadcast.value
          .split(",")
          .map { nodeStr =>
            val parts = nodeStr.trim.split(":")
            if (parts.length >= 2) { new HostAndPort(parts(0), parts(1).toInt) }
            else { new HostAndPort(parts(0), RedisKVStoreConstants.DefaultPort) }
          }
          .toSet
          .asJava

        val poolConfig = new org.apache.commons.pool2.impl.GenericObjectPoolConfig[redis.clients.jedis.Connection]()
        poolConfig.setMaxTotal(10)
        poolConfig.setMaxIdle(5)
        poolConfig.setMinIdle(1)

        val clientConfigBuilder = DefaultJedisClientConfig
          .builder()
          .connectionTimeoutMillis(10000)
          .socketTimeoutMillis(30000)
          .ssl(useSslBroadcast.value)
        if (useSslBroadcast.value) {
          // ElastiCache cluster mode returns node IPs in the slot map; disable endpoint
          // identification so Jedis can connect to those IPs without hostname mismatch.
          // Certificate chain validation still uses the JVM default trust store.
          clientConfigBuilder
            .hostnameVerifier((_, _) => true)
            .sslParameters(RedisKVStoreConstants.elastiCacheSslParams())
        }
        val clientConfig = clientConfigBuilder.build()

        val jedisCluster = new JedisCluster(nodes, clientConfig, 5, poolConfig)

        try {
          // Use pipeline for batching within each partition
          val pipeline = jedisCluster.pipelined()
          var batchCount = 0

          rows.foreach { row =>
            val key = row.getAs[String]("redis_key")
            val value = row.getAs[Array[Byte]]("redis_value")

            // Use SETEX for batch IR data (simple key-value with TTL)
            pipeline.setex(key.getBytes(StandardCharsets.UTF_8), ttlBroadcast.value.toLong, value)
            batchCount += 1

            // Flush pipeline every batchSize operations
            if (batchCount >= batchSize) {
              pipeline.sync()
              batchCount = 0
            }
          }

          // Flush remaining operations
          if (batchCount > 0) {
            pipeline.sync()
          }
        } finally {
          jedisCluster.close()
        }
      }
    }

    logger.info("Successfully wrote data to Redis using foreachPartition")
  }
}
