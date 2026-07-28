package ai.chronon.integrations.redis

import ai.chronon.api.Constants.{ContinuationKey, ListEntityType, ListLimit, MetadataDataset}
import ai.chronon.api.{GroupBy, MetaData, PartitionSpec, TilingUtils}
import ai.chronon.api.Extensions.{GroupByOps, WindowUtils}
import ai.chronon.integrations.redis.RedisKVStoreConstants.{DefaultListLimit, _}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, GetResponse, ListRequest, ListResponse, ListValue, PutRequest, TimedValue}
import ai.chronon.online.metrics.Metrics
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.{Jedis, JedisCluster, JedisPoolConfig}
import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.{ScanResult, Tuple}

import java.nio.charset.StandardCharsets
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

/** Redis Cluster-based KV store implementation with hash tags for load distribution.
  *
  * We store a few kinds of data in our KV store:
  * 1) Entity data - Configuration data like thrift serialized GroupBy / Join configs.
  * 2) Timeseries data - Batch IRs or streaming tiles for feature fetching.
  *
  * Key structure (with hash tags to prevent hotkey amplification):
  * - Batch IRs: chronon:<hash_tag> where hash_tag = {dataset:base64_key}
  * - Time-series: chronon:<hash_tag>:dayTs where hash_tag = {dataset:base64_key}
  *   (stored as sorted sets)
  *
  * The hash tag portion between curly braces determines which Redis cluster node stores the data.
  * Including both dataset AND base64_key in the hash tag ensures:
  * 1. All time-series data for same (dataset, entity) lands on same node → efficient multi-day queries
  * 2. Different datasets for same entity distribute across nodes → prevents hotkey amplification
  *
  * Why include dataset in hash tag?
  * Production experience at Airbnb (per Nikhil) showed that popular entity IDs caused severe
  * node hotspotting when only the entity key was hashed.
  *
  * Example: A trending entity "user_12345" appears as the primary key in 50 different GroupBys:
  *   - user_engagement_features (user_12345)
  *   - user_recommendation_scores (user_12345)
  *   - user_abuse_signals (user_12345)
  *   - ... 47 more feature GroupBys
  *
  * Trade-off: multiGet for batch+streaming from different datasets hits 2 nodes instead of 1
  * (~1-2ms extra latency), but this is vastly preferable to node saturation and cascading failures.
  *
  * Time-series data format in sorted sets:
  * - Score: timestamp in milliseconds
  * - Member: timestamp(8 bytes) + value bytes
  *
  * Why timestamp prefix in members?
  * In BigTable, cells are uniquely identified by (row, column, timestamp), allowing the same value
  * at different timestamps. Redis sorted sets require unique members - without the prefix,
  * the same value at different timestamps would overwrite each other (last score wins).
  * The 8-byte timestamp prefix makes each (timestamp, value) pair unique.
  *
  * Last-Write-Wins (LWW) semantics:
  * Writing to the same timestamp twice deletes the first value via ZREMRANGEBYSCORE before ZADD.
  * This matches BigTable's deleteCells + setCell pattern.
  *
  * Data is stored with a default TTL of 5 days (matching BigTable implementation).
  */
class RedisKVStoreImpl(jedisCluster: JedisCluster, conf: Map[String, String] = Map.empty) extends KVStore {
  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  import RedisKVStore._

  // Configurable key prefix (can be empty for dedicated Redis deployments)
  private val keyPrefix: String = conf.getOrElse("redis.key.prefix", DefaultKeyPrefix)

  // TTL is now configurable via RedisKVStoreConstants or via props in create()
  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("redis")
  protected val tableToContext = new TrieMap[String, Metrics.Context]()

  // Extract cluster nodes configuration for Spark executors
  private lazy val clusterNodesConfig: String = {
    conf.getOrElse("redis.cluster.nodes", System.getenv().getOrDefault("REDIS_CLUSTER_NODES", "localhost:6379"))
  }

  override def create(dataset: String): Unit = {
    logger.info(s"Dataset $dataset ready for use (Redis doesn't require explicit table creation)")
    metricsContext.increment("create.successes")
  }

  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
    logger.debug(s"Performing multi-get for ${requests.size} requests")

    // Group requests by dataset and time range
    val requestGroups = requests.groupBy { req =>
      val tableType = getTableType(req.dataset)
      tableType match {
        case StreamingTable =>
          (req.dataset, req.startTsMillis, req.endTsMillis)
        case _ =>
          (req.dataset, None, None)
      }
    }

    // Process each group separately
    val groupFutures: Seq[Future[Seq[GetResponse]]] = requestGroups.map {
      case ((dataset, startTs, endTs), groupRequests) =>
        readRowsMultiGet(dataset, groupRequests, startTs, endTs)
    }.toList

    Future.sequence(groupFutures).map(_.flatten)
  }

  private def readRowsMultiGet(
      dataset: String,
      requests: Seq[GetRequest],
      startTsMillis: Option[Long],
      endTsMillis: Option[Long]
  ): Future[Seq[GetResponse]] = {
    val datasetMetricsContext = tableToContext.getOrElseUpdate(
      dataset,
      metricsContext.copy(dataset = dataset)
    )
    val tableType = getTableType(dataset)

    Future {
      try {
        val startTs = System.currentTimeMillis()

        val responses: Seq[GetResponse] = tableType match {
          case BatchTable =>
            val pipeline = jedisCluster.pipelined()
            val pipelinedGets =
              try {
                val queuedGets = requests.map { request =>
                  val redisKey = buildRedisKey(request.keyBytes, dataset, keyPrefix = keyPrefix)
                  (request, pipeline.get(redisKey.getBytes(StandardCharsets.UTF_8)))
                }
                queuedGets
              } finally {
                // ClusterPipeline.close performs the sync and returns borrowed node connections to their pools.
                pipeline.close()
              }

            pipelinedGets.map { case (request, response) =>
              val timedValues = Try {
                val storedBytes = response.get()
                if (storedBytes != null && storedBytes.length >= 8) {
                  val timestamp = java.nio.ByteBuffer.wrap(storedBytes.take(8)).getLong
                  val valueBytes = storedBytes.drop(8)
                  Seq(TimedValue(valueBytes, timestamp))
                } else if (storedBytes != null) {
                  logger.warn(s"Malformed data in Redis: key has ${storedBytes.length} bytes, expected >= 8")
                  Seq.empty
                } else {
                  Seq.empty
                }
              }
              GetResponse(request, timedValues)
            }

          case StreamingTable if startTsMillis.isDefined =>
            val reqStartTs = startTsMillis.get
            val endTs = endTsMillis.getOrElse(System.currentTimeMillis())
            requests.map { request =>
              val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
              val tileSizeMs = tileKey.tileSizeMillis
              val baseKeyBytes = tileKey.keyBytes.asScala.map(_.toByte).toSeq
              val timedValues = Try(
                getTimeSeriesData(jedisCluster, baseKeyBytes, dataset, reqStartTs, endTs, Some(tileSizeMs), keyPrefix))
              GetResponse(request, timedValues)
            }

          case _ =>
            // Should not happen: non-batch tables without a time range.
            requests.map(req => GetResponse(req, Success(Seq.empty)))
        }

        datasetMetricsContext.distribution("multiGet.latency", System.currentTimeMillis() - startTs)
        datasetMetricsContext.increment("multiGet.successes")
        responses
      } catch {
        case e: Exception =>
          logger.error("Error getting values from Redis Cluster", e)
          datasetMetricsContext.increment("multiGet.redis_errors", Map("exception" -> e.getClass.getName))
          requests.map(req => GetResponse(req, Failure(e)))
      }
    }
  }

  private def getTimeSeriesData(
      jedisCluster: JedisCluster,
      keyBytes: Seq[Byte],
      dataset: String,
      startTs: Long,
      endTs: Long,
      maybeTileSize: Option[Long],
      keyPrefix: String
  ): Seq[TimedValue] = {
    val millisPerDay = 1.day.toMillis
    val startDay = startTs - (startTs % millisPerDay)
    val endDay = endTs - (endTs % millisPerDay)

    // Generate all day-based keys
    val dayRange = startDay to endDay by millisPerDay
    val allTimedValues = dayRange.flatMap { dayTs =>
      val redisKey = maybeTileSize match {
        case Some(tileSize) => buildTiledRedisKey(keyBytes, dataset, dayTs, tileSize, keyPrefix)
        case None           => buildRedisKey(keyBytes, dataset, Some(dayTs), keyPrefix)
      }

      // Use ZRANGEBYSCORE to get values in the time range
      val tuples = jedisCluster
        .zrangeByScoreWithScores(
          redisKey.getBytes(StandardCharsets.UTF_8),
          startTs.toDouble,
          endTs.toDouble
        )
        .asScala

      tuples.map { tuple =>
        // Extract value bytes from the member (skip first 8 bytes which contain the timestamp prefix)
        // The prefix is needed to make members unique in Redis ZSET (same value at different timestamps)
        // All time-series data is written with this prefix (see multiPut line 324-326)
        val memberBytes = tuple.getBinaryElement
        val valueBytes = memberBytes.drop(8) // Skip the 8-byte timestamp prefix
        val timestamp = tuple.getScore.toLong
        TimedValue(valueBytes, timestamp)
      }
    }

    allTimedValues.toSeq
  }

  override def list(request: ListRequest): Future[ListResponse] = {
    logger.debug(s"Performing list for ${request.dataset}")

    val listLimit = request.props.get(ListLimit) match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => DefaultListLimit
    }

    val maybeListEntityType = request.props.get(ListEntityType)
    val maybeStartKey = request.props.get(ContinuationKey)

    val datasetMetricsContext = tableToContext.getOrElseUpdate(
      request.dataset,
      metricsContext.copy(dataset = request.dataset)
    )

    Future {
      try {
        val startTs = System.currentTimeMillis()

        // Build scan pattern
        val prefix = if (keyPrefix.isEmpty) "" else s"$keyPrefix$KeySeparator"
        val pattern = (maybeStartKey, maybeListEntityType) match {
          case (_, Some(entityType)) =>
            s"$prefix${request.dataset}$KeySeparator${entityType}/*"
          case _ =>
            s"$prefix${request.dataset}$KeySeparator*"
        }

        val scanParams = new ScanParams()
          .`match`(pattern)
          .count(listLimit)

        val cursor = maybeStartKey match {
          case Some(key: Array[Byte]) => new String(key, StandardCharsets.UTF_8)
          case _                      => ScanParams.SCAN_POINTER_START
        }

        // For Redis Cluster, we need to scan all master nodes
        val allKeys = scala.collection.mutable.Set[String]()
        var lastCursor = cursor

        // Get cluster nodes and scan each master
        val clusterNodes = jedisCluster.getClusterNodes
        clusterNodes.asScala.foreach { case (nodeKey, pool) =>
          val connection = pool.getResource
          connection match {
            case jedis: Jedis =>
              try {
                val nodeInfo = jedis.info("replication")
                val isMaster = nodeInfo.contains("role:master")
                if (isMaster) {
                  var nodeCursor = cursor
                  var continue = true
                  while (continue && allKeys.size < listLimit) {
                    val scanResult = jedis.scan(nodeCursor, scanParams)
                    allKeys ++= scanResult.getResult.asScala
                    nodeCursor = scanResult.getCursor
                    lastCursor = nodeCursor
                    continue = nodeCursor != ScanParams.SCAN_POINTER_START
                  }
                }
              } finally {
                jedis.close()
              }
            case _ =>
              logger.warn(s"Unexpected connection type: ${connection.getClass}")
              connection.close()
          }
        }

        // Get values for found keys (limited to listLimit)
        val keys = allKeys.take(listLimit)
        val listValues = keys.flatMap { key =>
          val value = jedisCluster.get(key.getBytes(StandardCharsets.UTF_8))
          if (value != null) {
            Some(ListValue(key.getBytes(StandardCharsets.UTF_8), value))
          } else {
            None
          }
        }.toSeq

        datasetMetricsContext.distribution("list.latency", System.currentTimeMillis() - startTs)
        datasetMetricsContext.increment("list.successes")

        val propsMap: Map[String, Any] =
          if (lastCursor == ScanParams.SCAN_POINTER_START || listValues.size < listLimit) {
            Map.empty
          } else {
            Map(ContinuationKey -> lastCursor.getBytes(StandardCharsets.UTF_8))
          }

        ListResponse(request, Success(listValues), propsMap)
      } catch {
        case e: Exception =>
          logger.error("Error listing values from Redis Cluster", e)
          datasetMetricsContext.increment("list.redis_errors", Map("exception" -> e.getClass.getName))
          ListResponse(request, Failure(e), Map.empty)
      }
    }
  }

  override def multiPut(requests: Seq[PutRequest]): Future[Seq[Boolean]] = {
    logger.debug(s"Performing multi-put for ${requests.size} requests")

    val resultFutures = requests.map { request =>
      val datasetMetricsContext = tableToContext.getOrElseUpdate(
        request.dataset,
        metricsContext.copy(dataset = request.dataset)
      )
      val tableType = getTableType(request.dataset)
      val timestampInPutRequest = request.tsMillis.getOrElse(System.currentTimeMillis())

      Future {
        try {
          val startTs = System.currentTimeMillis()
          val (redisKey, timestamp) = (request.tsMillis, tableType) match {
            case (Some(ts), StreamingTable) =>
              val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
              val baseKeyBytes = tileKey.keyBytes.asScala.map(_.toByte).toSeq
              (buildTiledRedisKey(baseKeyBytes, request.dataset, ts, tileKey.tileSizeMillis, keyPrefix),
               tileKey.tileStartTimestampMillis)
            case _ =>
              (buildRedisKey(request.keyBytes, request.dataset, keyPrefix = keyPrefix), timestampInPutRequest)
          }

          tableType match {
            case StreamingTable =>
              // Use sorted set for time-series data with Last-Write-Wins semantics (matching BigTable)
              val keyBytes = redisKey.getBytes(StandardCharsets.UTF_8)
              // Remove any existing value at this exact timestamp (Last-Write-Wins)
              // Note: This removes ALL members with this score, which is what we want
              jedisCluster.zremrangeByScore(keyBytes, timestamp.toDouble, timestamp.toDouble)
              // Add new value: timestamp is both the score AND a prefix in the member
              // The prefix is needed because Redis ZSET members must be unique - without it,
              // the same value at different timestamps would overwrite each other
              // Format: timestamp(8 bytes) + value
              val timestampBytes = java.nio.ByteBuffer.allocate(8).putLong(timestamp).array()
              val memberBytes = timestampBytes ++ request.valueBytes
              jedisCluster.zadd(keyBytes, timestamp.toDouble, memberBytes)
              jedisCluster.expire(keyBytes, DataTTLSeconds)
            case _ =>
              // Simple key-value; store timestamp prefix + value to preserve write time
              val timestampBytes = java.nio.ByteBuffer.allocate(8).putLong(timestampInPutRequest).array()
              val storedBytes = timestampBytes ++ request.valueBytes
              jedisCluster.setex(redisKey.getBytes(StandardCharsets.UTF_8), DataTTLSeconds, storedBytes)
          }

          datasetMetricsContext.distribution("multiPut.latency", System.currentTimeMillis() - startTs)
          datasetMetricsContext.increment("multiPut.successes")
          true
        } catch {
          case e: Exception =>
            logger.error("Error putting data to Redis Cluster", e)
            datasetMetricsContext.increment("multiPut.failures", Map("exception" -> e.getClass.getName))
            false
        }
      }
    }

    Future.sequence(resultFutures)
  }

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    logger.info(
      s"Triggering bulk load for dataset: $destinationOnlineDataSet, " +
        s"table: $sourceOfflineTable, partition: $partition")
    // Read from Hive/Iceberg table and write to Redis in batches
    val startTs = System.currentTimeMillis()

    logger.info(
      s"Triggering Spark-based bulk load for dataset: $destinationOnlineDataSet, " +
        s"table: $sourceOfflineTable, partition: $partition"
    )

    try {
      // Use Spark2RedisLoader to load data from Hive/Delta tables
      // Similar to how BigTable calls Spark2BigTableLoader.main()
      val loaderArgs = Array(
        "--table-name",
        sourceOfflineTable,
        "--dataset",
        destinationOnlineDataSet,
        "--end-ds",
        partition,
        "--redis-cluster-nodes",
        clusterNodesConfig,
        "--key-prefix",
        keyPrefix,
        "--ttl",
        DataTTLSeconds.toString
      )

      // Run the Spark job
      Spark2RedisLoader.main(loaderArgs)

      logger.info("Spark-based bulk load completed successfully")
      metricsContext.distribution("bulkPut.latency", System.currentTimeMillis() - startTs)
      metricsContext.increment("bulkPut.successes")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to run Spark-based bulk load for $sourceOfflineTable", e)
        metricsContext.increment("bulkPut.failures", Map("exception" -> e.getClass.getName))
        throw e
    }
  }

  override def init(props: Map[String, Any]): Unit = {
    super.init(props)

    val warmupLengthMillis: Long = 5000L
    // Perform some dummy operations to warm up the connection pool
    // This can help reduce latency for the first real operations.
    // Intentionally getting non-existent keys below to warm up.
    val testKey = "warmup_key"
    logger.info(s"Warming up Redis KVStore with key prefix $testKey")
    try {
      val getFutures = this.multiGet(
        // create 100 requests to simulate load
        (1 to 100)
          .map(i =>
            GetRequest(
              keyBytes = s"${testKey}_$i".getBytes,
              dataset = MetadataDataset
            ))
          .toSeq
      )
      // Wait for the future to complete with a timeout
      try {
        Await.result(getFutures, warmupLengthMillis.milliseconds)
      } catch {
        case _: Exception => // swallow exception
      }
      logger.info("Redis KVStore warm-up completed successfully")
    } catch {
      case e: Exception =>
        logger.warn("Warm-up operations failed", e)
    }
  }
}

object RedisKVStore {
  sealed trait TableType
  case object BatchTable extends TableType
  case object StreamingTable extends TableType

  /** Build a Redis key with optional timestamp for time-series data.
    *
    * Key format examples:
    *   Batch IR:     chronon:{MY_GROUPBY:dXNlcg==}
    *   Time-series:  chronon:{MY_GROUPBY:dXNlcg==}:1704067200000
    *
    * The curly braces denote the hash tag - Redis uses this to determine cluster node placement.
    * Including dataset in the hash tag prevents hotkey amplification in production.
    *
    * @param keyPrefix Optional prefix for namespace isolation (can be empty string)
    */
  def buildRedisKey(baseKeyBytes: Seq[Byte],
                    dataset: String,
                    maybeTs: Option[Long] = None,
                    keyPrefix: String = DefaultKeyPrefix): String = {
    val base64Key = java.util.Base64.getEncoder.encodeToString(baseKeyBytes.toArray)
    val prefix = if (keyPrefix.isEmpty) "" else s"$keyPrefix$KeySeparator"
    // Use hash tag {dataset:base64Key} to distribute load across cluster nodes
    val baseKey = s"$prefix{$dataset$KeySeparator$base64Key}"
    maybeTs match {
      case Some(ts) =>
        // For time series data, append the day timestamp
        val dayTs = ts - (ts % 1.day.toMillis)
        s"$baseKey$KeySeparator$dayTs"
      case None => baseKey
    }
  }

  /** Build a Redis key for tiled data.
    *
    * Key format example:
    *   chronon:{MY_GROUPBY:dXNlcg==}:1704067200000:300000
    *   where 1704067200000 is dayTs and 300000 is tileSize
    *
    * The curly braces denote the hash tag - Redis uses this to determine cluster node placement.
    * Including dataset in the hash tag prevents hotkey amplification in production.
    *
    * @param keyPrefix Optional prefix for namespace isolation (can be empty string)
    */
  def buildTiledRedisKey(baseKeyBytes: Seq[Byte],
                         dataset: String,
                         ts: Long,
                         tileSizeMs: Long,
                         keyPrefix: String = DefaultKeyPrefix): String = {
    val base64Key = java.util.Base64.getEncoder.encodeToString(baseKeyBytes.toArray)
    val dayTs = ts - (ts % 1.day.toMillis)
    val prefix = if (keyPrefix.isEmpty) "" else s"$keyPrefix$KeySeparator"
    // Use hash tag {dataset:base64Key} to distribute load across cluster nodes
    s"$prefix{$dataset$KeySeparator$base64Key}$KeySeparator$dayTs$KeySeparator$tileSizeMs"
  }

  /** Determine table type from dataset name.
    */
  def getTableType(dataset: String): TableType = {
    dataset match {
      case d if d.endsWith("_BATCH")     => BatchTable
      case d if d.endsWith("_STREAMING") => StreamingTable
      case _                             => BatchTable
    }
  }
}
