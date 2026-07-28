package ai.chronon.integrations.redis

import ai.chronon.api.Constants.{ContinuationKey, ListEntityType, ListLimit, MetadataDataset}
import ai.chronon.api.{GroupBy, MetaData, PartitionSpec, TilingUtils}
import ai.chronon.api.Extensions.{GroupByOps, WindowUtils}
import ai.chronon.integrations.redis.RedisKVStoreConstants.{DefaultListLimit, _}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, GetResponse, ListRequest, ListResponse, ListValue, PutRequest, TimedValue}
import ai.chronon.online.metrics.Metrics
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.exceptions.{
  JedisAskDataException,
  JedisConnectionException,
  JedisException,
  JedisRedirectionException
}
import redis.clients.jedis.{ClusterPipeline, Jedis, JedisCluster, Response}
import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult
import redis.clients.jedis.util.JedisClusterCRC16

import java.nio.charset.StandardCharsets
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
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
  import RedisKVStoreImpl._

  // Configurable key prefix (can be empty for dedicated Redis deployments)
  private val keyPrefix: String = conf.getOrElse("redis.key.prefix", DefaultKeyPrefix)
  private val keyHashTagMode: String =
    conf.getOrElse("redis.key.hash.tag.mode", RedisKVStore.DatasetAndEntityHashTagMode)
  private val recordReadMetrics: Boolean =
    !conf.get("redis.read.metrics.enabled").exists(_.equalsIgnoreCase("false"))

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

    if (requests.isEmpty) {
      Future.successful(Seq.empty)
    } else {
      Future {
        val startedAt = System.currentTimeMillis()
        try {
          val defaultEndTs = System.currentTimeMillis()
          val plans = requests.map(request => planRead(request, defaultEndTs))
          val redisPlans = plans.collect { case plan: RedisReadPlan => plan }
          val redisResults = executeReadPipelineWithRetry(redisPlans).iterator
          val responses = plans.map {
            case ImmediateReadPlan(request, values) => GetResponse(request, values)
            case plan: RedisReadPlan                => GetResponse(plan.request, redisResults.next())
          }
          if (recordReadMetrics) recordMultiGetMetrics(responses, System.currentTimeMillis() - startedAt)
          responses
        } catch {
          case e: Exception =>
            logger.error("Error getting values from Redis Cluster", e)
            val responses = requests.map(request => GetResponse(request, Failure(e)))
            if (recordReadMetrics) recordMultiGetMetrics(responses, System.currentTimeMillis() - startedAt)
            responses
        }
      }
    }
  }

  private def planRead(request: GetRequest, defaultEndTs: Long): PlannedRead = {
    val planned = Try {
      getTableType(request.dataset) match {
        case BatchTable =>
          val redisKey =
            buildRedisKey(request.keyBytes, request.dataset, keyPrefix = keyPrefix, hashTagMode = keyHashTagMode)
          BatchReadPlan(request, redisKey.getBytes(StandardCharsets.UTF_8))
        case StreamingTable if request.startTsMillis.isDefined =>
          val startTs = request.startTsMillis.get
          val endTs = request.endTsMillis.getOrElse(defaultEndTs)
          val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
          val tileSizeMs = tileKey.tileSizeMillis
          val baseKeyBytes = tileKey.keyBytes.asScala.map(_.toByte).toSeq
          val millisPerDay = 1.day.toMillis
          val startDay = startTs - (startTs % millisPerDay)
          val endDay = endTs - (endTs % millisPerDay)
          val redisKeys = (startDay to endDay by millisPerDay).map { dayTs =>
            buildTiledRedisKey(baseKeyBytes,
                               request.dataset,
                               dayTs,
                               tileSizeMs,
                               keyPrefix,
                               hashTagMode = keyHashTagMode).getBytes(StandardCharsets.UTF_8)
          }.toVector
          if (redisKeys.isEmpty) ImmediateReadPlan(request, Success(Seq.empty))
          else StreamingReadPlan(request, redisKeys, startTs, endTs)
        case StreamingTable =>
          ImmediateReadPlan(request, Success(Seq.empty))
      }
    }

    planned.recover { case error => ImmediateReadPlan(request, Failure(error)) }.get
  }

  private def queueRead(plan: RedisReadPlan, pipeline: ClusterPipeline): PendingRead = plan match {
    case BatchReadPlan(request, redisKey) =>
      val response = pipeline.get(redisKey)
      PendingRead(request, () => Try(decodeBatchValue(response.get())))
    case StreamingReadPlan(request, redisKeys, startTs, endTs) =>
      val commands = redisKeys.map { redisKey =>
        StreamingCommand(
          redisKey,
          startTs,
          endTs,
          pipeline.zrangeByScore(redisKey, startTs.toDouble, endTs.toDouble)
        )
      }
      PendingRead(request, () => Try(decodeStreamingValues(commands)))
  }

  private def executeReadPipelineWithRetry(plans: Seq[RedisReadPlan]): Seq[Try[Seq[TimedValue]]] = {
    if (plans.isEmpty) {
      Seq.empty
    } else {
      val firstAttempt = executeReadPipeline(plans)
      val attemptAfterAskRecovery = askPipelineFailure(firstAttempt, plans) match {
        case Some((failedPlan, error)) =>
          recordPipelineRetry(failedPlan.request.dataset, error)
          recoverAskFailures(firstAttempt, plans)
        case None => firstAttempt
      }
      val finalAttempt = retryablePipelineFailure(attemptAfterAskRecovery, plans) match {
        case Some((_, error)) if isAskFailure(error) =>
          attemptAfterAskRecovery
        case Some((failedPlan, error)) =>
          recordPipelineRetry(failedPlan.request.dataset, error)
          Try(refreshPipelineRoute(failedPlan)) match {
            case Success(_) =>
              val retryAttempt = executeReadPipeline(plans)
              retryAttempt.results match {
                case Success(_) => mergePipelineAttempts(attemptAfterAskRecovery, retryAttempt)
                case Failure(retryError) =>
                  logger.error("Redis Cluster pipeline retry failed", retryError)
                  if (attemptAfterAskRecovery.results.isSuccess) attemptAfterAskRecovery else retryAttempt
              }
            case Failure(refreshError) =>
              logger.error("Redis Cluster route refresh failed", refreshError)
              attemptAfterAskRecovery
          }
        case None => attemptAfterAskRecovery
      }

      finalAttempt.results.getOrElse(plans.map(_ => Failure(finalAttempt.results.failed.get)))
    }
  }

  private def mergePipelineAttempts(firstAttempt: PipelineAttempt, retryAttempt: PipelineAttempt): PipelineAttempt = {
    (firstAttempt.results, retryAttempt.results) match {
      case (Success(firstResults), Success(retryResults)) if firstResults.size == retryResults.size =>
        val mergedResults = firstResults.zip(retryResults).map {
          case (_, retrySuccess @ Success(_))          => retrySuccess
          case (firstSuccess @ Success(_), Failure(_)) => firstSuccess
          case (_, retryFailure @ Failure(_))          => retryFailure
        }
        retryAttempt.copy(results = Success(mergedResults))
      case _ => retryAttempt
    }
  }

  private def executeReadPipeline(plans: Seq[RedisReadPlan]): PipelineAttempt = {
    var activePlan = Option.empty[RedisReadPlan]
    val results = Try {
      val pipeline = jedisCluster.pipelined()
      var queueError = Option.empty[Exception]
      val pendingReads =
        try {
          queueJedisReads(plans, pipeline) { plan =>
            activePlan = Some(plan)
          }
        } catch {
          case error: Exception =>
            queueError = Some(error)
            throw error
        } finally {
          try {
            // ClusterPipeline.close syncs all nodes and returns their connections to the pools.
            pipeline.close()
          } catch {
            case closeError: Exception =>
              queueError match {
                case Some(error) => error.addSuppressed(closeError)
                case None        => throw closeError
              }
          }
        }
      pendingReads.map(_.resolve())
    }
    PipelineAttempt(results, activePlan)
  }

  private def queueJedisReads(plans: Seq[RedisReadPlan], pipeline: ClusterPipeline)(
      markActive: RedisReadPlan => Unit): Seq[PendingRead] = {
    val pendingReads = Array.ofDim[PendingRead](plans.size)
    val mgetGroups = new java.util.LinkedHashMap[Int, java.util.ArrayList[(Int, BatchReadPlan)]]()

    plans.iterator.zipWithIndex.foreach {
      case (plan: BatchReadPlan, index) =>
        markActive(plan)
        val slot = JedisClusterCRC16.getSlot(plan.redisKey)
        var group = mgetGroups.get(slot)
        if (group == null) {
          group = new java.util.ArrayList[(Int, BatchReadPlan)]()
          mgetGroups.put(slot, group)
        }
        group.add(index -> plan)
      case (plan @ StreamingReadPlan(_, redisKeys, startTs, endTs), index) =>
        markActive(plan)
        pendingReads(index) = queueRead(plan, pipeline)
    }

    mgetGroups.values().asScala.foreach { group =>
      if (group.size() == 1) {
        val (index, plan) = group.get(0)
        pendingReads(index) = queueRead(plan, pipeline)
      } else {
        val redisKeys = Array.ofDim[Array[Byte]](group.size())
        var index = 0
        while (index < group.size()) {
          redisKeys(index) = group.get(index)._2.redisKey
          index += 1
        }
        val response = pipeline.mget(redisKeys: _*)
        lazy val values = response.get()
        index = 0
        while (index < group.size()) {
          val pendingIndex = group.get(index)._1
          val plan = group.get(index)._2
          val responseIndex = index
          pendingReads(pendingIndex) = PendingRead(plan.request, () => Try(decodeBatchValue(values.get(responseIndex))))
          index += 1
        }
      }
    }

    pendingReads.toVector
  }

  private def retryablePipelineFailure(
      attempt: PipelineAttempt,
      plans: Seq[RedisReadPlan]
  ): Option[(RedisReadPlan, Throwable)] = attempt.results match {
    case Failure(error) if isRetryablePipelineFailure(error) =>
      Some(attempt.failedPlan.getOrElse(plans.head) -> error)
    case Success(results) =>
      results.iterator.zip(plans.iterator).collectFirst {
        case (Failure(error), plan) if isRetryablePipelineFailure(error) => plan -> error
      }
    case _ => None
  }

  private def askPipelineFailure(
      attempt: PipelineAttempt,
      plans: Seq[RedisReadPlan]
  ): Option[(RedisReadPlan, Throwable)] = attempt.results match {
    case Failure(error) if isAskFailure(error) =>
      Some(attempt.failedPlan.getOrElse(plans.head) -> error)
    case Success(results) =>
      results.iterator.zip(plans.iterator).collectFirst {
        case (Failure(error), plan) if isAskFailure(error) => plan -> error
      }
    case _ => None
  }

  private def recoverAskFailures(attempt: PipelineAttempt, plans: Seq[RedisReadPlan]): PipelineAttempt = {
    val recoveredResults = attempt.results match {
      case Success(results) if results.size == plans.size =>
        Success(results.zip(plans).map {
          case (Failure(error), plan) if isAskFailure(error) => executeDirectRead(plan)
          case (result, _)                                   => result
        })
      case Failure(error) if isAskFailure(error) =>
        Success(plans.map(executeDirectRead))
      case other => other
    }
    attempt.copy(results = recoveredResults)
  }

  private def executeDirectRead(plan: RedisReadPlan): Try[Seq[TimedValue]] = Try {
    plan match {
      case BatchReadPlan(_, redisKey) =>
        decodeBatchValue(jedisCluster.get(redisKey))
      case StreamingReadPlan(_, redisKeys, startTs, endTs) =>
        redisKeys.flatMap { redisKey =>
          decodeStreamingMembers(jedisCluster.zrangeByScore(redisKey, startTs.toDouble, endTs.toDouble))
        }
    }
  }

  private def isAskFailure(error: Throwable): Boolean = error match {
    case _: JedisAskDataException => true
    case jedisError: JedisException =>
      Option(jedisError.getCause).exists(isAskFailure)
    case _ => false
  }

  private def isRetryablePipelineFailure(error: Throwable): Boolean = error match {
    case _: JedisRedirectionException      => true
    case _: JedisConnectionException       => true
    case jedisError: JedisException        => Option(jedisError.getCause).exists(isRetryablePipelineFailure)
    case stateError: IllegalStateException => stateError.getMessage == PipelineResponseUnsetMessage
    case _                                 => false
  }

  private def refreshPipelineRoute(plan: RedisReadPlan): Unit = plan match {
    case BatchReadPlan(_, redisKey) =>
      jedisCluster.get(redisKey)
      ()
    case StreamingReadPlan(_, redisKeys, startTs, endTs) =>
      redisKeys.headOption.foreach { redisKey =>
        jedisCluster.zrangeByScore(redisKey, startTs.toDouble, endTs.toDouble)
      }
  }

  private def recordPipelineRetry(dataset: String, error: Throwable): Unit = {
    val datasetMetricsContext = tableToContext.getOrElseUpdate(dataset, metricsContext.copy(dataset = dataset))
    datasetMetricsContext.increment("multiGet.pipeline_retries", Map("exception" -> error.getClass.getName))
  }

  private def decodeBatchValue(storedBytes: Array[Byte]): Seq[TimedValue] = {
    if (storedBytes != null && storedBytes.length >= 8) {
      val timestamp = java.nio.ByteBuffer.wrap(storedBytes, 0, 8).getLong
      Seq(TimedValue(storedBytes.drop(8), timestamp))
    } else if (storedBytes != null) {
      logger.warn(s"Malformed data in Redis: key has ${storedBytes.length} bytes, expected >= 8")
      Seq.empty
    } else {
      Seq.empty
    }
  }

  private def decodeStreamingValues(commands: Seq[StreamingCommand]): Seq[TimedValue] = {
    commands.flatMap { command =>
      decodeStreamingMembers(command.response.get())
    }
  }

  private def decodeStreamingMembers(members: java.util.List[Array[Byte]]): Seq[TimedValue] = {
    members.asScala.flatMap { memberBytes =>
      if (memberBytes.length >= 8) {
        val timestamp = java.nio.ByteBuffer.wrap(memberBytes, 0, 8).getLong
        Some(TimedValue(memberBytes.drop(8), timestamp))
      } else {
        logger.warn(s"Malformed streaming data in Redis: member has ${memberBytes.length} bytes, expected >= 8")
        None
      }
    }
  }

  private def recordMultiGetMetrics(responses: Seq[GetResponse], latencyMillis: Long): Unit = {
    metricsContext.distribution("multiGet.latency", latencyMillis)
    val datasets = mutable.LinkedHashSet.empty[String]
    val failures = mutable.HashMap.empty[String, Throwable]
    responses.foreach { response =>
      val dataset = response.request.dataset
      datasets += dataset
      if (!failures.contains(dataset)) {
        response.values.failed.foreach(error => failures.put(dataset, error))
      }
    }
    datasets.foreach { dataset =>
      val datasetMetricsContext = tableToContext.getOrElseUpdate(dataset, metricsContext.copy(dataset = dataset))
      failures.get(dataset) match {
        case Some(error) =>
          datasetMetricsContext.increment("multiGet.redis_errors", Map("exception" -> error.getClass.getName))
        case None =>
          datasetMetricsContext.increment("multiGet.successes")
      }
    }
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
              (buildTiledRedisKey(baseKeyBytes,
                                  request.dataset,
                                  ts,
                                  tileKey.tileSizeMillis,
                                  keyPrefix,
                                  hashTagMode = keyHashTagMode),
               tileKey.tileStartTimestampMillis)
            case _ =>
              (buildRedisKey(request.keyBytes, request.dataset, keyPrefix = keyPrefix, hashTagMode = keyHashTagMode),
               timestampInPutRequest)
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

private[redis] object RedisKVStoreImpl {
  private val PipelineResponseUnsetMessage = "Please close pipeline or multi block before calling this method."

  sealed trait PlannedRead {
    def request: GetRequest
  }

  sealed trait RedisReadPlan extends PlannedRead

  final case class ImmediateReadPlan(request: GetRequest, values: Try[Seq[TimedValue]]) extends PlannedRead

  final case class BatchReadPlan(request: GetRequest, redisKey: Array[Byte]) extends RedisReadPlan

  final case class StreamingReadPlan(
      request: GetRequest,
      redisKeys: Vector[Array[Byte]],
      startTs: Long,
      endTs: Long
  ) extends RedisReadPlan

  final case class StreamingCommand(
      redisKey: Array[Byte],
      startTs: Long,
      endTs: Long,
      response: Response[java.util.List[Array[Byte]]]
  )

  final case class PendingRead(request: GetRequest, resolve: () => Try[Seq[TimedValue]])

  final case class PipelineAttempt(
      results: Try[Seq[Try[Seq[TimedValue]]]],
      failedPlan: Option[RedisReadPlan]
  )
}

object RedisKVStore {
  val DatasetAndEntityHashTagMode = "dataset_and_entity"
  val EntityHashTagMode = "entity"

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
                    keyPrefix: String = DefaultKeyPrefix,
                    hashTagMode: String = DatasetAndEntityHashTagMode): String = {
    val base64Key = java.util.Base64.getEncoder.encodeToString(baseKeyBytes.toArray)
    val prefix = if (keyPrefix.isEmpty) "" else s"$keyPrefix$KeySeparator"
    // Keep dataset in the full key for uniqueness; hashTagMode only controls the Redis Cluster hash tag.
    val baseKey = buildBaseKey(prefix, dataset, base64Key, hashTagMode)
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
                         keyPrefix: String = DefaultKeyPrefix,
                         hashTagMode: String = DatasetAndEntityHashTagMode): String = {
    val base64Key = java.util.Base64.getEncoder.encodeToString(baseKeyBytes.toArray)
    val dayTs = ts - (ts % 1.day.toMillis)
    val prefix = if (keyPrefix.isEmpty) "" else s"$keyPrefix$KeySeparator"
    val baseKey = buildBaseKey(prefix, dataset, base64Key, hashTagMode)
    s"$baseKey$KeySeparator$dayTs$KeySeparator$tileSizeMs"
  }

  private def buildBaseKey(prefix: String, dataset: String, base64Key: String, hashTagMode: String): String = {
    hashTagMode match {
      case EntityHashTagMode           => s"$prefix$dataset$KeySeparator{$base64Key}"
      case DatasetAndEntityHashTagMode => s"$prefix{$dataset$KeySeparator$base64Key}"
      case null | ""                   => s"$prefix{$dataset$KeySeparator$base64Key}"
      case other                       => throw new IllegalArgumentException(s"Unknown Redis hash tag mode: $other")
    }
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
