package ai.chronon.integrations.redis

import ai.chronon.api.Constants.{ContinuationKey, ListEntityType, ListLimit, MetadataDataset}
import ai.chronon.api.{GroupBy, MetaData, PartitionSpec, TilingUtils}
import ai.chronon.api.Extensions.{GroupByOps, WindowUtils}
import ai.chronon.integrations.redis.GlideReadCommand.{
  Get => GlideGet,
  MGet => GlideMGet,
  ZRangeByScore => GlideZRangeByScore
}
import ai.chronon.integrations.redis.GlideReadResult.{
  OptionalValues => GlideOptionalValues,
  Value => GlideValue,
  Values => GlideValues
}
import ai.chronon.integrations.redis.RedisKVStoreConstants.{DefaultListLimit, _}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, GetResponse, ListRequest, ListResponse, ListValue, PutRequest, TimedValue}
import ai.chronon.online.metrics.Metrics
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.exceptions.{
  JedisAskDataException,
  JedisConnectionException,
  JedisException,
  JedisRedirectionException
}
import redis.clients.jedis.{ClusterPipeline, ConnectionPool, Jedis, JedisCluster, Response}
import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.ScanResult
import redis.clients.jedis.util.JedisClusterCRC16

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{
  CompletionException,
  ExecutionException,
  LinkedBlockingQueue,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}
import java.util.function.BiConsumer
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{Await, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal
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
  * Direct KV writes use a default TTL of 5 days (matching BigTable). Managed batch publication configures its lease
  * independently.
  */
class RedisKVStoreImpl(jedisCluster: JedisCluster,
                       conf: Map[String, String] = Map.empty,
                       conditionalObjectWriter: ConditionalObjectWriter = HadoopConditionalObjectWriter,
                       requireBatchStatus: Boolean = false,
                       glideReadClient: Option[GlideRedisReadClient] = None)
    extends KVStore
    with AutoCloseable {

  /** Retains the pre-GLIDE JVM constructor for separately compiled callers. */
  def this(jedisCluster: JedisCluster,
           conf: Map[String, String],
           conditionalObjectWriter: ConditionalObjectWriter,
           requireBatchStatus: Boolean) =
    this(jedisCluster, conf, conditionalObjectWriter, requireBatchStatus, None)

  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  import RedisKVStore._
  import RedisKVStoreImpl._

  // Configurable key prefix (can be empty for dedicated Redis deployments)
  private val keyPrefix: String = RedisKVStoreFactory.keyPrefix(conf)
  private val keyHashTagMode: String =
    conf.getOrElse("redis.key.hash.tag.mode", RedisKVStore.DatasetAndEntityHashTagMode)
  private val recordReadMetrics: Boolean =
    !conf.get("redis.read.metrics.enabled").exists(_.equalsIgnoreCase("false"))
  private val redisReadMetricTags: Map[String, String] =
    Map(
      "redis_client" -> (if (glideReadClient.isDefined) RedisClientGlide else RedisClientJedis),
      "read_from" -> conf.getOrElse(PropRedisReadFrom, RedisReadFromPrimary)
    ) ++ conf.get(PropRedisClientAz).map("client_az" -> _)
  private[redis] val pipelineSyncThreads: Int = conf
    .get(ConfigPipelineSyncThreads)
    .map { value =>
      Try(value.toInt).getOrElse(
        throw new IllegalArgumentException(s"$ConfigPipelineSyncThreads must be an integer, got '$value'"))
    }
    .getOrElse(DefaultPipelineSyncThreads)
  require(pipelineSyncThreads >= 0, s"$ConfigPipelineSyncThreads must be non-negative, got $pipelineSyncThreads")

  private[redis] val pipelineSyncExecutor: Option[ThreadPoolExecutor] =
    if (glideReadClient.isEmpty && pipelineSyncThreads > 0) Some(buildPipelineSyncExecutor(pipelineSyncThreads))
    else None

  // TTL is now configurable via RedisKVStoreConstants or via props in create()
  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("redis")
  protected val tableToContext = new TrieMap[String, Metrics.Context]()
  private val batchStatusCache = new TrieMap[String, RedisKVStore.CachedBatchStatus]()

  // Resolved lazily so direct store construction only needs client configuration when bulk upload is used.
  private lazy val clientSettings: RedisKVStoreFactory.ClientSettings = RedisKVStoreFactory.settings(conf)

  override def create(dataset: String): Unit = {
    logger.info(s"Dataset $dataset ready for use (Redis doesn't require explicit table creation)")
    metricsContext.increment("create.successes")
  }

  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
    logger.debug(s"Performing multi-get for ${requests.size} requests")
    if (requests.isEmpty) {
      Future.successful(Seq.empty)
    } else {
      glideReadClient match {
        case None =>
          Future {
            val startedAt = System.currentTimeMillis()
            val defaultEndTs = System.currentTimeMillis()
            try {
              val plans = requests.map(request => planRead(request, defaultEndTs))
              val redisPlans = plans.collect { case plan: RedisReadPlan => plan }
              val responses = buildReadResponses(plans, executeReadPipelineWithRetry(redisPlans))
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
        case Some(_) =>
          multiGetWithGlide(requests)
      }
    }
  }

  private def buildReadResponses(plans: Seq[PlannedRead], redisResults: Seq[Try[Seq[TimedValue]]]): Seq[GetResponse] = {
    val results = redisResults.iterator
    val responses = plans.map {
      case ImmediateReadPlan(request, values) => GetResponse(request, values)
      case plan: RedisReadPlan                => GetResponse(plan.request, results.next())
    }
    require(!results.hasNext, s"Read client returned more results than the ${plans.size} planned reads")
    responses
  }

  private final class PreparedGlideReads(val commands: IndexedSeq[GlideReadCommand],
                                         val commandRanges: Array[Range],
                                         val mgetValueIndexes: Array[Int])

  /** Runs one GLIDE multi-get with exactly two shared-executor tasks on the success path:
    *   1. Chronon read planning, GLIDE command planning, and native submission;
    *   2. native-result decoding, response construction, metrics, and promise completion.
    *
    * The native future waits on GLIDE's own event loop. Its callback only timestamps completion and submits task 2, so
    * no chronon-fetcher worker is occupied while Redis is in flight.
    */
  private def multiGetWithGlide(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
    val startedAt = System.currentTimeMillis()
    val responsePromise = Promise[Seq[GetResponse]]()

    def completeSuccess(responses: Seq[GetResponse]): Unit = responsePromise.synchronized {
      if (!responsePromise.isCompleted) {
        if (recordReadMetrics) recordMultiGetMetrics(responses, System.currentTimeMillis() - startedAt)
        responsePromise.success(responses)
      }
    }

    def completeFailure(error: Exception): Unit = responsePromise.synchronized {
      if (!responsePromise.isCompleted) {
        try {
          logger.error("Error getting values from Redis Cluster", error)
          val responses = requests.map(request => GetResponse(request, Failure(error)))
          if (recordReadMetrics) recordMultiGetMetrics(responses, System.currentTimeMillis() - startedAt)
          responsePromise.success(responses)
        } catch {
          case completionError: Throwable =>
            responsePromise.tryFailure(completionError)
            if (!NonFatal(completionError)) throw completionError
        }
      }
    }

    def failOuter(error: Throwable): Unit = responsePromise.synchronized {
      if (!responsePromise.isCompleted) responsePromise.failure(error)
    }

    def completeAsyncFailure(error: Throwable): Unit = error match {
      case exception: Exception => completeFailure(exception)
      case other                => failOuter(other)
    }

    def completeThrown(error: Throwable): Unit = error match {
      case exception: Exception => completeFailure(exception)
      case other =>
        failOuter(other)
        if (!NonFatal(other)) throw other
    }

    def submit(task: Runnable): Unit =
      try executionContext.execute(task)
      catch {
        case error: Throwable => completeThrown(error)
      }

    submit(
      new Runnable {
        override def run(): Unit = {
          try {
            val plans = recordReadPhase("multiGet.plan.latency") {
              val defaultEndTs = System.currentTimeMillis()
              requests.map(request => planRead(request, defaultEndTs))
            }
            val redisPlans = plans.collect { case plan: RedisReadPlan => plan }.toIndexedSeq
            val prepared = prepareGlideReads(redisPlans)
            val nativeStartedAtNanos = System.nanoTime()
            val nativeFuture = glideReadClient.get.execute(prepared.commands)

            nativeFuture.whenComplete(new BiConsumer[IndexedSeq[Try[GlideReadResult]], Throwable] {
              override def accept(commandResults: IndexedSeq[Try[GlideReadResult]], nativeError: Throwable): Unit = {
                val nativeCompletedAtNanos = System.nanoTime()
                val completionTask = new Runnable {
                  override def run(): Unit = {
                    try {
                      if (nativeError != null) {
                        completeAsyncFailure(unwrapCompletionError(nativeError))
                      } else {
                        val decodeStartedAtNanos = System.nanoTime()
                        recordReadDuration("multiGet.glide.completion_queue.latency",
                                           nativeCompletedAtNanos,
                                           decodeStartedAtNanos)
                        val redisResults = decodeGlideReads(redisPlans, prepared, commandResults, decodeStartedAtNanos)
                        val responses = buildReadResponses(plans, redisResults)
                        completeSuccess(responses)
                      }
                    } catch {
                      case error: Throwable => completeThrown(error)
                    }
                  }
                }

                submit(completionTask)
                // Submit the completion task first so metric reporting on GLIDE's callback thread is not counted as
                // shared-executor completion-queue latency.
                recordReadDuration("multiGet.glide.native.latency", nativeStartedAtNanos, nativeCompletedAtNanos)
              }
            })
          } catch {
            case error: Throwable => completeThrown(error)
          }
        }
      }
    )

    responsePromise.future
  }

  private def prepareGlideReads(plans: IndexedSeq[RedisReadPlan]): PreparedGlideReads = {
    val commands = mutable.ArrayBuffer.empty[GlideReadCommand]
    val commandRanges = Array.fill[Range](plans.size)(0 until 0)
    val mgetValueIndexes = Array.fill(plans.size)(-1)
    recordReadPhase("multiGet.glide.command_planning.latency") {
      val mgetGroups = new java.util.LinkedHashMap[Int, java.util.ArrayList[(Int, BatchReadPlan)]]()

      plans.iterator.zipWithIndex.foreach {
        case (plan: BatchReadPlan, index) =>
          val slot = JedisClusterCRC16.getSlot(plan.redisKey)
          var group = mgetGroups.get(slot)
          if (group == null) {
            group = new java.util.ArrayList[(Int, BatchReadPlan)]()
            mgetGroups.put(slot, group)
          }
          group.add(index -> plan)
        case (StreamingReadPlan(_, redisKeys, startTs, endTs), index) =>
          val start = commands.size
          redisKeys.foreach(redisKey => commands += GlideZRangeByScore(redisKey, startTs.toDouble, endTs.toDouble))
          commandRanges(index) = start until commands.size
      }

      mgetGroups.values().asScala.foreach { group =>
        val commandIndex = commands.size
        if (group.size() == 1) {
          val (planIndex, plan) = group.get(0)
          commands += GlideGet(plan.redisKey)
          commandRanges(planIndex) = commandIndex until commandIndex + 1
        } else {
          val redisKeys = Vector.newBuilder[Array[Byte]]
          var valueIndex = 0
          while (valueIndex < group.size()) {
            redisKeys += group.get(valueIndex)._2.redisKey
            valueIndex += 1
          }
          commands += GlideMGet(redisKeys.result())

          valueIndex = 0
          while (valueIndex < group.size()) {
            val planIndex = group.get(valueIndex)._1
            commandRanges(planIndex) = commandIndex until commandIndex + 1
            mgetValueIndexes(planIndex) = valueIndex
            valueIndex += 1
          }
        }
      }
    }

    if (recordReadMetrics)
      metricsContext.distribution("multiGet.glide.command_count", commands.size.toLong, redisReadMetricTags)
    new PreparedGlideReads(commands.toIndexedSeq, commandRanges, mgetValueIndexes)
  }

  private def decodeGlideReads(plans: IndexedSeq[RedisReadPlan],
                               prepared: PreparedGlideReads,
                               commandResults: IndexedSeq[Try[GlideReadResult]],
                               decodeStartedAtNanos: Long): Seq[Try[Seq[TimedValue]]] = {
    require(
      commandResults.size == prepared.commands.size,
      s"GLIDE returned ${commandResults.size} command results for ${prepared.commands.size} commands"
    )
    val decoded = plans.indices.map { index =>
      val plan = plans(index)
      val range = prepared.commandRanges(index)
      plan match {
        case batchPlan: BatchReadPlan if prepared.mgetValueIndexes(index) >= 0 =>
          decodeGlideMGet(batchPlan, commandResults(range.start), prepared.mgetValueIndexes(index))
        case _ =>
          decodeGlideRead(plan, commandResults.slice(range.start, range.end))
      }
    }
    recordReadDuration("multiGet.glide.decode.latency", decodeStartedAtNanos, System.nanoTime())
    decoded
  }

  private def decodeGlideMGet(batchPlan: BatchReadPlan,
                              commandResult: Try[GlideReadResult],
                              valueIndex: Int): Try[Seq[TimedValue]] = commandResult match {
    case Success(GlideOptionalValues(values)) if valueIndex < values.size =>
      Try(decodeBatchValue(values(valueIndex).orNull, batchPlan))
    case Success(GlideOptionalValues(values)) =>
      Failure(new IllegalStateException(s"GLIDE MGET returned ${values.size} values; missing result index $valueIndex"))
    case Success(other) =>
      Failure(new IllegalStateException(s"Unexpected GLIDE MGET result ${other.getClass.getName}"))
    case Failure(error) => Failure(error)
  }

  private def decodeGlideRead(plan: RedisReadPlan,
                              commandResults: IndexedSeq[Try[GlideReadResult]]): Try[Seq[TimedValue]] = plan match {
    case batchPlan: BatchReadPlan =>
      commandResults.headOption match {
        case Some(Success(GlideValue(value))) => Try(decodeBatchValue(value.orNull, batchPlan))
        case Some(Success(other)) =>
          Failure(new IllegalStateException(s"Unexpected GLIDE batch-read result ${other.getClass.getName}"))
        case Some(Failure(error)) => Failure(error)
        case None                 => Failure(new IllegalStateException("GLIDE returned no result for a batch read"))
      }
    case _: StreamingReadPlan =>
      val members = commandResults.foldLeft[Try[Vector[Array[Byte]]]](Success(Vector.empty)) {
        case (failure @ Failure(_), _)                            => failure
        case (Success(accumulated), Success(GlideValues(values))) => Success(accumulated ++ values)
        case (_, Success(other)) =>
          Failure(new IllegalStateException(s"Unexpected GLIDE streaming-read result ${other.getClass.getName}"))
        case (_, Failure(error)) => Failure(error)
      }
      members.map(decodeStreamingMembers)
  }

  private def unwrapCompletionError(error: Throwable): Throwable = error match {
    case wrapper: CompletionException if wrapper.getCause != null => unwrapCompletionError(wrapper.getCause)
    case wrapper: ExecutionException if wrapper.getCause != null  => unwrapCompletionError(wrapper.getCause)
    case other                                                    => other
  }

  private def planRead(request: GetRequest, defaultEndTs: Long): PlannedRead = {
    val planned = Try {
      getTableType(request.dataset) match {
        case BatchTable =>
          val redisKey =
            buildRedisKey(request.keyBytes, request.dataset, keyPrefix = keyPrefix, hashTagMode = keyHashTagMode)
          val publishedStatus =
            if (requireBatchStatus && request.dataset.endsWith("_BATCH")) {
              cachedBatchStatus(request.dataset) match {
                case Some(status) => Some(status)
                case None =>
                  throw new IllegalStateException(
                    s"Redis incremental batch dataset ${request.dataset} has no applied-status marker; " +
                      "reads are disabled until a full upload succeeds")
              }
            } else None
          if (publishedStatus.exists(_.retired)) ImmediateReadPlan(request, Success(Seq.empty))
          else BatchReadPlan(request, redisKey.getBytes(StandardCharsets.UTF_8), publishedStatus)
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

  private def cachedBatchStatus(dataset: String): Option[RedisBatchUpload.BatchStatus] = {
    val now = System.currentTimeMillis()
    batchStatusCache.get(dataset) match {
      case Some(cached) if cached.expiresAtMillis > now => cached.status
      case _ =>
        batchStatusCache.synchronized {
          val refreshedNow = System.currentTimeMillis()
          batchStatusCache.get(dataset) match {
            case Some(cached) if cached.expiresAtMillis > refreshedNow => cached.status
            case _ => refreshBatchStatusLocked(dataset, refreshedNow)
          }
        }
    }
  }

  private def refreshBatchStatus(dataset: String): Option[RedisBatchUpload.BatchStatus] =
    batchStatusCache.synchronized {
      refreshBatchStatusLocked(dataset, System.currentTimeMillis())
    }

  private def refreshBatchStatusLocked(dataset: String,
                                       refreshedAtMillis: Long): Option[RedisBatchUpload.BatchStatus] = {
    val status = RedisBatchUpload.readStatus(jedisCluster, RedisBatchUpload.buildStatusKey(dataset, keyPrefix))
    cacheBatchStatus(dataset, status, refreshedAtMillis)
    status
  }

  private def cacheBatchStatus(dataset: String,
                               status: Option[RedisBatchUpload.BatchStatus],
                               refreshedAtMillis: Long): Unit =
    status match {
      case Some(_) =>
        batchStatusCache.put(
          dataset,
          RedisKVStore.CachedBatchStatus(status, refreshedAtMillis + RedisKVStore.BatchStatusCacheMillis))
      case None => batchStatusCache.remove(dataset)
    }

  private def isAheadOfStatus(decoded: RedisBatchUpload.DecodedValue, status: RedisBatchUpload.BatchStatus): Boolean =
    decoded.storedTimestamp > status.batchTimestamp ||
      (decoded.storedTimestamp == status.batchTimestamp && decoded.writeEpoch > status.writeEpoch)

  private def queueRead(plan: RedisReadPlan, pipeline: ClusterPipeline): PendingRead = plan match {
    case batchPlan @ BatchReadPlan(request, redisKey, _) =>
      val response = pipeline.get(redisKey)
      PendingRead(request, () => Try(decodeBatchValue(response.get(), batchPlan)))
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
      val pipeline = pipelineSyncExecutor match {
        case Some(executor) => jedisCluster.pipelined(executor)
        case None           => jedisCluster.pipelined()
      }
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
          pendingReads(pendingIndex) =
            PendingRead(plan.request, () => Try(decodeBatchValue(values.get(responseIndex), plan)))
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
      case batchPlan @ BatchReadPlan(_, redisKey, _) =>
        decodeBatchValue(jedisCluster.get(redisKey), batchPlan)
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
    case BatchReadPlan(_, redisKey, _) =>
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

  private def decodeBatchValue(storedBytes: Array[Byte], plan: BatchReadPlan): Seq[TimedValue] =
    Option(storedBytes).toSeq.flatMap { bytes =>
      val decoded = RedisBatchUpload.decodeValue(bytes).get
      val effectiveStatus = plan.publishedStatus.map { cachedStatus =>
        val status =
          if (isAheadOfStatus(decoded, cachedStatus)) {
            refreshBatchStatus(plan.request.dataset).getOrElse(
              throw new IllegalStateException(
                s"Redis incremental batch dataset ${plan.request.dataset} has no applied-status marker; " +
                  "reads are disabled until a full upload succeeds"))
          } else cachedStatus
        if (isAheadOfStatus(decoded, status)) {
          throw new IllegalStateException(
            s"Redis batch value for ${plan.request.dataset} is newer than applied status ${status.generation}")
        }
        status
      }

      if (effectiveStatus.exists(_.retired) || decoded.operation == RedisBatchUpload.Delete) {
        Seq.empty
      } else {
        Seq(
          TimedValue(
            decoded.payload,
            effectiveStatus.fold(decoded.storedTimestamp)(status =>
              math.max(status.batchTimestamp, decoded.storedTimestamp))
          ))
      }
    }

  private def decodeStreamingValues(commands: Seq[StreamingCommand]): Seq[TimedValue] = {
    commands.flatMap { command =>
      decodeStreamingMembers(command.response.get())
    }
  }

  private def decodeStreamingMembers(members: java.util.List[Array[Byte]]): Seq[TimedValue] =
    decodeStreamingMembers(members.asScala.toSeq)

  private def decodeStreamingMembers(members: Seq[Array[Byte]]): Seq[TimedValue] = {
    members.iterator.flatMap { memberBytes =>
      if (memberBytes.length >= 8) {
        val timestamp = java.nio.ByteBuffer.wrap(memberBytes, 0, 8).getLong
        Some(TimedValue(memberBytes.drop(8), timestamp))
      } else {
        logger.warn(s"Malformed streaming data in Redis: member has ${memberBytes.length} bytes, expected >= 8")
        None
      }
    }.toSeq
  }

  private def recordMultiGetMetrics(responses: Seq[GetResponse], latencyMillis: Long): Unit = {
    metricsContext.distribution("multiGet.latency", latencyMillis, redisReadMetricTags)
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
          datasetMetricsContext.increment(
            "multiGet.redis_errors",
            redisReadMetricTags + ("exception" -> error.getClass.getName)
          )
        case None =>
          datasetMetricsContext.increment("multiGet.successes", redisReadMetricTags)
      }
    }
  }

  private def recordReadPhase[A](metric: String)(body: => A): A = {
    val startedAtNanos = System.nanoTime()
    try body
    finally recordReadDuration(metric, startedAtNanos, System.nanoTime())
  }

  private def recordReadDuration(metric: String, startedAtNanos: Long, completedAtNanos: Long): Unit =
    if (recordReadMetrics)
      metricsContext.distribution(
        metric,
        TimeUnit.NANOSECONDS.toMillis(math.max(0L, completedAtNanos - startedAtNanos)),
        redisReadMetricTags
      )

  override def list(request: ListRequest): Future[ListResponse] = {
    logger.debug(s"Performing list for ${request.dataset}")

    val listLimit = request.props.get(ListLimit) match {
      case Some(value: Int)    => value
      case Some(value: String) => value.toInt
      case _                   => DefaultListLimit
    }
    require(listLimit > 0, s"Redis list limit must be positive, got $listLimit")

    val maybeListEntityType = request.props.get(ListEntityType).map(_.toString)
    val maybeStartKey = request.props.get(ContinuationKey)

    val datasetMetricsContext = tableToContext.getOrElseUpdate(
      request.dataset,
      metricsContext.copy(dataset = request.dataset)
    )

    Future {
      try {
        val startTs = System.currentTimeMillis()
        val pattern = listScanPattern(request.dataset)
        val masterNodes = redisMasterNodes()
        require(masterNodes.nonEmpty, "Redis cluster topology contains no master nodes")

        val requestedCursor = decodeRequestedListCursor(maybeStartKey, masterNodes.head._1)
        var nodeIndex =
          if (requestedCursor.nodeKey.isEmpty) masterNodes.size
          else masterNodes.indexWhere(_._1 == requestedCursor.nodeKey)
        require(
          nodeIndex >= 0,
          s"Redis list continuation references a node that is no longer in the cluster: ${requestedCursor.nodeKey}")

        var nodeCursor = requestedCursor.scanCursor
        var pendingKeys = requestedCursor.pendingKeys
        val listValues = mutable.ArrayBuffer.empty[ListValue]

        while (listValues.size < listLimit && (pendingKeys.nonEmpty || nodeIndex < masterNodes.size)) {
          if (pendingKeys.nonEmpty) {
            val storageKey = pendingKeys.head
            pendingKeys = pendingKeys.tail
            readListValue(storageKey, request.dataset).foreach(listValues += _)
          } else {
            val (_, pool) = masterNodes(nodeIndex)
            val remaining = math.max(1, listLimit - listValues.size)
            val scanResult = scanNode(pool, nodeCursor, pattern, remaining)
            nodeCursor = scanResult.getCursor
            pendingKeys = scanResult.getResult.asScala.iterator.filter { storageKey =>
              val logicalKey = decodeLogicalKey(storageKey, request.dataset)
              maybeListEntityType.forall(entityType =>
                new String(logicalKey, StandardCharsets.UTF_8).startsWith(s"$entityType/"))
            }.toVector

            if (nodeCursor == ScanParams.SCAN_POINTER_START) {
              nodeIndex += 1
              nodeCursor = ScanParams.SCAN_POINTER_START
            }
          }
        }

        datasetMetricsContext.distribution("list.latency", System.currentTimeMillis() - startTs)
        datasetMetricsContext.increment("list.successes")

        val propsMap: Map[String, Any] =
          if (pendingKeys.nonEmpty || nodeIndex < masterNodes.size) {
            val nextNodeKey = if (nodeIndex < masterNodes.size) masterNodes(nodeIndex)._1 else ""
            Map(ContinuationKey -> encodeListCursor(ListCursor(nextNodeKey, nodeCursor, pendingKeys)))
          } else Map.empty

        ListResponse(request, Success(listValues.toSeq), propsMap)
      } catch {
        case e: Exception =>
          logger.error("Error listing values from Redis Cluster", e)
          datasetMetricsContext.increment("list.redis_errors", Map("exception" -> e.getClass.getName))
          ListResponse(request, Failure(e), Map.empty)
      }
    }
  }

  private def listScanPattern(dataset: String): String = {
    val prefix = if (keyPrefix.isEmpty) "" else s"$keyPrefix$KeySeparator"
    keyHashTagMode match {
      case DatasetAndEntityHashTagMode => s"$prefix{$dataset$KeySeparator*}"
      case EntityHashTagMode           => s"$prefix$dataset$KeySeparator{*}"
      case other                       => throw new IllegalArgumentException(s"Unknown Redis hash tag mode: $other")
    }
  }

  private def redisMasterNodes(): Vector[(String, ConnectionPool)] =
    jedisCluster.getClusterNodes.asScala.toVector.sortBy(_._1).filter { case (_, pool) =>
      val jedis = new Jedis(pool.getResource)
      try jedis.info("replication").contains("role:master")
      finally jedis.close()
    }

  private def scanNode(pool: ConnectionPool, cursor: String, pattern: String, count: Int): ScanResult[String] = {
    val jedis = new Jedis(pool.getResource)
    try jedis.scan(cursor, new ScanParams().`match`(pattern).count(count))
    finally jedis.close()
  }

  private def decodeLogicalKey(storageKey: String, dataset: String): Array[Byte] = {
    val prefix = if (keyPrefix.isEmpty) "" else s"$keyPrefix$KeySeparator"
    val encodedKey = keyHashTagMode match {
      case DatasetAndEntityHashTagMode =>
        val keyPrefix = s"$prefix{$dataset$KeySeparator"
        require(storageKey.startsWith(keyPrefix) && storageKey.endsWith("}"),
                s"Redis key does not match dataset-and-entity list layout: $storageKey")
        storageKey.substring(keyPrefix.length, storageKey.length - 1)
      case EntityHashTagMode =>
        val keyPrefix = s"$prefix$dataset$KeySeparator{"
        require(storageKey.startsWith(keyPrefix) && storageKey.endsWith("}"),
                s"Redis key does not match entity list layout: $storageKey")
        storageKey.substring(keyPrefix.length, storageKey.length - 1)
      case other => throw new IllegalArgumentException(s"Unknown Redis hash tag mode: $other")
    }
    Base64.getDecoder.decode(encodedKey)
  }

  private def readListValue(storageKey: String, dataset: String): Option[ListValue] = {
    val logicalKey = decodeLogicalKey(storageKey, dataset)
    Option(jedisCluster.get(storageKey.getBytes(StandardCharsets.UTF_8))).flatMap { storedBytes =>
      val decoded = RedisBatchUpload.decodeValue(storedBytes).get
      if (decoded.operation == RedisBatchUpload.Delete) None
      else Some(ListValue(logicalKey, decoded.payload))
    }
  }

  private def decodeRequestedListCursor(value: Option[Any], firstNodeKey: String): ListCursor = value match {
    case None => ListCursor(firstNodeKey, ScanParams.SCAN_POINTER_START, Vector.empty)
    case Some(bytes: Array[Byte]) if isStructuredListCursor(bytes) => decodeListCursor(bytes)
    case Some(bytes: Array[Byte]) =>
      ListCursor(firstNodeKey, new String(bytes, StandardCharsets.UTF_8), Vector.empty)
    case Some(cursor: String) => ListCursor(firstNodeKey, cursor, Vector.empty)
    case Some(other) =>
      throw new IllegalArgumentException(s"Unsupported Redis list continuation type: ${other.getClass.getName}")
  }

  private def isStructuredListCursor(bytes: Array[Byte]): Boolean =
    bytes.length >= 8 && ByteBuffer.wrap(bytes, 0, 4).getInt == ListCursorMagic

  private def encodeListCursor(cursor: ListCursor): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    try {
      out.writeInt(ListCursorMagic)
      out.writeInt(ListCursorVersion)
      out.writeUTF(cursor.nodeKey)
      out.writeUTF(cursor.scanCursor)
      out.writeInt(cursor.pendingKeys.size)
      cursor.pendingKeys.foreach(out.writeUTF)
      out.flush()
      bytes.toByteArray
    } finally {
      out.close()
    }
  }

  private def decodeListCursor(bytes: Array[Byte]): ListCursor = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      require(in.readInt() == ListCursorMagic, "Redis list continuation has an invalid magic header")
      require(in.readInt() == ListCursorVersion, "Redis list continuation has an unsupported version")
      val nodeKey = in.readUTF()
      val scanCursor = in.readUTF()
      val pendingCount = in.readInt()
      require(pendingCount >= 0 && pendingCount <= MaxListCursorPendingKeys,
              s"Redis list continuation has an invalid pending-key count: $pendingCount")
      val pendingKeys = Vector.fill(pendingCount)(in.readUTF())
      require(in.available() == 0, "Redis list continuation has trailing bytes")
      ListCursor(nodeKey, scanCursor, pendingKeys)
    } finally {
      in.close()
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
    require(
      requireBatchStatus,
      "Redis incremental bulk upload requires a managed store with a distributed conditional-object writer"
    )
    val startTs = System.currentTimeMillis()

    logger.info(
      s"Triggering Spark-based bulk load for dataset: $destinationOnlineDataSet, " +
        s"table: $sourceOfflineTable, partition: $partition"
    )

    try {
      val existingSpark = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
      val spark = existingSpark.getOrElse {
        val submissionConf = new SparkConf().getAll.toMap
        val sparkConf = RedisKVStore.sparkSessionConfig(conf, submissionConf)
        SparkSessionBuilder.build(
          s"Spark2RedisLoader-$destinationOnlineDataSet",
          additionalConfig = if (sparkConf.nonEmpty) Some(sparkConf) else None
        )
      }
      val summary =
        try {
          val uploadConf = spark.conf.getAll ++ conf
          val stateRoot = requiredBulkUploadSetting(uploadConf, PropRedisBulkUploadStateRoot)
          val dailySpec = PartitionSpec.daily
          val partitionSpec = PartitionSpec(
            uploadConf.getOrElse(Spark2RedisLoader.PartitionColumnProp, dailySpec.column),
            uploadConf.getOrElse(Spark2RedisLoader.PartitionFormatProp, dailySpec.format),
            configuredLong(uploadConf, Spark2RedisLoader.PartitionSpanMillisProp, dailySpec.spanMillis),
            configuredLong(uploadConf, Spark2RedisLoader.PartitionOffsetMillisProp, dailySpec.offsetMillis)
          )
          val settings = Spark2RedisLoader.Settings(
            sourceTable = sourceOfflineTable,
            destinationDataset = destinationOnlineDataSet,
            sourcePartition = partition,
            partitionSpec = partitionSpec,
            stateRoot = stateRoot,
            keyPrefix = keyPrefix,
            clientSettings = clientSettings,
            keyHashTagMode = keyHashTagMode,
            batchSize = configuredInt(uploadConf, PropRedisBulkUploadBatchSize, DefaultBulkUploadBatchSize),
            maxKeysPerSecond =
              configuredInt(uploadConf, PropRedisBulkUploadMaxKeysPerSecond, DefaultBulkUploadMaxKeysPerSecond),
            writerPartitions =
              configuredInt(uploadConf, PropRedisBulkUploadWriterPartitions, DefaultBulkUploadWriterPartitions),
            stateBuckets = configuredInt(uploadConf, PropRedisBulkUploadStateBuckets, DefaultBulkUploadStateBuckets),
            maxRetries = configuredInt(uploadConf, PropRedisBulkUploadMaxRetries, DefaultBulkUploadMaxRetries),
            retryBackoffMs =
              configuredLong(uploadConf, PropRedisBulkUploadRetryBackoffMs, DefaultBulkUploadRetryBackoffMs),
            liveTTLSeconds = configuredInt(uploadConf, PropRedisBulkUploadTTLSeconds, DefaultBulkUploadTTLSeconds),
            requireNoEviction = Spark2RedisLoader.configuredBoolean(uploadConf,
                                                                    PropRedisBulkUploadRequireNoEviction,
                                                                    DefaultBulkUploadRequireNoEviction),
            deleteOlderVersions = Spark2RedisLoader.configuredBoolean(uploadConf,
                                                                      PropRedisBulkUploadDeleteOlderVersions,
                                                                      DefaultBulkUploadDeleteOlderVersions)
          )
          Spark2RedisLoader.run(settings, spark, jedisCluster, conditionalObjectWriter)
        } finally if (existingSpark.isEmpty) spark.stop()

      logger.info(s"Spark-based incremental bulk load completed successfully: $summary")
      metricsContext.distribution("bulkPut.latency", System.currentTimeMillis() - startTs)
      metricsContext.increment("bulkPut.successes")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to run Spark-based bulk load for $sourceOfflineTable", e)
        metricsContext.increment("bulkPut.failures", Map("exception" -> e.getClass.getName))
        throw e
    }
  }

  private def requiredBulkUploadSetting(settings: Map[String, String], propKey: String): String =
    settings
      .get(propKey)
      .map(_.trim)
      .filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"$propKey must be set for Redis bulk upload"))

  private def configuredInt(settings: Map[String, String], propKey: String, default: Int): Int =
    settings
      .get(propKey)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.toInt)
      .getOrElse(default)

  private def configuredLong(settings: Map[String, String], propKey: String, default: Long): Long =
    settings
      .get(propKey)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.toLong)
      .getOrElse(default)

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

  private[redis] def shutdownPipelineSyncExecutor(): Unit = {
    pipelineSyncExecutor.foreach { executor =>
      executor.shutdown()
      try {
        if (!executor.awaitTermination(PipelineExecutorShutdownTimeoutSeconds, TimeUnit.SECONDS)) {
          executor.shutdownNow()
          executor.awaitTermination(PipelineExecutorShutdownTimeoutSeconds, TimeUnit.SECONDS)
        }
      } catch {
        case _: InterruptedException =>
          executor.shutdownNow()
          Thread.currentThread().interrupt()
      }
    }
  }

  private[redis] def shutdownGlideReadClient(): Unit =
    glideReadClient.foreach { client =>
      try client.close()
      catch {
        case e: Exception => logger.warn("Failed to close GLIDE Redis read client", e)
      }
    }

  override def close(): Unit = {
    shutdownPipelineSyncExecutor()
    shutdownGlideReadClient()
    jedisCluster.close()
  }
}

private[redis] object RedisKVStoreImpl {
  private val PipelineResponseUnsetMessage = "Please close pipeline or multi block before calling this method."
  private val PipelineExecutorShutdownTimeoutSeconds = 5L
  private val PipelineExecutorIds = new AtomicInteger()
  private val ListCursorMagic = 0x43524c43 // CRLC
  private val ListCursorVersion = 1
  private val MaxListCursorPendingKeys = 100000

  private case class ListCursor(nodeKey: String, scanCursor: String, pendingKeys: Vector[String])

  private def buildPipelineSyncExecutor(threads: Int): ThreadPoolExecutor = {
    val executorId = PipelineExecutorIds.incrementAndGet()
    val threadIds = new AtomicInteger()
    val threadFactory = new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, s"chronon-redis-pipeline-$executorId-${threadIds.incrementAndGet()}")
        thread.setDaemon(true)
        if (thread.getContextClassLoader == null) thread.setContextClassLoader(getClass.getClassLoader)
        thread
      }
    }
    val executor = new ThreadPoolExecutor(
      threads,
      threads,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](),
      threadFactory
    )
    executor.prestartAllCoreThreads()
    executor
  }

  sealed trait PlannedRead {
    def request: GetRequest
  }

  sealed trait RedisReadPlan extends PlannedRead

  final case class ImmediateReadPlan(request: GetRequest, values: Try[Seq[TimedValue]]) extends PlannedRead

  final case class BatchReadPlan(request: GetRequest,
                                 redisKey: Array[Byte],
                                 publishedStatus: Option[RedisBatchUpload.BatchStatus])
      extends RedisReadPlan

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
  private[redis] val BatchStatusCacheMillis = 5000L

  private[redis] case class CachedBatchStatus(status: Option[RedisBatchUpload.BatchStatus], expiresAtMillis: Long)

  /** Reapply metadata Spark defaults when the Redis publisher creates its session, while preserving the effective
    * spark-submit configuration. The submitted configuration wins over compiled metadata.
    */
  private[redis] def sparkSessionConfig(metadataConfig: Map[String, String],
                                        submissionConfig: Map[String, String]): Map[String, String] =
    (metadataConfig.filter { case (key, _) => key.startsWith("spark.") } ++
      submissionConfig.filter { case (key, _) => key.startsWith("spark.") }) + ("spark.speculation" -> "false")

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
