package ai.chronon.integrations.redis

import ai.chronon.api.Constants.{ContinuationKey, ListEntityType, ListLimit, MetadataDataset}
import ai.chronon.api.TilingUtils
import ai.chronon.api.Extensions.WindowUtils
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
import redis.clients.jedis.{ClusterPipeline, Jedis, JedisCluster, Response}
import redis.clients.jedis.params.ScanParams
import redis.clients.jedis.resps.Tuple

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
  * Direct KV writes use a default TTL of 5 days (matching BigTable), except Chronon config metadata, which is persistent.
  * Managed batch publication configures its lease independently.
  */
class RedisKVStoreImpl(jedisCluster: JedisCluster,
                       conf: Map[String, String] = Map.empty,
                       batchMode: RedisBatchMode = RedisBatchMode.FullSnapshot)
    extends KVStore {
  @transient override lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  import RedisKVStore._
  import RedisKVStoreImpl._

  private var factorySettings: Option[RedisKVStoreFactory.StoreSettings] = None

  // Configurable key prefix (can be empty for dedicated Redis deployments).
  private lazy val keyPrefix: String =
    factorySettings.map(_.keyPrefix).getOrElse(RedisKVStoreFactory.keyPrefix(conf))

  // Direct-write TTL and upload leases are intentionally configured on their separate write paths.
  protected val metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("redis")
  protected val tableToContext = new TrieMap[String, Metrics.Context]()
  private val batchStatusCache = new TrieMap[String, RedisKVStore.CachedBatchStatus]()
  private val batchPublicationModeCache = new TrieMap[String, RedisBatchModeSelection]()

  private var batchModeProvider: Map[String, String] => RedisBatchMode = (_: Map[String, String]) => batchMode

  /** Factory-only constructor that defers upload selection without changing the public constructor ABI. */
  private[redis] def this(jedisCluster: JedisCluster,
                          conf: Map[String, String],
                          batchModeProvider: Map[String, String] => RedisBatchMode) = {
    this(jedisCluster, conf, RedisBatchMode.FullSnapshot)
    this.batchModeProvider = batchModeProvider
  }

  /** Factory-only constructor that reuses the client configuration already resolved to create the control client. */
  private[redis] def this(jedisCluster: JedisCluster,
                          conf: Map[String, String],
                          batchModeProvider: Map[String, String] => RedisBatchMode,
                          factorySettings: RedisKVStoreFactory.StoreSettings) = {
    this(jedisCluster, conf, batchModeProvider)
    this.factorySettings = Some(factorySettings)
  }

  // Serving processes construct the same store, but never resolve the upload-only setting unless they actually issue
  // a direct batch write or bulkPut.
  private def resolvedBatchMode(uploadConf: Map[String, String]): RedisBatchMode = batchModeProvider(uploadConf)

  // Resolved lazily so direct store construction only needs client configuration when bulk upload is used.
  protected[redis] lazy val bulkUploadClientSettings: RedisKVStoreFactory.ClientSettings =
    factorySettings.map(_.client).getOrElse(RedisKVStoreFactory.settings(conf))

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
          val controlledDatasets = scala.collection.mutable.HashSet.empty[String]
          val plans = requests.map(request => planRead(request, defaultEndTs)).map {
            case plan: BatchReadPlan
                if plan.request.dataset.endsWith("_BATCH") && controlledDatasets.add(plan.request.dataset) =>
              plan.copy(controlKeys = Some(batchControlKeys(plan.request.dataset)))
            case plan => plan
          }
          val redisPlans = plans.collect { case plan: RedisReadPlan => plan }
          val redisResults = executeReadPipelineWithRetry(redisPlans).iterator
          val indexedReads = plans.zipWithIndex.map {
            case (plan @ ImmediateReadPlan(_, values), index) =>
              (index, plan, values.map(StreamingReadResult))
            case (plan: RedisReadPlan, index) =>
              (index, plan, redisResults.next())
          }
          val (managedBatchReads, ordinaryReads) =
            indexedReads.partition { case (_, plan, _) => plan.request.dataset.endsWith("_BATCH") }

          // Every Redis value read shares #2095's cross-request pipeline and retry lifecycle. Managed batch values are
          // decoded and publication-gated by dataset only after the pipeline resolves, so status failures stay local.
          val managedResponsesByIndex = managedBatchReads
            .groupBy { case (_, plan, _) => plan.request.dataset }
            .values
            .flatMap { indexedGroup =>
              val dataset = indexedGroup.head._2.request.dataset
              val datasetMetricsContext = tableToContext.getOrElseUpdate(
                dataset,
                metricsContext.copy(dataset = dataset)
              )
              val decodedResponses = indexedGroup.map { case (_, plan, result) =>
                plan.request -> decodeManagedBatchResult(result)
              }
              val control = indexedGroup
                .collectFirst {
                  case (_, plan: BatchReadPlan, result) if plan.controlKeys.isDefined =>
                    result.flatMap {
                      case BatchReadResult(_, Some(rawControl)) => decodeBatchControl(dataset, rawControl)
                      case BatchReadResult(_, None) =>
                        Failure(new IllegalStateException(s"Redis batch control result is missing for $dataset"))
                      case _: StreamingReadResult =>
                        Failure(
                          new IllegalStateException(s"Redis batch control produced a streaming result for $dataset"))
                    }
                }
                .getOrElse(Failure(new IllegalStateException(s"Redis batch control plan is missing for $dataset")))
              val responses = control
                .flatMap { batchControl =>
                  Try(readBatchRows(dataset, decodedResponses, datasetMetricsContext, batchControl))
                }
                .recover { case error =>
                  logger.error(s"Error getting managed batch values from Redis Cluster for $dataset", error)
                  decodedResponses.map { case (request, _) => GetResponse(request, Failure(error)) }
                }
                .get
              indexedGroup.map(_._1).zip(responses)
            }
            .toMap
          val ordinaryResponsesByIndex = ordinaryReads.map { case (index, plan, result) =>
            index -> renderOrdinaryRead(plan, result)
          }.toMap
          val responsesByIndex = managedResponsesByIndex ++ ordinaryResponsesByIndex
          val responses = requests.indices.map(responsesByIndex).toSeq
          recordMultiGetMetrics(responses, System.currentTimeMillis() - startedAt)
          responses
        } catch {
          case e: Exception =>
            logger.error("Error getting values from Redis Cluster", e)
            val responses = requests.map(request => GetResponse(request, Failure(e)))
            recordMultiGetMetrics(responses, System.currentTimeMillis() - startedAt)
            responses
        }
      }
    }
  }

  private def planRead(request: GetRequest, defaultEndTs: Long): PlannedRead = {
    val planned = Try {
      getTableType(request.dataset) match {
        case BatchTable =>
          val redisKey = buildRedisKey(request.keyBytes, request.dataset, keyPrefix = keyPrefix)
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
            buildTiledRedisKey(baseKeyBytes, request.dataset, dayTs, tileSizeMs, keyPrefix)
              .getBytes(StandardCharsets.UTF_8)
          }.toVector
          if (redisKeys.isEmpty) ImmediateReadPlan(request, Success(Seq.empty))
          else StreamingReadPlan(request, redisKeys, startTs, endTs)
        case StreamingTable =>
          ImmediateReadPlan(request, Success(Seq.empty))
      }
    }

    planned.recover { case error => ImmediateReadPlan(request, Failure(error)) }.get
  }

  private def batchControlKeys(dataset: String): BatchControlKeys =
    BatchControlKeys(
      publicationModeKey =
        if (batchPublicationModeCache.contains(dataset)) None
        else
          Some(
            RedisBatchUpload
              .buildPublicationModeKey(dataset, keyPrefix)
              .getBytes(StandardCharsets.UTF_8)),
      statusKey =
        if (freshCachedBatchStatus(dataset).isDefined) None
        else Some(RedisBatchUpload.buildStatusKey(dataset, keyPrefix).getBytes(StandardCharsets.UTF_8))
    )

  private def decodeBatchControl(dataset: String, rawControl: RawBatchControl): Try[BatchReadControl] = {
    val publicationMode = rawControl.publicationModeBytes match {
      case Some(Some(bytes)) =>
        Try(RedisBatchModeSelection.parse(new String(bytes, StandardCharsets.UTF_8))).map { mode =>
          batchPublicationModeCache.put(dataset, mode)
          Some(mode)
        }
      case Some(None) => Success(None)
      case None       => Success(batchPublicationModeCache.get(dataset))
    }
    val status = rawControl.statusBytes match {
      case Some(Some(bytes)) =>
        RedisBatchUpload.decodeStatus(bytes).map { decodedStatus =>
          cacheBatchStatus(dataset, Some(decodedStatus), System.currentTimeMillis())
          Some(decodedStatus)
        }
      case Some(None) =>
        cacheBatchStatus(dataset, None, System.currentTimeMillis())
        Success(None)
      case None => Success(freshCachedBatchStatus(dataset))
    }

    for {
      decodedPublicationMode <- publicationMode
      decodedStatus <- status
    } yield BatchReadControl(decodedPublicationMode, decodedStatus)
  }

  private def readBatchRows(dataset: String,
                            decodedResponses: Seq[(GetRequest, Try[Option[RedisBatchUpload.DecodedValue]])],
                            datasetMetricsContext: Metrics.Context,
                            control: BatchReadControl): Seq[GetResponse] = {
    control.publicationMode match {
      case Some(RedisBatchModeSelection.Incremental) =>
        readIncrementalBatchRows(
          dataset,
          decodedResponses,
          datasetMetricsContext,
          control.status.getOrElse(throw new IllegalStateException(
            s"Redis incremental batch dataset $dataset has no applied-status marker; reads are disabled until a full upload succeeds"))
        )
      case Some(RedisBatchModeSelection.FullSnapshot) =>
        control.status.foreach { _ =>
          throw new IllegalStateException(
            s"Redis full-snapshot batch dataset $dataset has an incremental applied-status marker")
        }
        readFullSnapshotBatchRows(dataset, decodedResponses, requireLegacyValues = true)
      case None =>
        control.status match {
          case Some(status) =>
            // Compatibility with incremental namespaces created before publication-mode markers were introduced.
            batchPublicationModeCache.put(dataset, RedisBatchModeSelection.Incremental)
            readIncrementalBatchRows(dataset, decodedResponses, datasetMetricsContext, status)
          case None =>
            readUnmarkedBatchRows(dataset, decodedResponses, datasetMetricsContext)
        }
    }
  }

  private def readFullSnapshotBatchRows(dataset: String,
                                        decodedResponses: Seq[
                                          (GetRequest, Try[Option[RedisBatchUpload.DecodedValue]])
                                        ],
                                        requireLegacyValues: Boolean): Seq[GetResponse] = {
    if (requireLegacyValues) requireNoVersionedValues(dataset, decodedResponses)
    renderBatchRows(dataset, decodedResponses, publishedStatus = None)
  }

  private def readUnmarkedBatchRows(dataset: String,
                                    decodedResponses: Seq[
                                      (GetRequest, Try[Option[RedisBatchUpload.DecodedValue]])
                                    ],
                                    datasetMetricsContext: Metrics.Context): Seq[GetResponse] = {
    if (containsVersionedValue(decodedResponses)) {
      refreshBatchStatus(dataset) match {
        case Some(status) =>
          batchPublicationModeCache.put(dataset, RedisBatchModeSelection.Incremental)
          readIncrementalBatchRows(dataset, decodedResponses, datasetMetricsContext, status)
        case None =>
          throw new IllegalStateException(
            s"Redis batch dataset $dataset contains incremental values without a publication-mode or applied-status marker")
      }
    } else renderBatchRows(dataset, decodedResponses, publishedStatus = None)
  }

  private def readIncrementalBatchRows(dataset: String,
                                       decodedResponses: Seq[
                                         (GetRequest, Try[Option[RedisBatchUpload.DecodedValue]])
                                       ],
                                       datasetMetricsContext: Metrics.Context,
                                       cachedPublishedStatus: RedisBatchUpload.BatchStatus): Seq[GetResponse] = {
    val requests = decodedResponses.map(_._1)
    if (cachedPublishedStatus.retired) return retiredBatchResponses(requests, datasetMetricsContext)

    // A warm fetcher can retain the previous status for five seconds after a successful publication. If a value is
    // ahead of that cached status, refresh the status once before failing the value. During a partial publication the
    // status remains old and the read still fails closed; after publication this avoids a deterministic cache brownout.
    val publishedStatus =
      if (
        decodedResponses.exists { case (_, decoded) =>
          decoded.toOption.flatten.exists(value => isAheadOfStatus(value, cachedPublishedStatus))
        }
      ) {
        refreshBatchStatus(dataset) match {
          case None =>
            throw new IllegalStateException(
              s"Redis incremental batch dataset $dataset has no applied-status marker; reads are disabled until a full upload succeeds")
          case Some(refreshed) if refreshed.retired =>
            return retiredBatchResponses(requests, datasetMetricsContext)
          case refreshed => refreshed
        }
      } else Some(cachedPublishedStatus)

    renderBatchRows(dataset, decodedResponses, publishedStatus)
  }

  private def containsVersionedValue(
      decodedResponses: Seq[(GetRequest, Try[Option[RedisBatchUpload.DecodedValue]])]): Boolean =
    decodedResponses.exists { case (_, decoded) =>
      decoded.toOption.flatten.exists(value => !value.legacy)
    }

  private def requireNoVersionedValues(
      dataset: String,
      decodedResponses: Seq[(GetRequest, Try[Option[RedisBatchUpload.DecodedValue]])]): Unit =
    if (containsVersionedValue(decodedResponses))
      throw new IllegalStateException(s"Redis full-snapshot batch dataset $dataset contains incremental values")

  private def renderBatchRows(dataset: String,
                              decodedResponses: Seq[(GetRequest, Try[Option[RedisBatchUpload.DecodedValue]])],
                              publishedStatus: Option[RedisBatchUpload.BatchStatus]): Seq[GetResponse] =
    decodedResponses.map { case (request, decodedResponse) =>
      val timedValues = decodedResponse.map {
        _.toSeq.flatMap { decoded =>
          publishedStatus.foreach { status =>
            if (isAheadOfStatus(decoded, status))
              throw new IllegalStateException(
                s"Redis batch value for $dataset is newer than applied status ${status.generation}")
          }
          decoded.operation match {
            case RedisBatchUpload.Upsert =>
              Seq(
                TimedValue(decoded.payload,
                           publishedStatus.fold(decoded.storedTimestamp) { status =>
                             math.max(status.batchTimestamp, decoded.storedTimestamp)
                           }))
            case RedisBatchUpload.Delete => Seq.empty
          }
        }
      }
      GetResponse(request, timedValues)
    }

  private def retiredBatchResponses(requests: Seq[GetRequest],
                                    datasetMetricsContext: Metrics.Context): Seq[GetResponse] = {
    datasetMetricsContext.count("multiGet.retired_batch_reads", requests.size.toLong)
    requests.map(request => GetResponse(request, Success(Seq.empty)))
  }

  private def freshCachedBatchStatus(dataset: String): Option[RedisBatchUpload.BatchStatus] = {
    val now = System.currentTimeMillis()
    batchStatusCache.get(dataset).filter(_.expiresAtMillis > now).flatMap(_.status)
  }

  private def refreshBatchStatus(dataset: String): Option[RedisBatchUpload.BatchStatus] =
    batchStatusCache.synchronized {
      val refreshedNow = System.currentTimeMillis()
      val status = RedisBatchUpload.readStatus(jedisCluster, RedisBatchUpload.buildStatusKey(dataset, keyPrefix))
      cacheBatchStatus(dataset, status, refreshedNow)
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
    case BatchReadPlan(request, redisKey, controlKeys) =>
      val response = pipeline.get(redisKey)
      val publicationModeResponse = controlKeys.flatMap(_.publicationModeKey.map(pipeline.get))
      val statusResponse = controlKeys.flatMap(_.statusKey.map(pipeline.get))
      PendingRead(
        request,
        () =>
          Try {
            val rawControl = controlKeys.map { _ =>
              RawBatchControl(
                publicationModeResponse.map(response => Option(response.get())),
                statusResponse.map(response => Option(response.get()))
              )
            }
            BatchReadResult(Option(response.get()), rawControl)
          }
      )
    case StreamingReadPlan(request, redisKeys, startTs, endTs) =>
      val commands = redisKeys.map { redisKey =>
        StreamingCommand(
          redisKey,
          startTs,
          endTs,
          pipeline.zrangeByScoreWithScores(redisKey, startTs.toDouble, endTs.toDouble)
        )
      }
      PendingRead(request, () => Try(StreamingReadResult(decodeStreamingValues(commands))))
  }

  private def executeReadPipelineWithRetry(plans: Seq[RedisReadPlan]): Seq[Try[RedisReadResult]] = {
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
          plans.map { plan =>
            activePlan = Some(plan)
            queueRead(plan, pipeline)
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

  private def executeDirectRead(plan: RedisReadPlan): Try[RedisReadResult] = Try {
    plan match {
      case BatchReadPlan(_, redisKey, controlKeys) =>
        val storedBytes = Option(jedisCluster.get(redisKey))
        val rawControl = controlKeys.map { keys =>
          RawBatchControl(
            keys.publicationModeKey.map(key => Option(jedisCluster.get(key))),
            keys.statusKey.map(key => Option(jedisCluster.get(key)))
          )
        }
        BatchReadResult(storedBytes, rawControl)
      case StreamingReadPlan(_, redisKeys, startTs, endTs) =>
        StreamingReadResult(redisKeys.flatMap { redisKey =>
          decodeStreamingTuples(jedisCluster.zrangeByScoreWithScores(redisKey, startTs.toDouble, endTs.toDouble))
        })
    }
  }

  private def decodeManagedBatchResult(result: Try[RedisReadResult]): Try[Option[RedisBatchUpload.DecodedValue]] =
    result.flatMap {
      case BatchReadResult(Some(storedBytes), _) => RedisBatchUpload.decodeValue(storedBytes).map(Some(_))
      case BatchReadResult(None, _)              => Success(None)
      case _: StreamingReadResult =>
        Failure(new IllegalStateException("Managed Redis batch request produced a streaming pipeline result"))
    }

  private def renderOrdinaryRead(plan: PlannedRead, result: Try[RedisReadResult]): GetResponse = plan match {
    case ImmediateReadPlan(request, values) => GetResponse(request, values)
    case BatchReadPlan(request, _, _) =>
      GetResponse(
        request,
        result.flatMap {
          case BatchReadResult(storedBytes, _) => decodeOrdinaryBatchResult(storedBytes)
          case _: StreamingReadResult =>
            Failure(new IllegalStateException("Redis batch request produced a streaming pipeline result"))
        }
      )
    case StreamingReadPlan(request, _, _, _) =>
      GetResponse(
        request,
        result.flatMap {
          case StreamingReadResult(values) => Success(values)
          case _: BatchReadResult =>
            Failure(new IllegalStateException("Redis streaming request produced a batch pipeline result"))
        }
      )
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
        jedisCluster.zrangeByScoreWithScores(redisKey, startTs.toDouble, endTs.toDouble)
      }
  }

  private def recordPipelineRetry(dataset: String, error: Throwable): Unit = {
    val datasetMetricsContext = tableToContext.getOrElseUpdate(dataset, metricsContext.copy(dataset = dataset))
    datasetMetricsContext.increment("multiGet.pipeline_retries", Map("exception" -> error.getClass.getName))
  }

  private def decodeOrdinaryBatchResult(storedBytes: Option[Array[Byte]]): Try[Seq[TimedValue]] =
    storedBytes match {
      case None => Success(Seq.empty)
      case Some(bytes) =>
        RedisBatchUpload.decodeValue(bytes).map { decoded =>
          decoded.operation match {
            case RedisBatchUpload.Upsert => Seq(TimedValue(decoded.payload, decoded.storedTimestamp))
            case RedisBatchUpload.Delete => Seq.empty
          }
        }
    }

  private def decodeStreamingValues(commands: Seq[StreamingCommand]): Seq[TimedValue] = {
    commands.flatMap { command =>
      decodeStreamingTuples(command.response.get())
    }
  }

  private def decodeStreamingTuples(tuples: java.util.List[Tuple]): Seq[TimedValue] = {
    tuples.asScala.flatMap { tuple =>
      val memberBytes = tuple.getBinaryElement
      if (memberBytes.length >= 8) {
        Some(TimedValue(memberBytes.drop(8), tuple.getScore.toLong))
      } else {
        logger.warn(s"Malformed streaming data in Redis: member has ${memberBytes.length} bytes, expected >= 8")
        None
      }
    }.toSeq
  }

  private def recordMultiGetMetrics(responses: Seq[GetResponse], latencyMillis: Long): Unit = {
    metricsContext.distribution("multiGet.latency", latencyMillis)
    responses.groupBy(_.request.dataset).foreach { case (dataset, datasetResponses) =>
      val datasetMetricsContext = tableToContext.getOrElseUpdate(dataset, metricsContext.copy(dataset = dataset))
      val failure = datasetResponses.iterator.flatMap(_.values.failed.toOption).toSeq.headOption
      failure match {
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
    val batchDatasets = requests.iterator.map(_.dataset).filter(_.endsWith("_BATCH")).toSet
    if (batchDatasets.isEmpty) multiPutDirect(requests, rejectBatchWrites = false)
    else
      resolvedBatchMode(conf) match {
        case RedisBatchMode.FullSnapshot =>
          batchDatasets.foreach { dataset =>
            RedisBatchUpload.claimPublicationMode(
              jedisCluster,
              dataset,
              keyPrefix,
              RedisBatchModeSelection.FullSnapshot
            )
          }
          multiPutFullSnapshot(requests)
        case RedisBatchMode.Incremental(_) => multiPutIncremental(requests)
      }
  }

  private def multiPutFullSnapshot(requests: Seq[PutRequest]): Future[Seq[Boolean]] =
    multiPutDirect(requests, rejectBatchWrites = false)

  private def multiPutIncremental(requests: Seq[PutRequest]): Future[Seq[Boolean]] =
    multiPutDirect(requests, rejectBatchWrites = true)

  private def multiPutDirect(requests: Seq[PutRequest], rejectBatchWrites: Boolean): Future[Seq[Boolean]] = {
    logger.debug(s"Performing multi-put for ${requests.size} requests")

    val resultFutures = requests.map { request =>
      val datasetMetricsContext = tableToContext.getOrElseUpdate(
        request.dataset,
        metricsContext.copy(dataset = request.dataset)
      )
      if (rejectBatchWrites && request.dataset.endsWith("_BATCH"))
        Future.successful(rejectIncrementalBatchWrite(request, datasetMetricsContext))
      else Future(writeDirect(request, datasetMetricsContext))
    }

    Future.sequence(resultFutures)
  }

  private def rejectIncrementalBatchWrite(request: PutRequest, datasetMetricsContext: Metrics.Context): Boolean = {
    logger.error(
      s"Rejecting direct write to incremental Redis batch dataset ${request.dataset}; use bulkPut publication")
    datasetMetricsContext.increment("multiPut.rejected_batch_writes")
    datasetMetricsContext.increment("multiPut.failures", Map("exception" -> classOf[IllegalStateException].getName))
    false
  }

  private def writeDirect(request: PutRequest, datasetMetricsContext: Metrics.Context): Boolean = {
    val tableType = getTableType(request.dataset)
    val timestampInPutRequest = request.tsMillis.getOrElse(System.currentTimeMillis())

    try {
      val startTs = System.currentTimeMillis()
      val (redisKey, timestamp) = (request.tsMillis, tableType) match {
        case (Some(_), StreamingTable) =>
          val tileKey = TilingUtils.deserializeTileKey(request.keyBytes)
          val baseKeyBytes = tileKey.keyBytes.asScala.map(_.toByte).toSeq
          (buildTiledRedisKey(baseKeyBytes,
                              request.dataset,
                              tileKey.tileStartTimestampMillis,
                              tileKey.tileSizeMillis,
                              keyPrefix),
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
          val keyBytes = redisKey.getBytes(StandardCharsets.UTF_8)
          if (request.dataset == MetadataDataset) {
            // Uploaded Join configs remain addressable until explicitly replaced or deleted.
            // Plain SET also clears a TTL left by older RedisKVStore versions.
            jedisCluster.set(keyBytes, storedBytes)
          } else {
            jedisCluster.setex(keyBytes, DataTTLSeconds, storedBytes)
          }
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

  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {
    val startTs = System.currentTimeMillis()

    logger.info(
      s"Triggering Redis bulk load for dataset: $destinationOnlineDataSet, " +
        s"table: $sourceOfflineTable, partition: $partition"
    )

    try {
      val uploadConf = RedisKVStore.uploadConfig(conf, currentSparkConfig)
      val result = resolvedBatchMode(uploadConf) match {
        case RedisBatchMode.FullSnapshot =>
          bulkPutFullSnapshot(sourceOfflineTable, destinationOnlineDataSet, partition, uploadConf)
        case RedisBatchMode.Incremental(writer) =>
          bulkPutIncremental(sourceOfflineTable, destinationOnlineDataSet, partition, writer, uploadConf)
      }

      logger.info(s"Redis bulk load completed successfully: $result")
      metricsContext.distribution("bulkPut.latency", System.currentTimeMillis() - startTs)
      metricsContext.increment("bulkPut.successes")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to run Spark-based bulk load for $sourceOfflineTable", e)
        metricsContext.increment("bulkPut.failures", Map("exception" -> e.getClass.getName))
        throw e
    }
  }

  protected[redis] def bulkPutFullSnapshot(sourceOfflineTable: String,
                                           destinationOnlineDataSet: String,
                                           partition: String,
                                           uploadConf: Map[String, String]): String =
    withUploadSpark(destinationOnlineDataSet) { spark =>
      val settings = Spark2RedisLoader.Settings(
        job = BatchUploadJob.from(sourceOfflineTable, destinationOnlineDataSet, partition, uploadConf),
        target = bulkUploadTarget
      )
      val written = Spark2RedisLoader.run(settings, spark, jedisCluster)
      s"full-snapshot upload wrote $written records"
    }

  protected[redis] def bulkPutIncremental(sourceOfflineTable: String,
                                          destinationOnlineDataSet: String,
                                          partition: String,
                                          writer: ConditionalObjectWriter,
                                          uploadConf: Map[String, String]): String =
    withUploadSpark(destinationOnlineDataSet) { spark =>
      val settings = IncrementalRedisBatchSettings(
        job = BatchUploadJob.from(sourceOfflineTable, destinationOnlineDataSet, partition, uploadConf),
        target = bulkUploadTarget,
        options = IncrementalOptions.from(uploadConf)
      )
      IncrementalRedisBatchLoader.run(settings, spark, jedisCluster, writer).toString
    }

  private def withUploadSpark[T](destinationOnlineDataSet: String)(run: SparkSession => T): T = {
    val existingSpark = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    val spark = existingSpark.getOrElse {
      val submissionConf = new SparkConf().getAll.toMap
      val sparkConf = RedisKVStore.sparkSessionConfig(conf, submissionConf)
      SparkSessionBuilder.build(
        s"Spark2RedisLoader-$destinationOnlineDataSet",
        additionalConfig = if (sparkConf.nonEmpty) Some(sparkConf) else None
      )
    }
    try run(spark)
    finally if (existingSpark.isEmpty) spark.stop()
  }

  private def currentSparkConfig: Map[String, String] =
    SparkSession.getActiveSession
      .orElse(SparkSession.getDefaultSession)
      .map(_.conf.getAll)
      .getOrElse(new SparkConf().getAll.toMap)

  private lazy val bulkUploadTarget: RedisKVStoreFactory.StoreSettings =
    factorySettings.getOrElse(RedisKVStoreFactory.StoreSettings(bulkUploadClientSettings, keyPrefix))

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

  final case class BatchReadPlan(
      request: GetRequest,
      redisKey: Array[Byte],
      controlKeys: Option[BatchControlKeys] = None
  ) extends RedisReadPlan

  final case class BatchControlKeys(
      publicationModeKey: Option[Array[Byte]],
      statusKey: Option[Array[Byte]]
  )

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
      response: Response[java.util.List[Tuple]]
  )

  sealed trait RedisReadResult

  final case class RawBatchControl(
      publicationModeBytes: Option[Option[Array[Byte]]],
      statusBytes: Option[Option[Array[Byte]]]
  )

  final case class BatchReadControl(
      publicationMode: Option[RedisBatchModeSelection],
      status: Option[RedisBatchUpload.BatchStatus]
  )

  final case class BatchReadResult(
      storedBytes: Option[Array[Byte]],
      control: Option[RawBatchControl]
  ) extends RedisReadResult

  final case class StreamingReadResult(values: Seq[TimedValue]) extends RedisReadResult

  final case class PendingRead(request: GetRequest, resolve: () => Try[RedisReadResult])

  final case class PipelineAttempt(
      results: Try[Seq[Try[RedisReadResult]]],
      failedPlan: Option[RedisReadPlan]
  )
}

object RedisKVStore {
  private[redis] val BatchStatusCacheMillis = 5000L

  private[redis] case class CachedBatchStatus(status: Option[RedisBatchUpload.BatchStatus], expiresAtMillis: Long)

  /** API properties are already ordered common < Spark submit < CLI by the planner node runner. For legacy Driver
    * uploads, add the current Spark configuration beneath them so both paths expose one effective configuration map.
    */
  private[redis] def uploadConfig(apiConfig: Map[String, String],
                                  sparkConfig: Map[String, String]): Map[String, String] =
    sparkConfig ++ apiConfig

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
