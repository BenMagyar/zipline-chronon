package ai.chronon.integrations.redis

import ai.chronon.api.Constants.GroupByServingInfoKey
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{GroupBy, MetaData, PartitionSpec}
import ai.chronon.integrations.redis.RedisBatchUpload.{BatchStatus, Delete, Operation, Upsert}
import ai.chronon.integrations.redis.RedisKVStoreConstants._
import ai.chronon.spark.catalog.TableUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import org.apache.spark.scheduler.{
  AccumulableInfo,
  SparkListener,
  SparkListenerExecutorMetricsUpdate,
  SparkListenerTaskEnd,
  SparkListenerTaskStart
}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator
import org.slf4j.LoggerFactory
import redis.clients.jedis.{ClusterPipeline, JedisCluster, Protocol, Response}
import redis.clients.jedis.exceptions.{
  JedisClusterOperationException,
  JedisConnectionException,
  JedisDataException,
  JedisMovedDataException,
  JedisNoScriptException,
  JedisRedirectionException
}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.{Arrays, Locale, UUID}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ThreadFactory, ThreadLocalRandom, TimeUnit}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

/** Incremental Spark loader for Chronon batch snapshots.
  *
  * S3 is the durable state machine. Each immutable generation contains a complete active key/value/digest snapshot, a
  * concrete value delta, TTL-bounded tombstones, and binary `_READY` and `_APPLIED` records. A small ETag-guarded S3
  * `_HEAD` admits exactly one generation at a time. If its generation is not yet applied, a later invocation resumes it
  * before advancing. Redis receives direct, timestamp-fenced mutations, so reads do not depend on a volatile Redis
  * generation pointer.
  *
  * Live values use a renewable lease. Changed values are written with an absolute deadline; unchanged active keys use
  * that expiration as a rollback fence, allowing the loader to verify and advance metadata without reading tiered
  * payloads. The lease must exceed the maximum expected gap between successful uploads plus recovery margin. Direct
  * publication is intentionally not an atomic whole-dataset cutover.
  */
object Spark2RedisLoader {
  private val logger = LoggerFactory.getLogger(getClass)
  private val UnsetPipelineResponseMessage = "Please close pipeline or multi block before calling this method."
  private val UploadProgressLogIntervalSeconds = 30L

  private[redis] val PartitionColumnProp = "spark.chronon.partition.column"
  private[redis] val PartitionFormatProp = "spark.chronon.partition.format"
  private[redis] val PartitionSpanMillisProp = "spark.chronon.partition.span.millis"
  private[redis] val PartitionOffsetMillisProp = "spark.chronon.partition.offset.millis"

  private[redis] val KeyColumn = "key_bytes"
  private[redis] val ValueColumn = "value_bytes"
  private[redis] val DigestColumn = "value_digest"
  private[redis] val ValueTimestampColumn = "value_timestamp"
  private[redis] val ValueWriteEpochColumn = "value_write_epoch"
  private[redis] val BucketColumn = "redis_bucket"
  private[redis] val OperationColumn = "redis_operation"
  private[redis] val BatchTimestampColumn = "batch_timestamp"
  private[redis] val WriteEpochColumn = "redis_write_epoch"
  private[redis] val TombstoneExpiresAtColumn = "tombstone_expires_at"
  private val AllowExpiredMutationColumn = "redis_allow_expired_mutation"
  private val PendingTombstoneExpiry = Long.MaxValue

  private val ReadyFile = "_READY"
  private val AppliedFile = "_APPLIED"
  private val LeaseFile = "_LEASE"
  private val HeadFile = "_HEAD"
  private val CandidateFile = "_CANDIDATE"
  private val DeltaDirectory = "delta"
  private val StateDirectory = "state"
  private val TombstoneDirectory = "tombstones"
  private val GenerationsDirectory = "generations"
  private val ClaimsDirectory = "claims"
  private val ReadyMagic = 0x43524252 // CRBR
  private val LegacyReadyVersion = 4
  private val HashTagReadyVersion = 5
  private val ReadyVersion = 6
  private val CandidateMagic = 0x43524243 // CRBC
  private val CandidateVersion = 2
  private val HeadMagic = 0x43524248 // CRBH
  private val HeadVersion = 1
  private val AppliedMagic = 0x43524241 // CRBA
  private val AppliedVersion = 1
  private val LeaseMagic = 0x43524245 // CRBE
  private val LeaseVersion = 1
  private val ClaimMagic = 0x4352424c // CRBL
  private val ClaimVersion = 1
  private val MaxAdmissionAttempts = 64
  private val GenerationCleanupGraceMillis = 6L * 60L * 60L * 1000L
  private val ServingInfoKeyBytes = GroupByServingInfoKey.getBytes(StandardCharsets.UTF_8)

  case class Settings(sourceTable: String,
                      destinationDataset: String,
                      sourcePartition: String,
                      partitionSpec: PartitionSpec,
                      stateRoot: String,
                      keyPrefix: String,
                      clientSettings: RedisKVStoreFactory.ClientSettings,
                      keyHashTagMode: String = RedisKVStore.DatasetAndEntityHashTagMode,
                      batchSize: Int = DefaultBulkUploadBatchSize,
                      maxKeysPerSecond: Int = DefaultBulkUploadMaxKeysPerSecond,
                      writerPartitions: Int = DefaultBulkUploadWriterPartitions,
                      stateBuckets: Int = DefaultBulkUploadStateBuckets,
                      maxRetries: Int = DefaultBulkUploadMaxRetries,
                      retryBackoffMs: Long = DefaultBulkUploadRetryBackoffMs,
                      liveTTLSeconds: Int = DefaultBulkUploadTTLSeconds,
                      requireNoEviction: Boolean = DefaultBulkUploadRequireNoEviction) {
    require(sourceTable != null && sourceTable.trim.nonEmpty, "Redis source table must be non-empty")
    require(destinationDataset != null && destinationDataset.trim.nonEmpty,
            "Redis destination dataset must be non-empty")
    require(sourcePartition != null && sourcePartition.nonEmpty, "Redis source partition must be non-empty")
    partitionSpec.epochMillis(sourcePartition)
    require(stateRoot != null && stateRoot.trim.nonEmpty, "Redis batch upload state root must be non-empty")
    require(
      keyHashTagMode == RedisKVStore.DatasetAndEntityHashTagMode ||
        keyHashTagMode == RedisKVStore.EntityHashTagMode,
      s"Unsupported Redis key hash tag mode: $keyHashTagMode"
    )
    require(batchSize > 0, s"Redis batch size must be positive: $batchSize")
    require(maxKeysPerSecond > 0, s"Redis max keys per second must be positive: $maxKeysPerSecond")
    require(writerPartitions > 0, s"Redis writer partitions must be positive: $writerPartitions")
    require(stateBuckets > 0, s"Redis state buckets must be positive: $stateBuckets")
    require(maxRetries >= 0, s"Redis max retries must be non-negative: $maxRetries")
    require(retryBackoffMs >= 0, s"Redis retry backoff must be non-negative: $retryBackoffMs")
    require(liveTTLSeconds > 0, s"Redis live-key TTL must be positive: $liveTTLSeconds")

    lazy val batchDataset: String =
      new GroupBy().setMetaData(new MetaData().setName(destinationDataset)).batchDataset
    lazy val batchTimestamp: Long = partitionSpec.partitionEndMillis(sourcePartition)
  }

  case class DiffFrames(currentState: DataFrame, dataDelta: DataFrame)

  case class WriteSummary(attempted: Long, written: Long, replayed: Long, rejected: Long, retries: Long) {
    def +(other: WriteSummary): WriteSummary =
      WriteSummary(attempted + other.attempted,
                   written + other.written,
                   replayed + other.replayed,
                   rejected + other.rejected,
                   retries + other.retries)
  }

  private case class WriteProgress(generation: String, phase: String, totalKeys: Option[Long]) {
    require(generation.nonEmpty, "Redis upload progress generation must be non-empty")
    require(phase.nonEmpty, "Redis upload progress phase must be non-empty")
    require(totalKeys.forall(_ >= 0), s"Redis upload progress total must be non-negative: $totalKeys")
  }

  private[redis] final class UploadProgressTracker {
    private val taskPartitions = mutable.Map.empty[Long, Int]
    private val partitionHighWater = mutable.Map.empty[Int, Long]

    def taskStarted(taskId: Long, partition: Int): Unit = synchronized {
      require(taskId >= 0, s"Spark task ID must be non-negative: $taskId")
      require(partition >= 0, s"Spark partition must be non-negative: $partition")
      taskPartitions.update(taskId, partition)
    }

    def taskProgress(taskId: Long, completed: Long): Unit = synchronized {
      require(completed >= 0, s"Redis task progress must be non-negative: $completed")
      taskPartitions.get(taskId).foreach { partition =>
        partitionHighWater.update(partition, math.max(partitionHighWater.getOrElse(partition, 0L), completed))
      }
    }

    def completed: Long = synchronized {
      partitionHighWater.valuesIterator.foldLeft(0L)(_ + _)
    }
  }

  private final class UploadProgressReporter(dataset: String,
                                             operation: String,
                                             progress: WriteProgress,
                                             accumulatorId: Long,
                                             writerPartitions: Int,
                                             batchSize: Int)
      extends SparkListener
      with AutoCloseable {
    private val tracker = new UploadProgressTracker
    private val startedAtMillis = System.currentTimeMillis()
    private val closed = new AtomicBoolean(false)
    private val scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, "redis-upload-progress")
        thread.setDaemon(true)
        thread
      }
    })

    def start(): Unit = {
      logger.info(s"Redis upload started dataset=$dataset generation=${progress.generation} phase=${progress.phase} " +
        s"operation=$operation ${totalText(progress.totalKeys)} writerPartitions=$writerPartitions batchSize=$batchSize")
      scheduler.scheduleAtFixedRate(
        new Runnable {
          override def run(): Unit =
            try logProgress()
            catch {
              case NonFatal(error) => logger.warn("Could not report Redis upload progress", error)
            }
        },
        UploadProgressLogIntervalSeconds,
        UploadProgressLogIntervalSeconds,
        TimeUnit.SECONDS
      )
    }

    override def onTaskStart(event: SparkListenerTaskStart): Unit =
      tracker.taskStarted(event.taskInfo.taskId, event.taskInfo.index)

    override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit =
      event.accumUpdates.foreach { case (taskId, _, _, updates) =>
        accumulatorValue(updates).foreach(value => tracker.taskProgress(taskId, value))
      }

    override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
      accumulatorValue(event.taskInfo.accumulables).foreach(value => tracker.taskProgress(event.taskInfo.taskId, value))

    def complete(completed: Long, retries: Long): Unit = {
      if (closed.compareAndSet(false, true)) {
        scheduler.shutdownNow()
        val elapsedMillis = math.max(1L, System.currentTimeMillis() - startedAtMillis)
        logger.info(
          s"Redis upload completed dataset=$dataset generation=${progress.generation} phase=${progress.phase} " +
            s"operation=$operation completed=$completed ${completedTotalText(progress.totalKeys)}" +
            s"elapsedMs=$elapsedMillis rate=${keysPerSecond(completed, elapsedMillis)} keys/s retries=$retries")
      }
    }

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) scheduler.shutdownNow()
    }

    private def logProgress(): Unit = {
      if (!closed.get()) {
        val elapsedMillis = math.max(1L, System.currentTimeMillis() - startedAtMillis)
        val completed = progress.totalKeys.fold(tracker.completed)(total => math.min(total, tracker.completed))
        logger.info(
          s"Redis upload progress dataset=$dataset generation=${progress.generation} phase=${progress.phase} " +
            s"operation=$operation completed=$completed ${progressTotalText(completed, progress.totalKeys)}" +
            s"elapsedMs=$elapsedMillis rate=${keysPerSecond(completed, elapsedMillis)} keys/s")
      }
    }

    private def accumulatorValue(updates: scala.collection.Seq[AccumulableInfo]): Option[Long] =
      updates.iterator
        .find(_.id == accumulatorId)
        .flatMap(info => info.update.orElse(info.value))
        .collect { case number: java.lang.Number => number.longValue() }

    private def totalText(totalKeys: Option[Long]): String =
      totalKeys.fold("total=unknown")(total => s"total=$total")

    private def progressTotalText(completed: Long, totalKeys: Option[Long]): String =
      totalKeys.fold("") { total =>
        val percent = if (total == 0) 99 else math.min(99, (completed.toDouble * 100.0 / total).toInt)
        s"total=$total percent=$percent "
      }

    private def completedTotalText(totalKeys: Option[Long]): String =
      totalKeys.fold("")(total => s"total=$total percent=100 ")

    private def keysPerSecond(completed: Long, elapsedMillis: Long): Long =
      math.round(completed.toDouble * 1000.0 / elapsedMillis)
  }

  private[redis] case class OomRetryEvent(attempt: Int, waitMillis: Long, elapsedMillis: Long, remainingMillis: Long)

  case class UploadSummary(sourceRows: Long,
                           changedKeys: Long,
                           deletedKeys: Long,
                           refreshedKeys: Long,
                           generation: Option[String],
                           superseded: Boolean,
                           fullRebuild: Boolean)

  private[redis] case class ReadyManifest(status: BatchStatus,
                                          parentGeneration: Option[String],
                                          stateGeneration: String,
                                          sourceTable: String,
                                          batchDataset: String,
                                          keyPrefix: String,
                                          keyHashTagMode: String,
                                          stateBuckets: Int,
                                          sourceRows: Long,
                                          dataRows: Long,
                                          changedKeys: Long,
                                          deletedKeys: Long,
                                          deltaRows: Long,
                                          tombstoneRows: Long,
                                          liveTTLSeconds: Int,
                                          deleteFenceTTLSeconds: Int,
                                          fullRebuild: Boolean,
                                          retired: Boolean)

  private case class HeadRecord(revision: Long,
                                generation: String,
                                parentGeneration: Option[String],
                                readyDigest: Array[Byte])
  private case class VersionedHead(record: HeadRecord, version: String, lastModifiedMillis: Long)
  private case class CandidateRecord(generation: String,
                                     parentGeneration: Option[String],
                                     createdAtMillis: Long,
                                     cleanupAfterMillis: Long)
  private case class HeadAdvanced(generation: String) extends RuntimeException
  private case class CandidateUnavailable(generation: String) extends RuntimeException
  private[redis] case class CandidateClaim(generation: String, abandoned: Boolean)
  private case class AppliedRecord(status: BatchStatus,
                                   readyDigest: Array[Byte],
                                   mutationKeys: Long,
                                   refreshedKeys: Long,
                                   recovered: Boolean)
  private case class CommittedApplied(record: AppliedRecord, lastModifiedMillis: Long) {
    def tombstoneRetainUntilMillis(liveTTLSeconds: Int): Long =
      java.lang.Math.addExact(lastModifiedMillis, java.lang.Math.multiplyExact(liveTTLSeconds.toLong, 1000L))
  }
  private[redis] case class LeaseRecord(status: BatchStatus,
                                        readyDigest: Array[Byte],
                                        minimumActiveDeadlineMillis: Long)
  private case class VersionedLease(record: LeaseRecord, version: String)
  private case class ApplyResult(mutationKeys: Long,
                                 refreshedKeys: Long,
                                 recovered: Boolean,
                                 minimumActiveDeadlineMillis: Long)
  private case class GenerationPaths(root: Path,
                                     candidate: Path,
                                     delta: Path,
                                     state: Path,
                                     tombstones: Path,
                                     ready: Path,
                                     applied: Path,
                                     lease: Path)

  private final class TaskClient(settings: RedisKVStoreFactory.ClientSettings) {
    private var current = RedisKVStoreFactory.createClient(settings)

    def client: JedisCluster = current

    def refresh(): Unit = {
      val replacement = RedisKVStoreFactory.createClient(settings)
      val previous = current
      current = replacement
      Try(previous.close())
    }

    def close(): Unit = Try(current.close())
  }

  private sealed trait WriteMode extends Product with Serializable
  private case object MutationWrite extends WriteMode
  private case class LeaseRefresh(expectedMinimumDeadlineMillis: Option[Long]) extends WriteMode

  def run(settings: Settings,
          spark: SparkSession,
          controlClient: JedisCluster,
          conditionalWriter: ConditionalObjectWriter = HadoopConditionalObjectWriter): UploadSummary = {
    require(
      !spark.sparkContext.getConf.getBoolean("spark.speculation", false),
      "Redis incremental upload requires spark.speculation=false so duplicate task attempts cannot bypass mutation accounting"
    )
    validateConditionalWriter(settings, conditionalWriter)
    if (settings.requireNoEviction) validateNoEviction(controlClient)
    else
      logger.warn(
        "Skipping Redis maxmemory-policy=noeviction validation; eviction can silently lose published feature values")
    val clientNoTouch = RedisKVStoreFactory.supportsClientNoTouch(controlClient)
    val effectiveSettings = settings.copy(clientSettings = settings.clientSettings.copy(clientNoTouch = clientNoTouch))
    if (clientNoTouch)
      logger.info("Redis bulk-writer connections will require CLIENT NO-TOUCH")
    else
      logger.warn(
        "Redis does not support or permit CLIENT NO-TOUCH; retaining header-based lease validation for compatibility")

    var attempts = 0
    while (attempts < MaxAdmissionAttempts) {
      try {
        val summary = runAttempt(effectiveSettings, spark, controlClient, conditionalWriter)
        cleanupObsoleteGenerations(effectiveSettings, spark, conditionalWriter)
        return summary
      } catch {
        case HeadAdvanced(generation) =>
          attempts += 1
          logger.info(
            s"Redis S3 head advanced to $generation during admission; recomputing (attempt $attempts/$MaxAdmissionAttempts)")
          Thread.sleep(ThreadLocalRandom.current().nextLong(25L, 251L))
        case CandidateUnavailable(generation) =>
          attempts += 1
          logger.info(
            s"Redis S3 candidate $generation expired before admission; rebuilding " +
              s"(attempt $attempts/$MaxAdmissionAttempts)")
      }
    }
    throw new IllegalStateException(
      s"Redis upload could not admit a generation after $MaxAdmissionAttempts concurrent S3 head changes")
  }

  private def runAttempt(settings: Settings,
                         spark: SparkSession,
                         controlClient: JedisCluster,
                         conditionalWriter: ConditionalObjectWriter): UploadSummary = {
    val tableUtils = TableUtils(spark, settings.partitionSpec)
    val head = resolveHead(settings, spark, controlClient, conditionalWriter)
    val statusKey = RedisBatchUpload.buildStatusKey(settings.batchDataset, settings.keyPrefix)
    var redisStatus = RedisBatchUpload.readStatus(controlClient, statusKey)

    if (head.isEmpty) {
      require(
        redisStatus.isEmpty,
        s"Redis contains batch status ${redisStatus.get.generation} but S3 has no lineage for ${settings.batchDataset}; " +
          "use a new state root or a clean key prefix"
      )
    }
    redisStatus.foreach { status =>
      head.foreach { committed =>
        if (compareStatus(status, committed.status) > 0) {
          if (headAdvancedPast(settings, committed, spark, conditionalWriter)) throw HeadAdvanced(status.generation)
          throw new IllegalStateException(
            s"Redis status ${status.generation} is newer than S3 head ${committed.status.generation}; refusing to advance")
        }
      }
    }

    head.foreach { committed =>
      require(
        !committed.retired,
        s"Redis dataset ${settings.batchDataset} was explicitly retired in generation ${committed.status.generation}")
      if (settings.batchTimestamp < committed.status.batchTimestamp) {
        return repairOlderSnapshot(settings,
                                   committed,
                                   spark,
                                   controlClient,
                                   conditionalWriter,
                                   !redisStatus.contains(committed.status))
      }
      if (settings.liveTTLSeconds < committed.liveTTLSeconds) {
        logger.info(
          s"Redis live-value TTL is decreasing from ${committed.liveTTLSeconds} to ${settings.liveTTLSeconds} seconds; " +
            s"retaining delete fences for ${committed.deleteFenceTTLSeconds} seconds")
      }

      // APPLIED makes the value-bearing S3 generation authoritative even if Redis loses its status marker or is
      // restored to an older snapshot. Repair that committed generation before consulting the mutable source table.
      // For the same source partition, the recovery is the complete operation; a later invocation can deliberately
      // compare a corrected source partition after Redis is back at the committed baseline. A newer partition proceeds
      // below only after the committed parent has been restored.
      if (!redisStatus.contains(committed.status)) {
        val recoverySettings = settings.copy(sourcePartition = committed.status.sourcePartition)
        val recovered = refreshCommittedSnapshot(recoverySettings,
                                                 committed,
                                                 spark,
                                                 controlClient,
                                                 conditionalWriter,
                                                 forceReplay = true)
        if (recovered.superseded) throw HeadAdvanced(readHead(settings, spark, conditionalWriter).get.record.generation)

        redisStatus = RedisBatchUpload.readStatus(controlClient, statusKey)
        require(redisStatus.contains(committed.status),
                s"Redis status for committed generation ${committed.status.generation} was not restored from S3 state")

        if (
          settings.batchTimestamp == committed.status.batchTimestamp &&
          settings.sourcePartition == committed.status.sourcePartition
        ) {
          logger.info(
            s"Recovered committed Redis generation ${committed.status.generation} from S3 state without reading " +
              s"source table ${settings.sourceTable}")
          return recovered
        }
      }
    }

    val needsFullRebuild = head.exists(committed => !redisStatus.contains(committed.status))
    val stateBuckets = head.map(_.stateBuckets).getOrElse(settings.stateBuckets)
    val source = loadSourceRows(tableUtils, settings).persist(StorageLevel.DISK_ONLY)
    var currentState: DataFrame = null
    var dataDelta: DataFrame = null
    var completeDelta: DataFrame = null
    var tombstoneState: DataFrame = null
    try {
      val counts = source
        .agg(
          count(lit(1)).as("source_rows"),
          count(when(isServingInfo(col(KeyColumn)), lit(1))).as("metadata_rows"),
          count(when(col(KeyColumn).isNull || col(ValueColumn).isNull, lit(1))).as("invalid_rows")
        )
        .head()
      val sourceRows = counts.getAs[Long]("source_rows")
      if (sourceRows == 0) {
        throw new IllegalStateException(
          s"Redis upload source ${settings.sourceTable} has no rows for ${settings.partitionSpec.column}=${settings.sourcePartition}")
      }

      val servingInfoPredicate = isServingInfo(col(KeyColumn))
      val metadataRows = source.where(servingInfoPredicate).persist(StorageLevel.MEMORY_AND_DISK)
      val dataRows = source.where(not(servingInfoPredicate))
      try {
        val metadataCount = counts.getAs[Long]("metadata_rows")
        require(
          metadataCount == 1,
          s"Redis upload requires exactly one GroupByServingInfo row, found $metadataCount in ${settings.sourceTable}")
        val invalidCount = counts.getAs[Long]("invalid_rows")
        require(invalidCount == 0, s"Redis upload source contains $invalidCount null key_bytes or value_bytes rows")
        val dataCount = sourceRows - metadataCount
        require(dataCount > 0, s"Redis upload contains no data keys in ${settings.sourceTable}")
        validateSourceRows(dataRows)

        val previousState =
          head.map(manifest =>
            loadStateIndex(settings, manifest.stateGeneration, spark).persist(StorageLevel.DISK_ONLY))
        try {
          val previousCount = previousState.map(_.count()).getOrElse(0L)
          head.foreach { manifest =>
            require(
              previousCount == manifest.dataRows,
              s"Redis state ${manifest.stateGeneration} contains $previousCount rows; its head records ${manifest.dataRows}"
            )
          }
          val writeEpoch = head.map(_.status.writeEpoch).getOrElse(0L) + 1L
          require(writeEpoch > 0, "Redis batch write epoch overflowed")
          val frames = buildDiff(dataRows,
                                 previousState,
                                 settings.batchTimestamp,
                                 stateBuckets,
                                 writeEpoch,
                                 forceAllUpserts = needsFullRebuild || head.isEmpty)
          currentState = frames.currentState.persist(StorageLevel.DISK_ONLY)
          dataDelta = frames.dataDelta
            .withColumn(
              TombstoneExpiresAtColumn,
              when(col(OperationColumn) === lit(RedisBatchUpload.DeleteOperation), lit(PendingTombstoneExpiry))
                .otherwise(lit(null).cast(LongType))
            )
            .persist(StorageLevel.DISK_ONLY)

          val operationCounts = dataDelta
            .groupBy(col(OperationColumn))
            .count()
            .collect()
            .map(row => row.getString(0) -> row.getLong(1))
            .toMap
          val changedKeys = operationCounts.getOrElse(RedisBatchUpload.UpsertOperation, 0L)
          val deletedKeys = operationCounts.getOrElse(RedisBatchUpload.DeleteOperation, 0L)
          val unchangedRerun = head.exists { committed =>
            settings.batchTimestamp == committed.status.batchTimestamp &&
            settings.sourcePartition == committed.status.sourcePartition &&
            changedKeys == 0L &&
            deletedKeys == 0L &&
            metadataMatches(metadataRows, settings, committed, spark) &&
            settings.liveTTLSeconds == committed.liveTTLSeconds &&
            !needsFullRebuild
          }
          if (unchangedRerun) {
            return refreshCommittedSnapshot(settings,
                                            head.get,
                                            spark,
                                            controlClient,
                                            conditionalWriter,
                                            forceReplay = false)
          }
          val generation = UUID.randomUUID().toString
          val stateGeneration = generation
          val status =
            BatchStatus(generation, settings.sourcePartition, settings.batchTimestamp, writeEpoch, retired = false)
          tombstoneState = buildTombstoneState(
            currentState,
            head.filter(_.tombstoneRows > 0).map(loadCommittedTombstones(settings, _, spark, conditionalWriter)),
            dataDelta.where(col(OperationColumn) === lit(RedisBatchUpload.DeleteOperation))
          ).persist(StorageLevel.DISK_ONLY)
          val tombstoneRows = tombstoneState.count()
          val metadataDelta = metadataRows
            .select(col(KeyColumn), col(ValueColumn))
            .withColumn(OperationColumn, lit(RedisBatchUpload.UpsertOperation))
            .withColumn(BatchTimestampColumn, lit(settings.batchTimestamp))
            .withColumn(WriteEpochColumn, lit(writeEpoch))
            .withColumn(BucketColumn, bucketColumn(stateBuckets))
            .withColumn(TombstoneExpiresAtColumn, lit(null).cast(LongType))
          completeDelta = dataDelta.unionByName(metadataDelta).persist(StorageLevel.DISK_ONLY)
          val deltaRows = completeDelta.count()
          require(deltaRows == changedKeys + deletedKeys + 1,
                  s"Redis delta count $deltaRows did not match changed=$changedKeys deleted=$deletedKeys metadata=1")
          source.unpersist()

          val manifest = ReadyManifest(
            status = status,
            parentGeneration = head.map(_.status.generation),
            stateGeneration = stateGeneration,
            sourceTable = settings.sourceTable,
            batchDataset = settings.batchDataset,
            keyPrefix = settings.keyPrefix,
            keyHashTagMode = settings.keyHashTagMode,
            stateBuckets = stateBuckets,
            sourceRows = sourceRows,
            dataRows = dataCount,
            changedKeys = changedKeys,
            deletedKeys = deletedKeys,
            deltaRows = deltaRows,
            tombstoneRows = tombstoneRows,
            liveTTLSeconds = settings.liveTTLSeconds,
            deleteFenceTTLSeconds =
              math.max(settings.liveTTLSeconds, head.map(_.deleteFenceTTLSeconds).getOrElse(settings.liveTTLSeconds)),
            fullRebuild = needsFullRebuild || head.isEmpty,
            retired = false
          )
          val paths = generationPaths(settings, generation)
          val candidateCreatedAt = System.currentTimeMillis()
          val candidate = CandidateRecord(generation,
                                          manifest.parentGeneration,
                                          candidateCreatedAt,
                                          java.lang.Math.addExact(candidateCreatedAt, GenerationCleanupGraceMillis))
          writeImmutable(paths.candidate, encodeCandidate(candidate), spark, conditionalWriter)
          writeBucketed(currentState, paths.state, stateBuckets)
          writeBucketed(completeDelta, paths.delta, stateBuckets)
          if (tombstoneRows > 0) writeBucketed(tombstoneState, paths.tombstones, stateBuckets)
          val readyBytes = encodeReady(manifest)
          writeImmutable(paths.ready, readyBytes, spark, conditionalWriter)

          val admitted =
            claimHead(settings, manifest.parentGeneration, generation, sha256(readyBytes), spark, conditionalWriter)

          metadataRows.unpersist()
          previousState.foreach(_.unpersist())
          currentState.unpersist()
          dataDelta.unpersist()
          completeDelta.unpersist()
          tombstoneState.unpersist()

          if (admitted.generation != generation) throw HeadAdvanced(admitted.generation)

          val committed = resolveHead(settings, spark, controlClient, conditionalWriter)
          require(committed.exists(_.status == status), s"Redis generation $generation did not become the S3 head")
          val applied = readCommittedApplied(paths.applied, spark, conditionalWriter).record
          logger.info(
            s"Applied Redis generation $generation for ${settings.batchDataset}: sourceRows=$sourceRows " +
              s"changed=$changedKeys deleted=$deletedKeys refreshed=${applied.refreshedKeys} " +
              s"fullRebuild=${applied.recovered || manifest.fullRebuild} rateLimit=${settings.maxKeysPerSecond} keys/s")
          UploadSummary(sourceRows,
                        changedKeys,
                        deletedKeys,
                        applied.refreshedKeys,
                        Some(generation),
                        superseded = false,
                        fullRebuild = applied.recovered || manifest.fullRebuild)
        } finally {
          previousState.foreach(_.unpersist())
        }
      } finally {
        metadataRows.unpersist()
      }
    } finally {
      Option(completeDelta).foreach(_.unpersist())
      Option(dataDelta).foreach(_.unpersist())
      Option(currentState).foreach(_.unpersist())
      Option(tombstoneState).foreach(_.unpersist())
      source.unpersist()
    }
  }

  private def resolveHead(settings: Settings,
                          spark: SparkSession,
                          controlClient: JedisCluster,
                          conditionalWriter: ConditionalObjectWriter): Option[ReadyManifest] = {
    readHead(settings, spark, conditionalWriter).map { head =>
      val (manifest, readyBytes) = validateReady(settings, head.record.generation, spark)
      require(Arrays.equals(head.record.readyDigest, sha256(readyBytes)),
              s"Redis S3 head digest does not match generation ${head.record.generation}")
      require(manifest.parentGeneration == head.record.parentGeneration,
              s"Redis generation ${manifest.status.generation} does not match its S3 head parent")
      val paths = generationPaths(settings, manifest.status.generation)
      if (pathExists(paths.applied, spark)) {
        validateApplied(readCommittedApplied(paths.applied, spark, conditionalWriter).record,
                        manifest,
                        head.record.readyDigest)
      } else {
        // Compute the deadline before the second APPLIED check. Every duplicate applier that can still proceed therefore
        // has a deadline no later than the first successful applier's completion time plus the generation TTL.
        val leaseDeadlineMillis = uploadDeadlineMillis(manifest.liveTTLSeconds)
        if (pathExists(paths.applied, spark)) {
          validateApplied(readCommittedApplied(paths.applied, spark, conditionalWriter).record,
                          manifest,
                          head.record.readyDigest)
        } else {
          val currentStatus =
            RedisBatchUpload.readStatus(controlClient,
                                        RedisBatchUpload.buildStatusKey(settings.batchDataset, settings.keyPrefix))
          currentStatus.filter(status => compareStatus(status, manifest.status) > 0).foreach { status =>
            if (headAdvancedPast(settings, manifest, spark, conditionalWriter)) throw HeadAdvanced(status.generation)
            throw new IllegalStateException(
              s"Redis status ${status.generation} is newer than S3 head ${manifest.status.generation}")
          }
          val recoverFull = requiresFullRecovery(manifest, currentStatus)
          val result = applyGeneration(settings,
                                       manifest,
                                       head.record.readyDigest,
                                       spark,
                                       controlClient,
                                       conditionalWriter,
                                       recoverFull,
                                       leaseDeadlineMillis)
          val applied = AppliedRecord(manifest.status,
                                      head.record.readyDigest,
                                      mutationKeys = result.mutationKeys,
                                      refreshedKeys = result.refreshedKeys,
                                      recovered = result.recovered)
          remainingLeaseSeconds(leaseDeadlineMillis)
          if (!manifest.retired) remainingLeaseSeconds(result.minimumActiveDeadlineMillis)
          conditionalWriter.putIfAbsent(paths.applied, encodeApplied(applied), spark.sparkContext.hadoopConfiguration)
          validateApplied(readCommittedApplied(paths.applied, spark, conditionalWriter).record,
                          manifest,
                          head.record.readyDigest)
          remainingLeaseSeconds(leaseDeadlineMillis)
          if (!manifest.retired) remainingLeaseSeconds(result.minimumActiveDeadlineMillis)
          val publishedStatus =
            RedisBatchUpload.readStatus(controlClient,
                                        RedisBatchUpload.buildStatusKey(settings.batchDataset, settings.keyPrefix))
          require(
            publishedStatus.exists(status => compareStatus(status, manifest.status) >= 0),
            s"Redis status for ${manifest.status.generation} expired before its APPLIED record was acknowledged"
          )
        }
      }
      manifest
    }
  }

  private def applyGeneration(settings: Settings,
                              manifest: ReadyManifest,
                              readyDigest: Array[Byte],
                              spark: SparkSession,
                              controlClient: JedisCluster,
                              conditionalWriter: ConditionalObjectWriter,
                              recoverFull: Boolean,
                              leaseDeadlineMillis: Long): ApplyResult = {
    if (headAdvancedPast(settings, manifest, spark, conditionalWriter)) {
      throw HeadAdvanced(readHead(settings, spark, conditionalWriter).get.record.generation)
    }
    val limiterKey = RedisBatchUpload.RateLimiterKey
    val persistedDelta = loadDelta(settings, manifest.status.generation, spark)
    val dataDelta = persistedDelta.where(not(isServingInfo(col(KeyColumn))))
    val metadataDelta = persistedDelta.where(isServingInfo(col(KeyColumn)))
    val unchangedKeys = manifest.dataRows - manifest.changedKeys
    val currentLeaseDeadline =
      readLease(generationPaths(settings, manifest.status.generation).lease,
                manifest,
                readyDigest,
                spark,
                conditionalWriter).map(_.record.minimumActiveDeadlineMillis)
    val parentLeaseDeadline =
      if (manifest.retired) None
      else manifest.parentGeneration.flatMap(readGenerationLease(settings, _, spark, conditionalWriter))
    val inheritedLeaseDeadline =
      (currentLeaseDeadline.toSeq ++ parentLeaseDeadline.toSeq).reduceOption((left, right) => math.max(left, right))
    val activeLeaseDeadlineMillis =
      if (manifest.retired) leaseDeadlineMillis
      else generationLeaseDeadline(leaseDeadlineMillis, currentLeaseDeadline)
    val deleteFenceDeadlineMillis = uploadDeadlineMillis(manifest.deleteFenceTTLSeconds)

    val dataSummary = writeToRedis(
      dataDelta,
      MutationWrite,
      settings,
      limiterKey,
      activeLeaseDeadlineMillis,
      deleteFenceDeadlineMillis,
      progress = Some(WriteProgress(manifest.status.generation, "delta-mutations", Some(manifest.deltaRows - 1)))
    )
    require(
      dataSummary.attempted == manifest.deltaRows - 1,
      s"Redis generation ${manifest.status.generation} attempted ${dataSummary.attempted} data mutations; " +
        s"manifest records ${manifest.deltaRows - 1}"
    )
    var rejectedKeys = dataSummary.rejected

    var recovered = recoverFull || manifest.fullRebuild
    var replayRequired = recoverFull && !manifest.fullRebuild
    var refreshSummary = WriteSummary(0, 0, 0, 0, 0)
    if (!manifest.retired && !manifest.fullRebuild && !recoverFull && unchangedKeys > 0) {
      val unchangedState = loadStateIndex(settings, manifest.stateGeneration, spark)
        .where(col(ValueWriteEpochColumn) < lit(manifest.status.writeEpoch))
        .select(KeyColumn, ValueTimestampColumn, ValueWriteEpochColumn, BucketColumn)
      val expectedDeadline =
        if (settings.clientSettings.clientNoTouch) inheritedLeaseDeadline else None
      val unchangedLeaseDeadlineMillis = math.max(activeLeaseDeadlineMillis, inheritedLeaseDeadline.getOrElse(0L))
      refreshSummary = writeToRedis(
        unchangedState,
        LeaseRefresh(expectedDeadline),
        settings,
        limiterKey,
        unchangedLeaseDeadlineMillis,
        progress = Some(WriteProgress(manifest.status.generation, "lease-refresh", Some(unchangedKeys)))
      )
      require(
        refreshSummary.attempted == unchangedKeys,
        s"Redis generation ${manifest.status.generation} checked ${refreshSummary.attempted} unchanged keys; " +
          s"expected $unchangedKeys"
      )
      if (refreshSummary.rejected > 0) {
        logger.warn(
          s"Redis generation ${manifest.status.generation} found ${refreshSummary.rejected} missing or rolled-back " +
            "active keys; promoting the apply to a full source replay")
        recovered = true
        replayRequired = true
      }
      rejectedKeys += refreshSummary.replayed
    }

    if (!manifest.retired && replayRequired) {
      val replay = buildFullReplay(settings, manifest, spark).persist(StorageLevel.DISK_ONLY)
      try {
        val replayCount = replay.count()
        require(replayCount == manifest.dataRows,
                s"Redis full replay contains $replayCount data keys; manifest records ${manifest.dataRows}")
        val replaySummary = writeToRedis(
          replay,
          MutationWrite,
          settings,
          limiterKey,
          activeLeaseDeadlineMillis,
          progress = Some(WriteProgress(manifest.status.generation, "full-replay", Some(manifest.dataRows)))
        )
        require(replaySummary.attempted == manifest.dataRows,
                s"Redis full replay attempted ${replaySummary.attempted} keys; expected ${manifest.dataRows}")
        rejectedKeys += replaySummary.rejected
      } finally {
        replay.unpersist()
      }
    }

    var tombstoneSummary = WriteSummary(0, 0, 0, 0, 0)
    if (manifest.tombstoneRows > 0) {
      tombstoneSummary = writeToRedis(
        buildTombstoneReplay(settings,
                             manifest,
                             spark,
                             deleteFenceDeadlineMillis,
                             excludeStatus = Some(manifest.status)),
        MutationWrite,
        settings,
        limiterKey,
        activeLeaseDeadlineMillis,
        deleteFenceDeadlineMillis,
        progress = Some(WriteProgress(manifest.status.generation, "tombstone-replay", None))
      )
      rejectedKeys += tombstoneSummary.rejected
    }

    val metadataSummary = writeToRedis(metadataDelta, MutationWrite, settings, limiterKey, activeLeaseDeadlineMillis)
    require(metadataSummary.attempted == 1,
            s"Redis generation ${manifest.status.generation} must apply exactly one serving-info mutation")
    rejectedKeys += metadataSummary.rejected

    val superseded = headAdvancedPast(settings, manifest, spark, conditionalWriter)
    require(
      rejectedKeys == 0 || superseded,
      s"Redis generation ${manifest.status.generation} encountered $rejectedKeys newer values without a newer S3 head"
    )
    val intendedActiveDeadlineMillis =
      if (manifest.retired) 0L
      else activeLeaseDeadlineMillis
    val minimumActiveDeadlineMillis =
      if (manifest.retired || superseded) intendedActiveDeadlineMillis
      else
        advanceLease(
          generationPaths(settings, manifest.status.generation).lease,
          LeaseRecord(manifest.status, readyDigest, intendedActiveDeadlineMillis),
          manifest,
          readyDigest,
          spark,
          conditionalWriter
        ).minimumActiveDeadlineMillis
    val statusOperationDeadlineMillis =
      if (manifest.retired) leaseDeadlineMillis else math.min(leaseDeadlineMillis, minimumActiveDeadlineMillis)
    val statusResult = writeStatusWithOomRetry(
      controlClient,
      RedisBatchUpload.buildStatusKey(settings.batchDataset, settings.keyPrefix),
      manifest.status,
      if (manifest.retired) 0L else minimumActiveDeadlineMillis,
      settings,
      statusOperationDeadlineMillis
    )
    val statusSuperseded = superseded ||
      (statusResult == 0L && headAdvancedPast(settings, manifest, spark, conditionalWriter))
    require(
      statusResult == 1L || statusResult == 2L || (statusResult == 0L && statusSuperseded),
      s"Redis status for ${manifest.status.generation} was rejected without a newer S3 head"
    )
    val mutationKeys =
      dataSummary.attempted + metadataSummary.attempted + tombstoneSummary.attempted +
        (if (replayRequired) manifest.dataRows else 0L)
    ApplyResult(mutationKeys, refreshSummary.attempted, recovered, minimumActiveDeadlineMillis)
  }

  private def requiresFullRecovery(child: ReadyManifest, currentStatus: Option[BatchStatus]): Boolean = {
    currentStatus match {
      case None if child.parentGeneration.isEmpty                             => false
      case Some(status) if status == child.status                             => false
      case Some(status) if child.parentGeneration.contains(status.generation) => false
      case Some(status) if compareStatus(status, child.status) > 0 =>
        throw new IllegalStateException(
          s"Redis status ${status.generation} is newer than S3 head ${child.status.generation}")
      case Some(status) if child.parentGeneration.isEmpty =>
        throw new IllegalStateException(
          s"Redis status ${status.generation} exists but initial S3 generation ${child.status.generation} has no parent")
      case _ => true
    }
  }

  private def repairOlderSnapshot(settings: Settings,
                                  head: ReadyManifest,
                                  spark: SparkSession,
                                  controlClient: JedisCluster,
                                  conditionalWriter: ConditionalObjectWriter,
                                  forceReplay: Boolean): UploadSummary = {
    val latestSettings = settings.copy(sourcePartition = head.status.sourcePartition)
    val refreshed =
      refreshCommittedSnapshot(latestSettings, head, spark, controlClient, conditionalWriter, forceReplay)
    logger.info(
      s"Redis older partition ${settings.sourcePartition} did not move S3 head ${head.status.generation}; " +
        s"the latest partition ${head.status.sourcePartition} was checked for lease renewal instead")
    refreshed.copy(superseded = true)
  }

  private def refreshCommittedSnapshot(settings: Settings,
                                       head: ReadyManifest,
                                       spark: SparkSession,
                                       controlClient: JedisCluster,
                                       conditionalWriter: ConditionalObjectWriter,
                                       forceReplay: Boolean): UploadSummary = {
    if (headAdvancedPast(settings, head, spark, conditionalWriter)) {
      return UploadSummary(head.sourceRows,
                           changedKeys = 0,
                           deletedKeys = 0,
                           refreshedKeys = 0,
                           generation = Some(head.status.generation),
                           superseded = true,
                           fullRebuild = false)
    }
    val paths = generationPaths(settings, head.status.generation)
    val readyDigest = sha256(readBytes(paths.ready, spark))
    val committedLease = readLease(paths.lease, head, readyDigest, spark, conditionalWriter)
    val renewLease =
      forceReplay || leaseRefreshRequired(committedLease.map(_.record.minimumActiveDeadlineMillis),
                                          head.liveTTLSeconds,
                                          System.currentTimeMillis())
    val candidateLeaseDeadlineMillis = uploadDeadlineMillis(head.liveTTLSeconds)
    val intendedLeaseDeadlineMillis =
      if (renewLease)
        renewedLeaseDeadline(candidateLeaseDeadlineMillis, committedLease.map(_.record.minimumActiveDeadlineMillis))
      else committedLease.get.record.minimumActiveDeadlineMillis
    val limiterKey = RedisBatchUpload.RateLimiterKey
    val useExpiryFence = settings.clientSettings.clientNoTouch && committedLease.isDefined
    val checkLease = !forceReplay
    val refreshed =
      if (!checkLease) WriteSummary(0, 0, 0, 0, 0)
      else {
        val state = loadStateIndex(settings, head.stateGeneration, spark)
          .select(KeyColumn, ValueTimestampColumn, ValueWriteEpochColumn, BucketColumn)
        val expectedDeadline =
          if (useExpiryFence) committedLease.map(_.record.minimumActiveDeadlineMillis) else None
        writeToRedis(
          state,
          LeaseRefresh(expectedDeadline),
          settings,
          limiterKey,
          intendedLeaseDeadlineMillis,
          progress = Some(WriteProgress(head.status.generation, "snapshot-lease-check", Some(head.dataRows)))
        )
      }
    if (forceReplay) {
      logger.info(
        s"Skipping Redis lease validation for committed generation ${head.status.generation}; a full replay is required")
    } else if (!renewLease) {
      logger.info(
        s"Checked Redis expiration fences for committed generation ${head.status.generation} without advancing " +
          s"deadline ${committedLease.get.record.minimumActiveDeadlineMillis}")
    }
    val replayRequired = forceReplay || refreshed.rejected > 0
    val activeLeaseDeadlineMillis =
      if (replayRequired && !renewLease)
        renewedLeaseDeadline(candidateLeaseDeadlineMillis, committedLease.map(_.record.minimumActiveDeadlineMillis))
      else intendedLeaseDeadlineMillis
    var replayRejected = 0L
    var tombstoneRejected = 0L
    if (replayRequired) {
      logger.warn(
        s"Redis lease refresh for ${head.status.generation} requires a full replay: " +
          s"forceReplay=$forceReplay missingOrRolledBackKeys=${refreshed.rejected}")
      val replay = buildFullReplay(settings, head, spark).persist(StorageLevel.DISK_ONLY)
      try {
        val replaySummary = writeToRedis(
          replay,
          MutationWrite,
          settings,
          limiterKey,
          activeLeaseDeadlineMillis,
          progress = Some(WriteProgress(head.status.generation, "snapshot-repair", Some(head.dataRows)))
        )
        require(replaySummary.attempted == head.dataRows,
                s"Redis full snapshot repair attempted ${replaySummary.attempted} keys; expected ${head.dataRows}")
        replayRejected = replaySummary.rejected
      } finally {
        replay.unpersist()
      }
    }
    if (head.tombstoneRows > 0) {
      val tombstoneDeadlineMillis = committedApplied(settings, head, spark, conditionalWriter)
        .tombstoneRetainUntilMillis(head.deleteFenceTTLSeconds)
      tombstoneRejected = writeToRedis(
        buildTombstoneReplay(settings, head, spark, tombstoneDeadlineMillis),
        MutationWrite,
        settings,
        limiterKey,
        activeLeaseDeadlineMillis,
        tombstoneDeadlineMillis,
        progress = Some(WriteProgress(head.status.generation, "tombstone-replay", None))
      ).rejected
    }

    val metadata = loadDelta(settings, head.status.generation, spark)
      .where(isServingInfo(col(KeyColumn)))
    require(metadata.count() == 1, "Redis committed snapshot refresh requires exactly one serving-info row")
    val metadataSummary = writeToRedis(metadata, MutationWrite, settings, limiterKey, activeLeaseDeadlineMillis)
    require(metadataSummary.attempted == 1, "Redis committed snapshot refresh must attempt one serving-info row")
    val superseded = headAdvancedPast(settings, head, spark, conditionalWriter)
    val rejectedKeys = refreshed.replayed + replayRejected + tombstoneRejected + metadataSummary.rejected
    require(
      rejectedKeys == 0 || superseded,
      s"Redis snapshot ${head.status.generation} encountered newer values without a newer S3 head"
    )
    val intendedActiveDeadlineMillis =
      if (renewLease || replayRequired) activeLeaseDeadlineMillis
      else committedLease.get.record.minimumActiveDeadlineMillis
    val minimumActiveDeadlineMillis =
      if (superseded) intendedActiveDeadlineMillis
      else if (renewLease || replayRequired)
        advanceLease(
          paths.lease,
          LeaseRecord(head.status, readyDigest, intendedActiveDeadlineMillis),
          head,
          readyDigest,
          spark,
          conditionalWriter
        ).minimumActiveDeadlineMillis
      else committedLease.get.record.minimumActiveDeadlineMillis
    val statusOperationDeadlineMillis = math.min(candidateLeaseDeadlineMillis, minimumActiveDeadlineMillis)
    val statusResult = writeStatusWithOomRetry(
      controlClient,
      RedisBatchUpload.buildStatusKey(settings.batchDataset, settings.keyPrefix),
      head.status,
      minimumActiveDeadlineMillis,
      settings,
      statusOperationDeadlineMillis
    )
    val statusSuperseded = superseded ||
      (statusResult == 0L && headAdvancedPast(settings, head, spark, conditionalWriter))
    require(
      statusResult == 1L || statusResult == 2L || (statusResult == 0L && statusSuperseded),
      s"Redis status for snapshot ${head.status.generation} was rejected without a newer S3 head"
    )
    UploadSummary(
      sourceRows = head.sourceRows,
      changedKeys = 0,
      deletedKeys = 0,
      refreshedKeys = refreshed.attempted,
      generation = Some(head.status.generation),
      superseded = statusSuperseded,
      fullRebuild = replayRequired
    )
  }

  private def buildFullReplay(settings: Settings, manifest: ReadyManifest, spark: SparkSession): DataFrame = {
    loadState(settings, manifest.stateGeneration, spark)
      .select(col(KeyColumn),
              col(ValueColumn),
              col(ValueTimestampColumn).as(BatchTimestampColumn),
              col(ValueWriteEpochColumn).as(WriteEpochColumn),
              col(BucketColumn))
      .withColumn(OperationColumn, lit(RedisBatchUpload.UpsertOperation))
      .select(KeyColumn, ValueColumn, OperationColumn, BatchTimestampColumn, WriteEpochColumn, BucketColumn)
  }

  private def buildTombstoneState(currentState: DataFrame,
                                  previousTombstones: Option[DataFrame],
                                  newDeletes: DataFrame): DataFrame = {
    val activeKeys = currentState.select(KeyColumn, BucketColumn)
    val retained = previousTombstones
      .map(
        _.where(col(TombstoneExpiresAtColumn) > lit(System.currentTimeMillis()))
          .join(activeKeys, Seq(KeyColumn, BucketColumn), "left_anti"))
      .getOrElse(emptyTombstones(currentState.sparkSession))
    val added = newDeletes
      .select(KeyColumn, BatchTimestampColumn, WriteEpochColumn, TombstoneExpiresAtColumn, BucketColumn)
    retained.unionByName(added)
  }

  private def buildTombstoneReplay(settings: Settings,
                                   manifest: ReadyManifest,
                                   spark: SparkSession,
                                   pendingDeadlineMillis: Long,
                                   excludeStatus: Option[BatchStatus] = None): DataFrame = {
    val nowMillis = System.currentTimeMillis()
    val unexpired = loadTombstones(settings, manifest, spark)
      .withColumn(
        TombstoneExpiresAtColumn,
        when(col(TombstoneExpiresAtColumn) === lit(PendingTombstoneExpiry), lit(pendingDeadlineMillis))
          .otherwise(col(TombstoneExpiresAtColumn))
      )
      .where(col(TombstoneExpiresAtColumn) > lit(nowMillis))
    val retained = excludeStatus.fold(unexpired) { status =>
      unexpired.where(
        not(col(BatchTimestampColumn) === lit(status.batchTimestamp) &&
          col(WriteEpochColumn) === lit(status.writeEpoch)))
    }
    retained
      .withColumn(ValueColumn, lit(null).cast(BinaryType))
      .withColumn(OperationColumn, lit(RedisBatchUpload.DeleteOperation))
      .withColumn(AllowExpiredMutationColumn, lit(true))
      .select(KeyColumn,
              ValueColumn,
              OperationColumn,
              BatchTimestampColumn,
              WriteEpochColumn,
              BucketColumn,
              TombstoneExpiresAtColumn,
              AllowExpiredMutationColumn)
  }

  private def emptyTombstones(spark: SparkSession): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(
        Seq(
          StructField(KeyColumn, BinaryType, nullable = false),
          StructField(BatchTimestampColumn, LongType, nullable = false),
          StructField(WriteEpochColumn, LongType, nullable = false),
          StructField(TombstoneExpiresAtColumn, LongType, nullable = false),
          StructField(BucketColumn, IntegerType, nullable = false)
        ))
    )

  private[redis] def buildDiff(currentRows: DataFrame,
                               previousState: Option[DataFrame],
                               batchTimestamp: Long,
                               stateBuckets: Int,
                               writeEpoch: Long = 0L,
                               forceAllUpserts: Boolean = false): DiffFrames = {
    val current = currentRows
      .select(col(KeyColumn), col(ValueColumn))
      .withColumn(DigestColumn, unhex(sha2(col(ValueColumn), 256)))
      .withColumn(BucketColumn, bucketColumn(stateBuckets))
    val state = previousState match {
      case _ if forceAllUpserts =>
        current
          .withColumn(ValueTimestampColumn, lit(batchTimestamp))
          .withColumn(ValueWriteEpochColumn, lit(writeEpoch))
      case None =>
        current
          .withColumn(ValueTimestampColumn, lit(batchTimestamp))
          .withColumn(ValueWriteEpochColumn, lit(writeEpoch))
      case Some(previous) =>
        val currentAlias = current.alias("current")
        val previousAlias = previous
          .select(col(KeyColumn),
                  col(BucketColumn),
                  col(DigestColumn),
                  col(ValueTimestampColumn),
                  col(ValueWriteEpochColumn))
          .alias("previous")
        val unchanged = col(s"previous.$DigestColumn").isNotNull &&
          col(s"current.$DigestColumn") === col(s"previous.$DigestColumn")
        currentAlias
          .join(previousAlias, Seq(KeyColumn, BucketColumn), "left")
          .select(
            col(s"current.$KeyColumn").as(KeyColumn),
            col(s"current.$ValueColumn").as(ValueColumn),
            col(s"current.$DigestColumn").as(DigestColumn),
            col(s"current.$BucketColumn").as(BucketColumn),
            when(unchanged, col(s"previous.$ValueTimestampColumn"))
              .otherwise(lit(batchTimestamp))
              .as(ValueTimestampColumn),
            when(unchanged, col(s"previous.$ValueWriteEpochColumn"))
              .otherwise(lit(writeEpoch))
              .as(ValueWriteEpochColumn)
          )
    }

    val upserts = previousState match {
      case _ if forceAllUpserts => current
      case None                 => current
      case Some(previous) =>
        val currentAlias = current.alias("current")
        val previousAlias = previous
          .select(col(KeyColumn), col(DigestColumn), col(BucketColumn))
          .alias("previous")
        currentAlias
          .join(previousAlias, Seq(KeyColumn, BucketColumn), "left")
          .where(col(s"previous.$DigestColumn").isNull ||
            col(s"current.$DigestColumn") =!= col(s"previous.$DigestColumn"))
          .select(col(s"current.$KeyColumn"), col(s"current.$ValueColumn"), col(s"current.$BucketColumn"))
    }

    val upsertDelta = upserts
      .withColumn(OperationColumn, lit(RedisBatchUpload.UpsertOperation))
      .withColumn(BatchTimestampColumn, lit(batchTimestamp))
      .withColumn(WriteEpochColumn, lit(writeEpoch))
      .select(KeyColumn, ValueColumn, OperationColumn, BatchTimestampColumn, WriteEpochColumn, BucketColumn)

    val deleteDelta = previousState match {
      case None => upsertDelta.limit(0)
      case Some(previous) =>
        previous
          .select(col(KeyColumn), col(BucketColumn))
          .join(state.select(col(KeyColumn), col(BucketColumn)), Seq(KeyColumn, BucketColumn), "left_anti")
          .withColumn(ValueColumn, lit(null).cast(BinaryType))
          .withColumn(OperationColumn, lit(RedisBatchUpload.DeleteOperation))
          .withColumn(BatchTimestampColumn, lit(batchTimestamp))
          .withColumn(WriteEpochColumn, lit(writeEpoch))
          .select(KeyColumn, ValueColumn, OperationColumn, BatchTimestampColumn, WriteEpochColumn, BucketColumn)
    }

    DiffFrames(state, upsertDelta.unionByName(deleteDelta))
  }

  private def loadSourceRows(tableUtils: TableUtils, settings: Settings): DataFrame = {
    require(
      tableUtils.tableReachable(settings.sourceTable),
      s"Redis incremental upload requires a catalog-backed upload table. ${settings.sourceTable} is not reachable; " +
        "configure spark.chronon.table_write.format and the deployment's Spark catalog for Redis uploads."
    )
    val source = tableUtils
      .loadTable(settings.sourceTable)
      .where(col(settings.partitionSpec.column) === lit(settings.sourcePartition))
    val missing = Seq(KeyColumn, ValueColumn).filterNot(source.columns.contains)
    require(missing.isEmpty,
            s"Redis upload source ${settings.sourceTable} is missing required columns: ${missing.mkString(", ")}")
    source.select(col(KeyColumn), col(ValueColumn))
  }

  private[redis] def validateSourceRows(dataRows: DataFrame): Unit = {
    val hasDuplicateKey = dataRows
      .groupBy(col(KeyColumn))
      .count()
      .where(col("count") > 1)
      .limit(1)
      .count() > 0
    require(!hasDuplicateKey, "Redis upload source contains duplicate key_bytes rows")
  }

  private[redis] def validateNoEviction(client: JedisCluster): Unit = {
    val clusterNodes = client.getClusterNodes.asScala.toSeq
    require(clusterNodes.nonEmpty, "Redis cluster topology contains no nodes")
    val policies = clusterNodes.map { case (node, pool) =>
      val connection = pool.getResource
      try {
        connection.sendCommand(Protocol.Command.INFO, "memory")
        val info = connection.getBulkReply
        val policy = redisInfoField(info, "maxmemory_policy").getOrElse(
          throw new IllegalStateException(s"Redis node $node did not report maxmemory_policy in INFO memory"))
        node -> policy
      } catch {
        case NonFatal(error) =>
          throw new IllegalStateException(
            s"Cannot verify maxmemory-policy=noeviction on Redis node $node; grant the uploader INFO access",
            error)
      } finally {
        connection.close()
      }
    }
    val unsafe = policies.filterNot { case (_, policy) => policy.equalsIgnoreCase("noeviction") }
    require(
      unsafe.isEmpty,
      s"Redis incremental upload requires maxmemory-policy=noeviction on every node; found ${unsafe.mkString(", ")}"
    )
  }

  private[redis] def redisInfoField(info: String, field: String): Option[String] = {
    val prefix = s"$field:"
    Option(info).toSeq
      .flatMap(_.split("\\r?\\n"))
      .find(_.startsWith(prefix))
      .map(_.substring(prefix.length).trim)
  }

  private[redis] def configuredBoolean(settings: Map[String, String], propKey: String, default: Boolean): Boolean =
    settings
      .get(propKey)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.toBoolean)
      .getOrElse(default)

  private def isServingInfo(column: org.apache.spark.sql.Column) =
    coalesce(column === lit(ServingInfoKeyBytes), lit(false))

  private def bucketColumn(stateBuckets: Int) =
    pmod(xxhash64(col(KeyColumn)), lit(stateBuckets)).cast(IntegerType)

  private def datasetRoot(settings: Settings): Path = {
    val stateRoot = new Path(settings.stateRoot.stripSuffix("/"))
    new Path(new Path(stateRoot, sanitizePathSegment(settings.sourceTable)), sanitizePathSegment(settings.batchDataset))
  }

  private def generationPaths(settings: Settings, generation: String): GenerationPaths = {
    require(RedisBatchUpload.isSafeIdentifier(generation), s"Invalid Redis batch generation: $generation")
    val root = new Path(new Path(datasetRoot(settings), GenerationsDirectory), generation)
    GenerationPaths(
      root,
      new Path(root, CandidateFile),
      new Path(root, DeltaDirectory),
      new Path(root, StateDirectory),
      new Path(root, TombstoneDirectory),
      new Path(root, ReadyFile),
      new Path(root, AppliedFile),
      new Path(root, LeaseFile)
    )
  }

  private[redis] def leaseCheckpointPath(settings: Settings, generation: String): Path =
    generationPaths(settings, generation).lease

  private def headPath(settings: Settings): Path = new Path(datasetRoot(settings), HeadFile)

  private def claimPath(settings: Settings, generation: String): Path = {
    require(RedisBatchUpload.isSafeIdentifier(generation), s"Invalid Redis batch generation: $generation")
    new Path(new Path(datasetRoot(settings), ClaimsDirectory), generation)
  }

  private def writeBucketed(df: DataFrame, path: Path, buckets: Int): Unit = {
    df.repartition(buckets, col(BucketColumn))
      .sortWithinPartitions(col(BucketColumn), col(KeyColumn))
      .write
      .mode(SaveMode.ErrorIfExists)
      .partitionBy(BucketColumn)
      .parquet(path.toString)
  }

  private def cleanupObsoleteGenerations(settings: Settings,
                                         spark: SparkSession,
                                         conditionalWriter: ConditionalObjectWriter): Unit =
    try {
      val root = new Path(datasetRoot(settings), GenerationsDirectory)
      val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
      if (fs.exists(root)) {
        val nowMillis = System.currentTimeMillis()
        val cleanupHead = readHead(settings, spark, conditionalWriter)
        val generationStatuses = fs
          .listStatus(root)
          .iterator
          .filter(status => status.isDirectory && RedisBatchUpload.isSafeIdentifier(status.getPath.getName))
          .toSeq
        val directChildAppliedLastModifiedByParent = generationStatuses
          .flatMap { status =>
            val generation = status.getPath.getName
            val paths = generationPaths(settings, generation)
            if (fs.exists(paths.applied) && fs.exists(paths.ready)) {
              val (manifest, readyBytes) = validateReady(settings, generation, spark)
              val applied = readCommittedApplied(paths.applied, spark, conditionalWriter)
              validateApplied(applied.record, manifest, sha256(readyBytes))
              manifest.parentGeneration.map(_ -> applied.lastModifiedMillis)
            } else None
          }
          .groupBy(_._1)
          .map { case (parent, children) => parent -> children.map(_._2).max }
        generationStatuses.foreach { status =>
          val generation = status.getPath.getName
          try {
            val paths = generationPaths(settings, generation)
            if (fs.exists(paths.candidate)) {
              val candidate = decodeCandidate(readBytes(paths.candidate, spark))
              require(candidate.generation == generation,
                      s"Redis candidate at ${paths.candidate} identifies ${candidate.generation}")
              val reachable =
                generationReachableFromCurrentHead(settings, generation, candidate, spark, conditionalWriter)
              val abandoned = reachable &&
                nowMillis >= candidate.cleanupAfterMillis &&
                candidateParentIsCurrent(settings, candidate, spark, conditionalWriter) &&
                abandonCandidate(settings, generation, spark, conditionalWriter)
              if (!reachable || abandoned) {
                val cleanupAfter = if (fs.exists(paths.applied) && fs.exists(paths.ready)) {
                  val (manifest, readyBytes) = validateReady(settings, generation, spark)
                  val applied = readCommittedApplied(paths.applied, spark, conditionalWriter)
                  validateApplied(applied.record, manifest, sha256(readyBytes))
                  math.max(candidate.cleanupAfterMillis,
                           java.lang.Math.addExact(applied.lastModifiedMillis, GenerationCleanupGraceMillis))
                } else candidate.cleanupAfterMillis
                val currentHeadSupersededAt = cleanupHead.flatMap { head =>
                  if (head.record.parentGeneration.contains(generation)) Some(head.lastModifiedMillis) else None
                }
                val unreachableGrace =
                  if (abandoned) cleanupAfter
                  else
                    unreachableGenerationCleanupDeadline(cleanupAfter,
                                                         directChildAppliedLastModifiedByParent.get(generation),
                                                         currentHeadSupersededAt,
                                                         GenerationCleanupGraceMillis)

                if (
                  nowMillis >= math.max(cleanupAfter, unreachableGrace) &&
                  (isAbandonedCandidate(settings, generation, spark, conditionalWriter) ||
                    !generationReachableFromCurrentHead(settings, generation, candidate, spark, conditionalWriter)) &&
                  readHead(settings, spark, conditionalWriter).map(_.version) == cleanupHead.map(_.version) &&
                  fs.delete(status.getPath, true)
                ) {
                  logger.info(s"Removed unreachable Redis S3 generation $generation for ${settings.batchDataset}")
                  if (!abandoned) fs.delete(claimPath(settings, generation), false)
                }
              }
            } else if (
              isAbandonedCandidate(settings, generation, spark, conditionalWriter) &&
              fs.delete(status.getPath, true)
            ) {
              logger.info(s"Removed abandoned Redis S3 generation $generation for ${settings.batchDataset}")
            }
          } catch {
            case NonFatal(error) =>
              logger.warn(s"Could not evaluate Redis S3 generation $generation for cleanup", error)
          }
        }
      }
    } catch {
      case NonFatal(error) =>
        logger.warn(s"Could not clean obsolete Redis S3 generations for ${settings.batchDataset}", error)
    }

  private def candidateParentIsCurrent(settings: Settings,
                                       candidate: CandidateRecord,
                                       spark: SparkSession,
                                       conditionalWriter: ConditionalObjectWriter): Boolean =
    readHead(settings, spark, conditionalWriter) match {
      case None       => candidate.parentGeneration.isEmpty
      case Some(head) => candidate.parentGeneration.contains(head.record.generation)
    }

  private[redis] def unreachableGenerationCleanupDeadline(cleanupAfterMillis: Long,
                                                          directChildAppliedLastModifiedMillis: Option[Long],
                                                          currentHeadSupersededAtMillis: Option[Long],
                                                          graceMillis: Long): Long = {
    require(cleanupAfterMillis > 0, s"Redis generation cleanup deadline must be positive: $cleanupAfterMillis")
    directChildAppliedLastModifiedMillis.foreach(value =>
      require(value > 0, s"Redis child APPLIED last-modified time must be positive: $value"))
    currentHeadSupersededAtMillis.foreach(value =>
      require(value > 0, s"Redis head last-modified time must be positive: $value"))
    require(graceMillis >= 0, s"Redis generation cleanup grace must be non-negative: $graceMillis")
    (directChildAppliedLastModifiedMillis.toSeq ++ currentHeadSupersededAtMillis.toSeq)
      .reduceOption((left, right) => math.max(left, right)) match {
      case Some(supersededAtMillis) =>
        math.max(cleanupAfterMillis, java.lang.Math.addExact(supersededAtMillis, graceMillis))
      case None => cleanupAfterMillis
    }
  }

  private def abandonCandidate(settings: Settings,
                               generation: String,
                               spark: SparkSession,
                               conditionalWriter: ConditionalObjectWriter): Boolean = {
    val abandoned = CandidateClaim(generation, abandoned = true)
    val winner = conditionalWriter.putIfAbsent(claimPath(settings, generation),
                                               encodeClaim(abandoned),
                                               spark.sparkContext.hadoopConfiguration)
    decodeClaim(winner.bytes) == abandoned
  }

  private def isAbandonedCandidate(settings: Settings,
                                   generation: String,
                                   spark: SparkSession,
                                   conditionalWriter: ConditionalObjectWriter): Boolean =
    conditionalWriter
      .read(claimPath(settings, generation), spark.sparkContext.hadoopConfiguration)
      .exists(value => decodeClaim(value.bytes) == CandidateClaim(generation, abandoned = true))

  private def generationReachableFromCurrentHead(settings: Settings,
                                                 generation: String,
                                                 candidate: CandidateRecord,
                                                 spark: SparkSession,
                                                 conditionalWriter: ConditionalObjectWriter): Boolean =
    readHead(settings, spark, conditionalWriter) match {
      case None => candidate.parentGeneration.isEmpty
      case Some(head) =>
        val (manifest, readyBytes) = validateReady(settings, head.record.generation, spark)
        require(Arrays.equals(head.record.readyDigest, sha256(readyBytes)),
                s"Redis S3 head digest does not match generation ${head.record.generation}")
        generation == head.record.generation ||
        generation == manifest.stateGeneration ||
        candidate.parentGeneration.contains(head.record.generation)
    }

  private def readHead(settings: Settings,
                       spark: SparkSession,
                       conditionalWriter: ConditionalObjectWriter): Option[VersionedHead] =
    conditionalWriter
      .read(headPath(settings), spark.sparkContext.hadoopConfiguration)
      .map(value => VersionedHead(decodeHead(value.bytes), value.version, value.lastModifiedMillis))

  private def readGenerationLease(settings: Settings,
                                  generation: String,
                                  spark: SparkSession,
                                  conditionalWriter: ConditionalObjectWriter): Option[Long] = {
    val (manifest, readyBytes) = validateReady(settings, generation, spark)
    readLease(generationPaths(settings, generation).lease, manifest, sha256(readyBytes), spark, conditionalWriter)
      .map(_.record.minimumActiveDeadlineMillis)
  }

  private def readLease(path: Path,
                        manifest: ReadyManifest,
                        readyDigest: Array[Byte],
                        spark: SparkSession,
                        conditionalWriter: ConditionalObjectWriter): Option[VersionedLease] =
    conditionalWriter
      .read(path, spark.sparkContext.hadoopConfiguration)
      .map { value =>
        val record = decodeLease(value.bytes)
        validateLease(record, manifest, readyDigest)
        VersionedLease(record, value.version)
      }

  private[redis] def advanceLease(path: Path,
                                  desired: LeaseRecord,
                                  manifest: ReadyManifest,
                                  readyDigest: Array[Byte],
                                  spark: SparkSession,
                                  conditionalWriter: ConditionalObjectWriter): LeaseRecord = {
    validateLease(desired, manifest, readyDigest)
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    var current = conditionalWriter.read(path, hadoopConf)
    var attempts = 0
    while (attempts < MaxAdmissionAttempts) {
      attempts += 1
      val winner = current match {
        case None => conditionalWriter.putIfAbsent(path, encodeLease(desired), hadoopConf)
        case Some(existing) =>
          val existingRecord = decodeLease(existing.bytes)
          validateLease(existingRecord, manifest, readyDigest)
          if (existingRecord.minimumActiveDeadlineMillis >= desired.minimumActiveDeadlineMillis) return existingRecord
          conditionalWriter.compareAndSet(path, existing.version, encodeLease(desired), hadoopConf)
      }
      val winnerRecord = decodeLease(winner.bytes)
      validateLease(winnerRecord, manifest, readyDigest)
      if (winnerRecord.minimumActiveDeadlineMillis >= desired.minimumActiveDeadlineMillis) return winnerRecord
      current = Some(winner)
    }
    throw new IllegalStateException(
      s"Redis lease for generation ${manifest.status.generation} could not advance after $MaxAdmissionAttempts attempts")
  }

  private def headAdvancedPast(settings: Settings,
                               manifest: ReadyManifest,
                               spark: SparkSession,
                               conditionalWriter: ConditionalObjectWriter): Boolean =
    readHead(settings, spark, conditionalWriter).exists { head =>
      if (head.record.generation == manifest.status.generation) false
      else {
        val (newer, readyBytes) = validateReady(settings, head.record.generation, spark)
        require(Arrays.equals(head.record.readyDigest, sha256(readyBytes)),
                s"Redis S3 head digest does not match generation ${head.record.generation}")
        require(
          compareStatus(newer.status, manifest.status) > 0,
          s"Redis S3 head moved from ${manifest.status.generation} to non-newer generation ${newer.status.generation}"
        )
        true
      }
    }

  private def claimHead(settings: Settings,
                        parentGeneration: Option[String],
                        generation: String,
                        readyDigest: Array[Byte],
                        spark: SparkSession,
                        conditionalWriter: ConditionalObjectWriter): HeadRecord = {
    val claimBytes = encodeClaim(CandidateClaim(generation, abandoned = false))
    val claimWinner =
      conditionalWriter.putIfAbsent(claimPath(settings, generation), claimBytes, spark.sparkContext.hadoopConfiguration)
    val claim = decodeClaim(claimWinner.bytes)
    if (claim.abandoned) throw CandidateUnavailable(generation)
    require(claim.generation == generation && Arrays.equals(claimWinner.bytes, claimBytes),
            s"Redis generation $generation has an inconsistent claim fence")
    val candidateRecord = decodeCandidate(readBytes(generationPaths(settings, generation).candidate, spark))
    require(
      candidateRecord.generation == generation && candidateRecord.parentGeneration == parentGeneration,
      s"Redis generation $generation has an inconsistent candidate record"
    )
    val current = readHead(settings, spark, conditionalWriter)
    val headCandidate = HeadRecord(current.fold(1L)(_.record.revision + 1L), generation, parentGeneration, readyDigest)
    require(headCandidate.revision > 0, "Redis S3 head revision overflowed")
    val candidateBytes = encodeHead(headCandidate)
    val winner = (parentGeneration, current) match {
      case (None, None) =>
        conditionalWriter.putIfAbsent(headPath(settings), candidateBytes, spark.sparkContext.hadoopConfiguration)
      case (Some(parent), Some(existing)) if existing.record.generation == parent =>
        conditionalWriter.compareAndSet(headPath(settings),
                                        existing.version,
                                        candidateBytes,
                                        spark.sparkContext.hadoopConfiguration)
      case (_, Some(existing)) => return existing.record
      case (Some(parent), None) =>
        throw new IllegalStateException(s"Redis S3 head for parent $parent disappeared")
    }
    decodeHead(winner.bytes)
  }

  private def writeImmutable(path: Path,
                             contents: Array[Byte],
                             spark: SparkSession,
                             conditionalWriter: ConditionalObjectWriter): Unit = {
    val winner = conditionalWriter.putIfAbsent(path, contents, spark.sparkContext.hadoopConfiguration)
    require(Arrays.equals(winner.bytes, contents),
            s"Immutable Redis S3 record already exists with different contents: $path")
  }

  private def validateReady(settings: Settings,
                            generation: String,
                            spark: SparkSession): (ReadyManifest, Array[Byte]) = {
    val path = generationPaths(settings, generation).ready
    require(pathExists(path, spark), s"Redis S3 head references an incomplete generation: $path")
    val candidatePath = generationPaths(settings, generation).candidate
    require(pathExists(candidatePath, spark), s"Redis S3 generation is missing its candidate record: $candidatePath")
    val candidate = decodeCandidate(readBytes(candidatePath, spark))
    require(candidate.generation == generation, s"Redis candidate record does not identify generation $generation")
    val readyBytes = readBytes(path, spark)
    val manifest = decodeReady(readyBytes)
    require(manifest.status.generation == generation,
            s"Redis generation manifest at $path identifies ${manifest.status.generation}, expected $generation")
    require(candidate.parentGeneration == manifest.parentGeneration,
            s"Redis generation $generation candidate and READY records disagree on their parent")
    require(manifest.sourceTable == settings.sourceTable,
            s"Redis generation $generation was built from ${manifest.sourceTable}, expected ${settings.sourceTable}")
    require(
      manifest.batchDataset == settings.batchDataset,
      s"Redis generation $generation targets dataset ${manifest.batchDataset}, expected ${settings.batchDataset}"
    )
    require(manifest.keyPrefix == settings.keyPrefix,
            s"Redis key prefix cannot change within a lineage: '${manifest.keyPrefix}' != '${settings.keyPrefix}'")
    require(
      manifest.keyHashTagMode == settings.keyHashTagMode,
      s"Redis key hash tag mode cannot change within a lineage: " +
        s"'${manifest.keyHashTagMode}' != '${settings.keyHashTagMode}'"
    )
    require(manifest.stateBuckets > 0, s"Redis generation $generation has no state buckets")
    require(manifest.liveTTLSeconds > 0, s"Redis generation $generation has an invalid TTL")
    require(manifest.deleteFenceTTLSeconds >= manifest.liveTTLSeconds,
            s"Redis generation $generation has a delete-fence TTL shorter than its live-value TTL")
    require(manifest.retired == manifest.status.retired, s"Redis generation $generation has inconsistent retired state")
    require(
      Seq(manifest.sourceRows,
          manifest.dataRows,
          manifest.changedKeys,
          manifest.deletedKeys,
          manifest.deltaRows,
          manifest.tombstoneRows)
        .forall(_ >= 0),
      s"Redis generation manifest contains negative counts: $path"
    )
    if (manifest.retired) {
      require(manifest.sourceRows == 0 && manifest.dataRows == 0,
              s"Retired Redis generation $generation cannot contain active source rows")
    } else {
      require(manifest.sourceRows == manifest.dataRows + 1,
              s"Redis generation manifest row counts are inconsistent: $path")
    }
    require(manifest.deltaRows == manifest.changedKeys + manifest.deletedKeys + 1,
            s"Redis generation manifest delta count is inconsistent: $path")
    if (manifest.tombstoneRows > 0) {
      require(pathExists(generationPaths(settings, generation).tombstones, spark),
              s"Redis generation $generation is missing retained tombstones")
    }
    manifest -> readyBytes
  }

  private def loadState(settings: Settings, generation: String, spark: SparkSession): DataFrame = {
    val path = generationPaths(settings, generation).state
    require(pathExists(path, spark), s"Redis state generation is missing from S3: $path")
    spark.read
      .parquet(path.toString)
      .select(KeyColumn, ValueColumn, DigestColumn, ValueTimestampColumn, ValueWriteEpochColumn, BucketColumn)
  }

  private def loadStateIndex(settings: Settings, generation: String, spark: SparkSession): DataFrame = {
    val path = generationPaths(settings, generation).state
    require(pathExists(path, spark), s"Redis state generation is missing from S3: $path")
    spark.read
      .parquet(path.toString)
      .select(KeyColumn, DigestColumn, ValueTimestampColumn, ValueWriteEpochColumn, BucketColumn)
  }

  private def loadTombstones(settings: Settings, manifest: ReadyManifest, spark: SparkSession): DataFrame = {
    if (manifest.tombstoneRows == 0) return emptyTombstones(spark)
    val path = generationPaths(settings, manifest.status.generation).tombstones
    require(pathExists(path, spark), s"Redis tombstones are missing from S3: $path")
    spark.read
      .parquet(path.toString)
      .select(KeyColumn, BatchTimestampColumn, WriteEpochColumn, TombstoneExpiresAtColumn, BucketColumn)
  }

  private def loadCommittedTombstones(settings: Settings,
                                      manifest: ReadyManifest,
                                      spark: SparkSession,
                                      conditionalWriter: ConditionalObjectWriter): DataFrame = {
    val applied = committedApplied(settings, manifest, spark, conditionalWriter)
    loadTombstones(settings, manifest, spark)
      .withColumn(
        TombstoneExpiresAtColumn,
        when(col(TombstoneExpiresAtColumn) === lit(PendingTombstoneExpiry),
             lit(applied.tombstoneRetainUntilMillis(manifest.deleteFenceTTLSeconds)))
          .otherwise(col(TombstoneExpiresAtColumn))
      )
  }

  private def committedApplied(settings: Settings,
                               manifest: ReadyManifest,
                               spark: SparkSession,
                               conditionalWriter: ConditionalObjectWriter): CommittedApplied = {
    val paths = generationPaths(settings, manifest.status.generation)
    require(pathExists(paths.applied, spark),
            s"Redis committed generation ${manifest.status.generation} has no APPLIED record")
    val readyBytes = readBytes(paths.ready, spark)
    val applied = readCommittedApplied(paths.applied, spark, conditionalWriter)
    validateApplied(applied.record, manifest, sha256(readyBytes))
    applied
  }

  private def loadDelta(settings: Settings, generation: String, spark: SparkSession): DataFrame = {
    val path = generationPaths(settings, generation).delta
    require(pathExists(path, spark), s"Redis generation delta is missing from S3: $path")
    spark.read
      .parquet(path.toString)
      .select(KeyColumn,
              ValueColumn,
              OperationColumn,
              BatchTimestampColumn,
              WriteEpochColumn,
              BucketColumn,
              TombstoneExpiresAtColumn)
  }

  private def metadataMatches(currentMetadata: DataFrame,
                              settings: Settings,
                              manifest: ReadyManifest,
                              spark: SparkSession): Boolean = {
    val previous = loadDelta(settings, manifest.status.generation, spark)
      .where(isServingInfo(col(KeyColumn)))
      .select(ValueColumn)
      .collect()
    require(previous.length == 1,
            s"Redis generation ${manifest.status.generation} must contain exactly one serving-info row")
    Arrays.equals(currentMetadata.select(ValueColumn).head().getAs[Array[Byte]](0), previous.head.getAs[Array[Byte]](0))
  }

  private def writeStatusWithOomRetry(client: JedisCluster,
                                      statusKey: String,
                                      status: BatchStatus,
                                      expireAtMillis: Long,
                                      settings: Settings,
                                      leaseDeadlineMillis: Long): Long = {
    val retryStartMillis = System.currentTimeMillis()
    val retryDeadlineMillis =
      redisOomRetryDeadline(retryStartMillis, DefaultBulkUploadOomRetryTimeoutMs, leaseDeadlineMillis)
    withRedisOomRetry(
      operation = () => {
        remainingLeaseSeconds(leaseDeadlineMillis)
        RedisBatchUpload.writeStatusUntil(client, statusKey, status, expireAtMillis)
      },
      retryStartMillis = retryStartMillis,
      retryDeadlineMillis = retryDeadlineMillis,
      retryWaitMillis = attempt => jitteredOomRetryBackoffMillis(attempt, settings.retryBackoffMs),
      nowMillis = () => System.currentTimeMillis(),
      sleepMillis = millis => interruptibleSleep(millis),
      onRetry = event => logRedisOomRetry(settings.batchDataset, "status", batchSize = 1, event)
    )
  }

  private def writeToRedis(df: DataFrame,
                           mode: WriteMode,
                           settings: Settings,
                           limiterKey: String,
                           leaseDeadlineMillis: Long,
                           tombstoneDeadlineMillis: Long = 0L,
                           progress: Option[WriteProgress] = None): WriteSummary = {
    val clientSettings = settings.clientSettings.bulkWriter
    val batchDataset = settings.batchDataset
    val keyPrefix = settings.keyPrefix
    val keyHashTagMode = settings.keyHashTagMode
    val batchSize = settings.batchSize
    val maxKeysPerSecond = settings.maxKeysPerSecond
    val maxRetries = settings.maxRetries
    val retryBackoffMs = settings.retryBackoffMs
    // This absolute driver-created deadline is captured by every task attempt in the Spark stage. A retried task cannot
    // start a fresh OOM window and multiply the 30-minute timeout by Spark's task retry count.
    val oomRetryStartMillis = System.currentTimeMillis()
    val oomRetryDeadlineMillis =
      redisOomRetryDeadline(oomRetryStartMillis, DefaultBulkUploadOomRetryTimeoutMs, leaseDeadlineMillis)
    val effectiveTombstoneDeadline =
      if (tombstoneDeadlineMillis > 0) tombstoneDeadlineMillis else leaseDeadlineMillis

    val sparkContext = df.sparkSession.sparkContext
    val activeProgress = progress.filter(_.totalKeys.forall(_ > 0))
    val progressAccumulator = activeProgress.map { spec =>
      sparkContext.longAccumulator(s"redis-upload-${spec.generation}-${spec.phase}")
    }
    val progressReporter = activeProgress.zip(progressAccumulator).map { case (spec, accumulator) =>
      new UploadProgressReporter(batchDataset,
                                 writeModeName(mode),
                                 spec,
                                 accumulator.id,
                                 settings.writerPartitions,
                                 batchSize)
    }
    progressReporter.foreach(sparkContext.addSparkListener)

    try {
      progressReporter.foreach(_.start())
      val summaries = df
        .repartition(settings.writerPartitions, col(BucketColumn))
        .rdd
        .mapPartitions { rows =>
          if (!rows.hasNext) Iterator.empty
          else {
            val taskClient = new TaskClient(clientSettings)
            var total = WriteSummary(0, 0, 0, 0, 0)
            try {
              rows.grouped(batchSize).foreach { batch =>
                val batchSummary = writeBatch(
                  taskClient,
                  batch,
                  mode,
                  batchDataset,
                  keyPrefix,
                  keyHashTagMode,
                  limiterKey,
                  batchSize,
                  maxKeysPerSecond,
                  maxRetries,
                  retryBackoffMs,
                  oomRetryStartMillis,
                  oomRetryDeadlineMillis,
                  leaseDeadlineMillis,
                  effectiveTombstoneDeadline
                )
                total = total + batchSummary
                progressAccumulator.foreach(_.add(batchSummary.attempted))
              }
              Iterator.single(total)
            } finally {
              taskClient.close()
            }
          }
        }
        .collect()

      val summary = summaries.foldLeft(WriteSummary(0, 0, 0, 0, 0))(_ + _)
      activeProgress.foreach { spec =>
        spec.totalKeys.foreach { expected =>
          require(summary.attempted == expected,
                  s"Redis upload phase ${spec.phase} attempted ${summary.attempted} keys; expected $expected")
        }
      }
      progressReporter.foreach(_.complete(summary.attempted, summary.retries))
      summary
    } finally {
      progressReporter.foreach { reporter =>
        sparkContext.removeSparkListener(reporter)
        reporter.close()
      }
    }
  }

  private def writeBatch(taskClient: TaskClient,
                         batch: Seq[Row],
                         mode: WriteMode,
                         dataset: String,
                         keyPrefix: String,
                         keyHashTagMode: String,
                         limiterKey: String,
                         maxBurst: Int,
                         maxKeysPerSecond: Int,
                         maxRetries: Int,
                         retryBackoffMs: Long,
                         oomRetryStartMillis: Long,
                         oomRetryDeadlineMillis: Long,
                         leaseDeadlineMillis: Long,
                         tombstoneDeadlineMillis: Long): WriteSummary = {
    var attempt = 0
    var lastFailure: Throwable = null
    var directReplay = false
    var refreshClient = false
    var pipelineMovedFailures = 0
    var oomRetries = 0L
    while (attempt <= maxRetries) {
      if (attempt > 0 && retryBackoffMs > 0) {
        val multiplier = 1L << math.min(attempt - 1, 20)
        val cap = java.lang.Math.multiplyExact(retryBackoffMs, multiplier)
        val waitMillis = ThreadLocalRandom.current().nextLong(cap + 1L)
        requireWaitWithinLease(waitMillis, leaseDeadlineMillis, System.currentTimeMillis())
        if (waitMillis > 0) Thread.sleep(waitMillis)
      }

      try {
        val counted = withRedisOomRetry(
          operation = () => {
            remainingLeaseSeconds(leaseDeadlineMillis)
            if (refreshClient) {
              taskClient.refresh()
              refreshClient = false
            }
            acquireRate(taskClient.client, limiterKey, batch.size, maxKeysPerSecond, maxBurst, leaseDeadlineMillis)
            remainingLeaseSeconds(leaseDeadlineMillis)
            val results =
              if (directReplay)
                executeDirect(taskClient.client,
                              batch,
                              mode,
                              dataset,
                              keyPrefix,
                              keyHashTagMode,
                              leaseDeadlineMillis,
                              tombstoneDeadlineMillis)
              else
                executePipeline(taskClient.client,
                                batch,
                                mode,
                                dataset,
                                keyPrefix,
                                keyHashTagMode,
                                leaseDeadlineMillis,
                                tombstoneDeadlineMillis)
            countResults(results, mode)
          },
          retryStartMillis = oomRetryStartMillis,
          retryDeadlineMillis = oomRetryDeadlineMillis,
          retryWaitMillis = oomAttempt => jitteredOomRetryBackoffMillis(oomAttempt, retryBackoffMs),
          nowMillis = () => System.currentTimeMillis(),
          sleepMillis = millis => interruptibleSleep(millis),
          onRetry = event => {
            oomRetries += 1
            logRedisOomRetry(dataset, writeModeName(mode), batch.size, event)
          }
        )
        return counted.copy(retries = attempt + oomRetries)
      } catch {
        case error if retryableRedisFailure(error) =>
          lastFailure = error
          val failedMode = if (directReplay) "direct" else "pipeline"
          if (!directReplay && error.isInstanceOf[JedisMovedDataException]) pipelineMovedFailures += 1
          attempt += 1
          val attemptsRemaining = maxRetries + 1 - attempt
          directReplay = directReplay || directReplayRequired(error, pipelineMovedFailures, attemptsRemaining)
          refreshClient = refreshClient || refreshRequired(error)
          val nextMode = if (attemptsRemaining == 0) "none" else if (directReplay) "direct" else "pipeline"
          logger.warn(
            s"Redis command batch failed dataset=$dataset operation=${writeModeName(mode)} batchSize=${batch.size} " +
              s"failedMode=$failedMode; attempt $attempt of ${maxRetries + 1}, attemptsRemaining=$attemptsRemaining " +
              s"nextMode=$nextMode refreshClient=$refreshClient",
            error
          )
      }
    }
    throw new RuntimeException(s"Redis command batch failed after ${maxRetries + 1} attempts", lastFailure)
  }

  private def executePipeline(client: JedisCluster,
                              batch: Seq[Row],
                              mode: WriteMode,
                              dataset: String,
                              keyPrefix: String,
                              keyHashTagMode: String,
                              leaseDeadlineMillis: Long,
                              tombstoneDeadlineMillis: Long): Seq[Any] = {
    val pipeline = client.pipelined()
    val responses = completePipeline(pipeline) {
      batch.map(row =>
        enqueueCommand(pipeline,
                       row,
                       mode,
                       dataset,
                       keyPrefix,
                       keyHashTagMode,
                       leaseDeadlineMillis,
                       tombstoneDeadlineMillis))
    }
    val (results, replayedFailures) = resolvePipelineResponses(batch, responses) { row =>
      executeCommand(client,
                     row,
                     mode,
                     dataset,
                     keyPrefix,
                     keyHashTagMode,
                     leaseDeadlineMillis,
                     tombstoneDeadlineMillis)
    }
    if (replayedFailures.nonEmpty) {
      val causes = replayedFailures
        .groupBy(_.getClass.getSimpleName)
        .toSeq
        .sortBy(_._1)
        .map { case (name, failures) => s"$name:${failures.size}" }
        .mkString(",")
      logger.warn(
        s"Redis pipeline replayed affected commands directly dataset=$dataset operation=${writeModeName(mode)} " +
          s"batchSize=${batch.size} replayCount=${replayedFailures.size} causes=$causes")
    }
    results
  }

  private[redis] def completePipeline(pipeline: ClusterPipeline)(enqueue: => Seq[Response[_]]): Seq[Response[_]] =
    try enqueue
    finally {
      // close is both the sync and the connection-release step. A second sync after a failed one can make Jedis
      // revisit response queues whose node connections it already removed and fail on its executor thread.
      pipeline.close()
    }

  private[redis] def resolvePipelineResponses[T](items: Seq[T], responses: Seq[Response[_]])(
      directReplay: T => Any): (Seq[Any], Seq[Throwable]) = {
    require(items.size == responses.size,
            s"Redis pipeline returned ${responses.size} responses for ${items.size} commands")
    val results = Array.fill[Any](items.size)(null)
    val replayBuilder = Vector.newBuilder[(Int, T, Throwable)]

    items.iterator.zip(responses.iterator).zipWithIndex.foreach { case ((item, response), index) =>
      try results(index) = response.get()
      catch {
        case error if pipelineResponseDirectReplayRequired(error) =>
          replayBuilder += ((index, item, error))
      }
    }

    val replays = replayBuilder.result()
    replays.foreach { case (index, item, _) => results(index) = directReplay(item) }
    (results.toIndexedSeq, replays.map(_._3))
  }

  private def executeDirect(client: JedisCluster,
                            batch: Seq[Row],
                            mode: WriteMode,
                            dataset: String,
                            keyPrefix: String,
                            keyHashTagMode: String,
                            leaseDeadlineMillis: Long,
                            tombstoneDeadlineMillis: Long): Seq[Any] =
    batch.map(row =>
      executeCommand(client,
                     row,
                     mode,
                     dataset,
                     keyPrefix,
                     keyHashTagMode,
                     leaseDeadlineMillis,
                     tombstoneDeadlineMillis))

  private def enqueueCommand(pipeline: ClusterPipeline,
                             row: Row,
                             mode: WriteMode,
                             dataset: String,
                             keyPrefix: String,
                             keyHashTagMode: String,
                             leaseDeadlineMillis: Long,
                             tombstoneDeadlineMillis: Long): Response[_] = {
    val key = redisKey(row, dataset, keyPrefix, keyHashTagMode)
    mode match {
      case MutationWrite =>
        val (timestamp, writeEpoch, encoded, expireAt, allowExpired) =
          encodedMutation(row, leaseDeadlineMillis, tombstoneDeadlineMillis)
        pipeline.evalsha(RedisBatchUpload.putBaseIfNewerSha,
                         List(key).asJava,
                         List(timestamp, writeEpoch, encoded, expireAt, allowExpired).asJava)
      case LeaseRefresh(Some(expectedDeadline)) =>
        pipeline.evalsha(
          RedisBatchUpload.validateExpiryAndAdvanceSha,
          List(key).asJava,
          expiryValidationArgs(row, expectedDeadline, leaseDeadlineMillis).asJava
        )
      case LeaseRefresh(None) =>
        pipeline.evalsha(RedisBatchUpload.validateAndExpireSha,
                         List(key).asJava,
                         leaseValidationArgs(row, leaseDeadlineMillis).asJava)
    }
  }

  private def executeCommand(client: JedisCluster,
                             row: Row,
                             mode: WriteMode,
                             dataset: String,
                             keyPrefix: String,
                             keyHashTagMode: String,
                             leaseDeadlineMillis: Long,
                             tombstoneDeadlineMillis: Long): Any = {
    val key = redisKey(row, dataset, keyPrefix, keyHashTagMode)
    mode match {
      case MutationWrite =>
        val (timestamp, writeEpoch, encoded, expireAt, allowExpired) =
          encodedMutation(row, leaseDeadlineMillis, tombstoneDeadlineMillis)
        RedisBatchUpload.putBaseIfNewer(client, key, List(timestamp, writeEpoch, encoded, expireAt, allowExpired))
      case LeaseRefresh(Some(expectedDeadline)) =>
        RedisBatchUpload.validateExpiryAndAdvance(client,
                                                  key,
                                                  expiryValidationArgs(row, expectedDeadline, leaseDeadlineMillis))
      case LeaseRefresh(None) =>
        RedisBatchUpload.validateAndExpire(client, key, leaseValidationArgs(row, leaseDeadlineMillis))
    }
  }

  private def redisKey(row: Row, dataset: String, keyPrefix: String, keyHashTagMode: String): Array[Byte] =
    RedisKVStore
      .buildRedisKey(row.getAs[Array[Byte]](KeyColumn).toSeq,
                     dataset,
                     keyPrefix = keyPrefix,
                     hashTagMode = keyHashTagMode)
      .getBytes(StandardCharsets.UTF_8)

  private def encodedMutation(
      row: Row,
      leaseDeadlineMillis: Long,
      tombstoneDeadlineMillis: Long): (Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte]) = {
    val operation = Operation.fromName(row.getAs[String](OperationColumn))
    val timestamp = row.getAs[Long](BatchTimestampColumn)
    val writeEpoch = row.getAs[Long](WriteEpochColumn)
    val payload = row.getAs[Array[Byte]](ValueColumn)
    val encoded = RedisBatchUpload.encodeValue(operation, timestamp, payload, writeEpoch)
    val explicitTombstoneDeadline = Try(row.fieldIndex(TombstoneExpiresAtColumn)).toOption.flatMap { index =>
      if (row.isNullAt(index)) None else Some(row.getLong(index))
    }
    val expireAtMillis =
      if (operation == Delete) {
        explicitTombstoneDeadline match {
          case Some(PendingTombstoneExpiry) => tombstoneDeadlineMillis
          case Some(deadline)               => deadline
          case None                         => tombstoneDeadlineMillis
        }
      } else leaseDeadlineMillis
    require(expireAtMillis > 0, s"Redis mutation deadline must be positive: $expireAtMillis")
    val allowExpired = Try(row.fieldIndex(AllowExpiredMutationColumn)).toOption.exists { index =>
      !row.isNullAt(index) && row.getBoolean(index)
    }
    (timestamp.toString.getBytes(StandardCharsets.UTF_8),
     writeEpoch.toString.getBytes(StandardCharsets.UTF_8),
     encoded,
     expireAtMillis.toString.getBytes(StandardCharsets.UTF_8),
     (if (allowExpired) "1" else "0").getBytes(StandardCharsets.UTF_8))
  }

  private def leaseValidationArgs(row: Row, leaseDeadlineMillis: Long): Seq[Array[Byte]] =
    Seq(
      row.getAs[Long](ValueTimestampColumn).toString.getBytes(StandardCharsets.UTF_8),
      row.getAs[Long](ValueWriteEpochColumn).toString.getBytes(StandardCharsets.UTF_8),
      leaseDeadlineMillis.toString.getBytes(StandardCharsets.UTF_8)
    )

  private def expiryValidationArgs(row: Row,
                                   expectedDeadlineMillis: Long,
                                   desiredDeadlineMillis: Long): Seq[Array[Byte]] =
    Seq(expectedDeadlineMillis,
        desiredDeadlineMillis,
        row.getAs[Long](ValueTimestampColumn),
        row.getAs[Long](ValueWriteEpochColumn))
      .map(_.toString.getBytes(StandardCharsets.UTF_8))

  private def countResults(results: Seq[Any], mode: WriteMode): WriteSummary = {
    mode match {
      case MutationWrite =>
        val codes = results.map(redisLong)
        WriteSummary(codes.size, codes.count(_ == 1L), codes.count(_ == 2L), codes.count(_ == 0L), retries = 0)
      case _: LeaseRefresh =>
        val codes = results.map(redisLong)
        WriteSummary(codes.size, codes.count(_ == 1L), codes.count(_ == 2L), codes.count(_ == 0L), retries = 0)
    }
  }

  private def redisLong(value: Any): Long = value match {
    case number: java.lang.Long => number.longValue()
    case other => throw new IllegalStateException(s"Redis command returned unexpected response: $other")
  }

  private[redis] def redisOutOfMemoryFailure(error: Throwable): Boolean = {
    val canonicalMessage = "OOM COMMAND NOT ALLOWED WHEN USED MEMORY > 'MAXMEMORY'"
    var current = error
    var depth = 0
    var redisOom = false
    while (current != null && depth < 32) {
      if (current.isInstanceOf[OutOfMemoryError]) return false
      current match {
        case dataError: JedisDataException =>
          val message = Option(dataError.getMessage).getOrElse("").trim.toUpperCase(Locale.ROOT)
          redisOom = redisOom || message == "OOM" || message.startsWith("OOM ") ||
            message == "ERR OOM" || message.startsWith("ERR OOM ") || message.contains(canonicalMessage)
        case _ =>
      }
      val cause = current.getCause
      current = if (cause eq current) null else cause
      depth += 1
    }
    redisOom
  }

  private[redis] def withRedisOomRetry[T](operation: () => T,
                                          retryStartMillis: Long,
                                          retryDeadlineMillis: Long,
                                          retryWaitMillis: Int => Long,
                                          nowMillis: () => Long,
                                          sleepMillis: Long => Unit,
                                          onRetry: OomRetryEvent => Unit): T = {
    require(retryDeadlineMillis >= retryStartMillis,
            s"Redis OOM retry deadline $retryDeadlineMillis precedes its start $retryStartMillis")
    var oomAttempt = 0
    var lastOom: Throwable = null
    while (true) {
      val beforeAttemptMillis = nowMillis()
      if (lastOom != null && beforeAttemptMillis >= retryDeadlineMillis) {
        throw redisOomRetryTimeout(retryStartMillis, beforeAttemptMillis, lastOom)
      }
      try {
        return operation()
      } catch {
        case error if redisOutOfMemoryFailure(error) =>
          lastOom = error
          oomAttempt += 1
          val retryAtMillis = nowMillis()
          val remainingMillis = retryDeadlineMillis - retryAtMillis
          if (remainingMillis <= 0) throw redisOomRetryTimeout(retryStartMillis, retryAtMillis, error)
          val requestedWaitMillis = retryWaitMillis(oomAttempt)
          require(requestedWaitMillis >= 0, s"Redis OOM retry wait must be non-negative: $requestedWaitMillis")
          val waitMillis = math.min(requestedWaitMillis, remainingMillis)
          onRetry(
            OomRetryEvent(oomAttempt, waitMillis, math.max(0L, retryAtMillis - retryStartMillis), remainingMillis))
          if (waitMillis > 0) sleepMillis(waitMillis)
      }
    }
    throw new IllegalStateException("Redis OOM retry loop terminated unexpectedly")
  }

  private[redis] def redisOomRetryDeadline(startMillis: Long, timeoutMillis: Long, leaseDeadlineMillis: Long): Long = {
    require(timeoutMillis >= 0, s"Redis OOM retry timeout must be non-negative: $timeoutMillis")
    val timeoutDeadline =
      if (startMillis > Long.MaxValue - timeoutMillis) Long.MaxValue else startMillis + timeoutMillis
    math.min(timeoutDeadline, leaseDeadlineMillis)
  }

  private[redis] def cappedOomRetryBackoffMillis(attempt: Int, initialBackoffMillis: Long): Long = {
    require(attempt > 0, s"Redis OOM retry attempt must be positive: $attempt")
    require(initialBackoffMillis >= 0, s"Redis OOM retry backoff must be non-negative: $initialBackoffMillis")
    var cap = math.min(math.max(1L, initialBackoffMillis), DefaultBulkUploadOomRetryMaxBackoffMs)
    var currentAttempt = 1
    while (currentAttempt < attempt && cap < DefaultBulkUploadOomRetryMaxBackoffMs) {
      cap = math.min(DefaultBulkUploadOomRetryMaxBackoffMs, cap * 2L)
      currentAttempt += 1
    }
    cap
  }

  private def jitteredOomRetryBackoffMillis(attempt: Int, initialBackoffMillis: Long): Long = {
    val cap = cappedOomRetryBackoffMillis(attempt, initialBackoffMillis)
    val lowerBound = (cap + 1L) / 2L
    if (lowerBound == cap) cap
    else ThreadLocalRandom.current().nextLong(lowerBound, cap + 1L)
  }

  private def redisOomRetryTimeout(startMillis: Long, nowMillis: Long, cause: Throwable): RuntimeException =
    new RuntimeException(
      s"Redis remained above maxmemory for ${math.max(0L, nowMillis - startMillis)} ms; " +
        "external scaling or memory release did not complete before the OOM retry or publication lease deadline",
      cause
    )

  private def interruptibleSleep(millis: Long): Unit =
    try Thread.sleep(millis)
    catch {
      case interrupted: InterruptedException =>
        Thread.currentThread().interrupt()
        throw interrupted
    }

  private def logRedisOomRetry(dataset: String, operation: String, batchSize: Int, event: OomRetryEvent): Unit = {
    val task = Option(TaskContext.get())
      .map(context =>
        s"stage=${context.stageId()} partition=${context.partitionId()} " +
          s"taskAttempt=${context.attemptNumber()} taskAttemptId=${context.taskAttemptId()}")
      .getOrElse("driver")
    logger.warn(
      s"Redis maxmemory OOM dataset=$dataset operation=$operation batchSize=$batchSize $task; " +
        s"OOM retry ${event.attempt} in ${event.waitMillis} ms " +
        s"(elapsed=${event.elapsedMillis} ms remaining=${event.remainingMillis} ms) while waiting for external " +
        "scaling or memory release")
  }

  private def writeModeName(mode: WriteMode): String = mode match {
    case MutationWrite   => "mutation"
    case _: LeaseRefresh => "lease-refresh"
  }

  private[redis] def retryableRedisFailure(error: Throwable): Boolean = error match {
    case _: JedisConnectionException       => true
    case _: JedisClusterOperationException => true
    case _: JedisRedirectionException      => true
    case _: JedisNoScriptException         => true
    case dataError: JedisDataException     => retryableDataError(dataError)
    case stateError: IllegalStateException => unsetPipelineResponse(stateError)
    case _                                 => false
  }

  private[redis] def directReplayRequired(error: Throwable,
                                          pipelineMovedFailures: Int,
                                          attemptsRemaining: Int): Boolean = {
    require(pipelineMovedFailures >= 0, s"Pipeline MOVED failures must be non-negative: $pipelineMovedFailures")
    require(attemptsRemaining >= 0, s"Redis attempts remaining must be non-negative: $attemptsRemaining")
    error match {
      // Response-level MOVED failures are normally replayed selectively. If one escapes that recovery, refresh
      // topology and re-pipeline once while preserving a final redirect-aware direct attempt.
      case _: JedisMovedDataException => pipelineMovedFailures > 1 || attemptsRemaining <= 1
      case _                          => pipelineResponseDirectReplayRequired(error)
    }
  }

  private[redis] def pipelineResponseDirectReplayRequired(error: Throwable): Boolean = error match {
    case _: JedisRedirectionException      => true
    case _: JedisNoScriptException         => true
    case stateError: IllegalStateException => unsetPipelineResponse(stateError)
    case _                                 => false
  }

  private def refreshRequired(error: Throwable): Boolean = error match {
    case _: JedisConnectionException       => true
    case _: JedisClusterOperationException => true
    case _: JedisRedirectionException      => true
    case dataError: JedisDataException =>
      val message = Option(dataError.getMessage).getOrElse("").toUpperCase(Locale.ROOT)
      Seq("CLUSTERDOWN", "MASTERDOWN", "READONLY").exists(message.startsWith)
    case stateError: IllegalStateException => unsetPipelineResponse(stateError)
    case _                                 => false
  }

  private def unsetPipelineResponse(error: IllegalStateException): Boolean =
    Option(error.getMessage).contains(UnsetPipelineResponseMessage)

  private def retryableDataError(error: JedisDataException): Boolean = {
    val message = Option(error.getMessage).getOrElse("").toUpperCase(Locale.ROOT)
    Seq("CLUSTERDOWN", "LOADING", "MASTERDOWN", "READONLY", "TRYAGAIN").exists(message.startsWith)
  }

  private def acquireRate(client: JedisCluster,
                          limiterKey: String,
                          keyCount: Int,
                          maxKeysPerSecond: Int,
                          maxBurst: Int,
                          leaseDeadlineMillis: Long): Unit = {
    val waitMillis = RedisBatchUpload.reserveRate(client, limiterKey, keyCount, maxKeysPerSecond, maxBurst)
    requireWaitWithinLease(waitMillis, leaseDeadlineMillis, System.currentTimeMillis())
    if (waitMillis > 0) Thread.sleep(waitMillis)
  }

  private[redis] def requireWaitWithinLease(waitMillis: Long, leaseDeadlineMillis: Long, nowMillis: Long): Unit = {
    require(waitMillis >= 0, s"Redis upload wait must be non-negative: $waitMillis")
    val remainingMillis = leaseDeadlineMillis - nowMillis
    require(
      remainingMillis > waitMillis,
      s"Redis upload cannot wait $waitMillis ms with only $remainingMillis ms remaining in its configured TTL"
    )
  }

  private def uploadDeadlineMillis(ttlSeconds: Int): Long =
    java.lang.Math.addExact(System.currentTimeMillis(), java.lang.Math.multiplyExact(ttlSeconds.toLong, 1000L))

  private[redis] def generationLeaseDeadline(candidateDeadlineMillis: Long,
                                             currentDeadlineMillis: Option[Long]): Long = {
    require(candidateDeadlineMillis > 0, s"Redis candidate lease deadline must be positive: $candidateDeadlineMillis")
    math.max(candidateDeadlineMillis, currentDeadlineMillis.getOrElse(0L))
  }

  private[redis] def renewedLeaseDeadline(candidateDeadlineMillis: Long, currentDeadlineMillis: Option[Long]): Long = {
    require(candidateDeadlineMillis > 0, s"Redis candidate lease deadline must be positive: $candidateDeadlineMillis")
    currentDeadlineMillis
      .map(current => math.max(candidateDeadlineMillis, java.lang.Math.addExact(current, 1L)))
      .getOrElse(candidateDeadlineMillis)
  }

  private[redis] def leaseRefreshRequired(minimumActiveDeadlineMillis: Option[Long],
                                          ttlSeconds: Int,
                                          nowMillis: Long): Boolean = {
    require(ttlSeconds > 0, s"Redis live-key TTL must be positive: $ttlSeconds")
    require(nowMillis >= 0, s"Redis lease check time must be non-negative: $nowMillis")
    val renewalWindowMillis = java.lang.Math.multiplyExact(ttlSeconds.toLong, 1000L) / 2L
    minimumActiveDeadlineMillis match {
      case None => true
      case Some(deadlineMillis) =>
        require(deadlineMillis > 0, s"Redis active lease deadline must be positive: $deadlineMillis")
        deadlineMillis <= nowMillis || deadlineMillis - nowMillis <= renewalWindowMillis
    }
  }

  private def remainingLeaseSeconds(deadlineMillis: Long): Int = {
    val remainingMillis = deadlineMillis - System.currentTimeMillis()
    require(remainingMillis > 0, "Redis upload exceeded its configured TTL")
    math.max(1L, (remainingMillis + 999L) / 1000L).toInt
  }

  private def compareStatus(left: BatchStatus, right: BatchStatus): Int = {
    val timestamp = java.lang.Long.compare(left.batchTimestamp, right.batchTimestamp)
    if (timestamp != 0) timestamp else java.lang.Long.compare(left.writeEpoch, right.writeEpoch)
  }

  private def validateConditionalWriter(settings: Settings, writer: ConditionalObjectWriter): Unit = {
    val scheme = Option(new Path(settings.stateRoot).toUri.getScheme).map(_.toLowerCase(Locale.ROOT))
    require(
      !scheme.exists(value => value == "s3" || value == "s3a" || value == "s3n") ||
        writer != HadoopConditionalObjectWriter,
      "Redis S3 state requires a real conditional object writer; Hadoop create(overwrite=false) is not a distributed CAS"
    )
  }

  private def validateApplied(record: AppliedRecord, manifest: ReadyManifest, digest: Array[Byte]): Unit = {
    require(record.status == manifest.status,
            s"Redis applied record does not match generation ${manifest.status.generation}")
    require(Arrays.equals(record.readyDigest, digest),
            s"Redis applied record digest does not match generation ${manifest.status.generation}")
    require(record.mutationKeys >= manifest.deltaRows,
            s"Redis applied record contains too few mutations for generation ${manifest.status.generation}")
    require(record.refreshedKeys >= 0,
            s"Redis applied record contains a negative refresh count for generation ${manifest.status.generation}")
  }

  private def validateLease(record: LeaseRecord, manifest: ReadyManifest, digest: Array[Byte]): Unit = {
    require(record.status == manifest.status,
            s"Redis lease record does not match generation ${manifest.status.generation}")
    require(Arrays.equals(record.readyDigest, digest),
            s"Redis lease record digest does not match generation ${manifest.status.generation}")
    if (manifest.retired) {
      require(record.minimumActiveDeadlineMillis == 0L,
              s"Retired Redis generation ${manifest.status.generation} has an active lease deadline")
    } else {
      require(record.minimumActiveDeadlineMillis > 0L,
              s"Redis generation ${manifest.status.generation} has an invalid active lease deadline")
    }
  }

  private def encodeCandidate(candidate: CandidateRecord): Array[Byte] = binaryRecord { out =>
    out.writeInt(CandidateMagic)
    out.writeInt(CandidateVersion)
    out.writeUTF(candidate.generation)
    writeOptionalUtf(out, candidate.parentGeneration)
    out.writeLong(candidate.createdAtMillis)
    out.writeLong(candidate.cleanupAfterMillis)
  }

  private def decodeCandidate(bytes: Array[Byte]): CandidateRecord = readRecord(bytes) { in =>
    require(in.readInt() == CandidateMagic, "Redis candidate record has an invalid magic header")
    require(in.readInt() == CandidateVersion, "Redis candidate record has an unsupported version")
    val generation = in.readUTF()
    require(RedisBatchUpload.isSafeIdentifier(generation), s"Invalid Redis candidate generation: $generation")
    val parent = readOptionalUtf(in)
    parent.foreach(value =>
      require(RedisBatchUpload.isSafeIdentifier(value), s"Invalid Redis candidate parent generation: $value"))
    val createdAtMillis = in.readLong()
    val cleanupAfterMillis = in.readLong()
    require(createdAtMillis > 0, s"Redis candidate $generation has an invalid creation time")
    require(cleanupAfterMillis > createdAtMillis, s"Redis candidate $generation has an invalid cleanup deadline")
    CandidateRecord(generation, parent, createdAtMillis, cleanupAfterMillis)
  }

  private[redis] def encodeClaim(claim: CandidateClaim): Array[Byte] = binaryRecord { out =>
    out.writeInt(ClaimMagic)
    out.writeInt(ClaimVersion)
    out.writeUTF(claim.generation)
    out.writeBoolean(claim.abandoned)
  }

  private[redis] def decodeClaim(bytes: Array[Byte]): CandidateClaim = readRecord(bytes) { in =>
    require(in.readInt() == ClaimMagic, "Redis candidate claim has an invalid magic header")
    require(in.readInt() == ClaimVersion, "Redis candidate claim has an unsupported version")
    val generation = in.readUTF()
    require(RedisBatchUpload.isSafeIdentifier(generation), s"Invalid Redis claim generation: $generation")
    CandidateClaim(generation, in.readBoolean())
  }

  private[redis] def encodeReady(manifest: ReadyManifest): Array[Byte] = binaryRecord { out =>
    val status = RedisBatchUpload.encodeStatus(manifest.status)
    out.writeInt(ReadyMagic)
    out.writeInt(ReadyVersion)
    out.writeInt(status.length)
    out.write(status)
    writeOptionalUtf(out, manifest.parentGeneration)
    out.writeUTF(manifest.stateGeneration)
    out.writeUTF(manifest.sourceTable)
    out.writeUTF(manifest.batchDataset)
    out.writeUTF(manifest.keyPrefix)
    out.writeUTF(manifest.keyHashTagMode)
    out.writeInt(manifest.stateBuckets)
    out.writeLong(manifest.sourceRows)
    out.writeLong(manifest.dataRows)
    out.writeLong(manifest.changedKeys)
    out.writeLong(manifest.deletedKeys)
    out.writeLong(manifest.deltaRows)
    out.writeLong(manifest.tombstoneRows)
    out.writeInt(manifest.liveTTLSeconds)
    out.writeInt(manifest.deleteFenceTTLSeconds)
    out.writeBoolean(manifest.fullRebuild)
    out.writeBoolean(manifest.retired)
  }

  private[redis] def decodeReady(bytes: Array[Byte]): ReadyManifest = readRecord(bytes) { in =>
    require(in.readInt() == ReadyMagic, "Redis READY record has an invalid magic header")
    val version = in.readInt()
    require(version == LegacyReadyVersion || version == HashTagReadyVersion || version == ReadyVersion,
            s"Redis READY record has an unsupported version: $version")
    val statusLength = in.readInt()
    require(statusLength > 0 && statusLength <= 1024 * 1024, s"Redis READY status length is invalid: $statusLength")
    val statusBytes = new Array[Byte](statusLength)
    in.readFully(statusBytes)
    val status = RedisBatchUpload.decodeStatus(statusBytes).get
    val parentGeneration = readOptionalUtf(in)
    val stateGeneration = in.readUTF()
    val sourceTable = in.readUTF()
    val batchDataset = in.readUTF()
    val keyPrefix = in.readUTF()
    val keyHashTagMode =
      if (version >= HashTagReadyVersion) in.readUTF() else RedisKVStore.DatasetAndEntityHashTagMode
    val stateBuckets = in.readInt()
    val sourceRows = in.readLong()
    val dataRows = in.readLong()
    val changedKeys = in.readLong()
    val deletedKeys = in.readLong()
    val deltaRows = in.readLong()
    val tombstoneRows = in.readLong()
    val liveTTLSeconds = in.readInt()
    val deleteFenceTTLSeconds = if (version == ReadyVersion) in.readInt() else liveTTLSeconds
    ReadyManifest(
      status,
      parentGeneration,
      stateGeneration,
      sourceTable,
      batchDataset,
      keyPrefix,
      keyHashTagMode,
      stateBuckets,
      sourceRows,
      dataRows,
      changedKeys,
      deletedKeys,
      deltaRows,
      tombstoneRows,
      liveTTLSeconds,
      deleteFenceTTLSeconds,
      fullRebuild = in.readBoolean(),
      retired = in.readBoolean()
    )
  }

  private def encodeHead(head: HeadRecord): Array[Byte] = binaryRecord { out =>
    out.writeInt(HeadMagic)
    out.writeInt(HeadVersion)
    out.writeLong(head.revision)
    out.writeUTF(head.generation)
    writeOptionalUtf(out, head.parentGeneration)
    out.writeInt(head.readyDigest.length)
    out.write(head.readyDigest)
  }

  private def decodeHead(bytes: Array[Byte]): HeadRecord = readRecord(bytes) { in =>
    require(in.readInt() == HeadMagic, "Redis HEAD record has an invalid magic header")
    require(in.readInt() == HeadVersion, "Redis HEAD record has an unsupported version")
    val revision = in.readLong()
    require(revision > 0, s"Redis HEAD revision must be positive: $revision")
    val generation = in.readUTF()
    require(RedisBatchUpload.isSafeIdentifier(generation), s"Invalid Redis HEAD generation: $generation")
    val parentGeneration = readOptionalUtf(in)
    parentGeneration.foreach(parent =>
      require(RedisBatchUpload.isSafeIdentifier(parent), s"Invalid Redis HEAD parent generation: $parent"))
    val digestLength = in.readInt()
    require(digestLength == 32, s"Redis HEAD digest length is invalid: $digestLength")
    val digest = new Array[Byte](digestLength)
    in.readFully(digest)
    HeadRecord(revision, generation, parentGeneration, digest)
  }

  private def encodeApplied(applied: AppliedRecord): Array[Byte] = binaryRecord { out =>
    val status = RedisBatchUpload.encodeStatus(applied.status)
    out.writeInt(AppliedMagic)
    out.writeInt(AppliedVersion)
    out.writeInt(status.length)
    out.write(status)
    out.writeInt(applied.readyDigest.length)
    out.write(applied.readyDigest)
    out.writeLong(applied.mutationKeys)
    out.writeLong(applied.refreshedKeys)
    out.writeBoolean(applied.recovered)
  }

  private[redis] def encodeLease(lease: LeaseRecord): Array[Byte] = binaryRecord { out =>
    val status = RedisBatchUpload.encodeStatus(lease.status)
    out.writeInt(LeaseMagic)
    out.writeInt(LeaseVersion)
    out.writeInt(status.length)
    out.write(status)
    out.writeInt(lease.readyDigest.length)
    out.write(lease.readyDigest)
    out.writeLong(lease.minimumActiveDeadlineMillis)
  }

  private[redis] def decodeLease(bytes: Array[Byte]): LeaseRecord =
    readRecord(bytes) { in =>
      require(in.readInt() == LeaseMagic, "Redis LEASE record has an invalid magic header")
      require(in.readInt() == LeaseVersion, "Redis LEASE record has an unsupported version")
      val statusLength = in.readInt()
      require(statusLength > 0 && statusLength <= 1024 * 1024, s"Redis LEASE status length is invalid: $statusLength")
      val statusBytes = new Array[Byte](statusLength)
      in.readFully(statusBytes)
      val digestLength = in.readInt()
      require(digestLength == 32, s"Redis LEASE digest length is invalid: $digestLength")
      val digest = new Array[Byte](digestLength)
      in.readFully(digest)
      LeaseRecord(RedisBatchUpload.decodeStatus(statusBytes).get, digest, in.readLong())
    }

  private def readCommittedApplied(path: Path,
                                   spark: SparkSession,
                                   conditionalWriter: ConditionalObjectWriter): CommittedApplied = {
    val versioned = conditionalWriter
      .read(path, spark.sparkContext.hadoopConfiguration)
      .getOrElse(throw new IllegalStateException(s"Redis APPLIED record does not exist: $path"))
    CommittedApplied(decodeApplied(versioned.bytes), versioned.lastModifiedMillis)
  }

  private def decodeApplied(bytes: Array[Byte]): AppliedRecord =
    readRecord(bytes) { in =>
      require(in.readInt() == AppliedMagic, "Redis APPLIED record has an invalid magic header")
      require(in.readInt() == AppliedVersion, "Redis APPLIED record has an unsupported version")
      val statusLength = in.readInt()
      require(statusLength > 0 && statusLength <= 1024 * 1024, s"Redis APPLIED status length is invalid: $statusLength")
      val statusBytes = new Array[Byte](statusLength)
      in.readFully(statusBytes)
      val digestLength = in.readInt()
      require(digestLength == 32, s"Redis APPLIED digest length is invalid: $digestLength")
      val digest = new Array[Byte](digestLength)
      in.readFully(digest)
      AppliedRecord(RedisBatchUpload.decodeStatus(statusBytes).get,
                    digest,
                    in.readLong(),
                    in.readLong(),
                    in.readBoolean())
    }

  private def binaryRecord(write: DataOutputStream => Unit): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    try {
      write(out)
      out.flush()
      bytes.toByteArray
    } finally {
      out.close()
    }
  }

  private def readRecord[T](bytes: Array[Byte])(read: DataInputStream => T): T = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      val result = read(in)
      require(in.available() == 0, "Redis binary control record has trailing bytes")
      result
    } finally {
      in.close()
    }
  }

  private def writeOptionalUtf(out: DataOutputStream, value: Option[String]): Unit = {
    out.writeBoolean(value.isDefined)
    value.foreach(out.writeUTF)
  }

  private def readOptionalUtf(in: DataInputStream): Option[String] =
    if (in.readBoolean()) Some(in.readUTF()) else None

  private def readBytes(path: Path, spark: SparkSession): Array[Byte] = {
    val in = path.getFileSystem(spark.sparkContext.hadoopConfiguration).open(path)
    val out = new ByteArrayOutputStream()
    val buffer = new Array[Byte](8192)
    try {
      var count = in.read(buffer)
      while (count >= 0) {
        if (count > 0) out.write(buffer, 0, count)
        count = in.read(buffer)
      }
      out.toByteArray
    } finally {
      in.close()
      out.close()
    }
  }

  private def pathExists(path: Path, spark: SparkSession): Boolean =
    path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path)

  private def sha256(bytes: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(bytes)

  private def sanitizePathSegment(value: String): String =
    value.replaceAll("[^A-Za-z0-9._-]", "_")

  /** Legacy transformation helper retained for callers that prepare a complete snapshot themselves. */
  def buildTransformedDataFrame(df: DataFrame,
                                keyPrefix: String,
                                batchTimestamp: Long,
                                spark: SparkSession): DataFrame = {
    val buildRedisKeyUDF = udf((keyBytes: Array[Byte], dataset: String) =>
      RedisKVStore.buildRedisKey(keyBytes.toSeq, dataset, keyPrefix = keyPrefix))
    val prependTimestampUDF = udf((valueBytes: Array[Byte], ts: Long) =>
      java.nio.ByteBuffer.allocate(8 + valueBytes.length).putLong(ts).put(valueBytes).array())

    df.withColumn("redis_key", buildRedisKeyUDF(col(KeyColumn), col("dataset")))
      .withColumn("redis_value", prependTimestampUDF(col(ValueColumn), lit(batchTimestamp)))
      .select("redis_key", "redis_value")
  }

  /** Driver-only compatibility helper used by the original Redis integration test. */
  def writeWithExistingConnection(df: DataFrame, jedisCluster: JedisCluster, ttl: Int, batchSize: Int = 1000): Unit = {
    val rows = df.collect()
    rows.grouped(batchSize).foreach { batch =>
      val pipeline = jedisCluster.pipelined()
      completePipeline(pipeline) {
        batch.toSeq.map { row =>
          pipeline.setex(row.getAs[String]("redis_key").getBytes(StandardCharsets.UTF_8),
                         ttl.toLong,
                         row.getAs[Array[Byte]]("redis_value"))
        }
      }
    }
  }
}
