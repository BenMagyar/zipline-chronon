package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisBatchUpload.BatchStatus
import ai.chronon.integrations.redis.RedisKVStoreConstants.PropRedisBulkUploadDeleteOlderVersions
import ai.chronon.spark.catalog.TableUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory
import redis.clients.jedis.{JedisCluster, Protocol}

import java.util.{Arrays, UUID}
import java.util.concurrent.ThreadLocalRandom
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
object IncrementalRedisBatchLoader {
  import IncrementalRedisBatchModel._
  import IncrementalRedisMutationWriter._
  import IncrementalRedisStateStore._

  private val logger = LoggerFactory.getLogger(getClass)

  private val VersionedDatasetPattern = """^(.*)__(\d+)$""".r
  private val VersionedBatchDatasetPattern = """^(.*)__(\d+)_BATCH$""".r

  private[redis] case class VersionedDataset(base: String, version: Long) {
    require(base.nonEmpty, "Redis versioned dataset base must be non-empty")
    require(version >= 0, s"Redis dataset version must be non-negative: $version")
  }

  private case class PriorLineage(settings: IncrementalRedisBatchSettings, manifest: ReadyManifest)

  case class DiffFrames(currentState: DataFrame, dataDelta: DataFrame)

  case class UploadSummary(sourceRows: Long,
                           changedKeys: Long,
                           deletedKeys: Long,
                           refreshedKeys: Long,
                           generation: Option[String],
                           superseded: Boolean,
                           fullRebuild: Boolean)

  private case class ApplyResult(mutationKeys: Long,
                                 refreshedKeys: Long,
                                 recovered: Boolean,
                                 minimumActiveDeadlineMillis: Long)

  def run(settings: IncrementalRedisBatchSettings,
          spark: SparkSession,
          controlClient: JedisCluster,
          conditionalWriter: ConditionalObjectWriter): UploadSummary = {
    require(
      !spark.sparkContext.getConf.getBoolean("spark.speculation", false),
      "Redis incremental upload requires spark.speculation=false so duplicate task attempts cannot bypass mutation accounting"
    )
    if (settings.deleteOlderVersions)
      requireMatchingVersionedDatasets(settings.destinationDataset, settings.batchDataset)
    validateConditionalWriter(settings, conditionalWriter)
    if (settings.requireNoEviction) validateNoEviction(controlClient)
    else
      logger.warn(
        "Skipping Redis maxmemory-policy=noeviction validation; eviction can silently lose published feature values")
    val clientNoTouch = RedisKVStoreFactory.supportsClientNoTouch(controlClient)
    val effectiveSettings =
      settings.copy(target = settings.target.copy(client = settings.clientSettings.copy(clientNoTouch = clientNoTouch)))
    if (clientNoTouch)
      logger.info("Redis bulk-writer connections will require CLIENT NO-TOUCH")
    else
      logger.warn(
        "Redis does not support or permit CLIENT NO-TOUCH; retaining header-based lease validation for compatibility")

    RedisBatchUpload.claimPublicationMode(
      controlClient,
      effectiveSettings.batchDataset,
      effectiveSettings.keyPrefix,
      RedisBatchModeSelection.Incremental
    )

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

  private def runAttempt(settings: IncrementalRedisBatchSettings,
                         spark: SparkSession,
                         controlClient: JedisCluster,
                         conditionalWriter: ConditionalObjectWriter): UploadSummary = {
    val tableUtils = TableUtils(spark, settings.partitionSpec)
    var priorVersionsRetired = false
    def retirePriorVersionsOnce(): Unit =
      if (settings.deleteOlderVersions && !priorVersionsRetired) {
        retireOlderVersions(settings, spark, controlClient, conditionalWriter)
        priorVersionsRetired = true
      }

    val head = resolveHead(
      settings,
      spark,
      controlClient,
      conditionalWriter,
      beforeApply = manifest => {
        require(
          !manifest.retired,
          s"Redis dataset ${settings.batchDataset} was explicitly retired in generation ${manifest.status.generation}")
        retirePriorVersionsOnce()
      }
    )
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
      if (
        settings.liveTTLSeconds < committed.liveTTLSeconds ||
        settings.liveTTLSeconds < committed.deleteFenceTTLSeconds
      ) {
        logger.info(
          s"Redis TTL is decreasing to ${settings.liveTTLSeconds} seconds: " +
            s"live values were ${committed.liveTTLSeconds} seconds and delete fences were " +
            s"${committed.deleteFenceTTLSeconds} seconds; existing keys retain their current absolute expiration")
      }

      // APPLIED makes the value-bearing S3 generation authoritative even if Redis loses its status marker or is
      // restored to an older snapshot. Repair that committed generation before consulting the mutable source table.
      // For the same source partition, the recovery is the complete operation; a later invocation can deliberately
      // compare a corrected source partition after Redis is back at the committed baseline. A newer partition proceeds
      // below only after the committed parent has been restored.
      if (!redisStatus.contains(committed.status)) {
        val recoverySettings =
          settings.copy(job = settings.job.copy(sourcePartition = committed.status.sourcePartition))
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
            settings.liveTTLSeconds == committed.deleteFenceTTLSeconds &&
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
            stateBuckets = stateBuckets,
            sourceRows = sourceRows,
            dataRows = dataCount,
            changedKeys = changedKeys,
            deletedKeys = deletedKeys,
            deltaRows = deltaRows,
            tombstoneRows = tombstoneRows,
            liveTTLSeconds = settings.liveTTLSeconds,
            deleteFenceTTLSeconds = settings.liveTTLSeconds,
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

          val committed =
            resolveHead(settings, spark, controlClient, conditionalWriter, beforeApply = _ => retirePriorVersionsOnce())
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

  private def resolveHead(settings: IncrementalRedisBatchSettings,
                          spark: SparkSession,
                          controlClient: JedisCluster,
                          conditionalWriter: ConditionalObjectWriter,
                          beforeApply: ReadyManifest => Unit = _ => ()): Option[ReadyManifest] = {
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
        beforeApply(manifest)
      } else {
        val statusKey = RedisBatchUpload.buildStatusKey(settings.batchDataset, settings.keyPrefix)
        def validatedCurrentStatus(): Option[BatchStatus] = {
          val status = RedisBatchUpload.readStatus(controlClient, statusKey)
          status.filter(value => compareStatus(value, manifest.status) > 0).foreach { value =>
            if (headAdvancedPast(settings, manifest, spark, conditionalWriter)) throw HeadAdvanced(value.generation)
            throw new IllegalStateException(
              s"Redis status ${value.generation} is newer than S3 head ${manifest.status.generation}")
          }
          status
        }

        // Validate the serving status before a capacity-first retirement can remove prior-version keys. The callback may
        // take time, so it is deliberately outside the new generation's lease and followed by another APPLIED/status
        // check for a concurrent publisher.
        validatedCurrentStatus()
        beforeApply(manifest)
        if (pathExists(paths.applied, spark)) {
          validateApplied(readCommittedApplied(paths.applied, spark, conditionalWriter).record,
                          manifest,
                          head.record.readyDigest)
        } else {
          val currentStatus = validatedCurrentStatus()
          val leaseDeadlineMillis = uploadDeadlineMillis(manifest.liveTTLSeconds)
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

  private def applyGeneration(settings: IncrementalRedisBatchSettings,
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
    // Stop serving this version before removing its keys. If physical cleanup is interrupted, the durable retired
    // head has no APPLIED record, so resolveHead replays the immutable delete set on the next invocation.
    if (manifest.retired) ensureRetiredStatus(settings, manifest, controlClient)
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

    val retirementMode = if (manifest.retired) PhysicalDelete else MutationWrite
    val dataSummary = writeToRedis(
      dataDelta,
      retirementMode,
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
      val tombstones =
        if (manifest.retired)
          loadTombstones(settings, manifest, spark).select(KeyColumn, BucketColumn)
        else
          buildTombstoneReplay(settings,
                               manifest,
                               spark,
                               deleteFenceDeadlineMillis,
                               excludeStatus = Some(manifest.status))
      tombstoneSummary = writeToRedis(
        tombstones,
        retirementMode,
        settings,
        limiterKey,
        activeLeaseDeadlineMillis,
        deleteFenceDeadlineMillis,
        progress = Some(WriteProgress(manifest.status.generation, "tombstone-replay", None))
      )
      rejectedKeys += tombstoneSummary.rejected
    }

    val metadataSummary =
      writeToRedis(metadataDelta, retirementMode, settings, limiterKey, activeLeaseDeadlineMillis)
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

  private def repairOlderSnapshot(settings: IncrementalRedisBatchSettings,
                                  head: ReadyManifest,
                                  spark: SparkSession,
                                  controlClient: JedisCluster,
                                  conditionalWriter: ConditionalObjectWriter,
                                  forceReplay: Boolean): UploadSummary = {
    val latestSettings = settings.copy(job = settings.job.copy(sourcePartition = head.status.sourcePartition))
    val refreshed =
      refreshCommittedSnapshot(latestSettings, head, spark, controlClient, conditionalWriter, forceReplay)
    logger.info(
      s"Redis older partition ${settings.sourcePartition} did not move S3 head ${head.status.generation}; " +
        s"the latest partition ${head.status.sourcePartition} was checked for lease renewal instead")
    refreshed.copy(superseded = true)
  }

  private def refreshCommittedSnapshot(settings: IncrementalRedisBatchSettings,
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

  private def buildFullReplay(settings: IncrementalRedisBatchSettings,
                              manifest: ReadyManifest,
                              spark: SparkSession): DataFrame = {
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

  private def buildTombstoneReplay(settings: IncrementalRedisBatchSettings,
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

  private def loadSourceRows(tableUtils: TableUtils, settings: IncrementalRedisBatchSettings): DataFrame = {
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

  private[redis] def parseVersionedDataset(name: String): Option[VersionedDataset] =
    Option(name).flatMap {
      case VersionedDatasetPattern(base, version) =>
        Try(version.toLong).toOption.map(VersionedDataset(base, _))
      case _ => None
    }

  private[redis] def parseVersionedBatchDataset(name: String): Option[VersionedDataset] =
    Option(name).flatMap {
      case VersionedBatchDatasetPattern(base, version) =>
        Try(version.toLong).toOption.map(VersionedDataset(base, _))
      case _ => None
    }

  private def requireVersionedDataset(name: String): VersionedDataset =
    parseVersionedDataset(name).getOrElse(
      throw new IllegalArgumentException(
        s"$PropRedisBulkUploadDeleteOlderVersions requires a GroupBy name ending in __<numeric-version>; got '$name'"))

  private def requireVersionedBatchDataset(name: String): VersionedDataset =
    parseVersionedBatchDataset(name).getOrElse(
      throw new IllegalArgumentException(
        s"$PropRedisBulkUploadDeleteOlderVersions requires a batch dataset ending in __<numeric-version>_BATCH; " +
          s"got '$name'"))

  private[redis] def requireMatchingVersionedDatasets(destinationDataset: String,
                                                      batchDataset: String): (VersionedDataset, VersionedDataset) = {
    val current = requireVersionedDataset(destinationDataset)
    val currentBatch = requireVersionedBatchDataset(batchDataset)
    require(
      current.version == currentBatch.version,
      s"Redis GroupBy version ${current.version} does not match batch dataset version ${currentBatch.version}"
    )
    current -> currentBatch
  }

  /** Retires every committed lower numeric version after the replacement is durably admitted but before its Redis
    * mutations begin. A retirement is an immutable child generation in the old lineage whose persisted delete set is
    * applied with Redis UNLINK. The durable retired head and persistent Redis status keep the removed version
    * unavailable while allowing retries to replay physical deletion idempotently.
    */
  private def retireOlderVersions(settings: IncrementalRedisBatchSettings,
                                  spark: SparkSession,
                                  controlClient: JedisCluster,
                                  conditionalWriter: ConditionalObjectWriter): Unit = {
    val (current, currentBatch) =
      requireMatchingVersionedDatasets(settings.destinationDataset, settings.batchDataset)

    val prior = discoverPriorLineages(settings, current, currentBatch, spark, conditionalWriter)
    prior.foreach { lineage =>
      retirePriorLineage(lineage.settings, spark, controlClient, conditionalWriter)
      cleanupObsoleteGenerations(lineage.settings, spark, conditionalWriter)
    }
  }

  private def discoverPriorLineages(settings: IncrementalRedisBatchSettings,
                                    current: VersionedDataset,
                                    currentBatch: VersionedDataset,
                                    spark: SparkSession,
                                    conditionalWriter: ConditionalObjectWriter): Seq[PriorLineage] = {
    val stateRoot = new Path(settings.stateRoot.stripSuffix("/"))
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = stateRoot.getFileSystem(hadoopConf)
    if (!fs.exists(stateRoot)) return Seq.empty

    // batchDataset is sanitized by construction, so its base is safe to place in a Hadoop glob.
    val candidateGlob = new Path(new Path(stateRoot, "*"), s"${currentBatch.base}__*_BATCH")
    val candidateRoots = Option(fs.globStatus(candidateGlob))
      .map(_.iterator.filter(_.isDirectory).map(_.getPath).toSeq)
      .getOrElse(Seq.empty)

    val discovered = candidateRoots.flatMap { candidateRoot =>
      parseVersionedBatchDataset(candidateRoot.getName) match {
        case Some(candidate) if candidate.base == currentBatch.base && candidate.version < current.version =>
          conditionalWriter.read(new Path(candidateRoot, HeadFile), hadoopConf).map { headValue =>
            val head = VersionedHead(decodeHead(headValue.bytes), headValue.version, headValue.lastModifiedMillis)
            val readyPath = new Path(
              new Path(new Path(candidateRoot, GenerationsDirectory), head.record.generation),
              ReadyFile
            )
            val readyBytes = readBytes(readyPath, spark)
            val manifest = decodeReady(readyBytes)
            val manifestVersion = requireVersionedBatchDataset(manifest.batchDataset)
            require(
              manifestVersion == candidate,
              s"Redis lineage at $candidateRoot contains batch dataset ${manifest.batchDataset}, " +
                s"which does not match its path"
            )
            require(
              Arrays.equals(head.record.readyDigest, sha256(readyBytes)),
              s"Redis S3 head digest does not match generation ${head.record.generation} at $candidateRoot"
            )

            val oldDestinationDataset = s"${current.base}__${candidate.version}"
            val oldSettings = settings.copy(
              job = settings.job.copy(sourceTable = manifest.sourceTable, destinationDataset = oldDestinationDataset),
              target = settings.target.copy(keyPrefix = manifest.keyPrefix),
              options = settings.options.copy(
                ttlSeconds = manifest.liveTTLSeconds,
                deleteOlderVersions = false,
                tuning = settings.options.tuning.copy(stateBuckets = manifest.stateBuckets)
              )
            )
            require(
              oldSettings.batchDataset == manifest.batchDataset,
              s"Redis prior-version destination $oldDestinationDataset maps to ${oldSettings.batchDataset}, " +
                s"but S3 records ${manifest.batchDataset}"
            )
            require(
              normalizedPath(datasetRoot(oldSettings)) == normalizedPath(candidateRoot),
              s"Redis prior-version manifest at $candidateRoot resolves to ${datasetRoot(oldSettings)}"
            )
            val (validated, validatedBytes) = validateReady(oldSettings, head.record.generation, spark)
            require(
              Arrays.equals(validatedBytes, readyBytes) && validated == manifest,
              s"Redis prior-version manifest changed while it was being discovered at $candidateRoot"
            )
            require(
              manifest.parentGeneration == head.record.parentGeneration,
              s"Redis generation ${manifest.status.generation} does not match its S3 head parent"
            )
            PriorLineage(oldSettings, manifest)
          }
        case _ => None
      }
    }

    discovered
      .groupBy(_.manifest.batchDataset)
      .foreach { case (batchDataset, lineages) =>
        require(
          lineages.size == 1,
          s"Redis older-version cleanup found multiple headed S3 lineages for $batchDataset: " +
            lineages.map(lineage => datasetRoot(lineage.settings)).mkString(", ")
        )
      }
    discovered.sortBy(_.manifest.batchDataset)
  }

  private def retirePriorLineage(settings: IncrementalRedisBatchSettings,
                                 spark: SparkSession,
                                 controlClient: JedisCluster,
                                 conditionalWriter: ConditionalObjectWriter): Unit = {
    var attempts = 0
    while (attempts < MaxAdmissionAttempts) {
      val head = readHead(settings, spark, conditionalWriter) match {
        case Some(value) => value
        case None        => return
      }
      val (parent, readyBytes) = validateReady(settings, head.record.generation, spark)
      require(
        Arrays.equals(head.record.readyDigest, sha256(readyBytes)),
        s"Redis S3 head digest does not match generation ${head.record.generation}"
      )
      require(parent.parentGeneration == head.record.parentGeneration,
              s"Redis generation ${parent.status.generation} does not match its S3 head parent")

      // The first retirement implementation persisted DELETE tombstones. Those manifests are retired but still have
      // tombstone rows, so admit one newer retirement generation that physically UNLINKs them. A zero-tombstone retired
      // head is the durable marker that physical cleanup has already completed.
      if (parent.retired && !requiresRetiredPhysicalDeleteMigration(parent)) {
        val statusKey = RedisBatchUpload.buildStatusKey(settings.batchDataset, settings.keyPrefix)
        val redisStatus = RedisBatchUpload.readStatus(controlClient, statusKey)
        if (!redisStatus.contains(parent.status)) {
          val result = applyGeneration(
            settings,
            parent,
            head.record.readyDigest,
            spark,
            controlClient,
            conditionalWriter,
            recoverFull = false,
            leaseDeadlineMillis = uploadDeadlineMillis(parent.deleteFenceTTLSeconds)
          )
          require(
            result.mutationKeys >= parent.deltaRows,
            s"Redis retired generation ${parent.status.generation} replayed ${result.mutationKeys} mutations; " +
              s"expected at least ${parent.deltaRows}"
          )
        }
        val committed = resolveHead(settings, spark, controlClient, conditionalWriter)
        require(committed.exists(_.retired), s"Redis retired lineage ${settings.batchDataset} lost its S3 head")
        ensureRetiredStatus(settings, parent, controlClient)
        return
      }

      val generation = buildRetirementGeneration(settings, parent, spark, conditionalWriter)
      val retirementReady = readBytes(generationPaths(settings, generation).ready, spark)
      val admitted = claimHead(settings,
                               Some(parent.status.generation),
                               generation,
                               sha256(retirementReady),
                               spark,
                               conditionalWriter)
      if (admitted.generation == generation) {
        val committed = resolveHead(settings, spark, controlClient, conditionalWriter)
        require(
          committed.exists(manifest => manifest.status.generation == generation && manifest.retired),
          s"Redis retirement generation $generation did not become the S3 head for ${settings.batchDataset}"
        )
        val retiredManifest = committed.get
        ensureRetiredStatus(settings, retiredManifest, controlClient)
        logger.info(
          s"Retired older Redis dataset ${settings.batchDataset} with ${retiredManifest.deletedKeys} keys in generation $generation")
        return
      }

      attempts += 1
      logger.info(
        s"Redis older-version lineage ${settings.batchDataset} advanced to ${admitted.generation} during retirement; " +
          s"recomputing (attempt $attempts/$MaxAdmissionAttempts)"
      )
    }
    throw new IllegalStateException(
      s"Redis older-version lineage ${settings.batchDataset} could not be retired after $MaxAdmissionAttempts S3 head changes")
  }

  private[redis] def requiresRetiredPhysicalDeleteMigration(manifest: ReadyManifest): Boolean =
    manifest.retired && manifest.tombstoneRows > 0

  private def buildRetirementGeneration(settings: IncrementalRedisBatchSettings,
                                        parent: ReadyManifest,
                                        spark: SparkSession,
                                        conditionalWriter: ConditionalObjectWriter): String = {
    val writeEpoch = java.lang.Math.addExact(parent.status.writeEpoch, 1L)
    val generation = UUID.randomUUID().toString
    val status = parent.status.copy(generation = generation, writeEpoch = writeEpoch, retired = true)
    val paths = generationPaths(settings, generation)
    val activeState = loadState(settings, parent.stateGeneration, spark).persist(StorageLevel.DISK_ONLY)
    var completeDelta: DataFrame = null
    var deleteKeys: DataFrame = null
    try {
      val activeCount = activeState.count()
      require(
        activeCount == parent.dataRows,
        s"Redis state ${parent.stateGeneration} contains $activeCount active rows; its head records ${parent.dataRows}"
      )
      val emptyState = activeState.limit(0)
      val previousTombstoneKeys =
        if (parent.tombstoneRows == 0) emptyTombstones(spark).select(KeyColumn, BucketColumn)
        else if (pathExists(generationPaths(settings, parent.status.generation).applied, spark))
          loadCommittedTombstones(settings, parent, spark, conditionalWriter).select(KeyColumn, BucketColumn)
        else loadTombstones(settings, parent, spark).select(KeyColumn, BucketColumn)
      deleteKeys = activeState
        .select(KeyColumn, BucketColumn)
        .unionByName(previousTombstoneKeys)
        .dropDuplicates(KeyColumn, BucketColumn)
        .persist(StorageLevel.DISK_ONLY)
      val deleteCount = deleteKeys.count()
      val entityDeletes = deleteKeys
        .withColumn(ValueColumn, lit(null).cast(BinaryType))
        .withColumn(OperationColumn, lit(RedisBatchUpload.DeleteOperation))
        .withColumn(BatchTimestampColumn, lit(status.batchTimestamp))
        .withColumn(WriteEpochColumn, lit(writeEpoch))
        .withColumn(TombstoneExpiresAtColumn, lit(PendingTombstoneExpiry))
        .select(KeyColumn,
                ValueColumn,
                OperationColumn,
                BatchTimestampColumn,
                WriteEpochColumn,
                BucketColumn,
                TombstoneExpiresAtColumn)
      val metadataDelete = spark
        .createDataFrame(
          java.util.Arrays.asList(Row(ServingInfoKeyBytes)),
          StructType(Seq(StructField(KeyColumn, BinaryType, nullable = false)))
        )
        .withColumn(ValueColumn, lit(null).cast(BinaryType))
        .withColumn(OperationColumn, lit(RedisBatchUpload.DeleteOperation))
        .withColumn(BatchTimestampColumn, lit(status.batchTimestamp))
        .withColumn(WriteEpochColumn, lit(writeEpoch))
        .withColumn(BucketColumn, bucketColumn(parent.stateBuckets))
        .withColumn(TombstoneExpiresAtColumn, lit(PendingTombstoneExpiry))
        .select(KeyColumn,
                ValueColumn,
                OperationColumn,
                BatchTimestampColumn,
                WriteEpochColumn,
                BucketColumn,
                TombstoneExpiresAtColumn)
      completeDelta = entityDeletes.unionByName(metadataDelete).persist(StorageLevel.DISK_ONLY)
      val deltaRows = completeDelta.count()
      require(deltaRows == deleteCount + 1L,
              s"Redis retirement delta contains $deltaRows rows; expected ${deleteCount + 1L}")

      val manifest = ReadyManifest(
        status = status,
        parentGeneration = Some(parent.status.generation),
        stateGeneration = generation,
        sourceTable = settings.sourceTable,
        batchDataset = settings.batchDataset,
        keyPrefix = parent.keyPrefix,
        stateBuckets = parent.stateBuckets,
        sourceRows = 0L,
        dataRows = 0L,
        changedKeys = 0L,
        deletedKeys = deleteCount,
        deltaRows = deltaRows,
        tombstoneRows = 0L,
        liveTTLSeconds = parent.liveTTLSeconds,
        deleteFenceTTLSeconds = parent.deleteFenceTTLSeconds,
        fullRebuild = false,
        retired = true
      )
      val createdAt = System.currentTimeMillis()
      val candidate = CandidateRecord(generation,
                                      manifest.parentGeneration,
                                      createdAt,
                                      java.lang.Math.addExact(createdAt, GenerationCleanupGraceMillis))
      writeImmutable(paths.candidate, encodeCandidate(candidate), spark, conditionalWriter)
      writeBucketed(emptyState, paths.state, parent.stateBuckets)
      writeBucketed(completeDelta, paths.delta, parent.stateBuckets)
      writeImmutable(paths.ready, encodeReady(manifest), spark, conditionalWriter)
      generation
    } finally {
      Option(completeDelta).foreach(_.unpersist())
      Option(deleteKeys).foreach(_.unpersist())
      activeState.unpersist()
    }
  }

  private def ensureRetiredStatus(settings: IncrementalRedisBatchSettings,
                                  manifest: ReadyManifest,
                                  controlClient: JedisCluster): Unit = {
    require(manifest.retired, s"Redis generation ${manifest.status.generation} is not retired")
    val operationDeadline = uploadDeadlineMillis(manifest.deleteFenceTTLSeconds)
    val result = writeStatusWithOomRetry(
      controlClient,
      RedisBatchUpload.buildStatusKey(settings.batchDataset, settings.keyPrefix),
      manifest.status,
      expireAtMillis = 0L,
      settings = settings,
      leaseDeadlineMillis = operationDeadline
    )
    require(result == 1L || result == 2L,
            s"Redis retired status for ${manifest.status.generation} was rejected without a newer S3 head")
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

}
