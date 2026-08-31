package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisBatchUpload.BatchStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.security.MessageDigest
import java.util.{Arrays, Locale}
import scala.util.control.NonFatal

/** Durable generation paths, conditional S3 state transitions, and binary publication records. */
private[redis] object IncrementalRedisStateStore {
  import IncrementalRedisBatchModel._

  private val logger = LoggerFactory.getLogger(getClass)

  private[redis] def normalizedPath(path: Path): String = path.toUri.normalize().toString.stripSuffix("/")

  private[redis] def isServingInfo(column: org.apache.spark.sql.Column) =
    coalesce(column === lit(ServingInfoKeyBytes), lit(false))

  private[redis] def bucketColumn(stateBuckets: Int) =
    pmod(xxhash64(col(KeyColumn)), lit(stateBuckets)).cast(IntegerType)

  private[redis] def emptyTombstones(spark: SparkSession): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(
        Seq(
          StructField(KeyColumn, org.apache.spark.sql.types.BinaryType, nullable = false),
          StructField(BatchTimestampColumn, LongType, nullable = false),
          StructField(WriteEpochColumn, LongType, nullable = false),
          StructField(TombstoneExpiresAtColumn, LongType, nullable = false),
          StructField(BucketColumn, IntegerType, nullable = false)
        ))
    )

  private[redis] def datasetRoot(settings: IncrementalRedisBatchSettings): Path = {
    val stateRoot = new Path(settings.stateRoot.stripSuffix("/"))
    new Path(new Path(stateRoot, sanitizePathSegment(settings.sourceTable)), sanitizePathSegment(settings.batchDataset))
  }

  private[redis] def generationPaths(settings: IncrementalRedisBatchSettings, generation: String): GenerationPaths = {
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

  private[redis] def leaseCheckpointPath(settings: IncrementalRedisBatchSettings, generation: String): Path =
    generationPaths(settings, generation).lease

  private[redis] def headPath(settings: IncrementalRedisBatchSettings): Path = new Path(datasetRoot(settings), HeadFile)

  private[redis] def claimPath(settings: IncrementalRedisBatchSettings, generation: String): Path = {
    require(RedisBatchUpload.isSafeIdentifier(generation), s"Invalid Redis batch generation: $generation")
    new Path(new Path(datasetRoot(settings), ClaimsDirectory), generation)
  }

  private[redis] def writeBucketed(df: DataFrame, path: Path, buckets: Int): Unit = {
    df.repartition(buckets, col(BucketColumn))
      .sortWithinPartitions(col(BucketColumn), col(KeyColumn))
      .write
      .mode(SaveMode.ErrorIfExists)
      .partitionBy(BucketColumn)
      .parquet(path.toString)
  }

  private[redis] def cleanupObsoleteGenerations(settings: IncrementalRedisBatchSettings,
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

  private[redis] def candidateParentIsCurrent(settings: IncrementalRedisBatchSettings,
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

  private[redis] def abandonCandidate(settings: IncrementalRedisBatchSettings,
                                      generation: String,
                                      spark: SparkSession,
                                      conditionalWriter: ConditionalObjectWriter): Boolean = {
    val abandoned = CandidateClaim(generation, abandoned = true)
    val winner = conditionalWriter.putIfAbsent(claimPath(settings, generation),
                                               encodeClaim(abandoned),
                                               spark.sparkContext.hadoopConfiguration)
    decodeClaim(winner.bytes) == abandoned
  }

  private[redis] def isAbandonedCandidate(settings: IncrementalRedisBatchSettings,
                                          generation: String,
                                          spark: SparkSession,
                                          conditionalWriter: ConditionalObjectWriter): Boolean =
    conditionalWriter
      .read(claimPath(settings, generation), spark.sparkContext.hadoopConfiguration)
      .exists(value => decodeClaim(value.bytes) == CandidateClaim(generation, abandoned = true))

  private[redis] def generationReachableFromCurrentHead(settings: IncrementalRedisBatchSettings,
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

  private[redis] def readHead(settings: IncrementalRedisBatchSettings,
                              spark: SparkSession,
                              conditionalWriter: ConditionalObjectWriter): Option[VersionedHead] =
    conditionalWriter
      .read(headPath(settings), spark.sparkContext.hadoopConfiguration)
      .map(value => VersionedHead(decodeHead(value.bytes), value.version, value.lastModifiedMillis))

  private[redis] def readGenerationLease(settings: IncrementalRedisBatchSettings,
                                         generation: String,
                                         spark: SparkSession,
                                         conditionalWriter: ConditionalObjectWriter): Option[Long] = {
    val (manifest, readyBytes) = validateReady(settings, generation, spark)
    readLease(generationPaths(settings, generation).lease, manifest, sha256(readyBytes), spark, conditionalWriter)
      .map(_.record.minimumActiveDeadlineMillis)
  }

  private[redis] def readLease(path: Path,
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

  private[redis] def headAdvancedPast(settings: IncrementalRedisBatchSettings,
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

  private[redis] def claimHead(settings: IncrementalRedisBatchSettings,
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

  private[redis] def writeImmutable(path: Path,
                                    contents: Array[Byte],
                                    spark: SparkSession,
                                    conditionalWriter: ConditionalObjectWriter): Unit = {
    val winner = conditionalWriter.putIfAbsent(path, contents, spark.sparkContext.hadoopConfiguration)
    require(Arrays.equals(winner.bytes, contents),
            s"Immutable Redis S3 record already exists with different contents: $path")
  }

  private[redis] def validateReady(settings: IncrementalRedisBatchSettings,
                                   generation: String,
                                   spark: SparkSession): (ReadyManifest, Array[Byte]) = {
    val paths = generationPaths(settings, generation)
    val path = paths.ready
    require(pathExists(path, spark), s"Redis S3 head references an incomplete generation: $path")
    val candidatePath = paths.candidate
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
    require(pathExists(paths.state, spark), s"Redis generation $generation is missing its durable state")
    require(pathExists(paths.delta, spark), s"Redis generation $generation is missing its durable delta")
    if (manifest.tombstoneRows > 0) {
      require(pathExists(paths.tombstones, spark), s"Redis generation $generation is missing retained tombstones")
    }
    manifest -> readyBytes
  }

  private[redis] def loadState(settings: IncrementalRedisBatchSettings,
                               generation: String,
                               spark: SparkSession): DataFrame = {
    val path = generationPaths(settings, generation).state
    require(pathExists(path, spark), s"Redis state generation is missing from S3: $path")
    spark.read
      .parquet(path.toString)
      .select(KeyColumn, ValueColumn, DigestColumn, ValueTimestampColumn, ValueWriteEpochColumn, BucketColumn)
  }

  private[redis] def loadStateIndex(settings: IncrementalRedisBatchSettings,
                                    generation: String,
                                    spark: SparkSession): DataFrame = {
    val path = generationPaths(settings, generation).state
    require(pathExists(path, spark), s"Redis state generation is missing from S3: $path")
    spark.read
      .parquet(path.toString)
      .select(KeyColumn, DigestColumn, ValueTimestampColumn, ValueWriteEpochColumn, BucketColumn)
  }

  private[redis] def loadTombstones(settings: IncrementalRedisBatchSettings,
                                    manifest: ReadyManifest,
                                    spark: SparkSession): DataFrame = {
    if (manifest.tombstoneRows == 0) return emptyTombstones(spark)
    val path = generationPaths(settings, manifest.status.generation).tombstones
    require(pathExists(path, spark), s"Redis tombstones are missing from S3: $path")
    spark.read
      .parquet(path.toString)
      .select(KeyColumn, BatchTimestampColumn, WriteEpochColumn, TombstoneExpiresAtColumn, BucketColumn)
  }

  private[redis] def loadCommittedTombstones(settings: IncrementalRedisBatchSettings,
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

  private[redis] def committedApplied(settings: IncrementalRedisBatchSettings,
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

  private[redis] def loadDelta(settings: IncrementalRedisBatchSettings,
                               generation: String,
                               spark: SparkSession): DataFrame = {
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

  private[redis] def metadataMatches(currentMetadata: DataFrame,
                                     settings: IncrementalRedisBatchSettings,
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

  private[redis] def compareStatus(left: BatchStatus, right: BatchStatus): Int = {
    val timestamp = java.lang.Long.compare(left.batchTimestamp, right.batchTimestamp)
    if (timestamp != 0) timestamp else java.lang.Long.compare(left.writeEpoch, right.writeEpoch)
  }

  private[redis] def validateConditionalWriter(settings: IncrementalRedisBatchSettings,
                                               writer: ConditionalObjectWriter): Unit = {
    val scheme = Option(new Path(settings.stateRoot).toUri.getScheme).map(_.toLowerCase(Locale.ROOT))
    require(
      !scheme.exists(value => value == "s3" || value == "s3a" || value == "s3n") ||
        writer.supportsDistributedCas,
      "Redis S3 state requires a distributed conditional object writer"
    )
  }

  private[redis] def validateApplied(record: AppliedRecord, manifest: ReadyManifest, digest: Array[Byte]): Unit = {
    require(record.status == manifest.status,
            s"Redis applied record does not match generation ${manifest.status.generation}")
    require(Arrays.equals(record.readyDigest, digest),
            s"Redis applied record digest does not match generation ${manifest.status.generation}")
    require(record.mutationKeys >= manifest.deltaRows,
            s"Redis applied record contains too few mutations for generation ${manifest.status.generation}")
    require(record.refreshedKeys >= 0,
            s"Redis applied record contains a negative refresh count for generation ${manifest.status.generation}")
  }

  private[redis] def validateLease(record: LeaseRecord, manifest: ReadyManifest, digest: Array[Byte]): Unit = {
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

  private[redis] def encodeCandidate(candidate: CandidateRecord): Array[Byte] = binaryRecord { out =>
    out.writeInt(CandidateMagic)
    out.writeInt(CandidateVersion)
    out.writeUTF(candidate.generation)
    writeOptionalUtf(out, candidate.parentGeneration)
    out.writeLong(candidate.createdAtMillis)
    out.writeLong(candidate.cleanupAfterMillis)
  }

  private[redis] def decodeCandidate(bytes: Array[Byte]): CandidateRecord = readRecord(bytes) { in =>
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
    require(version == LegacyReadyVersion || version == ReadyVersion,
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

  private[redis] def encodeHead(head: HeadRecord): Array[Byte] = binaryRecord { out =>
    out.writeInt(HeadMagic)
    out.writeInt(HeadVersion)
    out.writeLong(head.revision)
    out.writeUTF(head.generation)
    writeOptionalUtf(out, head.parentGeneration)
    out.writeInt(head.readyDigest.length)
    out.write(head.readyDigest)
  }

  private[redis] def decodeHead(bytes: Array[Byte]): HeadRecord = readRecord(bytes) { in =>
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

  private[redis] def encodeApplied(applied: AppliedRecord): Array[Byte] = binaryRecord { out =>
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

  private[redis] def readCommittedApplied(path: Path,
                                          spark: SparkSession,
                                          conditionalWriter: ConditionalObjectWriter): CommittedApplied = {
    val versioned = conditionalWriter
      .read(path, spark.sparkContext.hadoopConfiguration)
      .getOrElse(throw new IllegalStateException(s"Redis APPLIED record does not exist: $path"))
    CommittedApplied(decodeApplied(versioned.bytes), versioned.lastModifiedMillis)
  }

  private[redis] def decodeApplied(bytes: Array[Byte]): AppliedRecord =
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

  private[redis] def binaryRecord(write: DataOutputStream => Unit): Array[Byte] = {
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

  private[redis] def readRecord[T](bytes: Array[Byte])(read: DataInputStream => T): T = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      val result = read(in)
      require(in.available() == 0, "Redis binary control record has trailing bytes")
      result
    } finally {
      in.close()
    }
  }

  private[redis] def writeOptionalUtf(out: DataOutputStream, value: Option[String]): Unit = {
    out.writeBoolean(value.isDefined)
    value.foreach(out.writeUTF)
  }

  private[redis] def readOptionalUtf(in: DataInputStream): Option[String] =
    if (in.readBoolean()) Some(in.readUTF()) else None

  private[redis] def readBytes(path: Path, spark: SparkSession): Array[Byte] = {
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

  private[redis] def pathExists(path: Path, spark: SparkSession): Boolean =
    path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path)

  private[redis] def sha256(bytes: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(bytes)

  private[redis] def sanitizePathSegment(value: String): String =
    value.replaceAll("[^A-Za-z0-9._-]", "_")

}
