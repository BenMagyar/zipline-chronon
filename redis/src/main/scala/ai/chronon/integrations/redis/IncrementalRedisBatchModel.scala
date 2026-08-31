package ai.chronon.integrations.redis

import ai.chronon.api.Constants.GroupByServingInfoKey
import ai.chronon.integrations.redis.RedisBatchUpload.BatchStatus
import org.apache.hadoop.fs.Path

import java.nio.charset.StandardCharsets

/** Shared schema and durable record types for incremental Redis publication.
  *
  * Keeping these values independent of the orchestrator lets the mutation writer and state store remain leaf
  * components instead of depending back on [[IncrementalRedisBatchLoader]].
  */
private[redis] object IncrementalRedisBatchModel {
  val KeyColumn = "key_bytes"
  val ValueColumn = "value_bytes"
  val DigestColumn = "value_digest"
  val ValueTimestampColumn = "value_timestamp"
  val ValueWriteEpochColumn = "value_write_epoch"
  val BucketColumn = "redis_bucket"
  val OperationColumn = "redis_operation"
  val BatchTimestampColumn = "batch_timestamp"
  val WriteEpochColumn = "redis_write_epoch"
  val TombstoneExpiresAtColumn = "tombstone_expires_at"
  val AllowExpiredMutationColumn = "redis_allow_expired_mutation"
  val PendingTombstoneExpiry = Long.MaxValue

  val ReadyFile = "_READY"
  val AppliedFile = "_APPLIED"
  val LeaseFile = "_LEASE"
  val HeadFile = "_HEAD"
  val CandidateFile = "_CANDIDATE"
  val DeltaDirectory = "delta"
  val StateDirectory = "state"
  val TombstoneDirectory = "tombstones"
  val GenerationsDirectory = "generations"
  val ClaimsDirectory = "claims"

  val ReadyMagic = 0x43524252 // CRBR
  val LegacyReadyVersion = 4
  val ReadyVersion = 5
  val CandidateMagic = 0x43524243 // CRBC
  val CandidateVersion = 2
  val HeadMagic = 0x43524248 // CRBH
  val HeadVersion = 1
  val AppliedMagic = 0x43524241 // CRBA
  val AppliedVersion = 1
  val LeaseMagic = 0x43524245 // CRBE
  val LeaseVersion = 1
  val ClaimMagic = 0x4352424c // CRBL
  val ClaimVersion = 1

  val MaxAdmissionAttempts = 64
  val GenerationCleanupGraceMillis = 6L * 60L * 60L * 1000L
  val ServingInfoKeyBytes: Array[Byte] = GroupByServingInfoKey.getBytes(StandardCharsets.UTF_8)

  final case class ReadyManifest(status: BatchStatus,
                                 parentGeneration: Option[String],
                                 stateGeneration: String,
                                 sourceTable: String,
                                 batchDataset: String,
                                 keyPrefix: String,
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

  final case class HeadRecord(revision: Long,
                              generation: String,
                              parentGeneration: Option[String],
                              readyDigest: Array[Byte])
  final case class VersionedHead(record: HeadRecord, version: String, lastModifiedMillis: Long)
  final case class CandidateRecord(generation: String,
                                   parentGeneration: Option[String],
                                   createdAtMillis: Long,
                                   cleanupAfterMillis: Long)
  final case class HeadAdvanced(generation: String) extends RuntimeException
  final case class CandidateUnavailable(generation: String) extends RuntimeException
  final case class CandidateClaim(generation: String, abandoned: Boolean)
  final case class AppliedRecord(status: BatchStatus,
                                 readyDigest: Array[Byte],
                                 mutationKeys: Long,
                                 refreshedKeys: Long,
                                 recovered: Boolean)
  final case class CommittedApplied(record: AppliedRecord, lastModifiedMillis: Long) {
    def tombstoneRetainUntilMillis(liveTTLSeconds: Int): Long =
      java.lang.Math.addExact(lastModifiedMillis, java.lang.Math.multiplyExact(liveTTLSeconds.toLong, 1000L))
  }
  final case class LeaseRecord(status: BatchStatus, readyDigest: Array[Byte], minimumActiveDeadlineMillis: Long)
  final case class VersionedLease(record: LeaseRecord, version: String)
  final case class GenerationPaths(root: Path,
                                   candidate: Path,
                                   delta: Path,
                                   state: Path,
                                   tombstones: Path,
                                   ready: Path,
                                   applied: Path,
                                   lease: Path)
}
