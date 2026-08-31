package ai.chronon.integrations.redis

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer

class IncrementalRedisStateStoreTest extends AnyFlatSpec with Matchers {

  "Redis durable lease records" should "round-trip the generation-bound checkpoint" in {
    val status = RedisBatchUpload.BatchStatus("generation-a", "2024-07-27", 1000L, 2L)
    val digest = Array.tabulate[Byte](32)(_.toByte)
    val lease = IncrementalRedisBatchModel.LeaseRecord(status, digest, minimumActiveDeadlineMillis = 2000000L)

    val decoded = IncrementalRedisStateStore.decodeLease(IncrementalRedisStateStore.encodeLease(lease))

    decoded.status shouldBe status
    decoded.readyDigest.toSeq shouldBe digest.toSeq
    decoded.minimumActiveDeadlineMillis shouldBe 2000000L
  }

  "Redis READY records" should "read legacy records and round-trip a separate delete-fence TTL" in {
    val status = RedisBatchUpload.BatchStatus("generation-a", "2024-07-27", 1000L, 2L)
    val manifest = IncrementalRedisBatchModel.ReadyManifest(
      status = status,
      parentGeneration = None,
      stateGeneration = status.generation,
      sourceTable = "source",
      batchDataset = "dataset_BATCH",
      keyPrefix = "chronon",
      stateBuckets = 4,
      sourceRows = 2L,
      dataRows = 1L,
      changedKeys = 1L,
      deletedKeys = 0L,
      deltaRows = 2L,
      tombstoneRows = 0L,
      liveTTLSeconds = 1800,
      deleteFenceTTLSeconds = 3600,
      fullRebuild = true,
      retired = false
    )

    val encoded = IncrementalRedisStateStore.encodeReady(manifest)
    IncrementalRedisStateStore.decodeReady(encoded) shouldBe manifest

    val legacy = encoded.take(encoded.length - 6) ++ encoded.takeRight(2)
    ByteBuffer.wrap(legacy, 4, 4).putInt(4)
    val decodedLegacy = IncrementalRedisStateStore.decodeReady(legacy)
    decodedLegacy.liveTTLSeconds shouldBe 1800
    decodedLegacy.deleteFenceTTLSeconds shouldBe 1800
  }

  "Redis generation cleanup" should "retain grace from the direct admitted child after the head advances again" in {
    IncrementalRedisStateStore.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 1000L,
      directChildAppliedLastModifiedMillis = Some(2000L),
      currentHeadSupersededAtMillis = None,
      graceMillis = 500L
    ) shouldBe 2500L

    IncrementalRedisStateStore.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 1000L,
      directChildAppliedLastModifiedMillis = None,
      currentHeadSupersededAtMillis = Some(2000L),
      graceMillis = 500L
    ) shouldBe 2500L

    IncrementalRedisStateStore.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 1000L,
      directChildAppliedLastModifiedMillis = Some(2000L),
      currentHeadSupersededAtMillis = Some(2500L),
      graceMillis = 500L
    ) shouldBe 3000L

    IncrementalRedisStateStore.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 4000L,
      directChildAppliedLastModifiedMillis = Some(2000L),
      currentHeadSupersededAtMillis = Some(2500L),
      graceMillis = 500L
    ) shouldBe 4000L

    IncrementalRedisStateStore.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 1000L,
      directChildAppliedLastModifiedMillis = None,
      currentHeadSupersededAtMillis = None,
      graceMillis = 500L
    ) shouldBe 1000L
  }
}
