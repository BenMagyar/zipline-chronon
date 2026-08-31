package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class RedisBatchUploadTest extends AnyFlatSpec with Matchers {
  "Redis batch status" should "round-trip as a binary record" in {
    val status = RedisBatchUpload.BatchStatus(
      generation = "20240727T120000Z-abcd1234",
      sourcePartition = "2024-07-27-12",
      batchTimestamp = 1722081600000L,
      writeEpoch = 42L,
      retired = false
    )

    RedisBatchUpload.decodeStatus(RedisBatchUpload.encodeStatus(status)).get shouldBe status
  }

  it should "reject corrupt, truncated, and unsafe records" in {
    val status = RedisBatchUpload.BatchStatus("generation-1", "2024-07-27", 1234L, 1L)
    val encoded = RedisBatchUpload.encodeStatus(status)

    RedisBatchUpload.decodeStatus(null).isFailure shouldBe true
    RedisBatchUpload.decodeStatus(encoded.take(encoded.length - 1)).isFailure shouldBe true
    RedisBatchUpload.decodeStatus(encoded ++ Array(1.toByte)).isFailure shouldBe true
    RedisBatchUpload.decodeStatus(encoded.updated(17, 0.toByte)).isFailure shouldBe true
    an[IllegalArgumentException] should be thrownBy {
      RedisBatchUpload.BatchStatus("unsafe/generation", "2024-07-27", 1L, 1L)
    }
  }

  "Redis batch value encoding" should "round-trip upserts and tombstones with ordering metadata" in {
    val timestamp = 1722081600000L
    val payload = "payload".getBytes(StandardCharsets.UTF_8)
    val upsert = RedisBatchUpload.decodeValue(
      RedisBatchUpload.encodeValue(RedisBatchUpload.Upsert, timestamp, payload, writeEpoch = 42L)
    ).get

    upsert.operation shouldBe RedisBatchUpload.Upsert
    upsert.storedTimestamp shouldBe timestamp
    upsert.writeEpoch shouldBe 42L
    upsert.payload shouldBe payload
    upsert.legacy shouldBe false

    val tombstone = RedisBatchUpload.decodeValue(
      RedisBatchUpload.encodeValue(RedisBatchUpload.Delete, timestamp, null, writeEpoch = 43L)
    ).get
    tombstone.operation shouldBe RedisBatchUpload.Delete
    tombstone.payload shouldBe empty
    tombstone.writeEpoch shouldBe 43L
  }

  it should "remain compatible with the original timestamp-prefixed value" in {
    val timestamp = 123456789L
    val payload = Array[Byte](1, 2, 3)
    val legacy = ByteBuffer.allocate(8 + payload.length).putLong(timestamp).put(payload).array()

    val decoded = RedisBatchUpload.decodeValue(legacy).get
    decoded.operation shouldBe RedisBatchUpload.Upsert
    decoded.storedTimestamp shouldBe timestamp
    decoded.writeEpoch shouldBe 0L
    decoded.payload shouldBe payload
    decoded.legacy shouldBe true
  }

  it should "treat incomplete protocol markers in legacy payloads as legacy data" in {
    val timestamp = 123456789L
    val unsupportedVersion = "CRB2".getBytes(StandardCharsets.UTF_8) ++ Array[Byte](2, 1) ++ Array.fill[Byte](8)(0)
    val invalidOperation = "CRB2".getBytes(StandardCharsets.UTF_8) ++ Array[Byte](1, 3) ++ Array.fill[Byte](8)(0)

    Seq(unsupportedVersion, invalidOperation).foreach { payload =>
      val legacy = ByteBuffer.allocate(8 + payload.length).putLong(timestamp).put(payload).array()
      val decoded = RedisBatchUpload.decodeValue(legacy).get
      decoded.legacy shouldBe true
      decoded.payload shouldBe payload
    }
  }

  it should "reject invalid values and operations" in {
    RedisBatchUpload.decodeValue(null).isFailure shouldBe true
    RedisBatchUpload.decodeValue(Array.fill[Byte](7)(0)).isFailure shouldBe true
    an[IllegalArgumentException] should be thrownBy RedisBatchUpload.Operation.fromName("REPLACE")
    an[IllegalArgumentException] should be thrownBy {
      RedisBatchUpload.encodeValue(RedisBatchUpload.Upsert, 1L, null)
    }
    an[IllegalArgumentException] should be thrownBy {
      RedisBatchUpload.encodeValue(RedisBatchUpload.Delete, -1L, null)
    }
  }

  "Redis batch internal keys" should "route status and cluster-wide rate keys predictably" in {
    RedisBatchUpload.buildStatusKey("features_BATCH", "chronon") shouldBe
      "chronon:{features_BATCH}:__chronon_batch_status"
    RedisBatchUpload.buildPublicationModeKey("features_BATCH", "chronon") shouldBe
      "chronon:{features_BATCH}:__chronon_publication_mode"
    RedisBatchUpload.RateLimiterKey shouldBe "{__chronon_batch_rate}"
    RedisBatchUpload.buildStatusKey("features_BATCH", "") shouldBe
      "{features_BATCH}:__chronon_batch_status"
    RedisBatchUpload.buildPublicationModeKey("features_BATCH", "") shouldBe
      "{features_BATCH}:__chronon_publication_mode"
  }

  "Redis batch identifiers" should "reject path separators and spaces" in {
    RedisBatchUpload.isSafeIdentifier("20240727T120000Z-abcd_1234.1") shouldBe true
    RedisBatchUpload.isSafeIdentifier("contains/slash") shouldBe false
    RedisBatchUpload.isSafeIdentifier("contains space") shouldBe false
    RedisBatchUpload.isSafeIdentifier("") shouldBe false
    RedisBatchUpload.isSafeIdentifier(null) shouldBe false
  }

  "Redis batch defaults" should "count attempted key commands at the requested production rate" in {
    FullSnapshotOptions().batchSize shouldBe 1000
    val incremental = IncrementalOptions("s3://bucket/state")
    incremental.tuning.batchSize shouldBe 1000
    incremental.maxKeysPerSecond shouldBe 2500000
    RedisBatchUploadDefaults.OomRetryTimeoutMs shouldBe 60 * 60 * 1000L
    RedisBatchUploadDefaults.OomRetryMaxBackoffMs shouldBe 30 * 1000L
    incremental.ttlSeconds shouldBe 5 * 24 * 60 * 60
    incremental.deleteOlderVersions shouldBe false
  }

}
