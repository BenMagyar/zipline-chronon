package ai.chronon.integrations.redis

import ai.chronon.api.PartitionSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RedisBatchUploadConfigTest extends AnyFlatSpec with Matchers {

  "Incremental Redis upload options" should "parse the supported deployment settings together" in {
    val options = IncrementalOptions.from(
      Map(
        RedisKVStoreConstants.PropRedisBulkUploadStateRoot -> " s3://bucket/redis-state ",
        RedisKVStoreConstants.PropRedisBulkUploadMaxKeysPerSecond -> "100000",
        RedisKVStoreConstants.PropRedisBulkUploadWriterPartitions -> "16",
        RedisKVStoreConstants.PropRedisBulkUploadTTLSeconds -> "3600",
        RedisKVStoreConstants.PropRedisBulkUploadDeleteOlderVersions -> "true"
      ))

    options shouldBe IncrementalOptions(
      stateRoot = "s3://bucket/redis-state",
      maxKeysPerSecond = 100000,
      writerPartitions = 16,
      ttlSeconds = 3600,
      deleteOlderVersions = true
    )
    options.tuning shouldBe IncrementalTuning()
  }

  it should "require durable state and validate retained overrides" in {
    an[IllegalArgumentException] should be thrownBy IncrementalOptions.from(Map.empty)
    an[IllegalArgumentException] should be thrownBy IncrementalOptions.from(
      Map(
        RedisKVStoreConstants.PropRedisBulkUploadStateRoot -> "s3://bucket/redis-state",
        RedisKVStoreConstants.PropRedisBulkUploadMaxKeysPerSecond -> "0"
      ))
    an[IllegalArgumentException] should be thrownBy IncrementalOptions.from(
      Map(
        RedisKVStoreConstants.PropRedisBulkUploadStateRoot -> "s3://bucket/redis-state",
        RedisKVStoreConstants.PropRedisBulkUploadDeleteOlderVersions -> "not-a-boolean"
      ))
  }

  "Redis batch upload jobs" should "derive shared snapshot metadata once" in {
    val job = BatchUploadJob.from(
      sourceTable = "catalog.schema.upload",
      destinationDataset = "ranking.user.views",
      sourcePartition = "2024-07-27",
      conf = Map.empty
    )

    job.partitionSpec shouldBe PartitionSpec.daily
    job.batchDataset shouldBe "RANKING_USER_VIEWS_BATCH"
    job.batchTimestamp shouldBe PartitionSpec.daily.partitionEndMillis("2024-07-27")
  }

  "Full-snapshot Redis options" should "keep implementation mechanics internal" in {
    FullSnapshotOptions() shouldBe FullSnapshotOptions(batchSize = 1000, ttlSeconds = 5 * 24 * 60 * 60)
    IncrementalTuning().requireNoEviction shouldBe true
  }

}
