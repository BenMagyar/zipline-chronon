package ai.chronon.integrations.redis

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{GroupBy, MetaData, PartitionSpec}
import ai.chronon.integrations.redis.RedisKVStoreConstants._

/** The source snapshot and logical destination shared by both Redis batch upload modes. */
private[redis] final case class BatchUploadJob(sourceTable: String,
                                               destinationDataset: String,
                                               sourcePartition: String,
                                               partitionSpec: PartitionSpec) {
  require(sourceTable != null && sourceTable.trim.nonEmpty, "Redis source table must be non-empty")
  require(destinationDataset != null && destinationDataset.trim.nonEmpty, "Redis destination dataset must be non-empty")
  require(sourcePartition != null && sourcePartition.nonEmpty, "Redis source partition must be non-empty")
  partitionSpec.epochMillis(sourcePartition)

  lazy val batchDataset: String =
    new GroupBy().setMetaData(new MetaData().setName(destinationDataset)).batchDataset
  lazy val batchTimestamp: Long = partitionSpec.partitionEndMillis(sourcePartition)
}

private[redis] object BatchUploadJob {
  private val PartitionColumnProperty = "spark.chronon.partition.column"
  private val PartitionFormatProperty = "spark.chronon.partition.format"
  private val PartitionSpanMillisProperty = "spark.chronon.partition.span.millis"
  private val PartitionOffsetMillisProperty = "spark.chronon.partition.offset.millis"

  def from(sourceTable: String,
           destinationDataset: String,
           sourcePartition: String,
           conf: Map[String, String]): BatchUploadJob = {
    val defaults = PartitionSpec.daily
    val partitionSpec = PartitionSpec(
      RedisBatchUploadConfig.value(conf, PartitionColumnProperty).getOrElse(defaults.column),
      RedisBatchUploadConfig.value(conf, PartitionFormatProperty).getOrElse(defaults.format),
      RedisBatchUploadConfig.long(conf, PartitionSpanMillisProperty, defaults.spanMillis),
      RedisBatchUploadConfig.long(conf, PartitionOffsetMillisProperty, defaults.offsetMillis)
    )
    BatchUploadJob(sourceTable, destinationDataset, sourcePartition, partitionSpec)
  }
}

private[redis] object RedisBatchUploadDefaults {
  val BatchSize = 1000
  val MaxKeysPerSecond = 2500000
  val WriterPartitions = 2
  val StateBuckets = 256
  val MaxRetries = 3
  val RetryBackoffMs = 1000L
  val OomRetryTimeoutMs = 60L * 60L * 1000L
  val OomRetryMaxBackoffMs = 30L * 1000L
}

/** Fixed implementation settings for the legacy all-row writer. */
private[redis] final case class FullSnapshotOptions(batchSize: Int = RedisBatchUploadDefaults.BatchSize,
                                                    ttlSeconds: Int = DataTTLSeconds) {
  require(batchSize > 0, s"Redis batch size must be positive: $batchSize")
  require(ttlSeconds > 0, s"Redis TTL must be positive: $ttlSeconds")
}

/** Internal incremental-writer mechanics. These are deliberately not deployment configuration. */
private[redis] final case class IncrementalTuning(batchSize: Int = RedisBatchUploadDefaults.BatchSize,
                                                  stateBuckets: Int = RedisBatchUploadDefaults.StateBuckets,
                                                  maxRetries: Int = RedisBatchUploadDefaults.MaxRetries,
                                                  retryBackoffMs: Long = RedisBatchUploadDefaults.RetryBackoffMs,
                                                  requireNoEviction: Boolean = true) {
  require(batchSize > 0, s"Redis batch size must be positive: $batchSize")
  require(stateBuckets > 0, s"Redis state buckets must be positive: $stateBuckets")
  require(maxRetries >= 0, s"Redis max retries must be non-negative: $maxRetries")
  require(retryBackoffMs >= 0, s"Redis retry backoff must be non-negative: $retryBackoffMs")
}

/** Supported deployment settings for durable incremental publication. */
private[redis] final case class IncrementalOptions(stateRoot: String,
                                                   maxKeysPerSecond: Int = RedisBatchUploadDefaults.MaxKeysPerSecond,
                                                   writerPartitions: Int = RedisBatchUploadDefaults.WriterPartitions,
                                                   ttlSeconds: Int = DataTTLSeconds,
                                                   deleteOlderVersions: Boolean = false,
                                                   tuning: IncrementalTuning = IncrementalTuning()) {
  require(stateRoot != null && stateRoot.trim.nonEmpty, "Redis batch upload state root must be non-empty")
  require(maxKeysPerSecond > 0, s"Redis max keys per second must be positive: $maxKeysPerSecond")
  require(writerPartitions > 0, s"Redis writer partitions must be positive: $writerPartitions")
  require(ttlSeconds > 0, s"Redis live-key TTL must be positive: $ttlSeconds")
}

/** Fully resolved inputs to one incremental publication. Runtime controls are grouped separately from the immutable
  * source and Redis destination so callers do not pass a long, order-sensitive list of primitive values.
  */
private[redis] final case class IncrementalRedisBatchSettings(job: BatchUploadJob,
                                                              target: RedisKVStoreFactory.StoreSettings,
                                                              options: IncrementalOptions) {
  def sourceTable: String = job.sourceTable
  def destinationDataset: String = job.destinationDataset
  def sourcePartition: String = job.sourcePartition
  def partitionSpec = job.partitionSpec
  def batchDataset: String = job.batchDataset
  def batchTimestamp: Long = job.batchTimestamp
  def stateRoot: String = options.stateRoot
  def keyPrefix: String = target.keyPrefix
  def clientSettings: RedisKVStoreFactory.ClientSettings = target.client
  def batchSize: Int = options.tuning.batchSize
  def maxKeysPerSecond: Int = options.maxKeysPerSecond
  def writerPartitions: Int = options.writerPartitions
  def stateBuckets: Int = options.tuning.stateBuckets
  def maxRetries: Int = options.tuning.maxRetries
  def retryBackoffMs: Long = options.tuning.retryBackoffMs
  def liveTTLSeconds: Int = options.ttlSeconds
  def requireNoEviction: Boolean = options.tuning.requireNoEviction
  def deleteOlderVersions: Boolean = options.deleteOlderVersions
}

private[redis] object IncrementalOptions {
  def from(conf: Map[String, String]): IncrementalOptions =
    IncrementalOptions(
      stateRoot = RedisBatchUploadConfig.required(conf, PropRedisBulkUploadStateRoot),
      maxKeysPerSecond = RedisBatchUploadConfig.int(conf,
                                                    PropRedisBulkUploadMaxKeysPerSecond,
                                                    RedisBatchUploadDefaults.MaxKeysPerSecond),
      writerPartitions = RedisBatchUploadConfig.int(conf,
                                                    PropRedisBulkUploadWriterPartitions,
                                                    RedisBatchUploadDefaults.WriterPartitions),
      ttlSeconds = RedisBatchUploadConfig.int(conf, PropRedisBulkUploadTTLSeconds, DataTTLSeconds),
      deleteOlderVersions =
        RedisBatchUploadConfig.boolean(conf, PropRedisBulkUploadDeleteOlderVersions, default = false)
    )
}

private object RedisBatchUploadConfig {
  def value(conf: Map[String, String], key: String): Option[String] =
    conf.get(key).map(_.trim).filter(_.nonEmpty)

  def required(conf: Map[String, String], key: String): String =
    value(conf, key).getOrElse(throw new IllegalArgumentException(s"$key must be set for Redis bulk upload"))

  def int(conf: Map[String, String], key: String, default: Int): Int =
    value(conf, key).map(_.toInt).getOrElse(default)

  def long(conf: Map[String, String], key: String, default: Long): Long =
    value(conf, key).map(_.toLong).getOrElse(default)

  def boolean(conf: Map[String, String], key: String, default: Boolean): Boolean =
    value(conf, key).map(_.toBoolean).getOrElse(default)
}
