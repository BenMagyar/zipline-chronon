package ai.chronon.integrations.redis

/** Constants for Redis KVStore configuration and operation.
  */
object RedisKVStoreConstants {
  val DefaultKeyPrefix =
    "chronon" // Namespace isolation for multi-tenant deployments (set to "" for dedicated Redis to save ~8 bytes/key)
  val KeySeparator = ":" // Redis convention for hierarchical keys (e.g., "chronon:dataset:key")

  // TTL configuration
  val DataTTLSeconds = 5 * 24 * 60 * 60 // 5 days - matches BigTable retention, provides incident buffer before expiry

  // Connection pool defaults (Commons Pool2 best practices)
  val DefaultMaxConnections = 50 // Production-ready default (Jedis default of 8 is too low)
  val DefaultMinIdleConnections = 5 // Keep connections warm to avoid cold-start latency
  val DefaultMaxIdleConnections = 10 // Bound retained sockets per node; deployments can raise this for bursty traffic
  val DefaultConnectionTimeoutMs = 5000 // TCP connection timeout - allows for cluster topology discovery
  val DefaultPort = 6379

  // Pagination defaults
  val DefaultListLimit = 100 // Matches BigTable implementation, balances network efficiency and memory

  // Redis Cluster configuration
  val DefaultMaxRedirections = 5 // Matches Jedis default, protects against redirect loops during rebalancing
  val DefaultSoTimeoutMs = 2000 // Socket timeout for Redis commands (typical P99 < 10ms, 2s allows for network jitter)
  val DefaultPipelineSyncThreads = 12 // Set REDIS_PIPELINE_SYNC_THREADS=0 to restore Jedis' per-pipeline executors

  // Incremental batch upload defaults. Absolute expirations fence rollback without reading unchanged value payloads;
  // the five-day lease leaves several missed daily uploads of recovery margin.
  val DefaultBulkUploadBatchSize = 1000
  val DefaultBulkUploadMaxKeysPerSecond = 2500000
  val DefaultBulkUploadWriterPartitions = 2
  val DefaultBulkUploadStateBuckets = 256
  val DefaultBulkUploadMaxRetries = 3
  val DefaultBulkUploadRetryBackoffMs = 1000L
  val DefaultBulkUploadOomRetryTimeoutMs = 30L * 60L * 1000L
  val DefaultBulkUploadOomRetryMaxBackoffMs = 30L * 1000L
  val DefaultBulkUploadTTLSeconds = DataTTLSeconds
  val DefaultBulkUploadRequireNoEviction = true

  // Environment variable keys for Redis Cluster
  val EnvRedisClusterNodes = "REDIS_CLUSTER_NODES" // Comma-separated: "node1:6379,node2:6379,node3:6379"
  val EnvRedisUseSsl = "REDIS_USE_SSL"
  val EnvRedisPassword = "REDIS_PASSWORD"
  val EnvRedisUsername = "REDIS_USERNAME"
  val EnvRedisUseSSL = EnvRedisUseSsl
  val EnvRedisKeyPrefix = "REDIS_KEY_PREFIX"
  val EnvRedisMaxConnections = "REDIS_MAX_CONNECTIONS"
  val EnvRedisMinIdleConnections = "REDIS_MIN_IDLE_CONNECTIONS"
  val EnvRedisMaxIdleConnections = "REDIS_MAX_IDLE_CONNECTIONS"
  val EnvRedisConnectionTimeoutMs = "REDIS_CONNECTION_TIMEOUT_MS"
  val EnvRedisSoTimeoutMs = "REDIS_SO_TIMEOUT_MS"
  val EnvRedisMaxRedirections = "REDIS_MAX_REDIRECTIONS"
  val EnvRedisPipelineSyncThreads = "REDIS_PIPELINE_SYNC_THREADS"

  // RedisKVStoreImpl configuration keys
  val ConfigPipelineSyncThreads = "redis.pipeline.sync.threads"

  // Canonical map/Spark properties. Environment variables above take precedence.
  val PropRedisClusterNodes = "redis.cluster.nodes"
  val PropRedisPassword = "redis.password"
  val PropRedisUsername = "redis.username"
  val PropRedisSSL = "redis.ssl"
  val PropRedisKeyPrefix = "redis.key.prefix"
  val PropRedisMaxConnections = "redis.max.connections"
  val PropRedisMinIdleConnections = "redis.min.idle.connections"
  val PropRedisMaxIdleConnections = "redis.max.idle.connections"
  val PropRedisConnectionTimeoutMs = "redis.connection.timeout.ms"
  val PropRedisSoTimeoutMs = "redis.so.timeout.ms"
  val PropRedisMaxRedirections = "redis.max.redirections"

  val PropRedisBulkUploadStateRoot = "spark.chronon.kv_upload.redis.state_root"
  val PropRedisBulkUploadBatchSize = "spark.chronon.kv_upload.redis.batch_size"
  val PropRedisBulkUploadMaxKeysPerSecond = "spark.chronon.kv_upload.redis.max_keys_per_second"
  val PropRedisBulkUploadWriterPartitions = "spark.chronon.kv_upload.redis.writer_partitions"
  val PropRedisBulkUploadStateBuckets = "spark.chronon.kv_upload.redis.state_buckets"
  val PropRedisBulkUploadMaxRetries = "spark.chronon.kv_upload.redis.max_retries"
  val PropRedisBulkUploadRetryBackoffMs = "spark.chronon.kv_upload.redis.retry_backoff_ms"
  val PropRedisBulkUploadTTLSeconds = "spark.chronon.kv_upload.redis.ttl_seconds"
  val PropRedisBulkUploadRequireNoEviction = "spark.chronon.kv_upload.redis.require_noeviction"
}
