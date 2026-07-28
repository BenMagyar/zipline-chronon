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

  // Environment variable keys for Redis Cluster
  val EnvRedisClusterNodes = "REDIS_CLUSTER_NODES" // Comma-separated: "node1:6379,node2:6379,node3:6379"
  val EnvRedisPassword = "REDIS_PASSWORD"
  val EnvRedisKeyPrefix = "REDIS_KEY_PREFIX"
  val EnvRedisMaxConnections = "REDIS_MAX_CONNECTIONS"
  val EnvRedisMinIdleConnections = "REDIS_MIN_IDLE_CONNECTIONS"
  val EnvRedisMaxIdleConnections = "REDIS_MAX_IDLE_CONNECTIONS"
  val EnvRedisConnectionTimeoutMs = "REDIS_CONNECTION_TIMEOUT_MS"
  val EnvRedisSoTimeoutMs = "REDIS_SO_TIMEOUT_MS"
  val EnvRedisMaxRedirections = "REDIS_MAX_REDIRECTIONS"

  // Prop keys for create()
  val PropTTLSeconds = "ttl-seconds"
  val PropMaxConnections = "max-connections"
  val PropKeyPrefix = "key-prefix"
}
