package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants._
import org.slf4j.LoggerFactory
import redis.clients.jedis.{ConnectionPoolConfig, HostAndPort, JedisCluster}

import scala.jdk.CollectionConverters._

/** Factory for creating Redis KVStore instances.
  *
  * This is cloud-agnostic and can be used by any Api implementation (GcpApiImpl, AwsApiImpl, etc.)
  * or directly in tests and custom applications.
  *
  * Configuration is loaded from environment variables or provided conf map:
  *  - REDIS_CLUSTER_NODES: Comma-separated cluster nodes (e.g., "node1:6379,node2:6379,node3:6379") [required]
  *  - REDIS_PASSWORD: Redis password (optional)
  *  - REDIS_MAX_CONNECTIONS: Maximum pool connections (default: 50)
  *  - REDIS_MIN_IDLE_CONNECTIONS: Minimum idle connections (default: 5)
  *  - REDIS_MAX_IDLE_CONNECTIONS: Maximum idle connections (default: 10)
  *  - REDIS_CONNECTION_TIMEOUT_MS: Connection timeout in milliseconds (default: 5000)
  *  - REDIS_SO_TIMEOUT_MS: Socket timeout in milliseconds (default: 2000)
  *  - REDIS_MAX_REDIRECTIONS: Maximum cluster redirections (default: 5)
  *
  * Example usage:
  * {{{
  *  // In GcpApiImpl or AwsApiImpl
  *  override def genKvStore: KVStore = {
  *    if (shouldUseRedis) RedisKVStoreFactory.create(conf)
  *    else createDefaultKVStore()
  *  }
  *
  *  // In tests
  *  val testConf = Map("REDIS_CLUSTER_NODES" -> "localhost:7000")
  *  val kvStore = RedisKVStoreFactory.create(testConf)
  * }}}
  */
object RedisKVStoreFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  /** Create a Redis KVStore instance from configuration.
    *
    * @param conf Configuration map (environment variables take precedence)
    * @return Initialized RedisKVStoreImpl ready for use
    * @throws IllegalArgumentException if REDIS_CLUSTER_NODES is not set
    */
  def create(conf: Map[String, String]): RedisKVStoreImpl = {
    // Parse cluster nodes from environment or config
    val nodesStr = getOrElseThrow(EnvRedisClusterNodes, conf)
    val password = getOptional(EnvRedisPassword, conf)

    val maxConnections = getOptional(EnvRedisMaxConnections, conf).map(_.toInt).getOrElse(DefaultMaxConnections)
    val configuredMinIdleConnections = getOptional(EnvRedisMinIdleConnections, conf).map(_.toInt)
    val configuredMaxIdleConnections = getOptional(EnvRedisMaxIdleConnections, conf).map(_.toInt)
    val poolConfig = buildConnectionPoolConfig(
      maxConnections,
      configuredMinIdleConnections,
      configuredMaxIdleConnections
    )
    val connectionTimeoutMs =
      getOptional(EnvRedisConnectionTimeoutMs, conf).map(_.toInt).getOrElse(DefaultConnectionTimeoutMs)
    val soTimeoutMs = getOptional(EnvRedisSoTimeoutMs, conf).map(_.toInt).getOrElse(DefaultSoTimeoutMs)
    val maxRedirections = getOptional(EnvRedisMaxRedirections, conf).map(_.toInt).getOrElse(DefaultMaxRedirections)

    // Parse cluster nodes: "node1:6379,node2:6379,node3:6379"
    val clusterNodes = nodesStr
      .split(",")
      .map { node =>
        val parts = node.trim.split(":")
        if (parts.length == 2) {
          new HostAndPort(parts(0), parts(1).toInt)
        } else {
          new HostAndPort(parts(0), DefaultPort)
        }
      }
      .toSet

    logger.info(
      s"Creating Redis Cluster KVStore with nodes: ${clusterNodes.mkString(", ")}." +
        s"Params: maxConnections=$maxConnections, minIdle=${poolConfig.getMinIdle}, " +
        s"maxIdle=${poolConfig.getMaxIdle}, connectionTimeout=$connectionTimeoutMs, " +
        s"soTimeout=$soTimeoutMs, maxRedirections=$maxRedirections"
    )

    val jedisCluster = password match {
      case Some(pwd) =>
        new JedisCluster(
          clusterNodes.asJava,
          connectionTimeoutMs,
          soTimeoutMs,
          maxRedirections,
          pwd,
          poolConfig
        )

      case None =>
        new JedisCluster(
          clusterNodes.asJava,
          connectionTimeoutMs,
          soTimeoutMs,
          maxRedirections,
          poolConfig
        )
    }

    val kvStore = new RedisKVStoreImpl(jedisCluster, conf)
    kvStore.init()
    kvStore
  }

  private def getOptional(key: String, conf: Map[String, String]): Option[String] =
    sys.env.get(key).orElse(conf.get(key))

  private[redis] def buildConnectionPoolConfig(
      maxConnections: Int,
      minIdleConnections: Option[Int],
      maxIdleConnections: Option[Int]
  ): ConnectionPoolConfig = {
    val effectiveMaxIdle = maxIdleConnections.getOrElse(math.min(DefaultMaxIdleConnections, maxConnections))
    val effectiveMinIdle = minIdleConnections.getOrElse(math.min(DefaultMinIdleConnections, effectiveMaxIdle))
    buildConnectionPoolConfig(maxConnections, effectiveMinIdle, effectiveMaxIdle)
  }

  private[redis] def buildConnectionPoolConfig(
      maxConnections: Int,
      minIdleConnections: Int,
      maxIdleConnections: Int
  ): ConnectionPoolConfig = {
    require(maxConnections > 0, s"maxConnections must be positive, got $maxConnections")
    require(minIdleConnections >= 0, s"minIdleConnections must be non-negative, got $minIdleConnections")
    require(maxIdleConnections >= 0, s"maxIdleConnections must be non-negative, got $maxIdleConnections")
    require(
      minIdleConnections <= maxIdleConnections,
      s"minIdleConnections ($minIdleConnections) must not exceed maxIdleConnections ($maxIdleConnections)"
    )
    require(
      maxIdleConnections <= maxConnections,
      s"maxIdleConnections ($maxIdleConnections) must not exceed maxConnections ($maxConnections)"
    )

    val poolConfig = new ConnectionPoolConfig()
    poolConfig.setMaxTotal(maxConnections)
    poolConfig.setMaxIdle(maxIdleConnections)
    poolConfig.setMinIdle(minIdleConnections)
    poolConfig.setTestOnBorrow(false)
    poolConfig.setTestOnReturn(false)
    poolConfig.setTestWhileIdle(true)
    poolConfig
  }

  private def getOrElseThrow(key: String, conf: Map[String, String]): String =
    getOptional(key, conf).getOrElse(
      throw new IllegalArgumentException(s"$key environment variable not set")
    )
}
