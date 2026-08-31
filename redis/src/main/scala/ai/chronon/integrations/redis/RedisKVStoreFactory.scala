package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants._
import org.slf4j.LoggerFactory
import redis.clients.jedis.{
  Connection,
  ConnectionPoolConfig,
  DefaultJedisClientConfig,
  HostAndPort,
  HostAndPortMapper,
  JedisCluster,
  Protocol
}
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.providers.ClusterConnectionProvider

import java.time.Duration
import java.util.{Locale, WeakHashMap}
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Factory for creating Redis KVStore instances.
  *
  * This is cloud-agnostic and can be used by any Api implementation (GcpApiImpl, AwsApiImpl, etc.)
  * or directly in tests and custom applications.
  *
  * Configuration is loaded from environment variables or provided conf map:
  *  - REDIS_CLUSTER_NODES: Comma-separated cluster nodes (e.g., "node1:6379,node2:6379,node3:6379") [required]
  *  - REDIS_USE_SSL: Enable TLS (required for ElastiCache in-transit encryption; default false)
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
    val storeSettings = settingsForStore(conf)
    val jedisCluster = createClient(storeSettings.client)
    val kvStore = new RedisKVStoreImpl(jedisCluster, conf)
    kvStore.init()
    kvStore
  }

  /** Resolves the batch-upload mode from the properties submitted to the upload job. */
  def configuredBatchUploadMode(conf: Map[String, String],
                                defaultMode: RedisBatchModeSelection): RedisBatchModeSelection =
    conf
      .get(PropRedisBatchMode)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(RedisBatchModeSelection.parse)
      .getOrElse(defaultMode)

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
    // Keep connection validation off the request path and run it on idle connections instead.
    poolConfig.setTestOnBorrow(false)
    poolConfig.setTestOnReturn(false)
    poolConfig.setTestWhileIdle(true)
    poolConfig.setTimeBetweenEvictionRuns(Duration.ofSeconds(30))
    // Jedis' ConnectionPoolConfig supplies the 60-second idle-eviction default; the test locks that contract.
    poolConfig.setNumTestsPerEvictionRun(-1)
    poolConfig
  }

  case class RedisEndpoint(host: String, port: Int) extends Serializable

  /** Connection and namespace settings resolved together so environment precedence is applied exactly once. */
  private[redis] final case class StoreSettings(client: ClientSettings, keyPrefix: String)

  case class ClientSettings(nodes: String,
                            username: Option[String],
                            password: Option[String],
                            ssl: Boolean,
                            maxConnections: Int,
                            minIdleConnections: Int,
                            maxIdleConnections: Int,
                            connectionTimeoutMs: Int,
                            soTimeoutMs: Int,
                            maxRedirections: Int,
                            advertisedPortMappings: Map[Int, RedisEndpoint] = Map.empty,
                            clientNoTouch: Boolean = false)
      extends Serializable {

    /** Spark tasks issue one pipeline at a time. Large serving pools multiplied by hundreds of tasks can exhaust the
      * Redis connection limit before the cluster-wide write-rate cap becomes effective.
      */
    def bulkWriter: ClientSettings =
      copy(maxConnections = 2, minIdleConnections = 0, maxIdleConnections = 1)
  }

  def settings(conf: Map[String, String], env: Map[String, String] = sys.env): ClientSettings = {
    val maxConnections =
      intSetting(EnvRedisMaxConnections, PropRedisMaxConnections, DefaultMaxConnections, conf, env)
    val maxIdleConnections = getOptional(EnvRedisMaxIdleConnections, PropRedisMaxIdleConnections, conf, env)
      .map(_.toInt)
      .getOrElse(math.min(DefaultMaxIdleConnections, maxConnections))
    val minIdleConnections = getOptional(EnvRedisMinIdleConnections, PropRedisMinIdleConnections, conf, env)
      .map(_.toInt)
      .getOrElse(math.min(DefaultMinIdleConnections, maxIdleConnections))

    ClientSettings(
      nodes = getOrElseThrow(EnvRedisClusterNodes, PropRedisClusterNodes, conf, env),
      username = getOptional(EnvRedisUsername, PropRedisUsername, conf, env),
      password = getOptional(EnvRedisPassword, PropRedisPassword, conf, env),
      ssl = getOptional(EnvRedisUseSsl, PropRedisSSL, conf, env).exists(_.toBoolean),
      maxConnections = maxConnections,
      minIdleConnections = minIdleConnections,
      maxIdleConnections = maxIdleConnections,
      connectionTimeoutMs =
        intSetting(EnvRedisConnectionTimeoutMs, PropRedisConnectionTimeoutMs, DefaultConnectionTimeoutMs, conf, env),
      soTimeoutMs = intSetting(EnvRedisSoTimeoutMs, PropRedisSoTimeoutMs, DefaultSoTimeoutMs, conf, env),
      maxRedirections = intSetting(EnvRedisMaxRedirections, PropRedisMaxRedirections, DefaultMaxRedirections, conf, env)
    )
  }

  private[redis] def settingsForStore(conf: Map[String, String], env: Map[String, String] = sys.env): StoreSettings =
    StoreSettings(settings(conf, env), keyPrefix(conf, env))

  private[redis] def createClient(settings: ClientSettings): JedisCluster = {
    val clusterNodes = settings.nodes
      .split(",")
      .map(parseEndpoint)
      .toSet

    logger.info(
      s"Creating Redis Cluster KVStore with nodes: ${clusterNodes.mkString(", ")}." +
        s"Params: ssl=${settings.ssl}, usernameSet=${settings.username.isDefined}, " +
        s"maxConnections=${settings.maxConnections}, minIdle=${settings.minIdleConnections}, " +
        s"maxIdle=${settings.maxIdleConnections}, connectionTimeout=${settings.connectionTimeoutMs}, " +
        s"soTimeout=${settings.soTimeoutMs}, maxRedirections=${settings.maxRedirections}, " +
        s"clientNoTouch=${settings.clientNoTouch}"
    )

    // Keep #2097's no-borrow/no-return-PING policy for serving and bulk clients alike.
    val poolConfig = buildConnectionPoolConfig(
      settings.maxConnections,
      settings.minIdleConnections,
      settings.maxIdleConnections
    )

    val clientConfigBuilder = DefaultJedisClientConfig
      .builder()
      .connectionTimeoutMillis(settings.connectionTimeoutMs)
      .socketTimeoutMillis(settings.soTimeoutMs)
      .ssl(settings.ssl)
    if (settings.ssl) {
      // ElastiCache cluster mode returns node IPs in the slot map; disable endpoint
      // identification so Jedis can connect to those IPs without hostname mismatch.
      // Certificate chain validation still uses the JVM default trust store.
      clientConfigBuilder.hostnameVerifier((_, _) => true).sslParameters(elastiCacheSslParams())
    }
    settings.username.foreach(clientConfigBuilder.user)
    settings.password.foreach(clientConfigBuilder.password)
    if (settings.advertisedPortMappings.nonEmpty) {
      clientConfigBuilder.hostAndPortMapper(new HostAndPortMapper {
        override def getHostAndPort(advertised: HostAndPort): HostAndPort =
          settings.advertisedPortMappings
            .get(advertised.getPort)
            .map(endpoint => new HostAndPort(endpoint.host, endpoint.port))
            .getOrElse(advertised)
      })
    }

    val clientConfig = clientConfigBuilder.build()
    val connectionPoolConfig =
      poolConfig.asInstanceOf[org.apache.commons.pool2.impl.GenericObjectPoolConfig[Connection]]
    if (settings.clientNoTouch) {
      val provider = new NoTouchClusterConnectionProvider(clusterNodes.asJava, clientConfig, connectionPoolConfig)
      val retriesDuration =
        Duration.ofMillis(java.lang.Math.multiplyExact(settings.soTimeoutMs.toLong, settings.maxRedirections.toLong))
      new JedisCluster(provider, settings.maxRedirections, retriesDuration)
    } else {
      new JedisCluster(clusterNodes.asJava, clientConfig, settings.maxRedirections, connectionPoolConfig)
    }
  }

  /** Checks the current cluster for CLIENT NO-TOUCH without leaving the shared control pool in no-touch mode. */
  private[redis] def supportsClientNoTouch(client: JedisCluster): Boolean = {
    val pools = client.getClusterNodes.values().asScala.toSeq
    require(pools.nonEmpty, "Redis cluster has no node pools for CLIENT NO-TOUCH capability detection")
    pools.forall { pool =>
      val connection = pool.getResource
      var enabled = false
      var reusable = true
      try {
        setClientNoTouch(connection, enabled = true)
        enabled = true
        setClientNoTouch(connection, enabled = false)
        enabled = false
        true
      } catch {
        case error: JedisDataException if unavailableClientNoTouch(error) => false
        case error: Throwable =>
          reusable = false
          throw error
      } finally {
        if (enabled) {
          try setClientNoTouch(connection, enabled = false)
          catch {
            case _: Throwable => reusable = false
          }
        }
        if (!reusable) connection.setBroken()
        connection.close()
      }
    }
  }

  private[redis] def setClientNoTouch(connection: Connection, enabled: Boolean): Unit = {
    connection.sendCommand(Protocol.Command.CLIENT, "NO-TOUCH", if (enabled) "ON" else "OFF")
    val response = connection.getStatusCodeReply
    require(response == "OK", s"Redis CLIENT NO-TOUCH returned an unexpected response: $response")
  }

  private def unavailableClientNoTouch(error: JedisDataException): Boolean = {
    val message = Option(error.getMessage).getOrElse("").toUpperCase(Locale.ROOT)
    message.contains("UNKNOWN SUBCOMMAND") || message.contains("UNKNOWN COMMAND") || message.startsWith("NOPERM")
  }

  private final class NoTouchClusterConnectionProvider(
      nodes: java.util.Set[HostAndPort],
      clientConfig: redis.clients.jedis.JedisClientConfig,
      poolConfig: org.apache.commons.pool2.impl.GenericObjectPoolConfig[Connection]
  ) extends ClusterConnectionProvider(nodes, clientConfig, poolConfig) {

    private val noTouchInitializer =
      new ClientNoTouchInitializer(connection => setClientNoTouch(connection, enabled = true))

    override def getConnection(node: HostAndPort): Connection = enableNoTouch(super.getConnection(node))

    override def getConnection(): Connection = enableNoTouch(super.getConnection())

    override def getConnectionFromSlot(slot: Int): Connection = enableNoTouch(super.getConnectionFromSlot(slot))

    private def enableNoTouch(connection: Connection): Connection = {
      try {
        noTouchInitializer.initialize(connection)
        connection
      } catch {
        case error: Throwable =>
          connection.setBroken()
          Try(connection.close())
          throw error
      }
    }
  }

  private[redis] final class ClientNoTouchInitializer(initializeConnection: Connection => Unit) {
    // CLIENT state survives pool returns for the socket lifetime; weak keys do not retain removed or broken sockets.
    private val initializedConnections = new WeakHashMap[Connection, java.lang.Boolean]()

    def initialize(connection: Connection): Unit = connection.synchronized {
      val initialized = initializedConnections.synchronized {
        initializedConnections.containsKey(connection)
      }
      if (!initialized) {
        initializeConnection(connection)
        initializedConnections.synchronized {
          initializedConnections.put(connection, java.lang.Boolean.TRUE)
        }
      }
    }
  }

  private[redis] def parseEndpoint(node: String): HostAndPort = {
    val trimmed = node.trim
    require(trimmed.nonEmpty, "Redis cluster node must not be empty")
    require(!trimmed.exists(_.isWhitespace), s"Redis cluster node must not contain whitespace: $trimmed")

    val colonCount = trimmed.count(_ == ':')
    if (colonCount == 0) {
      new HostAndPort(trimmed, DefaultPort)
    } else {
      require(colonCount == 1, s"Redis cluster node must be host or host:port: $trimmed")
      val Array(host, portText) = trimmed.split(":", -1)
      require(host.nonEmpty && portText.nonEmpty, s"Redis cluster node must be host or host:port: $trimmed")
      new HostAndPort(host, portText.toInt)
    }
  }

  private[redis] def getOptional(envKey: String,
                                 propKey: String,
                                 conf: Map[String, String],
                                 env: Map[String, String] = sys.env): Option[String] =
    env
      .get(envKey)
      .orElse(conf.get(envKey))
      .orElse(conf.get(propKey))
      .map(_.trim)
      .filter(_.nonEmpty)

  private def getOrElseThrow(envKey: String,
                             propKey: String,
                             conf: Map[String, String],
                             env: Map[String, String]): String =
    getOptional(envKey, propKey, conf, env).getOrElse(
      throw new IllegalArgumentException(s"$envKey or $propKey must be set")
    )

  private def intSetting(envKey: String,
                         propKey: String,
                         default: Int,
                         conf: Map[String, String],
                         env: Map[String, String]): Int =
    getOptional(envKey, propKey, conf, env)
      .map(_.toInt)
      .getOrElse(default)

  private[redis] def keyPrefix(conf: Map[String, String], env: Map[String, String] = sys.env): String =
    env
      .get(EnvRedisKeyPrefix)
      .orElse(conf.get(EnvRedisKeyPrefix))
      .orElse(conf.get(PropRedisKeyPrefix))
      .map(_.trim)
      .getOrElse(DefaultKeyPrefix)
}
