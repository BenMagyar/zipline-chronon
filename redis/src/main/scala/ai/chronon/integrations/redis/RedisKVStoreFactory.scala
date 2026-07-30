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
  * Configuration is loaded from environment variables or the provided conf map. Environment variables take
  * precedence over env-style conf keys, which take precedence over canonical `redis.*` properties.
  */
object RedisKVStoreFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  def create(conf: Map[String, String],
             conditionalObjectWriter: ConditionalObjectWriter = HadoopConditionalObjectWriter): RedisKVStoreImpl =
    createStore(conf, conditionalObjectWriter, requireBatchStatus = false)

  /** Creates the Redis store used by an API whose batch uploads use the durable publication protocol. */
  def createManaged(conf: Map[String, String], conditionalObjectWriter: ConditionalObjectWriter): RedisKVStoreImpl =
    createStore(conf, conditionalObjectWriter, requireBatchStatus = true)

  private def createStore(conf: Map[String, String],
                          conditionalObjectWriter: ConditionalObjectWriter,
                          requireBatchStatus: Boolean): RedisKVStoreImpl = {
    val clientSettings = settings(conf)
    val jedisCluster = createClient(clientSettings)
    val effectiveConf = conf.updated(ConfigPipelineSyncThreads, clientSettings.pipelineSyncThreads.toString)
    val kvStore =
      new RedisKVStoreImpl(jedisCluster, effectiveConf, conditionalObjectWriter, requireBatchStatus)
    kvStore.init()
    kvStore
  }

  case class RedisEndpoint(host: String, port: Int) extends Serializable

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
                            pipelineSyncThreads: Int = DefaultPipelineSyncThreads,
                            validateConnections: Boolean = false,
                            advertisedPortMappings: Map[Int, RedisEndpoint] = Map.empty,
                            clientNoTouch: Boolean = false)
      extends Serializable {

    /** Spark tasks issue one pipeline at a time. Keep their per-node pools deliberately small. */
    def bulkWriter: ClientSettings =
      copy(maxConnections = 2, minIdleConnections = 0, maxIdleConnections = 1, validateConnections = false)
  }

  def settings(conf: Map[String, String], env: Map[String, String] = sys.env): ClientSettings = {
    val maxConnections =
      intSetting(EnvRedisMaxConnections, PropRedisMaxConnections, DefaultMaxConnections, conf, env)
    val poolConfig = buildConnectionPoolConfig(
      maxConnections,
      optionalIntSetting(EnvRedisMinIdleConnections, PropRedisMinIdleConnections, conf, env),
      optionalIntSetting(EnvRedisMaxIdleConnections, PropRedisMaxIdleConnections, conf, env)
    )
    val pipelineSyncThreads =
      intSetting(EnvRedisPipelineSyncThreads, ConfigPipelineSyncThreads, DefaultPipelineSyncThreads, conf, env)
    require(pipelineSyncThreads >= 0, s"$ConfigPipelineSyncThreads must be non-negative, got $pipelineSyncThreads")

    ClientSettings(
      nodes = getOrElseThrow(EnvRedisClusterNodes, PropRedisClusterNodes, conf, env),
      username = getOptional(EnvRedisUsername, PropRedisUsername, conf, env),
      password = getOptional(EnvRedisPassword, PropRedisPassword, conf, env),
      ssl = getOptional(EnvRedisUseSsl, PropRedisSSL, conf, env).exists(_.toBoolean),
      maxConnections = maxConnections,
      minIdleConnections = poolConfig.getMinIdle,
      maxIdleConnections = poolConfig.getMaxIdle,
      connectionTimeoutMs =
        intSetting(EnvRedisConnectionTimeoutMs, PropRedisConnectionTimeoutMs, DefaultConnectionTimeoutMs, conf, env),
      soTimeoutMs = intSetting(EnvRedisSoTimeoutMs, PropRedisSoTimeoutMs, DefaultSoTimeoutMs, conf, env),
      maxRedirections =
        intSetting(EnvRedisMaxRedirections, PropRedisMaxRedirections, DefaultMaxRedirections, conf, env),
      pipelineSyncThreads = pipelineSyncThreads
    )
  }

  private[redis] def createClient(settings: ClientSettings): JedisCluster = {
    val clusterNodes = settings.nodes
      .split(",", -1)
      .map(parseEndpoint)
      .toSet

    logger.info(
      s"Creating Redis Cluster KVStore with nodes: ${clusterNodes.mkString(", ")}. " +
        s"Params: ssl=${settings.ssl}, usernameSet=${settings.username.isDefined}, " +
        s"maxConnections=${settings.maxConnections}, minIdle=${settings.minIdleConnections}, " +
        s"maxIdle=${settings.maxIdleConnections}, connectionTimeout=${settings.connectionTimeoutMs}, " +
        s"soTimeout=${settings.soTimeoutMs}, maxRedirections=${settings.maxRedirections}, " +
        s"pipelineSyncThreads=${settings.pipelineSyncThreads}, clientNoTouch=${settings.clientNoTouch}"
    )

    val poolConfig =
      buildConnectionPoolConfig(settings.maxConnections, settings.minIdleConnections, settings.maxIdleConnections)
    poolConfig.setTestOnBorrow(settings.validateConnections)
    poolConfig.setTestOnReturn(settings.validateConnections)

    val clientConfigBuilder = DefaultJedisClientConfig
      .builder()
      .connectionTimeoutMillis(settings.connectionTimeoutMs)
      .socketTimeoutMillis(settings.soTimeoutMs)
      .ssl(settings.ssl)
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
    if (settings.clientNoTouch) {
      val provider = new NoTouchClusterConnectionProvider(clusterNodes.asJava, clientConfig, poolConfig)
      val retriesDuration =
        Duration.ofMillis(java.lang.Math.multiplyExact(settings.soTimeoutMs.toLong, settings.maxRedirections.toLong))
      new JedisCluster(provider, settings.maxRedirections, retriesDuration)
    } else {
      new JedisCluster(clusterNodes.asJava, clientConfig, settings.maxRedirections, poolConfig)
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
      val port = portText.toInt
      require(port > 0 && port <= 65535, s"Redis cluster node port must be between 1 and 65535: $trimmed")
      new HostAndPort(host, port)
    }
  }

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

  private def optionalIntSetting(envKey: String,
                                 propKey: String,
                                 conf: Map[String, String],
                                 env: Map[String, String]): Option[Int] =
    getOptional(envKey, propKey, conf, env).map(_.toInt)

  private[redis] def keyPrefix(conf: Map[String, String], env: Map[String, String] = sys.env): String =
    env
      .get(EnvRedisKeyPrefix)
      .orElse(conf.get(EnvRedisKeyPrefix))
      .orElse(conf.get(PropRedisKeyPrefix))
      .map(_.trim)
      .getOrElse(DefaultKeyPrefix)
}
