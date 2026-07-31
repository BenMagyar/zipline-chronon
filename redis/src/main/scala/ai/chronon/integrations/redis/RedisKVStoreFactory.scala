package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants._
import glide.api.GlideClusterClient
import glide.api.models.configuration.{
  AddressResolver,
  AdvancedGlideClusterClientConfiguration,
  GlideClusterClientConfiguration,
  NodeAddress,
  ReadFrom => GlideReadFrom,
  ResolvedAddress,
  ServerCredentials
}
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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.{Locale, WeakHashMap}
import scala.jdk.CollectionConverters._
import scala.language.existentials
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
    var glideReadClient = Option.empty[GlideRedisReadClient]
    try {
      glideReadClient =
        if (clientSettings.client == RedisClientGlide) Some(createGlideReadClient(clientSettings)) else None
      val effectiveConf = conf ++ Map(
        ConfigPipelineSyncThreads -> clientSettings.pipelineSyncThreads.toString,
        PropRedisClient -> clientSettings.client,
        PropRedisReadFrom -> clientSettings.readFrom
      ) ++ clientSettings.clientAz.map(PropRedisClientAz -> _)
      val kvStore =
        new RedisKVStoreImpl(jedisCluster, effectiveConf, conditionalObjectWriter, requireBatchStatus, glideReadClient)
      kvStore.init()
      kvStore
    } catch {
      case error: Throwable =>
        glideReadClient.foreach { client =>
          try client.close()
          catch { case closeError: Throwable => error.addSuppressed(closeError) }
        }
        try jedisCluster.close()
        catch { case closeError: Throwable => error.addSuppressed(closeError) }
        throw error
    }
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
                            clientNoTouch: Boolean = false,
                            client: String = DefaultRedisClient,
                            readFrom: String = DefaultRedisReadFrom,
                            clientAz: Option[String] = None)
      extends Serializable {

    /** Spark tasks issue one pipeline at a time. Keep their per-node pools deliberately small. */
    def bulkWriter: ClientSettings =
      copy(maxConnections = 2, minIdleConnections = 0, maxIdleConnections = 1, validateConnections = false)
  }

  def settings(conf: Map[String, String], env: Map[String, String] = sys.env): ClientSettings =
    settings(conf, env, () => projectedClientAz())

  private[redis] def settings(conf: Map[String, String],
                              env: Map[String, String],
                              projectedClientAzLookup: () => Option[String]): ClientSettings = {
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
    val connectionTimeoutMs =
      intSetting(EnvRedisConnectionTimeoutMs, PropRedisConnectionTimeoutMs, DefaultConnectionTimeoutMs, conf, env)
    val soTimeoutMs = intSetting(EnvRedisSoTimeoutMs, PropRedisSoTimeoutMs, DefaultSoTimeoutMs, conf, env)

    val client = enumSetting(
      EnvRedisClient,
      PropRedisClient,
      DefaultRedisClient,
      Set(RedisClientJedis, RedisClientGlide),
      conf,
      env
    )
    require(
      client != RedisClientGlide || connectionTimeoutMs > 0,
      s"$EnvRedisConnectionTimeoutMs/$PropRedisConnectionTimeoutMs must be positive for GLIDE, got $connectionTimeoutMs"
    )
    require(
      client != RedisClientGlide || soTimeoutMs > 0,
      s"$EnvRedisSoTimeoutMs/$PropRedisSoTimeoutMs must be positive for GLIDE, got $soTimeoutMs"
    )
    val readFrom = enumSetting(
      EnvRedisReadFrom,
      PropRedisReadFrom,
      DefaultRedisReadFrom,
      Set(RedisReadFromPrimary, RedisReadFromAllNodes, RedisReadFromAzAffinityReplicasAndPrimary),
      conf,
      env
    )
    val configuredClientAz = conf.get(PropRedisClientAz).map(_.trim).filter(_.nonEmpty)
    val clientAz =
      if (readFrom == RedisReadFromAzAffinityReplicasAndPrimary)
        configuredClientAz.orElse(projectedClientAzLookup())
      else configuredClientAz
    require(
      client == RedisClientGlide || readFrom == RedisReadFromPrimary,
      s"$EnvRedisReadFrom/$PropRedisReadFrom=$readFrom requires $EnvRedisClient/$PropRedisClient=$RedisClientGlide"
    )
    require(
      readFrom != RedisReadFromAzAffinityReplicasAndPrimary || clientAz.isDefined,
      s"A projected client availability zone at $DefaultRedisClientAzPath or $PropRedisClientAz is required when " +
        s"$EnvRedisReadFrom/$PropRedisReadFrom=$RedisReadFromAzAffinityReplicasAndPrimary"
    )

    ClientSettings(
      nodes = getOrElseThrow(EnvRedisClusterNodes, PropRedisClusterNodes, conf, env),
      username = getOptional(EnvRedisUsername, PropRedisUsername, conf, env),
      password = getOptional(EnvRedisPassword, PropRedisPassword, conf, env),
      ssl = getOptional(EnvRedisUseSsl, PropRedisSSL, conf, env).exists(_.toBoolean),
      maxConnections = maxConnections,
      minIdleConnections = poolConfig.getMinIdle,
      maxIdleConnections = poolConfig.getMaxIdle,
      connectionTimeoutMs = connectionTimeoutMs,
      soTimeoutMs = soTimeoutMs,
      maxRedirections =
        intSetting(EnvRedisMaxRedirections, PropRedisMaxRedirections, DefaultMaxRedirections, conf, env),
      pipelineSyncThreads = pipelineSyncThreads,
      client = client,
      readFrom = readFrom,
      clientAz = clientAz
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

  private[redis] def createGlideReadClient(settings: ClientSettings): GlideRedisReadClient =
    createGlideReadClient(settings, GlideExecutionClusterBatch)

  /** Benchmark/test hook for retaining the staged direct-versus-batch comparison without exposing a deployment knob. */
  private[redis] def createGlideReadClient(settings: ClientSettings, executionMode: String): GlideRedisReadClient = {
    require(
      settings.client == RedisClientGlide,
      s"Cannot create a GLIDE read client when ${PropRedisClient}=${settings.client}"
    )

    val clusterNodes = settings.nodes
      .split(",", -1)
      .map(parseEndpoint)

    logger.info(
      s"Creating GLIDE Redis Cluster read client with nodes: ${clusterNodes.mkString(", ")}. " +
        s"Params: ssl=${settings.ssl}, usernameSet=${settings.username.isDefined}, " +
        s"requestTimeout=${settings.soTimeoutMs}, connectionTimeout=${settings.connectionTimeoutMs}, " +
        s"executionMode=$executionMode, readFrom=${settings.readFrom}, clientAz=${settings.clientAz.getOrElse("unset")}"
    )

    val builder = GlideClusterClientConfiguration.builder()
    builder.useTLS(settings.ssl)
    builder.readFrom(glideReadFrom(settings.readFrom))
    settings.clientAz.foreach(builder.clientAZ)
    builder.requestTimeout(settings.soTimeoutMs)
    if (executionMode == GlideExecutionDirect)
      builder.inflightRequestsLimit(GlideRedisReadClient.InternalDirectNativeInflightLimit)
    builder.advancedConfiguration(
      AdvancedGlideClusterClientConfiguration
        .builder()
        .connectionTimeout(settings.connectionTimeoutMs)
        .build())

    clusterNodes.foreach { node =>
      builder.address(NodeAddress.builder().host(node.getHost).port(node.getPort).build())
    }

    if (settings.username.nonEmpty || settings.password.nonEmpty) {
      val credentials = ServerCredentials.builder()
      settings.username.foreach(credentials.username)
      settings.password.foreach(credentials.password)
      builder.credentials(credentials.build())
    }

    if (settings.advertisedPortMappings.nonEmpty) {
      builder.addressResolver(new AddressResolver {
        override def resolve(host: String, port: Int): ResolvedAddress =
          settings.advertisedPortMappings
            .get(port)
            .map(endpoint => new ResolvedAddress(endpoint.host, endpoint.port))
            .getOrElse(new ResolvedAddress(host, port))
      })
    }

    val client = GlideClusterClient.createClient(builder.build()).get()
    new GlideRedisReadClient(client,
                             executionMode,
                             GlideRedisReadClient.InternalDirectMaxInflight,
                             GlideRedisReadClient.InternalDirectMaxPending,
                             settings.soTimeoutMs)
  }

  private[redis] def glideReadFrom(readFrom: String): GlideReadFrom = readFrom match {
    case RedisReadFromPrimary                      => GlideReadFrom.PRIMARY
    case RedisReadFromAllNodes                     => GlideReadFrom.ALL_NODES
    case RedisReadFromAzAffinityReplicasAndPrimary => GlideReadFrom.AZ_AFFINITY_REPLICAS_AND_PRIMARY
    case other => throw new IllegalArgumentException(s"Unsupported Redis read strategy: $other")
  }

  private[redis] def projectedClientAz(): Option[String] =
    Try(new String(Files.readAllBytes(Paths.get(DefaultRedisClientAzPath)), StandardCharsets.UTF_8)).toOption
      .map(_.trim)
      .filter(_.nonEmpty)

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

  private def enumSetting(envKey: String,
                          propKey: String,
                          default: String,
                          allowed: Set[String],
                          conf: Map[String, String],
                          env: Map[String, String]): String = {
    val value = getOptional(envKey, propKey, conf, env).getOrElse(default).toLowerCase(Locale.ROOT)
    require(
      allowed.contains(value),
      s"$envKey/$propKey must be one of ${allowed.toSeq.sorted.mkString(", ")}, got '$value'"
    )
    value
  }

  private[redis] def keyPrefix(conf: Map[String, String], env: Map[String, String] = sys.env): String =
    env
      .get(EnvRedisKeyPrefix)
      .orElse(conf.get(EnvRedisKeyPrefix))
      .orElse(conf.get(PropRedisKeyPrefix))
      .map(_.trim)
      .getOrElse(DefaultKeyPrefix)
}
