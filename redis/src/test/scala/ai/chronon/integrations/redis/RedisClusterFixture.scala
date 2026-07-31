package ai.chronon.integrations.redis

import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import redis.clients.jedis.{DefaultJedisClientConfig, HostAndPort, HostAndPortMapper, Jedis, JedisCluster}

import java.time.Duration
import scala.jdk.CollectionConverters._

/** Test-only Redis cluster whose advertised container ports are mapped back to the host. */
private[redis] final class RedisClusterFixture private (
    val container: GenericContainer[_],
    val client: JedisCluster,
    val clientSettings: RedisKVStoreFactory.ClientSettings,
    val image: String,
    val primaryCount: Int,
    val replicasPerPrimary: Int,
    val cpuLimit: Option[Double],
    val ioThreads: Int,
    val ioThreadsDoReads: Boolean
) extends AutoCloseable {

  def resetCommandStats(): Unit =
    clientSettings.advertisedPortMappings.toVector.sortBy(_._1).foreach { case (_, endpoint) =>
      val jedis = new Jedis(endpoint.host, endpoint.port, 3000)
      try jedis.configResetStat()
      finally jedis.close()
    }

  def readCommandStats(): Vector[RedisClusterFixture.NodeReadStats] =
    clientSettings.advertisedPortMappings.toVector.sortBy(_._1).map { case (advertisedPort, endpoint) =>
      val jedis = new Jedis(endpoint.host, endpoint.port, 3000)
      try {
        val replication = jedis.info("replication")
        val server = jedis.info("server")
        val commandStats = jedis.info("commandstats")
        RedisClusterFixture.NodeReadStats(
          s"${endpoint.host}:${endpoint.port}(advertised:$advertisedPort)",
          RedisClusterFixture.infoValue(replication, "role").getOrElse("unknown"),
          RedisClusterFixture.infoValue(server, "availability_zone").filter(_.nonEmpty),
          RedisClusterFixture.commandCalls(commandStats, "get"),
          RedisClusterFixture.commandCalls(commandStats, "mget"),
          RedisClusterFixture.commandCalls(commandStats, "zrange") +
            RedisClusterFixture.commandCalls(commandStats, "zrangebyscore")
        )
      } finally jedis.close()
    }

  override def close(): Unit = {
    try client.close()
    finally container.stop()
  }
}

private[redis] object RedisClusterFixture {
  final case class NodeReadStats(address: String,
                                 role: String,
                                 availabilityZone: Option[String],
                                 getCalls: Long,
                                 mgetCalls: Long,
                                 zrangeCalls: Long) {
    def totalReadCalls: Long = getCalls + mgetCalls + zrangeCalls
  }

  private final class RedisContainer(image: DockerImageName) extends GenericContainer[RedisContainer](image)

  private val Image = "grokzen/redis-cluster:7.0.10"
  private val RedisNativeImage = "redis:7.2.5-alpine"
  private val ValkeyNativeImage = "valkey/valkey:9.0.5"
  private val FirstInternalPort = 7000
  private val DefaultPrimaryCount = 3
  private val DefaultReplicasPerPrimary = 1
  private val LegacyInternalPorts = (FirstInternalPort until FirstInternalPort + 6)

  private def infoValue(info: String, key: String): Option[String] =
    info.split("\\r?\\n").iterator.collectFirst {
      case line if line.startsWith(s"$key:") => line.substring(key.length + 1).trim
    }

  private def commandCalls(info: String, command: String): Long =
    info
      .split("\\r?\\n")
      .iterator
      .find(_.startsWith(s"cmdstat_$command:"))
      .flatMap(
        _.stripPrefix(s"cmdstat_$command:").split(",").iterator.find(_.startsWith("calls=")))
      .map(_.stripPrefix("calls=").toLong)
      .getOrElse(0L)

  private def nativeClusterCommand(serverBinary: String,
                                   cliBinary: String,
                                   internalPorts: Seq[Int],
                                   primaryCount: Int,
                                   replicasPerPrimary: Int,
                                   ioThreads: Int,
                                   ioThreadsDoReads: Boolean): String = {
    val nodes = internalPorts.map(port => s"127.0.0.1:$port").mkString(" ")
    val ioThreadArgs =
      if (ioThreads > 1) {
        val doReads = if (ioThreadsDoReads) "yes" else "no"
        s"--io-threads $ioThreads --io-threads-do-reads $doReads"
      } else {
        ""
      }
    val startCommands = internalPorts.zipWithIndex.map { case (port, index) =>
      val availabilityZoneArgs =
        if (serverBinary == "valkey-server") {
          val shardIndex = index % primaryCount
          val replicaOrdinal = index / primaryCount
          val zone = ('a' + ((shardIndex + replicaOrdinal) % 3)).toChar
          s"--availability-zone zone-$zone"
        } else ""
      s"mkdir -p /data/$port\n" +
        s"$serverBinary --port $port --cluster-enabled yes --cluster-config-file /data/$port/nodes.conf " +
        s"--cluster-node-timeout 5000 --appendonly no --save '' --protected-mode no --bind 0.0.0.0 " +
        s"--repl-diskless-sync yes --repl-diskless-sync-delay 0 " +
        s"--daemonize yes --dir /data/$port $availabilityZoneArgs $ioThreadArgs"
    }.mkString("\n")
    s"""
       |set -eu
       |$startCommands
       |for port in ${internalPorts.mkString(" ")}; do
       |  until $cliBinary -p $$port ping >/dev/null 2>&1; do sleep 0.1; done
       |done
       |$cliBinary --cluster create $nodes --cluster-replicas $replicasPerPrimary --cluster-yes
       |exec tail -f /dev/null
       |""".stripMargin
  }

  def start(maxConnections: Int): RedisClusterFixture =
    start(
      maxConnections,
      math.min(RedisKVStoreConstants.DefaultMinIdleConnections, maxConnections),
      math.min(RedisKVStoreConstants.DefaultMaxIdleConnections, maxConnections)
    )

  def start(maxConnections: Int, minIdleConnections: Int, maxIdleConnections: Int): RedisClusterFixture = {
    start(
      maxConnections,
      minIdleConnections,
      maxIdleConnections,
      false,
      LegacyInternalPorts,
      DefaultPrimaryCount,
      DefaultReplicasPerPrimary,
      None
    )
  }

  def startNative(maxConnections: Int,
                  minIdleConnections: Int,
                  maxIdleConnections: Int): RedisClusterFixture = {
    startNative(
      maxConnections,
      minIdleConnections,
      maxIdleConnections,
      DefaultPrimaryCount,
      DefaultReplicasPerPrimary,
      None
    )
  }

  def startNative(maxConnections: Int,
                  minIdleConnections: Int,
                  maxIdleConnections: Int,
                  primaryCount: Int,
                  replicasPerPrimary: Int): RedisClusterFixture =
    startNative(
      maxConnections,
      minIdleConnections,
      maxIdleConnections,
      primaryCount,
      replicasPerPrimary,
      None
    )

  def startNative(maxConnections: Int,
                  minIdleConnections: Int,
                  maxIdleConnections: Int,
                  primaryCount: Int,
                  replicasPerPrimary: Int,
                  cpuLimit: Option[Double],
                  server: String = "valkey"): RedisClusterFixture = {
    require(primaryCount >= 3, s"primaryCount must be at least 3, got $primaryCount")
    require(replicasPerPrimary >= 0, s"replicasPerPrimary must be non-negative, got $replicasPerPrimary")
    require(cpuLimit.forall(_ > 0), s"cpuLimit must be positive when set, got $cpuLimit")
    val nodeCount = primaryCount * (replicasPerPrimary + 1)
    val internalPorts = FirstInternalPort until FirstInternalPort + nodeCount
    start(
      maxConnections,
      minIdleConnections,
      maxIdleConnections,
      true,
      internalPorts,
      primaryCount,
      replicasPerPrimary,
      cpuLimit,
      server
    )
  }

  private def start(maxConnections: Int,
                    minIdleConnections: Int,
                    maxIdleConnections: Int,
                    native: Boolean,
                    internalPorts: Seq[Int],
                    primaryCount: Int,
                    replicasPerPrimary: Int,
                    cpuLimit: Option[Double],
                    server: String = "redis"): RedisClusterFixture = {
    require(maxConnections > 0, s"maxConnections must be positive, got $maxConnections")

    val (image, serverBinary, cliBinary) =
      if (!native) (Image, "redis-server", "redis-cli")
      else
        server.toLowerCase match {
          case "redis"  => (RedisNativeImage, "redis-server", "redis-cli")
          case "valkey" => (ValkeyNativeImage, "valkey-server", "valkey-cli")
          case other =>
            throw new IllegalArgumentException(s"Unsupported local cluster server '$other'; expected redis or valkey")
        }
    val container = new RedisContainer(DockerImageName.parse(image))
    var client: JedisCluster = null
    try {
      container.withExposedPorts(internalPorts.map(Integer.valueOf): _*)
      container.withStartupTimeout(Duration.ofSeconds(60))
      val ioThreads = if (native) sys.env.get("PERF_REDIS_IO_THREADS").map(_.toInt).getOrElse(1) else 1
      val ioThreadsDoReads =
        native && sys.env.get("PERF_REDIS_IO_THREADS_DO_READS").exists(_.equalsIgnoreCase("true"))
      require(ioThreads > 0, s"PERF_REDIS_IO_THREADS must be positive, got $ioThreads")
      if (native) {
        container.withCommand(
          "sh",
          "-c",
          nativeClusterCommand(serverBinary,
                               cliBinary,
                               internalPorts,
                               primaryCount,
                               replicasPerPrimary,
                               ioThreads,
                               ioThreadsDoReads)
        )
      }
      cpuLimit.foreach { cpus =>
        val nanoCpus = math.round(cpus * 1000000000L)
        container.withCreateContainerCmdModifier { command =>
          command.getHostConfig.withNanoCPUs(nanoCpus)
          ()
        }
      }
      container.start()
      cpuLimit.foreach { cpus =>
        val expectedNanoCpus = math.round(cpus * 1000000000L)
        val actualNanoCpus = container.getContainerInfo.getHostConfig.getNanoCPUs.longValue()
        require(
          actualNanoCpus == expectedNanoCpus,
          s"Redis container CPU limit mismatch: requested $expectedNanoCpus NanoCPUs, applied $actualNanoCpus"
        )
      }
      awaitClusterReady(container, internalPorts, replicasPerPrimary)

      val hostAndPortMapper = new HostAndPortMapper {
        override def getHostAndPort(hostAndPort: HostAndPort): HostAndPort = {
          val port = hostAndPort.getPort
          if (internalPorts.contains(port)) {
            new HostAndPort(container.getHost, container.getMappedPort(port))
          } else {
            hostAndPort
          }
        }
      }
      val clientConfig = DefaultJedisClientConfig.builder().hostAndPortMapper(hostAndPortMapper).build()
      val poolConfig = RedisKVStoreFactory.buildConnectionPoolConfig(
        maxConnections,
        minIdleConnections,
        maxIdleConnections
      )

      val seed = new HostAndPort(container.getHost, container.getMappedPort(internalPorts.head))
      val advertisedPortMappings = internalPorts.iterator.map { port =>
        port -> RedisKVStoreFactory.RedisEndpoint(container.getHost, container.getMappedPort(port))
      }.toMap
      val mappedClusterNodes = internalPorts
        .map(port => advertisedPortMappings(port))
        .map(endpoint => s"${endpoint.host}:${endpoint.port}")
        .mkString(",")
      val clientSettings = RedisKVStoreFactory.ClientSettings(
        nodes = mappedClusterNodes,
        username = None,
        password = None,
        ssl = false,
        maxConnections = maxConnections,
        minIdleConnections = minIdleConnections,
        maxIdleConnections = maxIdleConnections,
        connectionTimeoutMs = RedisKVStoreConstants.DefaultConnectionTimeoutMs,
        soTimeoutMs = RedisKVStoreConstants.DefaultSoTimeoutMs,
        maxRedirections = RedisKVStoreConstants.DefaultMaxRedirections,
        advertisedPortMappings = advertisedPortMappings
      )
      client = new JedisCluster(Set(seed).asJava, clientConfig, 5, poolConfig)
      new RedisClusterFixture(container,
                              client,
                              clientSettings,
                              image,
                              primaryCount,
                              replicasPerPrimary,
                              cpuLimit,
                              ioThreads,
                              ioThreadsDoReads)
    } catch {
      case throwable: Throwable =>
        if (client != null) {
          try client.close()
          catch { case _: Exception => () }
        }
        try container.stop()
        catch { case _: Exception => () }
        throw throwable
    }
  }

  private def awaitClusterReady(container: GenericContainer[_],
                                internalPorts: Seq[Int],
                                replicasPerPrimary: Int): Unit = {
    val deadlineNanos = System.nanoTime() + Duration.ofSeconds(45).toNanos
    var lastFailure: Throwable = null
    var consecutiveHealthyChecks = 0
    while (System.nanoTime() < deadlineNanos) {
      try {
        val allNodesHealthy = internalPorts.forall { port =>
          val probe = new Jedis(container.getHost, container.getMappedPort(port), 3000)
          try {
            val clusterInfo = probe.clusterInfo()
            val replicationInfo = probe.info("replication")
            val role = infoValue(replicationInfo, "role").getOrElse("unknown")
            val replicationReady =
              if (replicasPerPrimary == 0) true
              else if (role == "master") {
                infoValue(replicationInfo, "connected_slaves").exists(_.toInt >= replicasPerPrimary)
              } else if (role == "slave" || role == "replica") {
                infoValue(replicationInfo, "master_link_status").contains("up") &&
                !infoValue(replicationInfo, "master_sync_in_progress").contains("1")
              } else false
            clusterInfo.contains("cluster_state:ok") &&
            clusterInfo.contains("cluster_slots_assigned:16384") &&
            replicationReady
          } finally {
            probe.close()
          }
        }
        val slotsExposeReplicas = {
          val probe = new Jedis(container.getHost, container.getMappedPort(internalPorts.head), 3000)
          try {
            val slots = probe.clusterSlots().asScala
            slots.nonEmpty && slots.forall(_.asInstanceOf[java.util.List[_]].size() >= 3 + replicasPerPrimary)
          } finally probe.close()
        }
        consecutiveHealthyChecks =
          if (allNodesHealthy && slotsExposeReplicas) consecutiveHealthyChecks + 1 else 0
        if (consecutiveHealthyChecks >= 4) {
          return
        }
      } catch {
        case throwable: Throwable =>
          lastFailure = throwable
          consecutiveHealthyChecks = 0
      }
      Thread.sleep(500)
    }

    throw new IllegalStateException(
      s"Redis cluster did not become ready with $replicasPerPrimary replica(s) per primary within 45 seconds",
      lastFailure)
  }
}
