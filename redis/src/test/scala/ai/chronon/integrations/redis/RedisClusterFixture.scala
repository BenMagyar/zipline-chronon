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
    val image: String,
    val primaryCount: Int,
    val replicasPerPrimary: Int,
    val cpuLimit: Option[Double]
) extends AutoCloseable {

  override def close(): Unit = {
    try client.close()
    finally container.stop()
  }
}

private[redis] object RedisClusterFixture {
  private final class RedisContainer(image: DockerImageName) extends GenericContainer[RedisContainer](image)

  private val Image = "grokzen/redis-cluster:7.0.10"
  private val NativeImage = "redis:7.2.5-alpine"
  private val FirstInternalPort = 7000
  private val DefaultPrimaryCount = 3
  private val DefaultReplicasPerPrimary = 1
  private val LegacyInternalPorts = (FirstInternalPort until FirstInternalPort + 6)

  private def nativeClusterCommand(internalPorts: Seq[Int], replicasPerPrimary: Int): String = {
    val nodes = internalPorts.map(port => s"127.0.0.1:$port").mkString(" ")
    s"""
       |set -eu
       |for port in ${internalPorts.mkString(" ")}; do
       |  mkdir -p /data/$$port
       |  redis-server --port $$port --cluster-enabled yes --cluster-config-file /data/$$port/nodes.conf --cluster-node-timeout 5000 --appendonly no --save '' --protected-mode no --bind 0.0.0.0 --daemonize yes --dir /data/$$port
       |done
       |for port in ${internalPorts.mkString(" ")}; do
       |  until redis-cli -p $$port ping >/dev/null 2>&1; do sleep 0.1; done
       |done
       |redis-cli --cluster create $nodes --cluster-replicas $replicasPerPrimary --cluster-yes
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
                  cpuLimit: Option[Double]): RedisClusterFixture = {
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
      cpuLimit
    )
  }

  private def start(maxConnections: Int,
                    minIdleConnections: Int,
                    maxIdleConnections: Int,
                    native: Boolean,
                    internalPorts: Seq[Int],
                    primaryCount: Int,
                    replicasPerPrimary: Int,
                    cpuLimit: Option[Double]): RedisClusterFixture = {
    require(maxConnections > 0, s"maxConnections must be positive, got $maxConnections")

    val image = if (native) NativeImage else Image
    val container = new RedisContainer(DockerImageName.parse(image))
    var client: JedisCluster = null
    try {
      container.withExposedPorts(internalPorts.map(Integer.valueOf): _*)
      container.withStartupTimeout(Duration.ofSeconds(60))
      if (native) container.withCommand("sh", "-c", nativeClusterCommand(internalPorts, replicasPerPrimary))
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
      awaitClusterReady(container, internalPorts)

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
      client = new JedisCluster(Set(seed).asJava, clientConfig, 5, poolConfig)
      new RedisClusterFixture(container, client, image, primaryCount, replicasPerPrimary, cpuLimit)
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

  private def awaitClusterReady(container: GenericContainer[_], internalPorts: Seq[Int]): Unit = {
    val deadlineNanos = System.nanoTime() + Duration.ofSeconds(45).toNanos
    var lastFailure: Throwable = null
    var consecutiveHealthyChecks = 0
    while (System.nanoTime() < deadlineNanos) {
      try {
        val allNodesHealthy = internalPorts.forall { port =>
          val probe = new Jedis(container.getHost, container.getMappedPort(port), 3000)
          try {
            val clusterInfo = probe.clusterInfo()
            clusterInfo.contains("cluster_state:ok") && clusterInfo.contains("cluster_slots_assigned:16384")
          } finally {
            probe.close()
          }
        }
        consecutiveHealthyChecks = if (allNodesHealthy) consecutiveHealthyChecks + 1 else 0
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

    throw new IllegalStateException("Redis cluster did not become ready within 45 seconds", lastFailure)
  }
}
