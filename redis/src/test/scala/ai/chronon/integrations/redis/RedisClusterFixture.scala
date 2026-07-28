package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants.{DefaultMaxIdleConnections, DefaultMinIdleConnections}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import redis.clients.jedis.{Connection, DefaultJedisClientConfig, HostAndPort, HostAndPortMapper, Jedis, JedisCluster}

import java.time.Duration
import scala.jdk.CollectionConverters._

/** Test-only Redis cluster whose advertised container ports are mapped back to the host. */
private[redis] final class RedisClusterFixture private (
    val container: GenericContainer[_],
    val client: JedisCluster
) extends AutoCloseable {

  override def close(): Unit = {
    try client.close()
    finally container.stop()
  }
}

private[redis] object RedisClusterFixture {
  private val Image = "grokzen/redis-cluster:7.0.10"
  private val InternalPorts = Seq(7000, 7001, 7002, 7003, 7004, 7005)

  def start(maxConnections: Int): RedisClusterFixture = {
    require(maxConnections > 0, s"maxConnections must be positive, got $maxConnections")

    val container = new GenericContainer(DockerImageName.parse(Image))
    var client: JedisCluster = null
    try {
      container.withExposedPorts(InternalPorts.map(Integer.valueOf): _*)
      container.withStartupTimeout(Duration.ofSeconds(60))
      container.start()
      awaitClusterReady(container)

      val hostAndPortMapper = new HostAndPortMapper {
        override def getHostAndPort(hostAndPort: HostAndPort): HostAndPort = {
          val port = hostAndPort.getPort
          if (InternalPorts.contains(port)) {
            new HostAndPort(container.getHost, container.getMappedPort(port))
          } else {
            hostAndPort
          }
        }
      }
      val clientConfig = DefaultJedisClientConfig.builder().hostAndPortMapper(hostAndPortMapper).build()
      val poolConfig = new GenericObjectPoolConfig[Connection]()
      val maxIdleConnections = math.min(DefaultMaxIdleConnections, maxConnections)
      val minIdleConnections = math.min(DefaultMinIdleConnections, maxIdleConnections)
      poolConfig.setMaxTotal(maxConnections)
      poolConfig.setMaxIdle(maxIdleConnections)
      poolConfig.setMinIdle(minIdleConnections)
      poolConfig.setTestOnBorrow(true)
      poolConfig.setTestOnReturn(true)
      poolConfig.setTestWhileIdle(true)

      val seed = new HostAndPort(container.getHost, container.getMappedPort(InternalPorts.head))
      client = new JedisCluster(Set(seed).asJava, clientConfig, 5, poolConfig)
      new RedisClusterFixture(container, client)
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

  private def awaitClusterReady(container: GenericContainer[_]): Unit = {
    val deadlineNanos = System.nanoTime() + Duration.ofSeconds(45).toNanos
    var lastFailure: Throwable = null
    var consecutiveHealthyChecks = 0
    while (System.nanoTime() < deadlineNanos) {
      try {
        val allNodesHealthy = InternalPorts.forall { port =>
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
