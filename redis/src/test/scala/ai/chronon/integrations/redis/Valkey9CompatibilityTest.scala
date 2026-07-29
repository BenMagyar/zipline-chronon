package ai.chronon.integrations.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import redis.clients.jedis.{Connection, DefaultJedisClientConfig, HostAndPort, HostAndPortMapper, JedisCluster}

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

class Valkey9CompatibilityTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {
  private var container: GenericContainer[_] = _
  private var client: JedisCluster = _

  private def leaseDeadlineBytes(seconds: Int = 300): Array[Byte] =
    (System.currentTimeMillis() + seconds * 1000L).toString.getBytes(StandardCharsets.UTF_8)

  override def beforeAll(): Unit = {
    super.beforeAll()
    container = new GenericContainer(DockerImageName.parse("valkey/valkey:9.0.5"))
    container.withExposedPorts(6379)
    container.withCommand("valkey-server",
                          "--port",
                          "6379",
                          "--cluster-enabled",
                          "yes",
                          "--cluster-config-file",
                          "nodes.conf",
                          "--cluster-node-timeout",
                          "5000",
                          "--appendonly",
                          "no",
                          "--save",
                          "",
                          "--protected-mode",
                          "no")
    container.waitingFor(Wait.forLogMessage(".*Ready to accept connections.*\\n", 1))
    container.start()
    val addSlots = container.execInContainer("valkey-cli", "-p", "6379", "CLUSTER", "ADDSLOTSRANGE", "0", "16383")
    require(addSlots.getExitCode == 0 && addSlots.getStdout.contains("OK"),
            s"Could not initialize Valkey cluster slots: ${addSlots.getStdout} ${addSlots.getStderr}")
    val clusterReady = (1 to 50).exists { _ =>
      val info = container.execInContainer("valkey-cli", "-p", "6379", "CLUSTER", "INFO")
      val ready = info.getExitCode == 0 && info.getStdout.contains("cluster_state:ok")
      if (!ready) Thread.sleep(100L)
      ready
    }
    require(clusterReady, "Valkey 9 single-node cluster did not reach cluster_state:ok")

    val host = container.getHost
    val mappedPort = container.getMappedPort(6379)
    val config = DefaultJedisClientConfig
      .builder()
      .hostAndPortMapper(new HostAndPortMapper {
        override def getHostAndPort(advertised: HostAndPort): HostAndPort =
          if (advertised.getPort == 6379) new HostAndPort(host, mappedPort) else advertised
      })
      .build()
    client = new JedisCluster(
      Set(new HostAndPort(host, mappedPort)).asJava,
      config,
      5,
      new GenericObjectPoolConfig[Connection]())
  }

  override def afterAll(): Unit = {
    if (client != null) client.close()
    if (container != null) container.stop()
    super.afterAll()
  }

  "Valkey 9" should "support Chronon's Lua, status, pipelining, and TTL wire contract" in {
    val info = container.execInContainer("valkey-cli", "-p", "6379", "INFO", "server")
    info.getExitCode shouldBe 0
    info.getStdout should include("valkey_version:9.0")
    Spark2RedisLoader.validateNoEviction(client)
    val key = "chronon:{VALKEY9_BATCH:dGVzdA==}".getBytes(StandardCharsets.UTF_8)
    def apply(timestamp: Long, epoch: Long, payload: String): Long = {
      val value = RedisBatchUpload.encodeValue(
        RedisBatchUpload.Upsert,
        timestamp,
        payload.getBytes(StandardCharsets.UTF_8),
        epoch)
      RedisBatchUpload
        .putBaseIfNewer(
          client,
          key,
          List(timestamp.toString.getBytes(StandardCharsets.UTF_8),
               epoch.toString.getBytes(StandardCharsets.UTF_8),
               value,
               leaseDeadlineBytes()))
        .asInstanceOf[java.lang.Long]
        .longValue()
    }

    apply(2000L, 2L, "new") shouldBe 1L
    apply(2000L, 2L, "new") shouldBe 2L
    apply(1000L, 3L, "old") shouldBe 0L
    client.ttl(key) should be > 0L

    val pipeline = client.pipelined()
    try {
      val mutations = (1 to 1000).map { index =>
        val mutationKey = s"chronon:{VALKEY9_BATCH:key-$index}".getBytes(StandardCharsets.UTF_8)
        val value = RedisBatchUpload.encodeValue(
          RedisBatchUpload.Upsert,
          3000L,
          s"value-$index".getBytes(StandardCharsets.UTF_8),
          3L)
        pipeline.evalsha(
          RedisBatchUpload.putBaseIfNewerSha,
          List(mutationKey).asJava,
          List(
            "3000".getBytes(StandardCharsets.UTF_8),
            "3".getBytes(StandardCharsets.UTF_8),
            value,
            leaseDeadlineBytes()
          ).asJava
        )
      }
      pipeline.sync()
      all(mutations.map(_.get().asInstanceOf[java.lang.Long].longValue())) shouldBe 1L
    } finally {
      pipeline.close()
    }

    val statusKey = RedisBatchUpload.buildStatusKey("VALKEY9_BATCH", "chronon")
    val status = RedisBatchUpload.BatchStatus("valkey9-generation", "2026-07-27", 2000L, 2L)
    RedisBatchUpload.writeStatus(client, statusKey, status) shouldBe 1L
    RedisBatchUpload.readStatus(client, statusKey) shouldBe Some(status)
  }
}
