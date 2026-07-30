package ai.chronon.integrations.redis

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.Connection

import scala.collection.mutable.ArrayBuffer

class RedisKVStoreFactoryTest extends AnyFlatSpec with Matchers {

  "Redis client settings" should "accept canonical properties for authenticated TLS clusters" in {
    val settings = RedisKVStoreFactory.settings(
      Map(
        "redis.cluster.nodes" -> "node-a:6379,node-b:6380",
        "redis.username" -> "chronon-writer",
        "redis.password" -> "secret",
        "redis.ssl" -> "true",
        "redis.max.connections" -> "12",
        "redis.min.idle.connections" -> "1",
        "redis.max.idle.connections" -> "4",
        "redis.connection.timeout.ms" -> "7000",
        "redis.so.timeout.ms" -> "8000",
        "redis.max.redirections" -> "9"
      ),
      env = Map.empty
    )

    settings.nodes shouldBe "node-a:6379,node-b:6380"
    settings.username shouldBe Some("chronon-writer")
    settings.password shouldBe Some("secret")
    settings.ssl shouldBe true
    settings.maxConnections shouldBe 12
    settings.minIdleConnections shouldBe 1
    settings.maxIdleConnections shouldBe 4
    settings.connectionTimeoutMs shouldBe 7000
    settings.soTimeoutMs shouldBe 8000
    settings.maxRedirections shouldBe 9
  }

  it should "give process environment values precedence over submitted properties" in {
    val settings = RedisKVStoreFactory.settings(
      Map(
        "REDIS_CLUSTER_NODES" -> "conf-env-style:6379",
        "redis.cluster.nodes" -> "property:6379",
        "REDIS_PASSWORD" -> "conf-password",
        "redis.password" -> "property-password",
        "REDIS_USE_SSL" -> "false"
      ),
      env = Map(
        "REDIS_CLUSTER_NODES" -> "environment:6380",
        "REDIS_PASSWORD" -> "environment-password",
        "REDIS_USE_SSL" -> "true"
      )
    )

    settings.nodes shouldBe "environment:6380"
    settings.password shouldBe Some("environment-password")
    settings.ssl shouldBe true
  }

  it should "reject invalid boolean values" in {
    an[IllegalArgumentException] should be thrownBy {
      RedisKVStoreFactory.settings(
        Map("redis.cluster.nodes" -> "localhost:6379", "redis.ssl" -> "yes"),
        env = Map.empty
      )
    }
  }

  it should "reject ambiguous Redis cluster node endpoints" in {
    RedisKVStoreFactory.parseEndpoint("localhost").getPort shouldBe RedisKVStoreConstants.DefaultPort
    RedisKVStoreFactory.parseEndpoint("localhost:6380").getPort shouldBe 6380

    Seq("host:80:81", "[::1]:6379", ":6379", "localhost:", "localhost:0", "localhost:65536", "local host:6379")
      .foreach { endpoint =>
      an[IllegalArgumentException] should be thrownBy RedisKVStoreFactory.parseEndpoint(endpoint)
    }
  }

  it should "reject empty entries in a cluster node list" in {
    val settings = RedisKVStoreFactory.settings(Map("redis.cluster.nodes" -> "localhost:6379,"), env = Map.empty)
    an[IllegalArgumentException] should be thrownBy RedisKVStoreFactory.createClient(settings)
  }

  "Redis key prefix settings" should "use the same precedence and permit a dedicated empty namespace" in {
    RedisKVStoreFactory.keyPrefix(Map("redis.key.prefix" -> "custom"), env = Map.empty) shouldBe "custom"
    RedisKVStoreFactory.keyPrefix(Map("redis.key.prefix" -> ""), env = Map.empty) shouldBe ""
    RedisKVStoreFactory.keyPrefix(
      Map("redis.key.prefix" -> "property"),
      env = Map("REDIS_KEY_PREFIX" -> "environment")
    ) shouldBe "environment"
  }

  "ClientNoTouchInitializer" should "initialize each physical connection only once" in {
    val first = new Connection()
    val second = new Connection()
    val initialized = ArrayBuffer.empty[Connection]
    val initializer = new RedisKVStoreFactory.ClientNoTouchInitializer(initialized += _)

    initializer.initialize(first)
    initializer.initialize(first)
    initializer.initialize(second)
    initializer.initialize(first)
    initializer.initialize(second)

    initialized.toSeq shouldBe Seq(first, second)
  }

  it should "retry initialization after a failed attempt" in {
    val connection = new Connection()
    var attempts = 0
    val initializer = new RedisKVStoreFactory.ClientNoTouchInitializer(_ => {
      attempts += 1
      if (attempts == 1) throw new RuntimeException("initialization failed")
    })

    an[RuntimeException] should be thrownBy initializer.initialize(connection)
    initializer.initialize(connection)
    initializer.initialize(connection)

    attempts shouldBe 2
  }
}
