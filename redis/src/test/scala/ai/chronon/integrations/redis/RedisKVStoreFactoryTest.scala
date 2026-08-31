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

  it should "bound default idle connections by a smaller configured pool" in {
    val settings = RedisKVStoreFactory.settings(
      Map(
        "redis.cluster.nodes" -> "localhost:6379",
        "redis.max.connections" -> "4"
      ),
      env = Map.empty
    )

    settings.maxConnections shouldBe 4
    settings.minIdleConnections shouldBe 4
    settings.maxIdleConnections shouldBe 4
    val poolConfig = RedisKVStoreFactory.buildConnectionPoolConfig(
      settings.maxConnections,
      settings.minIdleConnections,
      settings.maxIdleConnections
    )
    poolConfig.getMaxTotal shouldBe 4
    poolConfig.getMinIdle shouldBe 4
    poolConfig.getMaxIdle shouldBe 4
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

    Seq("host:80:81", "[::1]:6379", ":6379", "localhost:", "local host:6379").foreach { endpoint =>
      an[IllegalArgumentException] should be thrownBy RedisKVStoreFactory.parseEndpoint(endpoint)
    }
  }

  "Redis key prefix settings" should "use the same precedence and permit a dedicated empty namespace" in {
    RedisKVStoreFactory.keyPrefix(Map("redis.key.prefix" -> "custom"), env = Map.empty) shouldBe "custom"
    RedisKVStoreFactory.keyPrefix(Map("redis.key.prefix" -> ""), env = Map.empty) shouldBe ""
    RedisKVStoreFactory.keyPrefix(
      Map("redis.key.prefix" -> "property"),
      env = Map("REDIS_KEY_PREFIX" -> "environment")
    ) shouldBe "environment"
  }

  it should "resolve the client and key prefix as one store configuration" in {
    val settings = RedisKVStoreFactory.settingsForStore(
      Map(
        "redis.cluster.nodes" -> "property:6379",
        "redis.key.prefix" -> "property-prefix"
      ),
      env = Map(
        "REDIS_CLUSTER_NODES" -> "environment:6380",
        "REDIS_KEY_PREFIX" -> "environment-prefix"
      )
    )

    settings.client.nodes shouldBe "environment:6380"
    settings.keyPrefix shouldBe "environment-prefix"
  }

  "Redis batch mode settings" should "use the caller-specific default when no override is configured" in {
    RedisKVStoreFactory.configuredBatchUploadMode(
      Map.empty,
      defaultMode = RedisBatchModeSelection.FullSnapshot
    ) shouldBe RedisBatchModeSelection.FullSnapshot

    RedisKVStoreFactory.configuredBatchUploadMode(
      Map.empty,
      defaultMode = RedisBatchModeSelection.Incremental
    ) shouldBe RedisBatchModeSelection.Incremental
  }

  it should "normalize the upload property" in {
    RedisKVStoreFactory.configuredBatchUploadMode(
      Map("spark.chronon.kv_upload.redis.mode" -> " InCreMenTal "),
      defaultMode = RedisBatchModeSelection.FullSnapshot
    ) shouldBe RedisBatchModeSelection.Incremental

    RedisKVStoreFactory.configuredBatchUploadMode(
      Map("spark.chronon.kv_upload.redis.mode" -> " Full_Snapshot "),
      defaultMode = RedisBatchModeSelection.Incremental
    ) shouldBe RedisBatchModeSelection.FullSnapshot

    RedisKVStoreFactory.configuredBatchUploadMode(
      Map("spark.chronon.kv_upload.redis.mode" -> " "),
      defaultMode = RedisBatchModeSelection.Incremental
    ) shouldBe RedisBatchModeSelection.Incremental
  }

  it should "reject unsupported values" in {
    val exception = intercept[IllegalArgumentException] {
      RedisKVStoreFactory.configuredBatchUploadMode(
        Map("spark.chronon.kv_upload.redis.mode" -> "hybrid"),
        defaultMode = RedisBatchModeSelection.FullSnapshot
      )
    }

    exception.getMessage should include("hybrid")
    exception.getMessage should include("incremental, full_snapshot")
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
