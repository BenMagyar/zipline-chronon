package ai.chronon.integrations.redis

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration

class RedisConnectionPoolConfigTest extends AnyFlatSpec with Matchers {

  "Redis connection pool config" should "validate idle connections without pinging on borrow or return" in {
    val config = RedisKVStoreFactory.buildConnectionPoolConfig(
      maxConnections = 64,
      minIdleConnections = 5,
      maxIdleConnections = 64
    )

    config.getMaxTotal shouldBe 64
    config.getMinIdle shouldBe 5
    config.getMaxIdle shouldBe 64
    config.getTestOnBorrow shouldBe false
    config.getTestOnReturn shouldBe false
    config.getTestWhileIdle shouldBe true
    config.getTimeBetweenEvictionRuns shouldBe Duration.ofSeconds(30)
    config.getMinEvictableIdleDuration shouldBe Duration.ofSeconds(60)
    config.getNumTestsPerEvictionRun shouldBe -1
  }

  it should "reject inconsistent limits" in {
    an[IllegalArgumentException] should be thrownBy
      RedisKVStoreFactory.buildConnectionPoolConfig(0, 0, 0)
    an[IllegalArgumentException] should be thrownBy
      RedisKVStoreFactory.buildConnectionPoolConfig(10, 6, 5)
    an[IllegalArgumentException] should be thrownBy
      RedisKVStoreFactory.buildConnectionPoolConfig(10, 0, 11)
  }

  it should "clamp the default minimum idle to an explicitly smaller maximum idle" in {
    val config = RedisKVStoreFactory.buildConnectionPoolConfig(
      maxConnections = 50,
      minIdleConnections = None,
      maxIdleConnections = Some(2)
    )

    config.getMaxTotal shouldBe 50
    config.getMinIdle shouldBe 2
    config.getMaxIdle shouldBe 2
  }

  it should "bound default idle connections without exceeding a smaller pool" in {
    val defaultPool = RedisKVStoreFactory.buildConnectionPoolConfig(50, None, None)
    val smallPool = RedisKVStoreFactory.buildConnectionPoolConfig(4, None, None)

    defaultPool.getMinIdle shouldBe 5
    defaultPool.getMaxIdle shouldBe 10
    smallPool.getMinIdle shouldBe 4
    smallPool.getMaxIdle shouldBe 4
  }
}
