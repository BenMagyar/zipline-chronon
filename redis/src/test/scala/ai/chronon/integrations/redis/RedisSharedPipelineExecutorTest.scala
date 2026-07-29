package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants.{
  ConfigPipelineSyncThreads,
  DefaultPipelineSyncThreads,
  EnvRedisClusterNodes,
  EnvRedisPipelineSyncThreads
}
import ai.chronon.online.KVStore.GetRequest
import org.mockito.ArgumentMatchers.{any, same}
import org.mockito.Mockito.{mock, never, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.{ClusterPipeline, JedisCluster, Response}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.Await
import scala.concurrent.duration._

class RedisSharedPipelineExecutorTest extends AnyFlatSpec with Matchers {

  "Redis multiGet" should "use the shared pipeline executor by default" in {
    val cluster = mock(classOf[JedisCluster])
    val pipeline = mock(classOf[ClusterPipeline])
    val response = mock(classOf[Response[Array[Byte]]])
    val kvStore = new RedisKVStoreImpl(cluster)
    val executor = kvStore.pipelineSyncExecutor.get

    when(cluster.pipelined(same(executor))).thenReturn(pipeline)
    when(pipeline.get(any(classOf[Array[Byte]]))).thenReturn(response)
    when(response.get()).thenReturn(encodeStoredValue("shared-default", 1L))

    try {
      read(kvStore, "default-key", "DEFAULT_EXECUTOR_BATCH") shouldBe "shared-default"
      kvStore.pipelineSyncThreads shouldBe DefaultPipelineSyncThreads
      executor.getPoolSize shouldBe DefaultPipelineSyncThreads
      verify(cluster, times(1)).pipelined(same(executor))
      verify(cluster, never()).pipelined()
    } finally {
      kvStore.shutdownPipelineSyncExecutor()
    }
  }

  it should "reuse a configured pipeline executor until its owner shuts it down" in {
    val cluster = mock(classOf[JedisCluster])
    val firstPipeline = mock(classOf[ClusterPipeline])
    val secondPipeline = mock(classOf[ClusterPipeline])
    val firstResponse = mock(classOf[Response[Array[Byte]]])
    val secondResponse = mock(classOf[Response[Array[Byte]]])
    val kvStore = new RedisKVStoreImpl(cluster, Map(ConfigPipelineSyncThreads -> "2"))
    val executor = kvStore.pipelineSyncExecutor.get

    when(cluster.pipelined(same(executor))).thenReturn(firstPipeline, secondPipeline)
    when(firstPipeline.get(any(classOf[Array[Byte]]))).thenReturn(firstResponse)
    when(secondPipeline.get(any(classOf[Array[Byte]]))).thenReturn(secondResponse)
    when(firstResponse.get()).thenReturn(encodeStoredValue("first", 1L))
    when(secondResponse.get()).thenReturn(encodeStoredValue("second", 2L))

    try {
      read(kvStore, "key-a", "EXECUTOR_A_BATCH") shouldBe "first"
      read(kvStore, "key-b", "EXECUTOR_B_BATCH") shouldBe "second"

      verify(cluster, times(2)).pipelined(same(executor))
      verify(cluster, never()).pipelined()
      executor.getPoolSize shouldBe 2
      executor.isShutdown shouldBe false
    } finally {
      kvStore.shutdownPipelineSyncExecutor()
    }

    executor.isShutdown shouldBe true
    executor.isTerminated shouldBe true
  }

  it should "preserve Jedis-managed pipelines when shared threads are disabled" in {
    val cluster = mock(classOf[JedisCluster])
    val pipeline = mock(classOf[ClusterPipeline])
    val response = mock(classOf[Response[Array[Byte]]])
    val kvStore = new RedisKVStoreImpl(cluster, Map(ConfigPipelineSyncThreads -> "0"))

    when(cluster.pipelined()).thenReturn(pipeline)
    when(pipeline.get(any(classOf[Array[Byte]]))).thenReturn(response)
    when(response.get()).thenReturn(encodeStoredValue("dedicated", 3L))

    read(kvStore, "key-c", "EXECUTOR_C_BATCH") shouldBe "dedicated"

    verify(cluster, times(1)).pipelined()
    kvStore.pipelineSyncExecutor shouldBe None
  }

  it should "reject a negative pipeline executor size" in {
    val error = intercept[IllegalArgumentException] {
      new RedisKVStoreImpl(mock(classOf[JedisCluster]), Map(ConfigPipelineSyncThreads -> "-1"))
    }

    error.getMessage should include(ConfigPipelineSyncThreads)
  }

  it should "reject a negative factory setting before constructing the Redis client" in {
    val error = intercept[IllegalArgumentException] {
      RedisKVStoreFactory.create(
        Map(EnvRedisClusterNodes -> "localhost:6379", EnvRedisPipelineSyncThreads -> "-1"))
    }

    error.getMessage should include(ConfigPipelineSyncThreads)
  }

  private def read(kvStore: RedisKVStoreImpl, key: String, dataset: String): String = {
    val request = GetRequest(key.getBytes(StandardCharsets.UTF_8), dataset)
    val response = Await.result(kvStore.multiGet(Seq(request)), 10.seconds).head
    new String(response.values.get.head.bytes, StandardCharsets.UTF_8)
  }

  private def encodeStoredValue(value: String, timestamp: Long): Array[Byte] = {
    val valueBytes = value.getBytes(StandardCharsets.UTF_8)
    ByteBuffer.allocate(java.lang.Long.BYTES + valueBytes.length).putLong(timestamp).put(valueBytes).array()
  }
}
