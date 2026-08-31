package ai.chronon.integrations.redis

import ai.chronon.online.KVStore.GetRequest
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.ArgumentMatcher
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.exceptions.{JedisAskDataException, JedisConnectionException}
import redis.clients.jedis.{ClusterPipeline, HostAndPort, JedisCluster, Response}

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.concurrent.Await
import scala.concurrent.duration._

class RedisPipelineRetryTest extends AnyFlatSpec with Matchers {

  "Redis multiGet pipeline recovery" should "retry the whole pipeline once after unset responses" in {
    val firstPipeline = mock(classOf[ClusterPipeline])
    val retryPipeline = mock(classOf[ClusterPipeline])
    val firstResponse = mock(classOf[Response[Array[Byte]]])
    val retryResponse = mock(classOf[Response[Array[Byte]]])
    val cluster = mock(classOf[JedisCluster])
    val storedValue = encodeStoredValue("value", 123L)

    when(cluster.pipelined()).thenReturn(firstPipeline, retryPipeline)
    when(firstPipeline.get(any(classOf[Array[Byte]]))).thenReturn(firstResponse)
    when(firstResponse.get())
      .thenThrow(new IllegalStateException("Please close pipeline or multi block before calling this method."))
    stubDataReads(cluster, storedValue)
    when(retryPipeline.get(any(classOf[Array[Byte]]))).thenReturn(retryResponse)
    when(retryResponse.get()).thenReturn(storedValue)

    val responses = readTwoBatchKeys(cluster)

    responses.map(_.values.get.head.millis) shouldBe Seq(123L, 123L)
    responses.map(response => new String(response.values.get.head.bytes, StandardCharsets.UTF_8)) shouldBe
      Seq("value", "value")
    verify(cluster, times(2)).pipelined()
    verify(cluster, times(1)).get(dataKey)
  }

  it should "retry the whole pipeline once after a queue-time connection failure" in {
    val firstPipeline = mock(classOf[ClusterPipeline])
    val retryPipeline = mock(classOf[ClusterPipeline])
    val retryResponse = mock(classOf[Response[Array[Byte]]])
    val cluster = mock(classOf[JedisCluster])
    val storedValue = encodeStoredValue("value", 456L)

    when(cluster.pipelined()).thenReturn(firstPipeline, retryPipeline)
    when(firstPipeline.get(any(classOf[Array[Byte]]))).thenThrow(new JedisConnectionException("disconnected"))
    stubDataReads(cluster, storedValue)
    when(retryPipeline.get(any(classOf[Array[Byte]]))).thenReturn(retryResponse)
    when(retryResponse.get()).thenReturn(storedValue)

    val responses = readTwoBatchKeys(cluster)

    responses.map(_.values.get.head.millis) shouldBe Seq(456L, 456L)
    verify(cluster, times(2)).pipelined()
    verify(cluster, times(1)).get(dataKey)
  }

  it should "preserve first-attempt successes when another command fails during retry" in {
    val firstPipeline = mock(classOf[ClusterPipeline])
    val retryPipeline = mock(classOf[ClusterPipeline])
    val firstSuccess = mock(classOf[Response[Array[Byte]]])
    val firstFailure = mock(classOf[Response[Array[Byte]]])
    val retryFailure = mock(classOf[Response[Array[Byte]]])
    val retrySuccess = mock(classOf[Response[Array[Byte]]])
    val cluster = mock(classOf[JedisCluster])
    val firstValue = encodeStoredValue("first", 111L)
    val retryValue = encodeStoredValue("retry", 222L)

    when(cluster.pipelined()).thenReturn(firstPipeline, retryPipeline)
    when(firstPipeline.get(any(classOf[Array[Byte]]))).thenReturn(firstSuccess, firstFailure)
    when(firstSuccess.get()).thenReturn(firstValue)
    when(firstFailure.get())
      .thenThrow(new IllegalStateException("Please close pipeline or multi block before calling this method."))
    stubDataReads(cluster, retryValue)
    when(retryPipeline.get(any(classOf[Array[Byte]]))).thenReturn(retryFailure, retrySuccess)
    when(retryFailure.get()).thenThrow(new JedisConnectionException("different node failed"))
    when(retrySuccess.get()).thenReturn(retryValue)

    val responses = readTwoBatchKeys(cluster)

    responses.map(_.values.get.head.millis) shouldBe Seq(111L, 222L)
    responses.map(response => new String(response.values.get.head.bytes, StandardCharsets.UTF_8)) shouldBe
      Seq("first", "retry")
    verify(firstPipeline, times(1)).close()
    verify(retryPipeline, times(1)).close()
  }

  it should "recover ASK redirects directly without discarding successful pipeline results" in {
    val pipeline = mock(classOf[ClusterPipeline])
    val askResponse = mock(classOf[Response[Array[Byte]]])
    val successfulResponse = mock(classOf[Response[Array[Byte]]])
    val cluster = mock(classOf[JedisCluster])
    val directValue = encodeStoredValue("direct", 333L)
    val pipelineValue = encodeStoredValue("pipeline", 444L)
    val ask = new JedisAskDataException("ASK", new HostAndPort("localhost", 7001), 42)

    when(cluster.pipelined()).thenReturn(pipeline)
    when(pipeline.get(any(classOf[Array[Byte]]))).thenReturn(askResponse, successfulResponse)
    when(askResponse.get()).thenThrow(ask)
    when(successfulResponse.get()).thenReturn(pipelineValue)
    stubDataReads(cluster, directValue)

    val responses = readTwoBatchKeys(cluster)

    responses.map(_.values.get.head.millis) shouldBe Seq(333L, 444L)
    responses.map(response => new String(response.values.get.head.bytes, StandardCharsets.UTF_8)) shouldBe
      Seq("direct", "pipeline")
    verify(cluster, times(1)).pipelined()
    verify(cluster, times(1)).get(dataKey)
    verify(pipeline, times(1)).close()
  }

  it should "isolate managed publication failures after one unified pipeline" in {
    val pipeline = mock(classOf[ClusterPipeline])
    val managedResponse = mock(classOf[Response[Array[Byte]]])
    val publicationModeResponse = mock(classOf[Response[Array[Byte]]])
    val statusResponse = mock(classOf[Response[Array[Byte]]])
    val ordinaryResponse = mock(classOf[Response[Array[Byte]]])
    val cluster = mock(classOf[JedisCluster])
    val managedValue = encodeStoredValue("managed", 555L)
    val ordinaryValue = encodeStoredValue("ordinary", 666L)

    when(cluster.pipelined()).thenReturn(pipeline)
    when(pipeline.get(any(classOf[Array[Byte]])))
      .thenReturn(managedResponse, publicationModeResponse, statusResponse, ordinaryResponse)
    when(managedResponse.get()).thenReturn(managedValue)
    when(publicationModeResponse.get()).thenReturn("incremental".getBytes(StandardCharsets.UTF_8))
    when(statusResponse.get()).thenReturn(null)
    when(ordinaryResponse.get()).thenReturn(ordinaryValue)

    val managed = GetRequest("managed-key".getBytes(StandardCharsets.UTF_8), "MANAGED_NO_STATUS_BATCH")
    val ordinary = GetRequest("ordinary-key".getBytes(StandardCharsets.UTF_8), "ORDINARY_BATCH_VALUE")
    val responses = Await.result(new RedisKVStoreImpl(cluster).multiGet(Seq(managed, ordinary)), 10.seconds)

    responses.map(_.request) shouldBe Seq(managed, ordinary)
    responses.head.values.isFailure shouldBe true
    responses.head.values.failed.get.getMessage should include("no applied-status marker")
    new String(responses(1).values.get.head.bytes, StandardCharsets.UTF_8) shouldBe "ordinary"
    verify(cluster, times(1)).pipelined()
    verify(pipeline, times(4)).get(any(classOf[Array[Byte]]))
    verify(pipeline, times(1)).close()
  }

  private def readTwoBatchKeys(cluster: JedisCluster) = {
    val kvStore = new RedisKVStoreImpl(cluster)
    val requests = Seq(
      GetRequest("key-a".getBytes(StandardCharsets.UTF_8), "RETRY_A"),
      GetRequest("key-b".getBytes(StandardCharsets.UTF_8), "RETRY_B")
    )
    Await.result(kvStore.multiGet(requests), 10.seconds)
  }

  private def encodeStoredValue(value: String, timestamp: Long): Array[Byte] = {
    val valueBytes = value.getBytes(StandardCharsets.UTF_8)
    ByteBuffer.allocate(java.lang.Long.BYTES + valueBytes.length).putLong(timestamp).put(valueBytes).array()
  }

  private def stubDataReads(cluster: JedisCluster, storedValue: Array[Byte]): Unit =
    when(cluster.get(any(classOf[Array[Byte]]))).thenAnswer { invocation =>
      val key = invocation.getArgument[Array[Byte]](0)
      if (isInternalKey(key)) null else storedValue
    }

  private def dataKey: Array[Byte] =
    argThat(new ArgumentMatcher[Array[Byte]] {
      override def matches(key: Array[Byte]): Boolean = key != null && !isInternalKey(key)
    })

  private def isInternalKey(key: Array[Byte]): Boolean =
    new String(key, StandardCharsets.UTF_8).contains(":__chronon_")
}
