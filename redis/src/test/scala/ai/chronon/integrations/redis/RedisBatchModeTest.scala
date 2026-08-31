package ai.chronon.integrations.redis

import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore.PutRequest
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, verifyNoInteractions, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import redis.clients.jedis.JedisCluster

import scala.concurrent.Await
import scala.concurrent.duration._

class RedisBatchModeTest extends AnyFlatSpec with Matchers with MockitoSugar {
  "Redis incremental mode" should "reject direct writes to incremental batch datasets" in {
    val client = mock[JedisCluster]
    val store =
      new RedisKVStoreImpl(client, batchMode = RedisBatchMode.Incremental(mock[ConditionalObjectWriter]))
    val request = PutRequest("key".getBytes, "value".getBytes, "FEATURES_BATCH", Some(1000L))

    Await.result(store.multiPut(Seq(request)), 10.seconds) shouldBe Seq(false)
    verifyNoInteractions(client)
  }

  it should "retain direct metadata writes" in {
    val client = mock[JedisCluster]
    when(client.set(any[Array[Byte]](), any[Array[Byte]]())).thenReturn("OK")
    val store =
      new RedisKVStoreImpl(client, batchMode = RedisBatchMode.Incremental(mock[ConditionalObjectWriter]))
    val request = PutRequest("key".getBytes, "value".getBytes, MetadataDataset, Some(1000L))

    Await.result(store.multiPut(Seq(request)), 10.seconds) shouldBe Seq(true)
    verify(client).set(any[Array[Byte]](), any[Array[Byte]]())
  }

  it should "retain direct streaming writes" in {
    val client = mock[JedisCluster]
    val store =
      new RedisKVStoreImpl(client, batchMode = RedisBatchMode.Incremental(mock[ConditionalObjectWriter]))
    val dataset = "FEATURES_STREAMING"
    val timestamp = 1728086400000L
    val tileKey = TilingUtils.buildTileKey(dataset, "key".getBytes, Some(1.hour.toMillis), Some(timestamp))
    val request = PutRequest(TilingUtils.serializeTileKey(tileKey), "value".getBytes, dataset, Some(timestamp))

    Await.result(store.multiPut(Seq(request)), 10.seconds) shouldBe Seq(true)
  }
}
