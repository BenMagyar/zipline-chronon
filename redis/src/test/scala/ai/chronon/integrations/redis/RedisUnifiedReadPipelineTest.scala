package ai.chronon.integrations.redis

import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class RedisUnifiedReadPipelineTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {
  private val HourMillis = 1.hour.toMillis
  private val DayStartMillis = Instant.parse("2026-02-01T00:00:00Z").toEpochMilli

  private var cluster: RedisClusterFixture = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    cluster = RedisClusterFixture.start(maxConnections = 4)
  }

  override def afterAll(): Unit = {
    try {
      if (cluster != null) cluster.close()
    } finally {
      super.afterAll()
    }
  }

  "Redis multiGet" should "pipeline interleaved batch and multi-day streaming reads in input order" in {
    val kvStore = new RedisKVStoreImpl(cluster.client)
    val batchA = GetRequest(bytes("batch-a"), "UNIFIED_A_BATCH")
    val batchB = GetRequest(bytes("batch-b"), "UNIFIED_B_BATCH")
    val streamingDataset = "UNIFIED_STREAMING"
    val streamingEntity = bytes("streaming-entity")
    val streamingReadKey = TilingUtils.serializeTileKey(
      TilingUtils.buildTileKey(streamingDataset, streamingEntity, Some(HourMillis), None))
    val queryStart = DayStartMillis + 23 * HourMillis
    val queryEnd = DayStartMillis + 25 * HourMillis
    val streaming = GetRequest(streamingReadKey, streamingDataset, Some(queryStart), Some(queryEnd))

    val batchPuts = Seq(
      PutRequest(batchA.keyBytes, bytes("value-a"), batchA.dataset, Some(DayStartMillis)),
      PutRequest(batchB.keyBytes, bytes("value-b"), batchB.dataset, Some(DayStartMillis))
    )
    val streamingPuts = (22 to 26).map { hour =>
      val timestamp = DayStartMillis + hour * HourMillis
      val writeKey = TilingUtils.serializeTileKey(
        TilingUtils.buildTileKey(streamingDataset, streamingEntity, Some(HourMillis), Some(timestamp)))
      PutRequest(writeKey, bytes(s"stream-$hour"), streamingDataset, Some(timestamp))
    }
    Await.result(kvStore.multiPut(batchPuts ++ streamingPuts), 10.seconds).forall(identity) shouldBe true

    val requests = Seq(batchA, streaming, batchB, streaming)
    val borrowedBefore = cluster.client.getClusterNodes.values().asScala.map(_.getBorrowedCount).sum
    val responses = Await.result(kvStore.multiGet(requests), 10.seconds)
    val borrowedAfter = cluster.client.getClusterNodes.values().asScala.map(_.getBorrowedCount).sum

    responses.map(_.request) shouldBe requests
    new String(responses(0).values.get.head.bytes, StandardCharsets.UTF_8) shouldBe "value-a"
    responses(1).values.get.map(_.millis) shouldBe Seq(queryStart, DayStartMillis + 24 * HourMillis, queryEnd)
    responses(1).values.get.map(value => new String(value.bytes, StandardCharsets.UTF_8)) shouldBe
      Seq("stream-23", "stream-24", "stream-25")
    new String(responses(2).values.get.head.bytes, StandardCharsets.UTF_8) shouldBe "value-b"
    responses(3).values.get.map(_.millis) shouldBe responses(1).values.get.map(_.millis)
    responses(3).values.get.map(value => new String(value.bytes, StandardCharsets.UTF_8)) shouldBe
      responses(1).values.get.map(value => new String(value.bytes, StandardCharsets.UTF_8))
    borrowedAfter - borrowedBefore should be <= 3L
    cluster.client.getClusterNodes.values().asScala.map(_.getNumActive).sum shouldBe 0
  }

  it should "isolate malformed plans and Redis command failures" in {
    val kvStore = new RedisKVStoreImpl(cluster.client)
    val goodBatch = GetRequest(bytes("good-batch"), "FAILURE_GOOD_BATCH")
    Await.result(
      kvStore.multiPut(Seq(PutRequest(goodBatch.keyBytes, bytes("good"), goodBatch.dataset, Some(DayStartMillis)))),
      10.seconds) shouldBe Seq(true)

    val malformed = GetRequest(Array[Byte](1, 2, 3),
                               "FAILURE_MALFORMED_STREAMING",
                               Some(DayStartMillis),
                               Some(DayStartMillis + HourMillis))
    val wrongTypeDataset = "FAILURE_WRONG_TYPE_STREAMING"
    val wrongTypeEntity = bytes("wrong-type")
    val wrongTypeReadKey = TilingUtils.serializeTileKey(
      TilingUtils.buildTileKey(wrongTypeDataset, wrongTypeEntity, Some(HourMillis), None))
    val wrongTypeRedisKey = RedisKVStore
      .buildTiledRedisKey(wrongTypeEntity.toSeq, wrongTypeDataset, DayStartMillis, HourMillis)
      .getBytes(StandardCharsets.UTF_8)
    cluster.client.set(wrongTypeRedisKey, bytes("not-a-sorted-set"))
    val wrongType = GetRequest(wrongTypeReadKey,
                               wrongTypeDataset,
                               Some(DayStartMillis),
                               Some(DayStartMillis + HourMillis))
    val validStreamingDataset = "FAILURE_VALID_STREAMING"
    val validStreamingEntity = bytes("valid-streaming")
    val validStreamingReadKey = TilingUtils.serializeTileKey(
      TilingUtils.buildTileKey(validStreamingDataset, validStreamingEntity, Some(HourMillis), None))
    val validStreamingWriteKey = TilingUtils.serializeTileKey(
      TilingUtils.buildTileKey(validStreamingDataset,
                               validStreamingEntity,
                               Some(HourMillis),
                               Some(DayStartMillis)))
    Await.result(
      kvStore.multiPut(
        Seq(PutRequest(validStreamingWriteKey, bytes("valid-stream"), validStreamingDataset, Some(DayStartMillis)))),
      10.seconds) shouldBe Seq(true)
    val validStreamingRedisKey = RedisKVStore
      .buildTiledRedisKey(validStreamingEntity.toSeq, validStreamingDataset, DayStartMillis, HourMillis)
      .getBytes(StandardCharsets.UTF_8)
    cluster.client.zadd(validStreamingRedisKey, (DayStartMillis + HourMillis / 2).toDouble, Array[Byte](1, 2, 3))
    val validStreaming = GetRequest(validStreamingReadKey,
                                    validStreamingDataset,
                                    Some(DayStartMillis),
                                    Some(DayStartMillis + HourMillis))

    val requests = Seq(goodBatch, malformed, validStreaming, wrongType, validStreaming, goodBatch)
    val responses = Await.result(kvStore.multiGet(requests), 10.seconds)

    responses.map(_.request) shouldBe requests
    responses(0).values.isSuccess shouldBe true
    responses(1).values.isFailure shouldBe true
    responses(2).values.get.map(value => new String(value.bytes, StandardCharsets.UTF_8)) shouldBe Seq("valid-stream")
    responses(3).values.isFailure shouldBe true
    responses(4).values.get.map(value => new String(value.bytes, StandardCharsets.UTF_8)) shouldBe Seq("valid-stream")
    responses(5).values.isSuccess shouldBe true
    cluster.client.getClusterNodes.values().asScala.map(_.getNumActive).sum shouldBe 0
  }

  private def bytes(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)
}
