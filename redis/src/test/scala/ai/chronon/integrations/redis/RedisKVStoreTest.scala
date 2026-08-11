package ai.chronon.integrations.redis

import ai.chronon.api.Constants.{
  ContinuationKey,
  GroupByFolder,
  GroupByServingInfoKey,
  JoinFolder,
  ListEntityOnline,
  ListEntityType,
  ListKeysOnly,
  ListLimit,
  ListValuesAreKeysOnly,
  MetadataDataset
}
import ai.chronon.api.{Join, MetaData, PartitionSpec, ThriftJsonCodec, TilingUtils}
import ai.chronon.online.KVStore._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.JedisCluster
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * Comprehensive tests for Redis Cluster KVStore following BigTableKVStoreTest patterns.
 * Uses Testcontainers with grokzen/redis-cluster for fast, reliable testing (~6s startup).
 */
class RedisKVStoreTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {
  import RedisKVStore._

  private var redisFixture: RedisClusterFixture = _
  private var jedisCluster: JedisCluster = _

  private def leaseDeadlineBytes(seconds: Int = 300): Array[Byte] =
    (System.currentTimeMillis() + seconds * 1000L).toString.getBytes(StandardCharsets.UTF_8)

  private def joinPut(name: String, online: Boolean, timestamp: Option[Long] = None): PutRequest = {
    val join = new Join().setMetaData(new MetaData().setName(name).setOnline(online))
    PutRequest(
      s"$JoinFolder/$name".getBytes(StandardCharsets.UTF_8),
      ThriftJsonCodec.toJsonStr(join).getBytes(StandardCharsets.UTF_8),
      MetadataDataset,
      timestamp
    )
  }

  private def listAll(kvStore: RedisKVStoreImpl,
                      dataset: String,
                      limit: Int,
                      props: Map[String, Any] = Map.empty): Seq[ListValue] = {
    val values = ArrayBuffer.empty[ListValue]
    var continuation = Option.empty[Any]
    var pageCount = 0
    var hasMore = true

    while (hasMore) {
      val requestProps = props ++ Map(ListLimit -> limit) ++ continuation.map(ContinuationKey -> _)
      val response = Await.result(kvStore.list(ListRequest(dataset, requestProps)), 10.seconds)
      response.values.isSuccess shouldBe true
      val page = response.values.get
      page.size should be <= limit
      values ++= page
      continuation = response.resultProps.get(ContinuationKey)
      hasMore = continuation.nonEmpty
      pageCount += 1
      pageCount should be <= 1000
    }
    values.toSeq
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    redisFixture = RedisClusterFixture.start(maxConnections = 10, minIdleConnections = 2, maxIdleConnections = 10)
    jedisCluster = redisFixture.client
  }

  override def afterAll(): Unit = {
    if (redisFixture != null) {
      try redisFixture.close()
      catch { case _: Exception => }
    }
    super.afterAll()
  }

  // Clean data before each test
  override def withFixture(test: NoArgTest) = {
    try {
      jedisCluster.flushAll()
    } catch {
      case _: Exception => // Ignore cleanup errors (e.g., when Redis is stopped for failure tests)
    }
    super.withFixture(test)
  }

  it should "create Redis dataset successfully" in {
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    val dataset = "test-table"
    kvStore.create(dataset)
    succeed
    // Redis doesn't need explicit table creation, just verify no errors
  }

  // Test write & read of simple blob dataset
  it should "blob data round trip" in {
    val dataset = "models"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    val key1 = "alice"
    val key2 = "bob"
    // some blob json payloads
    val value1 = """{"name": "alice", "age": 30}"""
    val value2 = """{"name": "bob", "age": 40}"""

    val putReq1 = PutRequest(key1.getBytes, value1.getBytes, dataset, None)
    val putReq2 = PutRequest(key2.getBytes, value2.getBytes, dataset, None)
    val putResults = Await.result(kvStore.multiPut(Seq(putReq1, putReq2)), 10.seconds)
    putResults shouldBe Seq(true, true)

    // let's try and read these
    val getReq1 = GetRequest(key1.getBytes, dataset, None, None)
    val getReq2 = GetRequest(key2.getBytes, dataset, None, None)
    val getResult1 = Await.result(kvStore.multiGet(Seq(getReq1)), 10.seconds)
    val getResult2 = Await.result(kvStore.multiGet(Seq(getReq2)), 10.seconds)

    getResult1.size shouldBe 1
    validateBlobValueExpectedPayload(getResult1.head, value1)
    getResult2.size shouldBe 1
    validateBlobValueExpectedPayload(getResult2.head, value2)
  }

  it should "blob data updates" in {
    val dataset = "models"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    val key1 = "alice"
    // some blob json payloads
    val value = """{"name": "alice", "age": 30}"""
    val putReq = PutRequest(key1.getBytes, value.getBytes, dataset, None)
    val putResults = Await.result(kvStore.multiPut(Seq(putReq)), 10.seconds)
    putResults shouldBe Seq(true)

    // let's try and read this record
    val getReq = GetRequest(key1.getBytes, dataset, None, None)
    val getResult = Await.result(kvStore.multiGet(Seq(getReq)), 10.seconds)
    getResult.size shouldBe 1
    validateBlobValueExpectedPayload(getResult.head, value)

    // let's now mutate this record
    val valueUpdated = """{"name": "alice", "age": 35}"""
    val putReqUpdated = PutRequest(key1.getBytes, valueUpdated.getBytes, dataset, None)
    val putResultsUpdated = Await.result(kvStore.multiPut(Seq(putReqUpdated)), 10.seconds)
    putResultsUpdated shouldBe Seq(true)

    // and read & verify
    val getResultUpdated = Await.result(kvStore.multiGet(Seq(getReq)), 10.seconds)
    getResultUpdated.size shouldBe 1
    validateBlobValueExpectedPayload(getResultUpdated.head, valueUpdated)
  }

  it should "store Chronon config metadata without expiry" in {
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    val firstPut = joinPut("example.join__1", online = true)
    val key = firstPut.keyBytes
    val redisKey = RedisKVStore.buildRedisKey(key.toSeq, MetadataDataset)
    val redisKeyBytes = redisKey.getBytes(StandardCharsets.UTF_8)

    Await.result(
      kvStore.multiPut(Seq(firstPut)),
      10.seconds) shouldBe Seq(true)
    jedisCluster.ttl(redisKeyBytes) shouldBe -1L

    jedisCluster.expire(redisKeyBytes, 60L) shouldBe 1L
    jedisCluster.ttl(redisKeyBytes) should be > 0L

    Await.result(
      kvStore.multiPut(Seq(joinPut("example.join__1", online = false))),
      10.seconds) shouldBe Seq(true)
    jedisCluster.ttl(redisKeyBytes) shouldBe -1L
  }

  it should "index uploaded online join versions by upload time without removing old entries" in {
    val keyPrefix = "join-set-test"
    val kvStore = new RedisKVStoreImpl(
      jedisCluster,
      Map(RedisKVStoreConstants.PropRedisKeyPrefix -> keyPrefix)
    )
    val indexKey = RedisKVStore.onlineJoinNamesIndexKey(keyPrefix)
    val request = ListRequest(
      MetadataDataset,
      Map(ListEntityType -> JoinFolder, ListEntityOnline -> true, ListKeysOnly -> true)
    )

    Await.result(
      kvStore.multiPut(
        Seq(
          joinPut("ranking.search.attempt__50", online = true, Some(1000L)),
          joinPut("ranking.search.attempt__51", online = true, Some(2000L)),
          joinPut("ranking.search.attempt__51", online = true, Some(3000L)),
          joinPut("ranking.search.offline__1", online = false, Some(4000L))
        )),
      10.seconds
    ) shouldBe Seq(true, true, true, true)

    jedisCluster.zrange(indexKey, 0, -1).asScala.toSet shouldBe
      Set("ranking.search.attempt__50", "ranking.search.attempt__51")
    jedisCluster.zcard(indexKey) shouldBe 2L
    jedisCluster.zscore(indexKey, "ranking.search.attempt__50") shouldBe 1000.0
    jedisCluster.zscore(indexKey, "ranking.search.attempt__51") shouldBe 3000.0
    jedisCluster.ttl(indexKey) shouldBe -1L

    Await.result(
      kvStore.multiPut(Seq(joinPut("ranking.search.attempt__51", online = true, Some(2500L)))),
      10.seconds
    ) shouldBe Seq(true)
    jedisCluster.zscore(indexKey, "ranking.search.attempt__51") shouldBe 3000.0

    val initial = Await.result(kvStore.list(request), 10.seconds)
    initial.values.get.map(value => new String(value.keyBytes, StandardCharsets.UTF_8)).toSet shouldBe
      Set("ranking.search.attempt__50", "ranking.search.attempt__51")
    initial.values.get.foreach(_.valueBytes shouldBe empty)
    initial.resultProps.get(ListValuesAreKeysOnly) shouldBe Some(true)

    // Re-uploading an existing version as offline does not remove it from the append-only index.
    Await.result(
      kvStore.multiPut(Seq(joinPut("ranking.search.attempt__50", online = false, Some(5000L)))),
      10.seconds
    ) shouldBe Seq(true)
    jedisCluster.zrange(indexKey, 0, -1).asScala.toSet shouldBe
      Set("ranking.search.attempt__50", "ranking.search.attempt__51")
    jedisCluster.zscore(indexKey, "ranking.search.attempt__50") shouldBe 1000.0
  }

  it should "paginate the online join index" in {
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    val names = (0 until 25).map(index => f"ranking.search.join__$index%02d")
    Await.result(kvStore.multiPut(names.map(name => joinPut(name, online = true))), 10.seconds).foreach(_ shouldBe true)

    val listed = listAll(
      kvStore,
      MetadataDataset,
      limit = 7,
      props = Map(ListEntityType -> JoinFolder, ListEntityOnline -> true, ListKeysOnly -> true)
    )
    listed.map(value => new String(value.keyBytes, StandardCharsets.UTF_8)).toSet shouldBe names.toSet
    listed.foreach(_.valueBytes shouldBe empty)
  }

  it should "fail a join metadata upload when its index cannot be updated" in {
    val keyPrefix = "join-set-failure-test"
    val kvStore = new RedisKVStoreImpl(
      jedisCluster,
      Map(RedisKVStoreConstants.PropRedisKeyPrefix -> keyPrefix)
    )
    jedisCluster.set(RedisKVStore.onlineJoinNamesIndexKey(keyPrefix), "wrong-type")

    Await.result(kvStore.multiPut(Seq(joinPut("ranking.search.failed__1", online = true))), 10.seconds) shouldBe
      Seq(false)
  }

  it should "list with pagination" in {
    val dataset = "CHRONON_METADATA"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    val putReqs = (0 until 100).map { i =>
      val key = s"key-$i"
      val value = s"""{"name": "name-$i", "age": $i}"""
      PutRequest(key.getBytes, value.getBytes, dataset, None)
    }
    val putResults = Await.result(kvStore.multiPut(putReqs), 10.seconds)
    putResults.foreach(r => r shouldBe true)

    val limit = 10
    val listed = listAll(kvStore, dataset, limit)
    listed.size shouldBe putReqs.size
    listed.map(value => new String(value.keyBytes, StandardCharsets.UTF_8)).toSet shouldBe
      (0 until 100).map(i => s"key-$i").toSet
    listed.map(value => new String(value.valueBytes, StandardCharsets.UTF_8)).toSet shouldBe
      (0 until 100).map(i => s"""{"name": "name-$i", "age": $i}""").toSet
  }

  it should "list entity types with pagination" in {
    Seq(DatasetAndEntityHashTagMode, EntityHashTagMode).zipWithIndex.foreach { case (hashTagMode, modeIndex) =>
      val dataset = s"CHRONON_METADATA_$modeIndex"
      val kvStore = new RedisKVStoreImpl(jedisCluster, Map("redis.key.hash.tag.mode" -> hashTagMode))
      kvStore.create(dataset)

      val putGrpByReqs = (0 until 50).map { i =>
        val key = s"$GroupByFolder/gbkey-$i"
        val value = s"""{"name": "group-by-$i"}"""
        PutRequest(key.getBytes, value.getBytes, dataset, None)
      }
      val putJoinReqs = (0 until 50).map { i =>
        val key = s"$JoinFolder/joinkey-$i"
        val value = s"""{"name": "join-$i"}"""
        PutRequest(key.getBytes, value.getBytes, dataset, None)
      }
      Await.result(kvStore.multiPut(putGrpByReqs ++ putJoinReqs), 10.seconds).foreach(_ shouldBe true)

      val listed = listAll(kvStore, dataset, limit = 7, props = Map(ListEntityType -> JoinFolder))
      listed.size shouldBe putJoinReqs.size
      listed.map(value => new String(value.keyBytes, StandardCharsets.UTF_8)).toSet shouldBe
        (0 until 50).map(i => s"$JoinFolder/joinkey-$i").toSet
      listed.map(value => new String(value.valueBytes, StandardCharsets.UTF_8)).toSet shouldBe
        (0 until 50).map(i => s"""{"name": "join-$i"}""").toSet
    }
  }

  it should "handle multiple key time series query" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16 and write out payloads for key1
    val fakePayload1 = """{"name": "my_key1", "my_feature": "123"}"""
    val tsRange = 1728000000000L until 1729036800000L by 1.hour.toMillis
    generateAndWriteTimeSeriesData(kvStore, dataset, tsRange, fakePayload1, "my_key1")

    // generate some hourly timestamps from 10/04/24 00:00 to 10/16 and write out payloads for key2
    val fakePayload2 = """{"name": "my_key2", "my_feature": "456"}"""
    generateAndWriteTimeSeriesData(kvStore, dataset, tsRange, fakePayload2, "my_key2")

    // query in time range: 10/05/24 00:00 to 10/10/24
    val queryStartTs = 1728086400000L
    val queryEndTs = 1728518400000L
    val readTileKey1 = TilingUtils.buildTileKey(dataset, "my_key1".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)
    val readTileKey2 = TilingUtils.buildTileKey(dataset, "my_key2".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)
    val getRequest1 = GetRequest(readKeyBytes1, dataset, Some(queryStartTs), Some(queryEndTs))
    val getRequest2 = GetRequest(readKeyBytes2, dataset, Some(queryStartTs), Some(queryEndTs))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest1, getRequest2)), 10.seconds)
    getResult.size shouldBe 2
    val expectedTimeSeriesPoints = (queryStartTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult.head, expectedTimeSeriesPoints, fakePayload1)
    validateTimeSeriesValueExpectedPayload(getResult.last, expectedTimeSeriesPoints, fakePayload2)
  }

  // Test repeated writes to the same streaming tile - should return the latest value (Last-Write-Wins)
  it should "repeated streaming tile updates return latest value" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    // tile timestamp - 10/04/24 00:00
    val tileTimestamp = 1728000000000L
    val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(tileTimestamp))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)

    // write a series of updates to the tile to mimic streaming updates
    for (i <- 0 to 10) {
      val fakePayload = s"""{"name": "my_key", "my_feature_ir": "$i"}"""
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(tileTimestamp + i * 1000), fakePayload)
    }

    // query in time range: 10/04/24 00:00 to 10/04/24 10:00 (we just expect the one tile though)
    val queryStartTs = 1728000000000L
    val queryEndTs = 1728036000000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 10.seconds)
    getResult1.size shouldBe 1
    val expectedTiles = Seq(tileTimestamp)
    val expectedPayload = """{"name": "my_key", "my_feature_ir": "10"}""" // latest value
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, expectedPayload)
  }

  // Test Last-Write-Wins semantics: duplicate timestamps should overwrite
  it should "last write wins for duplicate timestamps" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)
    val key = "test_key"
    val timestamp = 1728000000000L

    // Build tiled key for STREAMING dataset
    val tileKey = TilingUtils.buildTileKey(dataset, key.getBytes, Some(1.hour.toMillis), Some(timestamp))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)

    // Write value1 at timestamp T
    val value1 = """{"value": 1}"""
    val putReq1 = PutRequest(tileKeyBytes, value1.getBytes, dataset, Some(timestamp))
    Await.result(kvStore.multiPut(Seq(putReq1)), 10.seconds)

    // Read and verify value1
    val readTileKey = TilingUtils.buildTileKey(dataset, key.getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
    val getReq = GetRequest(readKeyBytes, dataset, Some(timestamp), Some(timestamp))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getReq)), 10.seconds)
    getResult1.head.values.get.size shouldBe 1
    new String(getResult1.head.values.get.head.bytes) shouldBe value1

    // Write value2 at the SAME timestamp T (should overwrite value1)
    val value2 = """{"value": 2}"""
    val putReq2 = PutRequest(tileKeyBytes, value2.getBytes, dataset, Some(timestamp))
    Await.result(kvStore.multiPut(Seq(putReq2)), 10.seconds)

    // Read and verify value2 (not value1)
    val getResult2 = Await.result(kvStore.multiGet(Seq(getReq)), 10.seconds)
    getResult2.head.values.get.size shouldBe 1 // Still only one value
    new String(getResult2.head.values.get.head.bytes) shouldBe value2 // Latest value
  }

  // Test that non-time-series data preserves write timestamp
  it should "preserve write timestamp for non-time-series data" in {
    val dataset = "models"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)
    val key = "test_key"
    val value = """{"name": "test", "age": 30}"""
    val writeTimestamp = System.currentTimeMillis()

    // Write with explicit timestamp
    val putReq = PutRequest(key.getBytes, value.getBytes, dataset, None)
    Await.result(kvStore.multiPut(Seq(putReq)), 10.seconds)

    // Wait a bit to ensure read time is different from write time
    Thread.sleep(100)

    // Read the data
    val getReq = GetRequest(key.getBytes, dataset, None, None)
    val getResult = Await.result(kvStore.multiGet(Seq(getReq)), 10.seconds)
    getResult.size shouldBe 1
    getResult.head.values.isSuccess shouldBe true
    val timedValues = getResult.head.values.get
    timedValues.size shouldBe 1

    // The returned timestamp should be close to write time, NOT current time
    val returnedTimestamp = timedValues.head.millis
    val currentTime = System.currentTimeMillis()

    // Timestamp should be within 1 second of write time
    Math.abs(returnedTimestamp - writeTimestamp) should be < 1000L
    // And should NOT be current time (should be at least 50ms older)
    (currentTime - returnedTimestamp) should be >= 50L
  }

  // Test streaming tiled data
  it should "streaming tiled query multiple days" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123"}"""
    val tsRange = 1728000000000L until 1729036800000L by 1.hour.toMillis
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }

    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }

    // query in time range: 10/05/24 00:00 to 10/10/24
    val queryStartTs = 1728086400000L
    val queryEndTs = 1728518400000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 10.seconds)
    getResult1.size shouldBe 1
    val expectedTiles = (queryStartTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, fakePayload)
  }

  it should "streaming tiled query multiple days and multiple keys" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16 for key1
    val fakePayload1 = """{"name": "my_key1", "my_feature_ir": "123"}"""
    val tsRange = 1728000000000L until 1729036800000L by 1.hour.toMillis
    generateAndWriteTimeSeriesData(kvStore, dataset, tsRange, fakePayload1, "my_key1")

    val fakePayload2 = """{"name": "my_key2", "my_feature_ir": "456"}"""
    generateAndWriteTimeSeriesData(kvStore, dataset, tsRange, fakePayload2, "my_key2")

    // query in time range: 10/05/24 00:00 to 10/10/24
    val queryStartTs = 1728086400000L
    val queryEndTs = 1728518400000L
    // read key1
    val readTileKey1 = TilingUtils.buildTileKey(dataset, "my_key1".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)
    // and key2
    val readTileKey2 = TilingUtils.buildTileKey(dataset, "my_key2".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    val getRequest1 = GetRequest(readKeyBytes1, dataset, Some(queryStartTs), Some(queryEndTs))
    val getRequest2 = GetRequest(readKeyBytes2, dataset, Some(queryStartTs), Some(queryEndTs))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest1, getRequest2)), 10.seconds)
    getResult.size shouldBe 2
    val expectedTiles = (queryStartTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult.head, expectedTiles, fakePayload1)
    validateTimeSeriesValueExpectedPayload(getResult.last, expectedTiles, fakePayload2)
  }

  // handle case where the two keys have different batch end times
  it should "streaming tiled query with different batch end times" in {
    val dataset1 = "GROUPBY_A_STREAMING"
    val dataset2 = "GROUPBY_B_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset1)
    kvStore.create(dataset2)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16/24 for key1
    val fakePayload1 = """{"name": "my_key1", "my_feature_ir": "123"}"""
    val tsRange = 1728000000000L until 1729036800000L by 1.hour.toMillis
    generateAndWriteTimeSeriesData(kvStore, dataset1, tsRange, fakePayload1, "my_key1")

    val fakePayload2 = """{"name": "my_key2", "my_feature_ir": "456"}"""
    generateAndWriteTimeSeriesData(kvStore, dataset2, tsRange, fakePayload2, "my_key2")

    // read key1
    val readTileKey1 = TilingUtils.buildTileKey(dataset1, "my_key1".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)
    // and key2
    val readTileKey2 = TilingUtils.buildTileKey(dataset2, "my_key2".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    // query in time range: 10/05/24 00:00 to 10/10/24 for key1
    val queryStartTs1 = 1728086400000L
    val queryEndTs1 = 1728518400000L
    val getRequest1 = GetRequest(readKeyBytes1, dataset1, Some(queryStartTs1), Some(queryEndTs1))
    // query in time range: 10/10/24 00:00 to 10/11/24 for key2
    val queryStartTs2 = 1728518400000L
    val queryEndTs2 = 1728604800000L
    val getRequest2 = GetRequest(readKeyBytes2, dataset2, Some(queryStartTs2), Some(queryEndTs2))
    val getResult = Await.result(kvStore.multiGet(Seq(getRequest1, getRequest2)), 10.seconds)
    getResult.size shouldBe 2

    // map dataset to result
    val datasetToResult = getResult.map { r =>
      (r.request.dataset, r)
    }.toMap

    // validate two sets of tiles
    val expectedTilesKey1Tiles = (queryStartTs1 to queryEndTs1 by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(datasetToResult(dataset1), expectedTilesKey1Tiles, fakePayload1)
    val expectedTilesKey2Tiles = (queryStartTs2 to queryEndTs2 by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(datasetToResult(dataset2), expectedTilesKey2Tiles, fakePayload2)
  }

  it should "streaming tiled query one day" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123"}"""
    val tsRange = 1728000000000L until 1729036800000L by 1.hour.toMillis
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }

    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }

    // query in time range: 10/05/24 00:00 to 10/06/24 00:00
    val queryStartTs = 1728086400000L
    val queryEndTs = 1728172800000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 10.seconds)
    getResult1.size shouldBe 1
    val expectedTiles = (queryStartTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, fakePayload)
  }

  it should "streaming tiled query same day" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123"}"""
    val tsRange = 1728000000000L until 1729036800000L by 1.hour.toMillis
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }

    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }

    // query in time range: 10/05/24 00:00 to 10/05/24 22:00
    val queryStartTs = 1728086400000L
    val queryEndTs = 1728166800000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 10.seconds)
    getResult1.size shouldBe 1
    val expectedTiles = (queryStartTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, fakePayload)
  }

  it should "streaming tiled query days without data" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    // generate some hourly timestamps & tiles from 10/04/24 00:00 to 10/16
    val fakePayload = """{"name": "my_key", "my_feature_ir": "123"}"""
    val dataStartTs = 1728000000000L
    val dataEndTs = 1729036800000L
    val tsRange = dataStartTs until dataEndTs by 1.hour.toMillis
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }

    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }

    // query in time range: 10/15/24 00:00 to 10/30/24 00:00
    val queryStartTs = 1728950400000L
    val queryEndTs = 1730246400000L
    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
    val getRequest1 = GetRequest(readKeyBytes, dataset, Some(queryStartTs), Some(queryEndTs))
    val getResult1 = Await.result(kvStore.multiGet(Seq(getRequest1)), 10.seconds)
    getResult1.size shouldBe 1
    // we expect results to only cover the time range where we have data
    val expectedTiles = (queryStartTs until dataEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResult1.head, expectedTiles, fakePayload)
  }

  it should "handle multiple entities with different time ranges in single query" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    // Create data for two different entities
    val entity1Key = "entity1"
    val entity2Key = "entity2"
    val fakePayload1 = """{"name": "entity1", "feature": "value1"}"""
    val fakePayload2 = """{"name": "entity2", "feature": "value2"}"""

    // Generate hourly data from 10/01/24 00:00 to 10/10/24 00:00
    val tsRange = 1727740800000L until 1728518400000L by 1.hour.toMillis

    // Write data for both entities using tiled keys
    generateAndWriteTimeSeriesData(kvStore, dataset, tsRange, fakePayload1, entity1Key)
    generateAndWriteTimeSeriesData(kvStore, dataset, tsRange, fakePayload2, entity2Key)

    // Query with different time ranges
    // Entity 1: 10/02/24 00:00 to 10/04/24 00:00
    val queryStartTs1 = 1727827200000L
    val queryEndTs1 = 1728000000000L
    // Entity 2: 10/05/24 00:00 to 10/07/24 00:00
    val queryStartTs2 = 1728086400000L
    val queryEndTs2 = 1728259200000L

    val readTileKey1 = TilingUtils.buildTileKey(dataset, entity1Key.getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)
    val readTileKey2 = TilingUtils.buildTileKey(dataset, entity2Key.getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    val getRequest1 = GetRequest(readKeyBytes1, dataset, Some(queryStartTs1), Some(queryEndTs1))
    val getRequest2 = GetRequest(readKeyBytes2, dataset, Some(queryStartTs2), Some(queryEndTs2))

    // Fetch both entities in a single multiGet call
    val getResults = Await.result(kvStore.multiGet(Seq(getRequest1, getRequest2)), 10.seconds)

    // Verify we get results for both entities
    getResults.size shouldBe 2

    // Find responses by matching the request (order may vary due to grouping)
    val result1 = getResults.find(_.request == getRequest1).get
    val result2 = getResults.find(_.request == getRequest2).get

    // Each should have data for their respective time ranges
    val expectedTimestamps1 = (queryStartTs1 to queryEndTs1 by 1.hour.toMillis).toSeq
    val expectedTimestamps2 = (queryStartTs2 to queryEndTs2 by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(result1, expectedTimestamps1, fakePayload1)
    validateTimeSeriesValueExpectedPayload(result2, expectedTimestamps2, fakePayload2)
  }

  it should "handle mixed request types - time series and non-time series" in {
    val timeSeriesDataset = "GROUPBY_STREAMING"
    val blobDataset = "CHRONON_METADATA"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(timeSeriesDataset)
    kvStore.create(blobDataset)

    // Write time series data (using tiled keys for STREAMING dataset)
    val tsKey = "ts_key"
    val tsPayload = """{"name": "ts_key", "feature": "timeseries"}"""
    val tsRange = 1728000000000L until 1728172800000L by 1.hour.toMillis
    generateAndWriteTimeSeriesData(kvStore, timeSeriesDataset, tsRange, tsPayload, tsKey)

    // Write blob data
    val blobKey = "blob_key"
    val blobPayload = """{"name": "blob_key", "feature": "blob"}"""
    val putReq = PutRequest(blobKey.getBytes, blobPayload.getBytes, blobDataset, None)
    Await.result(kvStore.multiPut(Seq(putReq)), 10.seconds)

    // Query both types
    val queryStartTs = 1728043200000L
    val queryEndTs = 1728129600000L
    val readTileKey = TilingUtils.buildTileKey(timeSeriesDataset, tsKey.getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes = TilingUtils.serializeTileKey(readTileKey)
    val tsGetRequest = GetRequest(readKeyBytes, timeSeriesDataset, Some(queryStartTs), Some(queryEndTs))
    val blobGetRequest = GetRequest(blobKey.getBytes, blobDataset, None, None)

    // Note: These would go to different datasets, so they'd be in separate multiGet calls in practice
    // Test them separately as they would be in real usage
    val tsResult = Await.result(kvStore.multiGet(Seq(tsGetRequest)), 10.seconds)
    val blobResult = Await.result(kvStore.multiGet(Seq(blobGetRequest)), 10.seconds)

    tsResult.size shouldBe 1
    blobResult.size shouldBe 1
    val expectedTimestamps = (queryStartTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(tsResult.head, expectedTimestamps, tsPayload)
    validateBlobValueExpectedPayload(blobResult.head, blobPayload)
  }

  it should "handle entities where some have data and others don't" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    // Create data only for entity1, not for entity2
    val entity1Key = "entity1_with_data"
    val entity2Key = "entity2_without_data"
    val fakePayload1 = """{"name": "entity1", "feature": "value1"}"""

    // Generate hourly data from 10/04/24 00:00 to 10/06/24 00:00
    val tsRange = 1728000000000L until 1728172800000L by 1.hour.toMillis

    // Write data only for entity1
    tsRange.foreach { ts =>
      val tileKey1 = TilingUtils.buildTileKey(dataset, entity1Key.getBytes, Some(1.hour.toMillis), Some(ts))
      val tileKeyBytes1 = TilingUtils.serializeTileKey(tileKey1)
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes1, Seq(ts), fakePayload1)
    }
    // entity2 has no data written

    // Query both entities with the same time range
    val queryStartTs = 1728043200000L
    val queryEndTs = 1728129600000L
    val readTileKey1 = TilingUtils.buildTileKey(dataset, entity1Key.getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes1 = TilingUtils.serializeTileKey(readTileKey1)
    val readTileKey2 = TilingUtils.buildTileKey(dataset, entity2Key.getBytes, Some(1.hour.toMillis), None)
    val readKeyBytes2 = TilingUtils.serializeTileKey(readTileKey2)

    val getRequest1 = GetRequest(readKeyBytes1, dataset, Some(queryStartTs), Some(queryEndTs))
    val getRequest2 = GetRequest(readKeyBytes2, dataset, Some(queryStartTs), Some(queryEndTs))

    // Fetch both entities in a single multiGet call
    val getResults = Await.result(kvStore.multiGet(Seq(getRequest1, getRequest2)), 10.seconds)

    // Verify we get results for both requests (even if one is empty)
    getResults.size shouldBe 2
    // Entity1 should have data
    val expectedTimestamps = (queryStartTs to queryEndTs by 1.hour.toMillis).toSeq
    validateTimeSeriesValueExpectedPayload(getResults.head, expectedTimestamps, fakePayload1)
    // Entity2 should have no data (empty response)
    getResults.last.values.isSuccess shouldBe true
    getResults.last.values.get.isEmpty shouldBe true
  }

  it should "handle missing keys gracefully" in {
    val dataset = "models"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    val getReq = GetRequest("nonexistent_key".getBytes, dataset, None, None)
    val getResult = Await.result(kvStore.multiGet(Seq(getReq)), 10.seconds)

    getResult.size shouldBe 1
    getResult.head.values.isSuccess shouldBe true
    getResult.head.values.get.isEmpty shouldBe true
  }

  // ===== Cluster-Specific Tests
  it should "use hash tags in batch IR keys for cluster co-location" in {
    val dataset = "MY_GROUPBY_BATCH"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    val key = "users"
    val value = "{\"name\": \"alice\", \"age\": 30}".getBytes
    val putReq = PutRequest(key.getBytes, value, dataset, None)
    Await.result(kvStore.multiPut(Seq(putReq)), 10.seconds)

    // Verify the key was created with hash tags (dataset:base64_key format)
    val expectedKey = s"chronon:{$dataset:${java.util.Base64.getEncoder.encodeToString(key.getBytes)}}"
    val storedValue = jedisCluster.get(expectedKey.getBytes(StandardCharsets.UTF_8))
    storedValue should not be null
    storedValue.length should be > 8 // Has timestamp prefix
  }

  it should "use hash tags in streaming tile keys for cluster co-location" in {
    val dataset = "MY_GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    val key = "user456"
    val timestamp = 1728086400000L // Oct 5, 2024
    val value = """{"tile": "hourly_data"}"""
    val tileKey = TilingUtils.buildTileKey(dataset, key.getBytes, Some(1.hour.toMillis), Some(timestamp))
    val serializedTileKey = TilingUtils.serializeTileKey(tileKey)
    val putReq = PutRequest(serializedTileKey, value.getBytes, dataset, Some(timestamp))
    Await.result(kvStore.multiPut(Seq(putReq)), 10.seconds)

    // Verify the key was created with hash tags (dataset:base64_key format)
    val base64Key = java.util.Base64.getEncoder.encodeToString(key.getBytes)
    val dayTs = timestamp - (timestamp % (24 * 3600 * 1000))
    val expectedKey = s"chronon:{$dataset:$base64Key}:$dayTs:${1.hour.toMillis}"

    // Check that the sorted set exists
    val members = jedisCluster.zrangeByScore(expectedKey.getBytes(StandardCharsets.UTF_8), timestamp.toDouble, timestamp.toDouble)
    members should not be empty
  }

  it should "co-locate batch IR and streaming tiles for same entity" in {
    val batchDataset = "MY_GB_BATCH"
    val streamingDataset = "MY_GB_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(batchDataset)
    kvStore.create(streamingDataset)

    val entityKey = "entity789"
    val base64Key = java.util.Base64.getEncoder.encodeToString(entityKey.getBytes)

    // Write batch IR
    val batchValue = """{"batch": "ir"}"""
    val batchPutReq = PutRequest(entityKey.getBytes, batchValue.getBytes, batchDataset, None)
    Await.result(kvStore.multiPut(Seq(batchPutReq)), 10.seconds)

    // Write streaming tile
    val streamValue = """{"stream": "tile"}"""
    val timestamp = 1728086400000L
    val tileKey = TilingUtils.buildTileKey(streamingDataset, entityKey.getBytes, Some(1.hour.toMillis), Some(timestamp))
    val serializedTileKey = TilingUtils.serializeTileKey(tileKey)
    val streamPutReq = PutRequest(serializedTileKey, streamValue.getBytes, streamingDataset, Some(timestamp))
    Await.result(kvStore.multiPut(Seq(streamPutReq)), 10.seconds)

    // Verify both keys use different hash tags (different nodes in real cluster to prevent hotkey amplification)
    val batchKey = s"chronon:{$batchDataset:$base64Key}"
    val dayTs = timestamp - (timestamp % (24 * 3600 * 1000))
    val streamKey = s"chronon:{$streamingDataset:$base64Key}:$dayTs:${1.hour.toMillis}"

    // Both keys should exist
    val batchExists = jedisCluster.exists(batchKey.getBytes(StandardCharsets.UTF_8))
    val streamExists = jedisCluster.exists(streamKey.getBytes(StandardCharsets.UTF_8))
    batchExists shouldBe true
    streamExists shouldBe true

    // With {dataset:base64_key} hash tags, batch and streaming for same entity distribute across nodes
    // This prevents hotkey amplification when popular entities appear in multiple datasets
    batchKey should include(s"{$batchDataset:$base64Key}")
    streamKey should include(s"{$streamingDataset:$base64Key}")
  }

  it should "advance the recoverable batch status monotonically" in {
    val statusKey = RedisBatchUpload.buildStatusKey("INCREMENTAL_BATCH", "chronon")
    val initial = RedisBatchUpload.BatchStatus("generation-1", "2024-07-27-00", 1000L, 1L)
    val replacement = RedisBatchUpload.BatchStatus("generation-2", "2024-07-27-01", 2000L, 2L)

    RedisBatchUpload.writeStatus(jedisCluster, statusKey, initial) shouldBe 1L
    RedisBatchUpload.writeStatus(jedisCluster, statusKey, initial) shouldBe 2L
    RedisBatchUpload.writeStatus(jedisCluster, statusKey, replacement) shouldBe 1L
    RedisBatchUpload.writeStatus(jedisCluster, statusKey, initial) shouldBe 0L
    RedisBatchUpload.readStatus(jedisCluster, statusKey) shouldBe Some(replacement)

    val retired = replacement.copy(generation = "generation-3", writeEpoch = 3L, retired = true)
    RedisBatchUpload.writeStatus(jedisCluster, statusKey, retired) shouldBe 1L
    RedisBatchUpload.writeStatus(
      jedisCluster,
      statusKey,
      retired.copy(generation = "generation-4", writeEpoch = 4L, retired = false)) shouldBe 0L
  }

  it should "reserve one cluster-wide rate queue across independent writers" in {
    val limiterKey = RedisBatchUpload.RateLimiterKey

    // The first writer consumes the one-batch burst. Later writers reserve distinct positions on the shared timeline.
    val firstWriterWait =
      RedisBatchUpload.reserveRate(jedisCluster, limiterKey, keyCount = 600, maxKeysPerSecond = 10, maxBurst = 600)
    val secondWriterWait =
      RedisBatchUpload.reserveRate(jedisCluster, limiterKey, keyCount = 600, maxKeysPerSecond = 10, maxBurst = 600)
    val thirdWriterWait =
      RedisBatchUpload.reserveRate(jedisCluster, limiterKey, keyCount = 600, maxKeysPerSecond = 10, maxBurst = 600)

    firstWriterWait shouldBe 0L
    secondWriterWait should be >= 59000L
    thirdWriterWait should be >= 119000L

    val mismatch = the[redis.clients.jedis.exceptions.JedisDataException] thrownBy {
      RedisBatchUpload.reserveRate(jedisCluster, limiterKey, keyCount = 1, maxKeysPerSecond = 20, maxBurst = 600)
    }
    mismatch.getMessage should include("different max keys per second")
    jedisCluster.hget(limiterKey, "rate") shouldBe "10"
  }

  it should "not let an older generation overwrite a newer materialized base value" in {
    val baseKey = RedisKVStore.buildRedisKey("entity-1".getBytes(StandardCharsets.UTF_8).toSeq,
                                             "INCREMENTAL_BATCH")

    def putBase(timestamp: Long, payload: String, writeEpoch: Long): Long = {
      val encoded = RedisBatchUpload.encodeValue(
        RedisBatchUpload.Upsert,
        timestamp,
        payload.getBytes(StandardCharsets.UTF_8),
        writeEpoch
      )
      jedisCluster
        .eval(
          RedisBatchUpload.putBaseIfNewerScript,
          List(baseKey.getBytes(StandardCharsets.UTF_8)).asJava,
          List(
            timestamp.toString.getBytes(StandardCharsets.UTF_8),
            writeEpoch.toString.getBytes(StandardCharsets.UTF_8),
            encoded,
            leaseDeadlineBytes()
          ).asJava
        )
        .asInstanceOf[java.lang.Long]
        .longValue()
    }

    putBase(2000L, "new", writeEpoch = 2L) shouldBe 1L
    putBase(2000L, "new", writeEpoch = 2L) shouldBe 2L
    an[redis.clients.jedis.exceptions.JedisDataException] should be thrownBy {
      putBase(2000L, "conflicting", writeEpoch = 2L)
    }
    putBase(2000L, "same-timestamp-old-generation", writeEpoch = 1L) shouldBe 0L
    putBase(1000L, "old", writeEpoch = 3L) shouldBe 0L

    val stored = RedisBatchUpload.decodeValue(
      jedisCluster.get(baseKey.getBytes(StandardCharsets.UTF_8))
    ).get
    stored.storedTimestamp shouldBe 2000L
    new String(stored.payload, StandardCharsets.UTF_8) shouldBe "new"

    val missingKey = RedisKVStore.buildRedisKey("entity-missing".getBytes(StandardCharsets.UTF_8).toSeq,
                                                "INCREMENTAL_BATCH")
    val olderValue = RedisBatchUpload.encodeValue(
      RedisBatchUpload.Upsert,
      1000L,
      "older-but-missing".getBytes(StandardCharsets.UTF_8),
      1L)
    RedisBatchUpload.putBaseIfNewer(
      jedisCluster,
      missingKey.getBytes(StandardCharsets.UTF_8),
      List("1000".getBytes(StandardCharsets.UTF_8),
           "1".getBytes(StandardCharsets.UTF_8),
           olderValue,
           leaseDeadlineBytes())) shouldBe 1L
  }

  it should "replace a legacy value whose payload starts with an incomplete protocol marker" in {
    val dataset = "LEGACY_PROTOCOL_PREFIX_BATCH"
    val key = RedisKVStore.buildRedisKey("key".getBytes(StandardCharsets.UTF_8).toSeq, dataset)
      .getBytes(StandardCharsets.UTF_8)
    val timestamp = 1000L
    val legacyPayload = "CRB2".getBytes(StandardCharsets.UTF_8) ++
      Array[Byte](1, 3) ++ java.nio.ByteBuffer.allocate(8).putLong(99L).array()
    val legacyValue = java.nio.ByteBuffer.allocate(8 + legacyPayload.length).putLong(timestamp).put(legacyPayload).array()
    jedisCluster.set(key, legacyValue)

    val replacement = RedisBatchUpload.encodeValue(
      RedisBatchUpload.Upsert,
      timestamp,
      "replacement".getBytes(StandardCharsets.UTF_8),
      writeEpoch = 1L)
    RedisBatchUpload.putBaseIfNewer(
      jedisCluster,
      key,
      Seq(
        timestamp.toString.getBytes(StandardCharsets.UTF_8),
        "1".getBytes(StandardCharsets.UTF_8),
        replacement,
        leaseDeadlineBytes(),
        "0".getBytes(StandardCharsets.UTF_8)
      )) shouldBe 1L

    val decoded = RedisBatchUpload.decodeValue(jedisCluster.get(key)).get
    decoded.legacy shouldBe false
    new String(decoded.payload, StandardCharsets.UTF_8) shouldBe "replacement"
  }

  it should "enforce absolute upload deadlines at the Redis command boundary" in {
    val dataset = "EXPIRED_INCREMENTAL_BATCH"
    val key = RedisKVStore.buildRedisKey("entity-1".getBytes(StandardCharsets.UTF_8).toSeq, dataset)
    val encoded = RedisBatchUpload.encodeValue(
      RedisBatchUpload.Delete,
      batchTimestamp = 1000L,
      payload = Array.emptyByteArray,
      writeEpoch = 1L)
    val expiredDeadline = (System.currentTimeMillis() - 1000L).toString.getBytes(StandardCharsets.UTF_8)
    val mutationArgs = List(
      "1000".getBytes(StandardCharsets.UTF_8),
      "1".getBytes(StandardCharsets.UTF_8),
      encoded,
      expiredDeadline)

    val mutationError = the[redis.clients.jedis.exceptions.JedisDataException] thrownBy {
      RedisBatchUpload.putBaseIfNewer(jedisCluster, key.getBytes(StandardCharsets.UTF_8), mutationArgs)
    }
    mutationError.getMessage should include("mutation lease expired")
    jedisCluster.exists(key.getBytes(StandardCharsets.UTF_8)) shouldBe false

    RedisBatchUpload.putBaseIfNewer(
      jedisCluster,
      key.getBytes(StandardCharsets.UTF_8),
      mutationArgs :+ "1".getBytes(StandardCharsets.UTF_8)) shouldBe 2L
    jedisCluster.exists(key.getBytes(StandardCharsets.UTF_8)) shouldBe false

    val staleValue = RedisBatchUpload.encodeValue(
      RedisBatchUpload.Upsert,
      batchTimestamp = 999L,
      payload = "stale".getBytes(StandardCharsets.UTF_8),
      writeEpoch = 1L)
    jedisCluster.set(key.getBytes(StandardCharsets.UTF_8), staleValue)
    RedisBatchUpload.putBaseIfNewer(
      jedisCluster,
      key.getBytes(StandardCharsets.UTF_8),
      mutationArgs :+ "1".getBytes(StandardCharsets.UTF_8)) shouldBe 1L
    jedisCluster.exists(key.getBytes(StandardCharsets.UTF_8)) shouldBe false

    jedisCluster.set(key.getBytes(StandardCharsets.UTF_8), encoded)
    RedisBatchUpload.putBaseIfNewer(
      jedisCluster,
      key.getBytes(StandardCharsets.UTF_8),
      mutationArgs :+ "1".getBytes(StandardCharsets.UTF_8)) shouldBe 1L
    jedisCluster.exists(key.getBytes(StandardCharsets.UTF_8)) shouldBe false

    val status = RedisBatchUpload.BatchStatus("generation-1", "2024-07-27", 1000L, 1L)
    val statusError = the[redis.clients.jedis.exceptions.JedisDataException] thrownBy {
      RedisBatchUpload.writeStatusUntil(
        jedisCluster,
        RedisBatchUpload.buildStatusKey(dataset, "chronon"),
        status,
        System.currentTimeMillis() - 1000L)
    }
    statusError.getMessage should include("status lease expired")
  }

  it should "never shorten an existing upload lease on an exact replay" in {
    val dataset = "LEASE_EXTENSION_BATCH"
    val key = RedisKVStore.buildRedisKey("entity-1".getBytes(StandardCharsets.UTF_8).toSeq, dataset)
      .getBytes(StandardCharsets.UTF_8)
    val timestamp = 1000L
    val writeEpoch = 1L
    val encoded = RedisBatchUpload.encodeValue(
      RedisBatchUpload.Upsert,
      timestamp,
      "value".getBytes(StandardCharsets.UTF_8),
      writeEpoch)
    val laterDeadline = System.currentTimeMillis() + 300000L
    val earlierDeadline = laterDeadline - 120000L

    def mutationArgs(deadline: Long): List[Array[Byte]] =
      List(
        timestamp.toString.getBytes(StandardCharsets.UTF_8),
        writeEpoch.toString.getBytes(StandardCharsets.UTF_8),
        encoded,
        deadline.toString.getBytes(StandardCharsets.UTF_8))

    RedisBatchUpload.putBaseIfNewer(jedisCluster, key, mutationArgs(laterDeadline)) shouldBe 1L
    RedisBatchUpload.putBaseIfNewer(jedisCluster, key, mutationArgs(earlierDeadline)) shouldBe 2L
    jedisCluster.pttl(key) should be > 250000L

    RedisBatchUpload.validateAndExpire(
      jedisCluster,
      key,
      List(
        timestamp.toString.getBytes(StandardCharsets.UTF_8),
        writeEpoch.toString.getBytes(StandardCharsets.UTF_8),
        earlierDeadline.toString.getBytes(StandardCharsets.UTF_8))) shouldBe 1L
    jedisCluster.pttl(key) should be > 250000L

    val statusKey = RedisBatchUpload.buildStatusKey(dataset, "chronon")
    val status = RedisBatchUpload.BatchStatus("generation-1", "2024-07-27", timestamp, writeEpoch)
    RedisBatchUpload.writeStatusUntil(jedisCluster, statusKey, status, laterDeadline) shouldBe 1L
    RedisBatchUpload.writeStatusUntil(jedisCluster, statusKey, status, earlierDeadline) shouldBe 2L
    jedisCluster.pttl(statusKey.getBytes(StandardCharsets.UTF_8)) should be > 250000L
  }

  it should "compare an absolute expiration before advancing its lease fence" in {
    val key = "chronon:{EXPIRY_FENCE_BATCH:entity-1}".getBytes(StandardCharsets.UTF_8)
    val timestamp = 2000L
    val writeEpoch = 2L
    val previousDeadline = System.currentTimeMillis() + 180000L
    val nextDeadline = previousDeadline + 120000L
    val currentValue = RedisBatchUpload.encodeValue(
      RedisBatchUpload.Upsert,
      timestamp,
      "value".getBytes(StandardCharsets.UTF_8),
      writeEpoch)
    val olderValue = RedisBatchUpload.encodeValue(
      RedisBatchUpload.Upsert,
      timestamp - 1L,
      "older".getBytes(StandardCharsets.UTF_8),
      writeEpoch - 1L)
    def args(expectedDeadline: Long, desiredDeadline: Long): Seq[Array[Byte]] =
      Seq(expectedDeadline, desiredDeadline, timestamp, writeEpoch)
        .map(_.toString.getBytes(StandardCharsets.UTF_8))

    jedisCluster.set(key, currentValue) shouldBe "OK"
    jedisCluster.pexpireAt(key, previousDeadline) shouldBe 1L
    RedisBatchUpload.validateExpiryAndAdvance(
      jedisCluster,
      key,
      args(previousDeadline, nextDeadline)) shouldBe 1L
    jedisCluster.pexpireTime(key) shouldBe nextDeadline

    val aheadDeadline = nextDeadline + 60000L
    val advancedDeadline = aheadDeadline + 60000L
    jedisCluster.pexpireAt(key, aheadDeadline) shouldBe 1L
    RedisBatchUpload.validateExpiryAndAdvance(
      jedisCluster,
      key,
      args(nextDeadline, advancedDeadline)) shouldBe 1L
    jedisCluster.pexpireTime(key) shouldBe advancedDeadline

    val mismatchedDeadline = advancedDeadline + 60000L
    jedisCluster.set(key, olderValue) shouldBe "OK"
    jedisCluster.pexpireAt(key, mismatchedDeadline) shouldBe 1L
    RedisBatchUpload.validateExpiryAndAdvance(
      jedisCluster,
      key,
      args(advancedDeadline, mismatchedDeadline + 60000L)) shouldBe 0L
    jedisCluster.pexpireTime(key) shouldBe mismatchedDeadline

    val rolledBackDeadline = advancedDeadline - 60000L
    jedisCluster.set(key, currentValue) shouldBe "OK"
    jedisCluster.pexpireAt(key, rolledBackDeadline) shouldBe 1L
    RedisBatchUpload.validateExpiryAndAdvance(
      jedisCluster,
      key,
      args(advancedDeadline, advancedDeadline + 120000L)) shouldBe 0L
    jedisCluster.pexpireTime(key) shouldBe rolledBackDeadline

    jedisCluster.set(key, currentValue) shouldBe "OK"
    jedisCluster.pexpireTime(key) shouldBe -1L
    RedisBatchUpload.validateExpiryAndAdvance(
      jedisCluster,
      key,
      args(advancedDeadline, advancedDeadline + 120000L)) shouldBe 0L
    jedisCluster.pexpireTime(key) shouldBe -1L

    jedisCluster.del(key) shouldBe 1L
    RedisBatchUpload.validateExpiryAndAdvance(
      jedisCluster,
      key,
      args(advancedDeadline, advancedDeadline + 120000L)) shouldBe 0L
  }

  it should "fall back when Redis does not support client no-touch mode" in {
    RedisKVStoreFactory.supportsClientNoTouch(jedisCluster) shouldBe false
    jedisCluster.ping() shouldBe "PONG"
  }

  it should "decode a versioned direct batch value" in {
    val dataset = "BOOTSTRAP_BATCH"
    val key = "bootstrap-key".getBytes(StandardCharsets.UTF_8)
    val timestamp = 4000L
    val payload = "bootstrap-value".getBytes(StandardCharsets.UTF_8)
    val baseKey = RedisKVStore.buildRedisKey(key.toSeq, dataset)
    jedisCluster.set(
      baseKey.getBytes(StandardCharsets.UTF_8),
      RedisBatchUpload.encodeValue(RedisBatchUpload.Upsert, timestamp, payload)
    )

    val response = Await.result(
      new RedisKVStoreImpl(jedisCluster).multiGet(Seq(GetRequest(key, dataset, None, None))),
      10.seconds
    ).head
    val value = response.values.get.head
    value.bytes shouldBe payload
    value.millis shouldBe timestamp
  }

  it should "gate incremental reads on status and hide direct tombstones" in {
    val dataset = "INCREMENTAL_BATCH"
    val publishedTimestamp = 3000L
    val valueKeyBytes = "value".getBytes(StandardCharsets.UTF_8)
    val deletedKeyBytes = "deleted".getBytes(StandardCharsets.UTF_8)
    val missingKeyBytes = "missing".getBytes(StandardCharsets.UTF_8)
    val aheadKeyBytes = "ahead".getBytes(StandardCharsets.UTF_8)

    def baseKey(key: Array[Byte]): String = RedisKVStore.buildRedisKey(key.toSeq, dataset)
    def put(key: Array[Byte],
            operation: RedisBatchUpload.Operation,
            value: String = "",
            timestamp: Long = 1000L,
            writeEpoch: Long = 1L): Unit =
      jedisCluster.set(
        baseKey(key).getBytes(StandardCharsets.UTF_8),
        RedisBatchUpload.encodeValue(operation, timestamp, value.getBytes(StandardCharsets.UTF_8), writeEpoch)
      )

    put(valueKeyBytes, RedisBatchUpload.Upsert, "new-value")
    put(deletedKeyBytes, RedisBatchUpload.Delete)
    put(aheadKeyBytes, RedisBatchUpload.Upsert, "not-published", timestamp = 4000L, writeEpoch = 3L)
    val status = RedisBatchUpload.BatchStatus("generation-2", "2024-07-27-02", publishedTimestamp, 2L)
    RedisBatchUpload.writeStatus(jedisCluster, RedisBatchUpload.buildStatusKey(dataset, "chronon"), status)

    val requests = Seq(valueKeyBytes, deletedKeyBytes, missingKeyBytes, aheadKeyBytes)
      .map(key => GetRequest(key, dataset, None, None))
    val responses = Await.result(
      new RedisKVStoreImpl(jedisCluster, requireBatchStatus = true).multiGet(requests),
      10.seconds)
    val byKey = responses
      .map(response => new String(response.request.keyBytes, StandardCharsets.UTF_8) -> response)
      .toMap

    def assertValue(key: String, expected: String): Unit = {
      val values = byKey(key).values.get
      values should have size 1
      new String(values.head.bytes, StandardCharsets.UTF_8) shouldBe expected
      values.head.millis shouldBe publishedTimestamp
    }

    assertValue("value", "new-value")
    byKey("deleted").values.get shouldBe empty
    byKey("missing").values.get shouldBe empty
    byKey("ahead").values.isFailure shouldBe true

    val cachedStore = new RedisKVStoreImpl(jedisCluster, requireBatchStatus = true)
    Await.result(cachedStore.multiGet(requests.take(1)), 10.seconds).head.values.get should have size 1
    jedisCluster.unlink(RedisBatchUpload.buildStatusKey(dataset, "chronon").getBytes(StandardCharsets.UTF_8)) shouldBe 1L
    Await.result(cachedStore.multiGet(requests.take(1)), 10.seconds).head.values.get should have size 1
    RedisBatchUpload.writeStatus(jedisCluster, RedisBatchUpload.buildStatusKey(dataset, "chronon"), status) shouldBe 1L

    val missingStatusDataset = "NO_STATUS_BATCH"
    val missingStatusRequest = GetRequest("key".getBytes(StandardCharsets.UTF_8), missingStatusDataset, None, None)
    val missingStatusStore = new RedisKVStoreImpl(jedisCluster, requireBatchStatus = true)
    val missingStatus = Await.result(missingStatusStore.multiGet(Seq(missingStatusRequest)), 10.seconds)
    missingStatus.head.values.isFailure shouldBe true
    RedisBatchUpload.writeStatus(
      jedisCluster,
      RedisBatchUpload.buildStatusKey(missingStatusDataset, "chronon"),
      status.copy(generation = "recovered-generation")) shouldBe 1L
    Thread.sleep(RedisKVStore.MissingBatchStatusCacheMillis + 50L)
    Await.result(missingStatusStore.multiGet(Seq(missingStatusRequest)), 10.seconds).head.values.get shouldBe empty

    val metadataDataset = "CHRONON_METADATA"
    val metadataKey = "metadata-key".getBytes(StandardCharsets.UTF_8)
    val metadataRedisKey = RedisKVStore.buildRedisKey(metadataKey.toSeq, metadataDataset)
    jedisCluster.set(
      metadataRedisKey.getBytes(StandardCharsets.UTF_8),
      RedisBatchUpload.encodeValue(RedisBatchUpload.Upsert,
                                   4000L,
                                   "metadata-value".getBytes(StandardCharsets.UTF_8),
                                   1L))
    val metadataRead = Await.result(
      new RedisKVStoreImpl(jedisCluster, requireBatchStatus = true)
        .multiGet(Seq(GetRequest(metadataKey, metadataDataset, None, None))),
      10.seconds)
    new String(metadataRead.head.values.get.head.bytes, StandardCharsets.UTF_8) shouldBe "metadata-value"
    metadataRead.head.values.get.head.millis shouldBe 4000L

    val retired = status.copy(generation = "generation-3", writeEpoch = 3L, retired = true)
    RedisBatchUpload.writeStatus(jedisCluster, RedisBatchUpload.buildStatusKey(dataset, "chronon"), retired)
    val retiredRead = Await.result(
      new RedisKVStoreImpl(jedisCluster, requireBatchStatus = true).multiGet(requests.take(1)),
      10.seconds)
    retiredRead.head.values.get shouldBe empty
  }

  it should "refresh a stale local status when a completed publication is already visible" in {
    val dataset = "STATUS_REFRESH_BATCH"
    val key = "key".getBytes(StandardCharsets.UTF_8)
    val redisKey = RedisKVStore.buildRedisKey(key.toSeq, dataset)
    val statusKey = RedisBatchUpload.buildStatusKey(dataset, "chronon")
    val initial = RedisBatchUpload.BatchStatus("generation-1", "2024-07-27", 1000L, 1L)
    val replacement = RedisBatchUpload.BatchStatus("generation-2", "2024-07-28", 2000L, 2L)
    val store = new RedisKVStoreImpl(jedisCluster, requireBatchStatus = true)
    val request = GetRequest(key, dataset, None, None)

    jedisCluster.set(
      redisKey.getBytes(StandardCharsets.UTF_8),
      RedisBatchUpload.encodeValue(RedisBatchUpload.Upsert,
                                   initial.batchTimestamp,
                                   "old".getBytes(StandardCharsets.UTF_8),
                                   initial.writeEpoch))
    RedisBatchUpload.writeStatus(jedisCluster, statusKey, initial) shouldBe 1L
    new String(Await.result(store.multiGet(Seq(request)), 10.seconds).head.values.get.head.bytes,
               StandardCharsets.UTF_8) shouldBe "old"

    jedisCluster.set(
      redisKey.getBytes(StandardCharsets.UTF_8),
      RedisBatchUpload.encodeValue(RedisBatchUpload.Upsert,
                                   replacement.batchTimestamp,
                                   "new".getBytes(StandardCharsets.UTF_8),
                                   replacement.writeEpoch))
    RedisBatchUpload.writeStatus(jedisCluster, statusKey, replacement) shouldBe 1L

    val refreshed = Await.result(store.multiGet(Seq(request)), 10.seconds).head.values.get.head
    new String(refreshed.bytes, StandardCharsets.UTF_8) shouldBe "new"
    refreshed.millis shouldBe replacement.batchTimestamp
  }

  it should "publish and materialize incremental generations end to end" in {
    val container = redisFixture.container
    val host = container.getHost
    val port = container.getMappedPort(7000)
    val internalPorts =
      7000 until (7000 + redisFixture.primaryCount * (redisFixture.replicasPerPrimary + 1))
    val spark = RedisSparkSessionTestHelper.acquire("RedisIncrementalUploadTest")
    val sourceTable = s"redis_incremental_${System.nanoTime()}"
    val destinationDataset = "incremental_e2e"
    val partitionSpec = PartitionSpec.daily
    val advertisedPortMappings = internalPorts.map { internalPort =>
      internalPort -> RedisKVStoreFactory.RedisEndpoint(host, container.getMappedPort(internalPort))
    }.toMap
    val clientSettings = RedisKVStoreFactory
      .settings(Map("redis.cluster.nodes" -> s"$host:$port"), env = Map.empty)
      .copy(advertisedPortMappings = advertisedPortMappings)
    val settings = Spark2RedisLoader.Settings(
      sourceTable = sourceTable,
      destinationDataset = destinationDataset,
      sourcePartition = "2024-07-27",
      partitionSpec = partitionSpec,
      stateRoot = Files.createTempDirectory("redis-incremental-state").toUri.toString,
      keyPrefix = "chronon",
      clientSettings = clientSettings,
      keyHashTagMode = RedisKVStore.EntityHashTagMode,
      batchSize = 10,
      maxKeysPerSecond = 100000,
      writerPartitions = 1,
      stateBuckets = 4,
      liveTTLSeconds = 3600
    )
    val batchDataset = settings.batchDataset
    val statusKey = RedisBatchUpload.buildStatusKey(batchDataset, settings.keyPrefix)

    def rows(partition: String, values: (String, String)*): org.apache.spark.sql.DataFrame = {
      import spark.implicits._
      (values.map { case (key, value) =>
        (key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), partition)
      } :+ (GroupByServingInfoKey.getBytes(StandardCharsets.UTF_8),
            s"metadata-$partition".getBytes(StandardCharsets.UTF_8),
            partition)).toDF("key_bytes", "value_bytes", "ds")
    }

    def read(keys: String*): Map[String, Seq[TimedValue]] = {
      val requests = keys.map(key => GetRequest(key.getBytes(StandardCharsets.UTF_8), batchDataset, None, None))
      Await
        .result(
          new RedisKVStoreImpl(
            jedisCluster,
            Map("redis.key.hash.tag.mode" -> settings.keyHashTagMode),
            requireBatchStatus = true
          ).multiGet(requests),
          10.seconds)
        .map(response => new String(response.request.keyBytes, StandardCharsets.UTF_8) -> response.values.get)
        .toMap
    }

    def replaceRows(partition: String, values: (String, String)*): Unit = {
      val view = s"redis_replacement_${System.nanoTime()}"
      rows(partition, values: _*).drop("ds").createOrReplaceTempView(view)
      try {
        spark.sql(
          s"INSERT OVERWRITE TABLE $sourceTable PARTITION (ds='$partition') SELECT key_bytes, value_bytes FROM $view")
      } finally {
        spark.catalog.dropTempView(view)
      }
    }

    def deleteLease(generation: String): Unit = {
      val path = Spark2RedisLoader.leaseCheckpointPath(settings, generation)
      path.getFileSystem(spark.sparkContext.hadoopConfiguration).delete(path, false) shouldBe true
    }

    try {
      rows("2024-07-27", "a" -> "a-1", "b" -> "b-1", "d" -> "d-1")
        .write
        .mode("overwrite")
        .partitionBy("ds")
        .saveAsTable(sourceTable)
      implicit val executionContext: ExecutionContext = ExecutionContext.global
      val contendersReady = new CountDownLatch(2)
      val startContenders = new CountDownLatch(1)
      val contenders = Seq.fill(2)(Future {
        contendersReady.countDown()
        startContenders.await(10, TimeUnit.SECONDS) shouldBe true
        Spark2RedisLoader.run(settings, spark, jedisCluster)
      })
      contendersReady.await(10, TimeUnit.SECONDS) shouldBe true
      startContenders.countDown()
      val initialRuns = Await.result(Future.sequence(contenders), 2.minutes)
      initialRuns.flatMap(_.generation).distinct should have size 1
      initialRuns.map(_.changedKeys).sum shouldBe 3L
      val initial = initialRuns.maxBy(_.changedKeys)
      initial.changedKeys shouldBe 3L
      initial.deletedKeys shouldBe 0L
      initial.superseded shouldBe false
      initial.fullRebuild shouldBe true
      val initialStatus = RedisBatchUpload
        .readStatus(jedisCluster, statusKey)
        .get
      initialStatus.generation shouldBe initial.generation.get
      jedisCluster.ttl(statusKey.getBytes(StandardCharsets.UTF_8)) should (be > 0L and be <= 3600L)

      val incompatibleHashMode = the[IllegalArgumentException] thrownBy {
        Spark2RedisLoader.run(
          settings.copy(keyHashTagMode = RedisKVStore.DatasetAndEntityHashTagMode),
          spark,
          jedisCluster)
      }
      incompatibleHashMode.getMessage should include("key hash tag mode cannot change within a lineage")

      val aRedisKey = RedisKVStore.buildRedisKey(
        "a".getBytes(StandardCharsets.UTF_8).toSeq,
        batchDataset,
        keyPrefix = settings.keyPrefix,
        hashTagMode = settings.keyHashTagMode)
      jedisCluster.expire(aRedisKey.getBytes(StandardCharsets.UTF_8), 30L) shouldBe 1L

      val checkedInitial = Spark2RedisLoader.run(settings, spark, jedisCluster)
      checkedInitial.generation shouldBe initial.generation
      checkedInitial.refreshedKeys shouldBe 3L
      jedisCluster.ttl(aRedisKey.getBytes(StandardCharsets.UTF_8)) should be > 3500L

      // Existing generations have no _LEASE record. Removing it simulates one and must trigger one conservative
      // validation pass before the generation adopts checkpoint-based renewal.
      deleteLease(initial.generation.get)
      val refreshedInitial = Spark2RedisLoader.run(settings, spark, jedisCluster)
      refreshedInitial.generation shouldBe initial.generation
      refreshedInitial.refreshedKeys shouldBe 3L
      jedisCluster.ttl(aRedisKey.getBytes(StandardCharsets.UTF_8)) should be > 3500L

      rows("2024-07-28", "a" -> "a-1", "b" -> "b-2", "c" -> "c-1")
        .write
        .mode("append")
        .insertInto(sourceTable)
      // A new loader must also build a child directly from an older parent with no lease checkpoint.
      deleteLease(initial.generation.get)
      val second = Spark2RedisLoader.run(settings.copy(sourcePartition = "2024-07-28"), spark, jedisCluster)
      second.changedKeys shouldBe 2L
      second.deletedKeys shouldBe 1L
      second.refreshedKeys shouldBe 1L

      val secondValues = read("a", "b", "c", "d")
      new String(secondValues("a").head.bytes, StandardCharsets.UTF_8) shouldBe "a-1"
      new String(secondValues("b").head.bytes, StandardCharsets.UTF_8) shouldBe "b-2"
      new String(secondValues("c").head.bytes, StandardCharsets.UTF_8) shouldBe "c-1"
      secondValues("d") shouldBe empty

      val bRedisKey = RedisKVStore.buildRedisKey(
        "b".getBytes(StandardCharsets.UTF_8).toSeq,
        batchDataset,
        keyPrefix = settings.keyPrefix,
        hashTagMode = settings.keyHashTagMode)
      jedisCluster.set(
        bRedisKey.getBytes(StandardCharsets.UTF_8),
        RedisBatchUpload.encodeValue(
          RedisBatchUpload.Upsert,
          settings.batchTimestamp,
          "b-1".getBytes(StandardCharsets.UTF_8),
          writeEpoch = 1L))
      deleteLease(second.generation.get)
      val staleValueRepair =
        Spark2RedisLoader.run(settings.copy(sourcePartition = "2024-07-28"), spark, jedisCluster)
      staleValueRepair.fullRebuild shouldBe true
      new String(read("b")("b").head.bytes, StandardCharsets.UTF_8) shouldBe "b-2"

      replaceRows("2024-07-28", "a" -> "a-1", "b" -> "b-2-corrected", "c" -> "c-1")
      val samePartitionCorrection =
        Spark2RedisLoader.run(settings.copy(sourcePartition = "2024-07-28"), spark, jedisCluster)
      samePartitionCorrection.changedKeys shouldBe 1L
      samePartitionCorrection.deletedKeys shouldBe 0L
      new String(read("b")("b").head.bytes, StandardCharsets.UTF_8) shouldBe "b-2-corrected"

      val dRedisKey = RedisKVStore.buildRedisKey(
        "d".getBytes(StandardCharsets.UTF_8).toSeq,
        batchDataset,
        keyPrefix = settings.keyPrefix,
        hashTagMode = settings.keyHashTagMode)
      RedisBatchUpload
        .decodeValue(jedisCluster.get(dRedisKey.getBytes(StandardCharsets.UTF_8)))
        .get
        .operation shouldBe RedisBatchUpload.Delete
      jedisCluster.ttl(dRedisKey.getBytes(StandardCharsets.UTF_8)) should (be > 0L and be <= 3600L)

      // Simulate restoring Redis to a snapshot from before d was deleted. Recovery from the current S3 head must
      // replay retained tombstones, not just the current active values, and must not need the mutable source table.
      jedisCluster.set(
        dRedisKey.getBytes(StandardCharsets.UTF_8),
        RedisBatchUpload.encodeValue(
          RedisBatchUpload.Upsert,
          settings.batchTimestamp,
          "d-1".getBytes(StandardCharsets.UTF_8),
          writeEpoch = 1L))
      deleteLease(samePartitionCorrection.generation.get)
      jedisCluster.unlink(statusKey.getBytes(StandardCharsets.UTF_8)) shouldBe 1L
      spark.sql(s"DROP TABLE $sourceTable")
      val statusLossRepair =
        Spark2RedisLoader.run(settings.copy(sourcePartition = "2024-07-28"), spark, jedisCluster)
      statusLossRepair.generation shouldBe samePartitionCorrection.generation
      statusLossRepair.superseded shouldBe false
      statusLossRepair.fullRebuild shouldBe true
      statusLossRepair.refreshedKeys shouldBe 0L
      RedisBatchUpload.readStatus(jedisCluster, statusKey).get.generation shouldBe samePartitionCorrection.generation.get
      val afterStaleRepair = read("a", "b", "c", "d")
      new String(afterStaleRepair("b").head.bytes, StandardCharsets.UTF_8) shouldBe "b-2-corrected"
      afterStaleRepair("d") shouldBe empty
      RedisBatchUpload.decodeValue(jedisCluster.get(dRedisKey.getBytes(StandardCharsets.UTF_8))).get.operation shouldBe
        RedisBatchUpload.Delete

      rows("2024-07-28", "a" -> "a-1", "b" -> "b-2-corrected", "c" -> "c-1")
        .write
        .mode("overwrite")
        .partitionBy("ds")
        .saveAsTable(sourceTable)

      deleteLease(samePartitionCorrection.generation.get)
      jedisCluster.unlink(aRedisKey.getBytes(StandardCharsets.UTF_8))
      Spark2RedisLoader.run(settings, spark, jedisCluster)
      new String(read("a")("a").head.bytes, StandardCharsets.UTF_8) shouldBe "a-1"

      deleteLease(samePartitionCorrection.generation.get)
      jedisCluster.unlink(bRedisKey.getBytes(StandardCharsets.UTF_8))
      val repairedLatest = Spark2RedisLoader.run(settings, spark, jedisCluster)
      repairedLatest.fullRebuild shouldBe true
      new String(read("b")("b").head.bytes, StandardCharsets.UTF_8) shouldBe "b-2-corrected"

      rows("2024-07-29", "a" -> "a-1", "b" -> "b-3", "c" -> "c-1")
        .write
        .mode("append")
        .insertInto(sourceTable)

      var droppedHeadResponse = false
      val lostHeadResponse = new ConditionalObjectWriter {
        override def read(path: org.apache.hadoop.fs.Path,
                          hadoopConf: org.apache.hadoop.conf.Configuration): Option[VersionedBytes] =
          HadoopConditionalObjectWriter.read(path, hadoopConf)

        override def putIfAbsent(path: org.apache.hadoop.fs.Path,
                                 contents: Array[Byte],
                                 hadoopConf: org.apache.hadoop.conf.Configuration): VersionedBytes =
          HadoopConditionalObjectWriter.putIfAbsent(path, contents, hadoopConf)

        override def compareAndSet(path: org.apache.hadoop.fs.Path,
                                   expectedVersion: String,
                                   contents: Array[Byte],
                                   hadoopConf: org.apache.hadoop.conf.Configuration): VersionedBytes = {
          val result = HadoopConditionalObjectWriter.compareAndSet(path, expectedVersion, contents, hadoopConf)
          if (!droppedHeadResponse && path.getName == "_HEAD") {
            droppedHeadResponse = true
            throw new RuntimeException("simulated lost head response")
          }
          result
        }
      }
      val interrupted = the[RuntimeException] thrownBy {
        Spark2RedisLoader.run(
          settings.copy(sourcePartition = "2024-07-29"),
          spark,
          jedisCluster,
          lostHeadResponse)
      }
      interrupted.getMessage shouldBe "simulated lost head response"

      // Recovery must use the immutable S3 state for the admitted generation even if the source partition is corrected
      // before the retry. The same invocation can then publish the correction as a newer generation.
      replaceRows("2024-07-29", "a" -> "a-1", "b" -> "b-4", "c" -> "c-1")

      val thirdTimestamp = settings.copy(sourcePartition = "2024-07-29").batchTimestamp
      val partialValue = RedisBatchUpload.encodeValue(
        RedisBatchUpload.Upsert,
          thirdTimestamp,
          "b-3".getBytes(StandardCharsets.UTF_8),
          writeEpoch = 4L)
      RedisBatchUpload.putBaseIfNewer(
        jedisCluster,
        bRedisKey.getBytes(StandardCharsets.UTF_8),
        List(
          thirdTimestamp.toString.getBytes(StandardCharsets.UTF_8),
          "4".getBytes(StandardCharsets.UTF_8),
          partialValue,
          leaseDeadlineBytes())) shouldBe 1L

      val third = Spark2RedisLoader.run(settings.copy(sourcePartition = "2024-07-29"), spark, jedisCluster)
      third.changedKeys shouldBe 1L
      third.deletedKeys shouldBe 0L
      third.refreshedKeys shouldBe 2L
      val thirdValues = read("a", "b", "c", "d")
      new String(thirdValues("b").head.bytes, StandardCharsets.UTF_8) shouldBe "b-4"
      thirdValues("d") shouldBe empty

      rows("2024-07-30")
        .write
        .mode("append")
        .insertInto(sourceTable)
      val emptySnapshot = the[IllegalArgumentException] thrownBy {
        Spark2RedisLoader.run(settings.copy(sourcePartition = "2024-07-30"), spark, jedisCluster)
      }
      emptySnapshot.getMessage should include("contains no data keys")
      new String(read("a")("a").head.bytes, StandardCharsets.UTF_8) shouldBe "a-1"

      deleteLease(third.generation.get)
      jedisCluster.unlink(aRedisKey.getBytes(StandardCharsets.UTF_8)) shouldBe 1L
      val olderAfterEmpty = Spark2RedisLoader.run(
        settings.copy(sourcePartition = "2024-07-29"),
        spark,
        jedisCluster)
      olderAfterEmpty.superseded shouldBe false
      olderAfterEmpty.fullRebuild shouldBe true
      RedisBatchUpload.decodeValue(jedisCluster.get(aRedisKey.getBytes(StandardCharsets.UTF_8))).get.operation shouldBe
        RedisBatchUpload.Upsert
      new String(read("a")("a").head.bytes, StandardCharsets.UTF_8) shouldBe "a-1"

      val shortSettings = settings.copy(sourcePartition = "2024-07-29", liveTTLSeconds = 1800)
      val transition = Spark2RedisLoader.run(shortSettings, spark, jedisCluster)
      transition.generation should not equal olderAfterEmpty.generation
      transition.changedKeys shouldBe 0L
      transition.deletedKeys shouldBe 0L
      transition.refreshedKeys shouldBe 3L
      jedisCluster.ttl(statusKey.getBytes(StandardCharsets.UTF_8)) should (be > 0L and be <= 1800L)
      jedisCluster.ttl(aRedisKey.getBytes(StandardCharsets.UTF_8)) should be > 3000L

      replaceRows("2024-07-29", "a" -> "a-1", "b" -> "b-short")
      val shortDelta = Spark2RedisLoader.run(shortSettings, spark, jedisCluster)
      shortDelta.changedKeys shouldBe 1L
      shortDelta.deletedKeys shouldBe 1L
      shortDelta.refreshedKeys shouldBe 1L
      val cRedisKey = RedisKVStore.buildRedisKey(
        "c".getBytes(StandardCharsets.UTF_8).toSeq,
        batchDataset,
        keyPrefix = settings.keyPrefix,
        hashTagMode = settings.keyHashTagMode)
      jedisCluster.ttl(bRedisKey.getBytes(StandardCharsets.UTF_8)) should (be > 0L and be <= 1800L)
      jedisCluster.ttl(cRedisKey.getBytes(StandardCharsets.UTF_8)) should (be > 0L and be <= 1800L)
      RedisBatchUpload.decodeValue(jedisCluster.get(cRedisKey.getBytes(StandardCharsets.UTF_8))).get.operation shouldBe
        RedisBatchUpload.Delete
      read("c")("c") shouldBe empty
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
      RedisSparkSessionTestHelper.release()
    }
  }

  it should "retire lower GroupBy versions after durable admission and before applying the new version" in {
    val container = redisFixture.container
    val host = container.getHost
    val port = container.getMappedPort(7000)
    val internalPorts =
      7000 until (7000 + redisFixture.primaryCount * (redisFixture.replicasPerPrimary + 1))
    val spark = RedisSparkSessionTestHelper.acquire("RedisOlderVersionRetirementTest")
    val suffix = System.nanoTime()
    val sourceBase = s"redis_retirement_$suffix"
    val destinationBase = s"retirement_e2e_$suffix"
    val stateRoot = Files.createTempDirectory("redis-retirement-state").toUri.toString
    val partition = "2024-07-27"
    val advertisedPortMappings = internalPorts.map { internalPort =>
      internalPort -> RedisKVStoreFactory.RedisEndpoint(host, container.getMappedPort(internalPort))
    }.toMap
    val clientSettings = RedisKVStoreFactory
      .settings(Map("redis.cluster.nodes" -> s"$host:$port"), env = Map.empty)
      .copy(advertisedPortMappings = advertisedPortMappings)

    def settings(version: Int, deleteOlderVersions: Boolean): Spark2RedisLoader.Settings =
      Spark2RedisLoader.Settings(
        sourceTable = s"${sourceBase}__${version}__upload",
        destinationDataset = s"${destinationBase}__${version}",
        sourcePartition = partition,
        partitionSpec = PartitionSpec.daily,
        stateRoot = stateRoot,
        keyPrefix = "chronon",
        clientSettings = clientSettings,
        batchSize = 10,
        maxKeysPerSecond = 100000,
        writerPartitions = 1,
        stateBuckets = 4,
        liveTTLSeconds = 3600,
        deleteOlderVersions = deleteOlderVersions
      )

    def rows(values: (String, String)*): org.apache.spark.sql.DataFrame = {
      import spark.implicits._
      (values.map { case (key, value) =>
        (key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8), partition)
      } :+ (GroupByServingInfoKey.getBytes(StandardCharsets.UTF_8),
            "metadata".getBytes(StandardCharsets.UTF_8),
            partition)).toDF("key_bytes", "value_bytes", "ds")
    }

    val v1 = settings(version = 1, deleteOlderVersions = false)
    val v2 = settings(version = 2, deleteOlderVersions = true)
    try {
      rows("a" -> "a-v1", "b" -> "b-v1")
        .write
        .mode("overwrite")
        .partitionBy("ds")
        .saveAsTable(v1.sourceTable)
      rows("b" -> "b-v2", "c" -> "c-v2")
        .write
        .mode("overwrite")
        .partitionBy("ds")
        .saveAsTable(v2.sourceTable)

      val v1Upload = Spark2RedisLoader.run(v1, spark, jedisCluster)
      v1Upload.changedKeys shouldBe 2L
      rows("a" -> "a-v1")
        .write
        .mode("overwrite")
        .partitionBy("ds")
        .saveAsTable(v1.sourceTable)
      val correctedV1Upload = Spark2RedisLoader.run(v1, spark, jedisCluster)
      correctedV1Upload.changedKeys shouldBe 0L
      correctedV1Upload.deletedKeys shouldBe 1L
      val v1StatusKey = RedisBatchUpload.buildStatusKey(v1.batchDataset, v1.keyPrefix)
      val v2StatusKey = RedisBatchUpload.buildStatusKey(v2.batchDataset, v2.keyPrefix)
      val v1Keys = Seq("a", "b", GroupByServingInfoKey).map { key =>
        RedisKVStore.buildRedisKey(key.getBytes(StandardCharsets.UTF_8).toSeq, v1.batchDataset)
      }
      val v2Keys = Seq("b", "c", GroupByServingInfoKey).map { key =>
        RedisKVStore.buildRedisKey(key.getBytes(StandardCharsets.UTF_8).toSeq, v2.batchDataset)
      }
      val activeV1Status = RedisBatchUpload.readStatus(jedisCluster, v1StatusKey).get
      val preRetirementValues = v1Keys.map { key =>
        key -> jedisCluster.get(key.getBytes(StandardCharsets.UTF_8))
      }

      def assertV1Untouched(): Unit = {
        RedisBatchUpload.readStatus(jedisCluster, v1StatusKey).get shouldBe activeV1Status
        preRetirementValues.foreach { case (key, expected) =>
          Option(jedisCluster.get(key.getBytes(StandardCharsets.UTF_8))).map(_.toSeq) shouldBe
            Option(expected).map(_.toSeq)
        }
        v2Keys.foreach(key => jedisCluster.get(key.getBytes(StandardCharsets.UTF_8)) shouldBe null)
        RedisBatchUpload.readStatus(jedisCluster, v2StatusKey) shouldBe None
      }

      val v2StateRoot = new org.apache.hadoop.fs.Path(
        new org.apache.hadoop.fs.Path(new org.apache.hadoop.fs.Path(stateRoot), v2.sourceTable),
        v2.batchDataset)
      val stateFs = v2StateRoot.getFileSystem(spark.sparkContext.hadoopConfiguration)

      rows("duplicate" -> "first", "duplicate" -> "second")
        .write
        .mode("overwrite")
        .partitionBy("ds")
        .saveAsTable(v2.sourceTable)
      val duplicate = the[IllegalArgumentException] thrownBy Spark2RedisLoader.run(v2, spark, jedisCluster)
      duplicate.getMessage should include("duplicate key_bytes")
      stateFs.exists(v2StateRoot) shouldBe false
      assertV1Untouched()

      rows()
        .write
        .mode("overwrite")
        .partitionBy("ds")
        .saveAsTable(v2.sourceTable)
      val empty = the[IllegalArgumentException] thrownBy Spark2RedisLoader.run(v2, spark, jedisCluster)
      empty.getMessage should include("contains no data keys")
      stateFs.exists(v2StateRoot) shouldBe false
      assertV1Untouched()

      rows("b" -> "b-v2", "c" -> "c-v2")
        .write
        .mode("overwrite")
        .partitionBy("ds")
        .saveAsTable(v2.sourceTable)

      def belongsTo(path: org.apache.hadoop.fs.Path, dataset: String): Boolean =
        path.toUri.getPath.split('/').contains(dataset)

      var lostV2HeadResponse = false
      val lostHeadWriter = new ConditionalObjectWriter {
        override def read(path: org.apache.hadoop.fs.Path,
                          hadoopConf: org.apache.hadoop.conf.Configuration): Option[VersionedBytes] =
          HadoopConditionalObjectWriter.read(path, hadoopConf)

        override def putIfAbsent(path: org.apache.hadoop.fs.Path,
                                 contents: Array[Byte],
                                 hadoopConf: org.apache.hadoop.conf.Configuration): VersionedBytes = {
          val result = HadoopConditionalObjectWriter.putIfAbsent(path, contents, hadoopConf)
          if (!lostV2HeadResponse && path.getName == "_HEAD" && belongsTo(path, v2.batchDataset)) {
            lostV2HeadResponse = true
            throw new RuntimeException("simulated lost v2 head response")
          }
          result
        }

        override def compareAndSet(path: org.apache.hadoop.fs.Path,
                                   expectedVersion: String,
                                   contents: Array[Byte],
                                   hadoopConf: org.apache.hadoop.conf.Configuration): VersionedBytes =
          HadoopConditionalObjectWriter.compareAndSet(path, expectedVersion, contents, hadoopConf)
      }

      val interrupted = the[RuntimeException] thrownBy {
        Spark2RedisLoader.run(v2, spark, jedisCluster, lostHeadWriter)
      }
      interrupted.getMessage shouldBe "simulated lost v2 head response"
      lostV2HeadResponse shouldBe true
      assertV1Untouched()

      val v2HeadPath = new org.apache.hadoop.fs.Path(v2StateRoot, "_HEAD")
      val v2ReadyGlob = new org.apache.hadoop.fs.Path(
        new org.apache.hadoop.fs.Path(v2StateRoot, "generations"),
        "*/_READY")
      val v2AppliedGlob = new org.apache.hadoop.fs.Path(
        new org.apache.hadoop.fs.Path(v2StateRoot, "generations"),
        "*/_APPLIED")
      stateFs.exists(v2HeadPath) shouldBe true
      Option(stateFs.globStatus(v2ReadyGlob)).fold(0)(_.length) shouldBe 1
      Option(stateFs.globStatus(v2AppliedGlob)).fold(0)(_.length) shouldBe 0

      val appliedOrder = ArrayBuffer.empty[String]
      val orderingWriter = new ConditionalObjectWriter {
        override def read(path: org.apache.hadoop.fs.Path,
                          hadoopConf: org.apache.hadoop.conf.Configuration): Option[VersionedBytes] =
          HadoopConditionalObjectWriter.read(path, hadoopConf)

        override def putIfAbsent(path: org.apache.hadoop.fs.Path,
                                 contents: Array[Byte],
                                 hadoopConf: org.apache.hadoop.conf.Configuration): VersionedBytes = {
          val result = HadoopConditionalObjectWriter.putIfAbsent(path, contents, hadoopConf)
          if (path.getName == "_APPLIED" && belongsTo(path, v1.batchDataset)) {
            stateFs.exists(v2HeadPath) shouldBe true
            Option(stateFs.globStatus(v2ReadyGlob)).fold(0)(_.length) shouldBe 1
            v1Keys.foreach(key => jedisCluster.get(key.getBytes(StandardCharsets.UTF_8)) shouldBe null)
            v2Keys.foreach(key => jedisCluster.get(key.getBytes(StandardCharsets.UTF_8)) shouldBe null)
            RedisBatchUpload.readStatus(jedisCluster, v2StatusKey) shouldBe None
            appliedOrder += v1.batchDataset
          } else if (path.getName == "_APPLIED" && belongsTo(path, v2.batchDataset)) {
            v2Keys.foreach(key => jedisCluster.get(key.getBytes(StandardCharsets.UTF_8)) should not be null)
            RedisBatchUpload.readStatus(jedisCluster, v2StatusKey) should not be empty
            appliedOrder += v2.batchDataset
          }
          result
        }

        override def compareAndSet(path: org.apache.hadoop.fs.Path,
                                   expectedVersion: String,
                                   contents: Array[Byte],
                                   hadoopConf: org.apache.hadoop.conf.Configuration): VersionedBytes =
          HadoopConditionalObjectWriter.compareAndSet(path, expectedVersion, contents, hadoopConf)
      }

      val v2Upload = Spark2RedisLoader.run(v2, spark, jedisCluster, orderingWriter)
      // The retry applies the already-admitted generation while resolving its head, then reports the unchanged-source
      // follow-up from the same invocation.
      v2Upload.changedKeys shouldBe 0L
      appliedOrder shouldBe Seq(v1.batchDataset, v2.batchDataset)
      Option(stateFs.globStatus(v2AppliedGlob)).fold(0)(_.length) shouldBe 1

      val retiredStatus = RedisBatchUpload.readStatus(jedisCluster, v1StatusKey).get
      retiredStatus.retired shouldBe true
      jedisCluster.ttl(v1StatusKey.getBytes(StandardCharsets.UTF_8)) shouldBe -1L

      v1Keys.foreach { key =>
        jedisCluster.get(key.getBytes(StandardCharsets.UTF_8)) shouldBe null
      }

      Seq("b" -> "b-v2", "c" -> "c-v2").foreach { case (key, expected) =>
        val redisKey = RedisKVStore.buildRedisKey(key.getBytes(StandardCharsets.UTF_8).toSeq, v2.batchDataset)
        val decoded = RedisBatchUpload.decodeValue(jedisCluster.get(redisKey.getBytes(StandardCharsets.UTF_8))).get
        decoded.operation shouldBe RedisBatchUpload.Upsert
        new String(decoded.payload, StandardCharsets.UTF_8) shouldBe expected
      }

      val missingKeyTtlBefore = jedisCluster.pttl(v1Keys.head.getBytes(StandardCharsets.UTF_8))
      Spark2RedisLoader.run(v2, spark, jedisCluster)
      val rerunStatus = RedisBatchUpload.readStatus(jedisCluster, v1StatusKey).get
      rerunStatus shouldBe retiredStatus
      missingKeyTtlBefore shouldBe -2L
      v1Keys.foreach { key =>
        jedisCluster.get(key.getBytes(StandardCharsets.UTF_8)) shouldBe null
      }

      // Simulate a Redis restore from before v1 was retired. The durable retired S3 head must replay its deletes
      // rather than merely recreating the status marker and leaving the restored values live.
      preRetirementValues.foreach { case (key, value) =>
        jedisCluster.set(key.getBytes(StandardCharsets.UTF_8), value)
      }
      jedisCluster.del(v1StatusKey.getBytes(StandardCharsets.UTF_8))
      Spark2RedisLoader.run(v2, spark, jedisCluster)
      RedisBatchUpload.readStatus(jedisCluster, v1StatusKey).get shouldBe retiredStatus
      v1Keys.foreach { key =>
        jedisCluster.get(key.getBytes(StandardCharsets.UTF_8)) shouldBe null
      }
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS ${v1.sourceTable}")
      spark.sql(s"DROP TABLE IF EXISTS ${v2.sourceTable}")
      RedisSparkSessionTestHelper.release()
    }
  }

  it should "bulk put batch IR data from Spark" in {
    // Note: bulkPut appends "_BATCH" suffix via GroupBy.batchDataset (same as BigTable)
    val dataset = "TEST_GROUPBY"
    val batchDataset = s"${dataset}_BATCH"  // The actual dataset name after transformation

    // Get cluster connection info from the test container
    val host = redisFixture.container.getHost
    val port = redisFixture.container.getMappedPort(7000)

    val clusterConfig = Map(
      "redis.cluster.nodes" -> s"$host:$port"
    )

    val kvStore = new RedisKVStoreImpl(jedisCluster, clusterConfig)
    kvStore.create(batchDataset) // Create the actual dataset that will be written to

    val spark = RedisSparkSessionTestHelper.acquire("RedisBulkPutTest")

    try {
      import spark.implicits._

      // Create test data - simulating batch IR snapshots (100 records)
      val testData = (1 to 100).map { i =>
        (s"user$i".getBytes, s"""{"feature1": $i, "feature2": "value$i"}""".getBytes, "2024-10-05")
      }

      val batchDf = testData.toDF("key_bytes", "value_bytes", "ds")

      // Create temporary table
      val tempTable = "test_batch_upload_table"
      batchDf.write.mode("overwrite").saveAsTable(tempTable)

      // Test bulkPut using the test-friendly method
      // Read data from the temp table
      val partition = "2024-10-05"
      val tableUtils = ai.chronon.spark.catalog.TableUtils(spark)
      val dataDf = tableUtils.sql(s"SELECT key_bytes, value_bytes, '$batchDataset' as dataset FROM $tempTable WHERE ds = '$partition'")

      import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
      val partitionSpec = ai.chronon.api.PartitionSpec("ds", "yyyy-MM-dd", WindowUtils.Day.millis)
      val endDsPlusOne = partitionSpec.epochMillis(partition) + partitionSpec.spanMillis

      // Transform data using Spark2RedisLoader's transformation logic
      val transformedDf = ai.chronon.integrations.redis.Spark2RedisLoader.buildTransformedDataFrame(dataDf, "chronon", endDsPlusOne, spark)

      // Write using the test-friendly method (avoids executor connection issues)
      ai.chronon.integrations.redis.Spark2RedisLoader.writeWithExistingConnection(transformedDf, jedisCluster, 432000)

      // Verify a sample of data was written correctly
      val sampleKeys = Seq("user1", "user50", "user100")
      val getRequests = sampleKeys.map(key => GetRequest(key.getBytes, batchDataset, None, None))
      val getResults = Await.result(kvStore.multiGet(getRequests), 10.seconds)

      getResults.size shouldBe 3
      getResults.foreach { result =>
        result.values.isSuccess shouldBe true
        result.values.get.size shouldBe 1
        // Value should have 8-byte timestamp prefix
        val timedValue = result.values.get.head
        timedValue.bytes.length should be > 8
      }

      // Verify specific values (timestamps are stripped automatically by multiGet)
      val value1 = new String(getResults(0).values.get.head.bytes, StandardCharsets.UTF_8)
      val value50 = new String(getResults(1).values.get.head.bytes, StandardCharsets.UTF_8)
      val value100 = new String(getResults(2).values.get.head.bytes, StandardCharsets.UTF_8)

      value1 should include("{\"feature1\": 1")
      value50 should include("{\"feature1\": 50")
      value100 should include("{\"feature1\": 100")

      // Verify hash tags were used in keys (dataset:base64_key format)
      val base64Key1 = java.util.Base64.getEncoder.encodeToString("user1".getBytes)
      val expectedKey1 = s"chronon:{$batchDataset:$base64Key1}"
      jedisCluster.exists(expectedKey1.getBytes(StandardCharsets.UTF_8)) shouldBe true

    } finally {
      RedisSparkSessionTestHelper.release()
    }
  }

  // Helper methods
  private def writeGeneratedTimeSeriesData(
    kvStore: RedisKVStoreImpl,
    dataset: String,
    keyBytes: Array[Byte],
    tsRange: Seq[Long],
    payload: String
  ): Unit = {
    val points = Seq.fill(tsRange.size)(payload)
    val putRequests = tsRange.zip(points).map { case (ts, point) =>
      PutRequest(keyBytes, point.getBytes, dataset, Some(ts))
    }
    val putResult = Await.result(kvStore.multiPut(putRequests), 10.seconds)
    putResult.length shouldBe tsRange.length
    putResult.foreach(_ shouldBe true)
  }

  private def generateAndWriteTimeSeriesData(
    kvStore: RedisKVStoreImpl,
    dataset: String,
    tsRange: Seq[Long],
    fakePayload: String,
    key: String
  ): Unit = {
    val tileKeys = tsRange.map { ts =>
      val tileKey = TilingUtils.buildTileKey(dataset, key.getBytes, Some(1.hour.toMillis), Some(ts))
      TilingUtils.serializeTileKey(tileKey)
    }
    tsRange.zip(tileKeys).foreach { case (ts, tileKeyBytes) =>
      writeGeneratedTimeSeriesData(kvStore, dataset, tileKeyBytes, Seq(ts), fakePayload)
    }
  }

  private def validateBlobValueExpectedPayload(response: GetResponse, expectedPayload: String): Unit = {
    for {
      tSeq <- response.values
      tv <- tSeq
    } {
      tSeq.length shouldBe 1
      val jsonStr = new String(tv.bytes, StandardCharsets.UTF_8)
      jsonStr shouldBe expectedPayload
    }
  }

  private def validateTimeSeriesValueExpectedPayload(
    response: GetResponse,
    expectedTimestamps: Seq[Long],
    expectedPayload: String
  ): Unit = {
    for (tSeq <- response.values) {
      tSeq.map(_.millis).toSet shouldBe expectedTimestamps.toSet
      tSeq.map(v => new String(v.bytes, StandardCharsets.UTF_8)).foreach(v => v shouldBe expectedPayload)
      tSeq.length shouldBe expectedTimestamps.length
    }
  }
}
