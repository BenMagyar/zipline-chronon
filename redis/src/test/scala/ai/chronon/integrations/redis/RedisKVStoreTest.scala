package ai.chronon.integrations.redis

import ai.chronon.api.Constants.{ContinuationKey, GroupByFolder, JoinFolder, ListEntityType, ListLimit}
import ai.chronon.api.TilingUtils
import ai.chronon.online.KVStore._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.{HostAndPort, JedisCluster, DefaultJedisClientConfig, HostAndPortMapper}
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import java.nio.charset.StandardCharsets
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
 * Comprehensive tests for Redis Cluster KVStore following BigTableKVStoreTest patterns.
 * Uses Testcontainers with grokzen/redis-cluster for fast, reliable testing (~6s startup).
 */
class RedisKVStoreTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {
  import RedisKVStore._

  private var redisContainer: GenericContainer[_] = _
  private var jedisCluster: JedisCluster = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Use grokzen/redis-cluster - minimal Redis cluster in single container
    // Provides a 6-node cluster (3 masters, 3 replicas) with fast startup (~5-7 seconds)
    val internalPorts = Seq(7000,7001,7002,7003,7004,7005)
    val container = new GenericContainer(DockerImageName.parse("grokzen/redis-cluster:7.0.10"))
    container.withExposedPorts(internalPorts.map(Integer.valueOf): _*)
    container.withStartupTimeout(java.time.Duration.ofSeconds(60))
    container.start()
    println(s"\nRedis cluster container started")
    redisContainer = container

    val host = container.getHost
    val basePort = container.getMappedPort(7000)
    // Wait for cluster to be ready - ensure all 3 masters have slot assignments



    if (!waitForClusterReady(host, basePort, 30, "Cluster Initiated")) {
      throw new RuntimeException(s"Redis cluster failed to initialize after 30 seconds")
    }

    val configBuilder = DefaultJedisClientConfig.builder()
      .hostAndPortMapper(new HostAndPortMapper {
        override def getHostAndPort(hap: HostAndPort): HostAndPort = {
          // Redis CLuster announces internal ports (7000-7005)
          // map them to the external ports assigned by Testcontainers
          val internalPort = hap.getPort
          if(internalPorts.contains(internalPort)) {
            val mappedPort = container.getMappedPort(internalPort)
            new HostAndPort(host, mappedPort)
          } else {
            // if port is not in our known list, return as-is
            hap
          }
        }
      })

    val poolConfig = new org.apache.commons.pool2.impl.GenericObjectPoolConfig[redis.clients.jedis.Connection]()
    poolConfig.setMaxTotal(10)
    poolConfig.setMaxIdle(10)
    poolConfig.setMinIdle(2)
    poolConfig.setTestOnBorrow(true)

    // Initialize JedisCLuster with the mapped entry point and the port mapper
    val initNode = new HostAndPort(host, basePort)
    jedisCluster = new JedisCluster(Set(initNode).asJava, configBuilder.build(), 5, poolConfig)
    println(s"JedisCluster connected with port mapping \n")

    // Reconfigure each Redis node to announce external (mapped) ports
    // This makes the cluster topology accessible from Spark executors!
    println("Reconfiguring Redis cluster to announce external ports...")
    internalPorts.foreach { internalPort =>
      val mappedPort = container.getMappedPort(internalPort)
      val jedis = new redis.clients.jedis.Jedis(host, mappedPort, 5000)

      try {
        // Configure Redis to announce the external host and mapped port
        jedis.configSet("cluster-announce-ip", host)
        jedis.configSet("cluster-announce-port", mappedPort.toString)
        // Bus port is typically port + 5000, use mapped port for this too
        jedis.configSet("cluster-announce-bus-port", (mappedPort + 5000).toString)

        println(s"  ✓ Node $internalPort now announces $host:$mappedPort")
      } catch {
        case e: Exception =>
          println(s"  ✕ Failed to reconfigure node $internalPort: ${e.getMessage}")
          throw e
      } finally {
        jedis.close()
      }
    }

    // Wait for cluster gossip protocol to propagate the new topology
    println("Waiting for cluster topology to propagate...")
    Thread.sleep(3000)
    println("✓ Cluster topology reconfigured")
  }

  override def afterAll(): Unit = {
    if (jedisCluster != null) {
      try jedisCluster.close() catch { case _: Exception => }
    }
    if (redisContainer != null) {
      try redisContainer.stop() catch { case _: Exception => }
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

  private def waitForClusterReady(host: String, port: Int, maxAttempts: Int = 30, successMessage: String = "Cluster ready") : Boolean = {
    var clusterReady = false
    var attempts = 0
    while (!clusterReady && attempts < maxAttempts) {
      try {
        val testJedis = new redis.clients.jedis.Jedis(host, port, 3000)
        val clusterSlots = testJedis.clusterSlots()
        testJedis.close()
        if (clusterSlots.size() >= 3) {
          clusterReady = true
          println(s"\n$successMessage (${attempts + 1}s)")
        }
      } catch {
        case _: Exception => // Cluster not ready yet
      }
      if (!clusterReady) {
        Thread.sleep(1000)
        attempts += 1
      }
    }
    clusterReady
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

    // let's try and read these with pagination
    val limit = 10
    val listReq1 = ListRequest(dataset, Map(ListLimit -> limit))
    val listResult1 = Await.result(kvStore.list(listReq1), 10.seconds)
    listResult1.values.isSuccess shouldBe true
    val listValues1 = listResult1.values.get
    listValues1.size should be <= limit

    // If we got a continuation key, try to get more
    if (listResult1.resultProps.contains(ContinuationKey)) {
      val limit2 = 1000
      val continuationKey = listResult1.resultProps(ContinuationKey)
      val listReq2 = ListRequest(dataset, Map(ListLimit -> limit2, ContinuationKey -> continuationKey))
      val listResult2 = Await.result(kvStore.list(listReq2), 10.seconds)
      listResult2.values.isSuccess shouldBe true
    }
  }

  it should "list entity types with pagination" in {
    val dataset = "CHRONON_METADATA"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    val putGrpByReqs = (0 until 50).map { i =>
      val key = s"$GroupByFolder/gbkey-$i"
      val value = s"""{"name": "name-$i", "age": $i}"""
      PutRequest(key.getBytes, value.getBytes, dataset, None)
    }

    val putJoinReqs = (0 until 50).map { i =>
      val key = s"$JoinFolder/joinkey-$i"
      val value = s"""{"name": "name-$i", "age": $i}"""
      PutRequest(key.getBytes, value.getBytes, dataset, None)
    }
    val putResults = Await.result(kvStore.multiPut(putGrpByReqs ++ putJoinReqs), 10.seconds)
    putResults.foreach(r => r shouldBe true)

    // let's try and read just the joins - tests that filtering works correctly
    val limit = 10
    val listReq1 = ListRequest(dataset, Map(ListLimit -> limit, ListEntityType -> JoinFolder))
    val listResult1 = Await.result(kvStore.list(listReq1), 10.seconds)
    listResult1.values.isSuccess shouldBe true
    val listValues1 = listResult1.values.get
    listValues1.size should be <= limit

    // Verify entity type filter works
    listValues1.foreach { value =>
      val keyStr = new String(value.keyBytes, StandardCharsets.UTF_8)
      keyStr should include(JoinFolder)
      keyStr should not include GroupByFolder
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

  it should "keep updates to an offset daily tile in one bucket across midnight" in {
    val dataset = "GROUPBY_STREAMING"
    val kvStore = new RedisKVStoreImpl(jedisCluster)
    kvStore.create(dataset)

    val tileStart = java.time.Instant.parse("2024-10-04T01:00:00Z").toEpochMilli
    val beforeMidnight = java.time.Instant.parse("2024-10-04T23:30:00Z").toEpochMilli
    val afterMidnight = java.time.Instant.parse("2024-10-05T00:30:00Z").toEpochMilli
    val tileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.day.toMillis), Some(tileStart))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)

    Await.result(kvStore.multiPut(Seq(PutRequest(tileKeyBytes, "before".getBytes, dataset, Some(beforeMidnight)))),
                 10.seconds) shouldBe Seq(true)
    Await.result(kvStore.multiPut(Seq(PutRequest(tileKeyBytes, "after".getBytes, dataset, Some(afterMidnight)))),
                 10.seconds) shouldBe Seq(true)

    val readTileKey = TilingUtils.buildTileKey(dataset, "my_key".getBytes, Some(1.day.toMillis), None)
    val request = GetRequest(
      TilingUtils.serializeTileKey(readTileKey),
      dataset,
      Some(tileStart),
      Some(tileStart + 1.day.toMillis)
    )
    val values = Await.result(kvStore.multiGet(Seq(request)), 10.seconds).head.values.get

    values.map(_.millis) shouldBe Seq(tileStart)
    new String(values.head.bytes, StandardCharsets.UTF_8) shouldBe "after"
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

  it should "bulk put batch IR data from Spark" in {
    // Note: bulkPut appends "_BATCH" suffix via GroupBy.batchDataset (same as BigTable)
    val dataset = "TEST_GROUPBY"
    val batchDataset = s"${dataset}_BATCH"  // The actual dataset name after transformation

    // Get cluster connection info from the test container
    val host = redisContainer.getHost
    val port = redisContainer.getMappedPort(7000)

    val clusterConfig = Map(
      "redis.cluster.nodes" -> s"$host:$port"
    )

    val kvStore = new RedisKVStoreImpl(jedisCluster, clusterConfig)
    kvStore.create(batchDataset) // Create the actual dataset that will be written to

    // Create SparkSession for test
    // Note: Jedis must be on classpath for spark-redis connector
    val spark = org.apache.spark.sql.SparkSession.builder()
      .appName("RedisBulkPutTest")
      .master("local[1]") // Single thread to avoid overwhelming Testcontainers Redis
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", java.nio.file.Files.createTempDirectory("spark-warehouse").toString)
      // Explicitly set Redis config for connector
      .config("spark.redis.host", host)
      .config("spark.redis.port", port.toString)
      .enableHiveSupport()
      .getOrCreate()

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
      spark.stop()
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
