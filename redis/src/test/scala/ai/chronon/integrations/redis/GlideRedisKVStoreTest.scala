package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants._
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.util.JedisClusterCRC16

import java.nio.charset.StandardCharsets
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

class GlideRedisKVStoreTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val timeout = 30.seconds
  private val keyPrefix = "chronon-glide-test"
  private val workload = RedisFetcherReadWorkload.build(RedisFetcherReadWorkload.Config(candidates = 3))
  private var cluster: RedisClusterFixture = _
  private var expectedReadValues = Map.empty[String, Vector[Vector[(Long, Vector[Byte])]]]

  override def beforeAll(): Unit = {
    super.beforeAll()
    cluster = RedisClusterFixture.startNative(
      maxConnections = 16,
      minIdleConnections = 1,
      maxIdleConnections = 4,
      primaryCount = 3,
      replicasPerPrimary = 1,
      cpuLimit = None,
      server = "valkey"
    )
    val loader = new RedisKVStoreImpl(cluster.client, storeConf)
    workload.datasets.foreach(loader.create)
    workload.puts.grouped(100).foreach { puts =>
      Await.result(loader.multiPut(puts), timeout).forall(identity) shouldBe true
    }
    expectedReadValues = correctnessProfiles.map { profile =>
      val responses = Await.result(loader.multiGet(profile.requests), timeout)
      require(responses.map(_.request) == profile.requests, s"Jedis baseline reordered ${profile.name}")
      require(responses.forall(_.values.isSuccess), s"Jedis baseline failed for ${profile.name}")
      profile.name -> exactValues(responses)
    }.toMap
  }

  override def afterAll(): Unit = {
    try if (cluster != null) cluster.close()
    finally super.afterAll()
  }

  "GLIDE Redis reads" should "preserve mixed batch and streaming results in direct and cluster-batch modes" in {
    Seq(GlideExecutionDirect, GlideExecutionClusterBatch).foreach { executionMode =>
      withStore(executionMode, RedisReadFromPrimary) { store =>
        correctnessProfiles.foreach(profile => validateEventually(store, profile))
      }
    }
  }

  it should "route cluster-batch reads across primary, all-node, and AZ-local policies" in {
    val policies = Seq(
      (GlideExecutionClusterBatch, RedisReadFromPrimary, None),
      (GlideExecutionClusterBatch, RedisReadFromAllNodes, None),
      (GlideExecutionClusterBatch, RedisReadFromAzAffinityReplicasAndPrimary, Some("zone-a"))
    )

    policies.foreach { case (executionMode, readFrom, clientAz) =>
      withStore(executionMode, readFrom, clientAz = clientAz) { store =>
        validateEventually(store)
        cluster.resetCommandStats()
        validate(store)

        val stats = cluster.readCommandStats()
        val primaryCalls = stats.filter(_.role == "master").map(_.totalReadCalls).sum
        val replicaStats = stats.filter(node => node.role == "slave" || node.role == "replica")
        val replicaCalls = replicaStats.map(_.totalReadCalls).sum
        readFrom match {
          case RedisReadFromPrimary =>
            withClue(stats.mkString("routing stats: ", ", ", "")) {
              primaryCalls should be > 0L
              replicaCalls shouldBe 0L
            }
          case RedisReadFromAllNodes =>
            withClue(stats.mkString("routing stats: ", ", ", "")) {
              primaryCalls should be > 0L
              replicaCalls should be > 0L
            }
          case RedisReadFromAzAffinityReplicasAndPrimary =>
            val localStats = stats.filter(_.availabilityZone.contains(clientAz.get))
            val localPrimaryCalls = localStats.filter(_.role == "master").map(_.totalReadCalls).sum
            val localReplicaCalls = localStats
              .filter(node => node.role == "slave" || node.role == "replica")
              .map(_.totalReadCalls)
              .sum
            val totalCalls = primaryCalls + replicaCalls
            val localCalls = localPrimaryCalls + localReplicaCalls
            withClue(stats.mkString("routing stats: ", ", ", "")) {
              localPrimaryCalls should be > 0L
              localReplicaCalls should be > 0L
              localCalls.toDouble / totalCalls should be > 0.5
            }
        }
      }
    }
  }

  it should "coalesce same-slot batch reads without losing order or per-key failures" in {
    val entity = Array[Byte](0, 1, -1, 58, 123, 125)
    val goodA = GetRequest(entity, "GLIDE_MGET_A_BATCH")
    val missing = GetRequest(entity, "GLIDE_MGET_MISSING_BATCH")
    val malformed = GetRequest(entity, "GLIDE_MGET_MALFORMED_BATCH")
    val goodB = GetRequest(entity, "GLIDE_MGET_B_BATCH")
    val requests = Seq(goodA, missing, malformed, goodB)
    val entityStoreConf = storeConf.updated("redis.key.hash.tag.mode", RedisKVStore.EntityHashTagMode)
    val valueA = Array[Byte](0, 10, -1, 20)
    val valueB = Array[Byte](-128, 0, 127)
    val loader = new RedisKVStoreImpl(cluster.client, entityStoreConf)

    Await.result(
      loader.multiPut(
        Seq(
          PutRequest(entity, valueA, goodA.dataset, Some(1L)),
          PutRequest(entity, valueB, goodB.dataset, Some(2L))
        )),
      timeout
    ) shouldBe Seq(true, true)
    val redisKeys = requests.map { request =>
      RedisKVStore
        .buildRedisKey(request.keyBytes,
                       request.dataset,
                       keyPrefix = keyPrefix,
                       hashTagMode = RedisKVStore.EntityHashTagMode)
        .getBytes(StandardCharsets.UTF_8)
    }
    redisKeys.map(JedisClusterCRC16.getSlot).distinct should have size 1
    cluster.client.set(redisKeys(2), Array[Byte](1, 2, 3))

    Seq(GlideExecutionDirect, GlideExecutionClusterBatch).foreach { executionMode =>
      withStore(executionMode, RedisReadFromPrimary, entityStoreConf) { store =>
        cluster.resetCommandStats()
        val responses = Await.result(store.multiGet(requests), timeout)

        responses.map(_.request) shouldBe requests
        responses(0).values.get.head.bytes.toSeq shouldBe valueA.toSeq
        responses(1).values.get shouldBe empty
        responses(2).values.isFailure shouldBe true
        responses(3).values.get.head.bytes.toSeq shouldBe valueB.toSeq

        val stats = cluster.readCommandStats()
        withClue(stats.mkString("MGET stats: ", ", ", "")) {
          stats.map(_.mgetCalls).sum shouldBe 1L
          stats.map(_.getCalls).sum shouldBe 0L
        }
      }
    }
  }

  private def withStore(executionMode: String,
                        readFrom: String,
                        storeConfiguration: Map[String, String] = storeConf,
                        clientAz: Option[String] = None)(test: RedisKVStoreImpl => Unit): Unit = {
    val settings = cluster.clientSettings.copy(
      client = RedisClientGlide,
      readFrom = readFrom,
      clientAz = clientAz
    )
    val glideClient = RedisKVStoreFactory.createGlideReadClient(settings, executionMode)
    val store = new RedisKVStoreImpl(cluster.client, storeConfiguration, glideReadClient = Some(glideClient))
    try test(store)
    finally store.shutdownGlideReadClient()
  }

  private def validateEventually(store: RedisKVStoreImpl,
                                 profile: RedisFetcherReadWorkload.ReadProfile = workload.deduplicated): Unit = {
    val deadline = System.nanoTime() + timeout.toNanos
    var lastFailure: Throwable = null
    do {
      Try(validate(store, profile)) match {
        case scala.util.Success(_) => return
        case scala.util.Failure(error) =>
          lastFailure = error
          Thread.sleep(25)
      }
    } while (System.nanoTime() < deadline)
    throw new IllegalStateException("GLIDE did not expose the loaded fixture before timeout", lastFailure)
  }

  private def validate(store: RedisKVStoreImpl,
                       profile: RedisFetcherReadWorkload.ReadProfile = workload.deduplicated): Unit = {
    val responses = Await.result(store.multiGet(profile.requests), timeout)
    responses.map(_.request) shouldBe profile.requests
    responses.foreach(_.values.isSuccess shouldBe true)
    exactValues(responses) shouldBe expectedReadValues(profile.name)
  }

  private def correctnessProfiles = Seq(workload.deduplicated, workload.deduplicatedCrossDay)

  private def exactValues(responses: Seq[GetResponse]): Vector[Vector[(Long, Vector[Byte])]] =
    responses.map { response =>
      response.values.get.iterator.map(value => value.millis -> value.bytes.toVector).toVector
    }.toVector

  private def storeConf: Map[String, String] = Map(
    PropRedisKeyPrefix -> keyPrefix,
    "redis.key.hash.tag.mode" -> RedisKVStore.DatasetAndEntityHashTagMode,
    "redis.read.metrics.enabled" -> "false",
    ConfigPipelineSyncThreads -> "0"
  )
}
