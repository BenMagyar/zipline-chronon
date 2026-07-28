package ai.chronon.integrations.redis

import ai.chronon.online.KVStore.{GetRequest, PutRequest}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class RedisBatchPipelineLifecycleTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {
  private var cluster: RedisClusterFixture = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    cluster = RedisClusterFixture.start(maxConnections = 1)
  }

  override def afterAll(): Unit = {
    try {
      if (cluster != null) cluster.close()
    } finally {
      super.afterAll()
    }
  }

  "Redis batch multiGet" should "return pipeline connections after reading responses" in {
    val kvStore = new RedisKVStoreImpl(cluster.client)
    val requests = (0 until 50).map(index => GetRequest(s"key-$index".getBytes, "pipeline_connections_BATCH"))
    val puts = requests.map(request => PutRequest(request.keyBytes, "value".getBytes, request.dataset, None))

    Await.result(kvStore.multiPut(puts), 10.seconds) shouldBe Seq.fill(puts.size)(true)

    val responses = Await.result(kvStore.multiGet(requests), 10.seconds)
    responses should have size requests.size
    responses.foreach(response => new String(response.values.get.head.bytes) shouldBe "value")
    cluster.client.getClusterNodes.values().asScala.map(_.getNumActive).sum shouldBe 0
  }
}
