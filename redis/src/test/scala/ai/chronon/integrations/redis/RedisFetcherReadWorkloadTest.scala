package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisFetcherReadWorkload.{Candidate, Context, HourlyTiled, LastValue, Snapshot}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class RedisFetcherReadWorkloadTest extends AnyFlatSpec with Matchers {

  "Redis fetcher read workload" should "match the 50-candidate mixed GroupBy shape" in {
    val workload = RedisFetcherReadWorkload.build()

    workload.config.totalGroupBys shouldBe 50
    workload.config.totalStreamingGroupBys shouldBe 10
    workload.groupBys.count(spec => spec.ownership == Context && spec.readShape == LastValue) shouldBe 3
    workload.groupBys.count(spec => spec.ownership == Context && spec.readShape == HourlyTiled) shouldBe 2
    workload.groupBys.count(spec => spec.ownership == Candidate && spec.readShape == LastValue) shouldBe 2
    workload.groupBys.count(spec => spec.ownership == Candidate && spec.readShape == HourlyTiled) shouldBe 3
    workload.groupBys.count(spec => spec.ownership == Context && spec.readShape == Snapshot) shouldBe 20
    workload.groupBys.count(spec => spec.ownership == Candidate && spec.readShape == Snapshot) shouldBe 20
    workload.groupBys.count(_.payloadBytes == workload.config.embeddingPayloadBytes) shouldBe 2
    workload.logicalGroupByRequests shouldBe 2500
    workload.logical.requests should have size 3000

    workload.uniqueGroupByRequests shouldBe 1275
    workload.uniqueBatchRequests shouldBe 1275
    workload.uniqueStreamingRequests shouldBe 255
    workload.deduplicated.requests should have size 1530
    workload.deduplicated.batchGetCommands shouldBe 1275
    workload.deduplicated.zrangeCommands shouldBe 255
    workload.deduplicated.redisCommands shouldBe 1530
    workload.deduplicatedCrossDay.requests should have size 1530
    workload.deduplicatedCrossDay.batchGetCommands shouldBe 1275
    workload.deduplicatedCrossDay.zrangeCommands shouldBe 510
    workload.deduplicatedCrossDay.redisCommands shouldBe 1785
    workload.batchOnly.requests should have size 1275
    workload.batchCacheDemandAt(50).requests should have size 880
    workload.batchCacheDemandAt(80).requests should have size 505
    workload.batchCacheDemandAt(100).requests should have size 255
    workload.batchCacheDemandAt(50).redisCommands shouldBe 880
    workload.batchCacheDemandAt(80).redisCommands shouldBe 505
    workload.batchCacheDemandAt(100).redisCommands shouldBe 255
    workload.logical.groupByRequests shouldBe 2500
    workload.deduplicated.groupByRequests shouldBe 1275
  }

  it should "materialize the expected payload and tile counts" in {
    val workload = RedisFetcherReadWorkload.build()

    workload.puts should have size 5281
    workload.deduplicated.expectedTimedValues shouldBe 5026L
    workload.deduplicated.expectedPayloadBytes shouldBe 224504L
    workload.deduplicatedCrossDay.expectedTimedValues shouldBe 5026L
    workload.deduplicatedCrossDay.expectedPayloadBytes shouldBe 224504L
    workload.batchOnly.expectedTimedValues shouldBe 1275L
    workload.batchOnly.expectedPayloadBytes shouldBe 209500L
    workload.batchCacheDemandAt(50).expectedTimedValues shouldBe 4376L
    workload.batchCacheDemandAt(50).expectedPayloadBytes shouldBe 119704L
    workload.batchCacheDemandAt(80).expectedTimedValues shouldBe 4001L
    workload.batchCacheDemandAt(80).expectedPayloadBytes shouldBe 56884L
    workload.batchCacheDemandAt(100).expectedTimedValues shouldBe 3751L
    workload.batchCacheDemandAt(100).expectedPayloadBytes shouldBe 15004L
    workload.logical.expectedTimedValues shouldBe 8750L
    workload.logical.expectedPayloadBytes shouldBe 239400L

    val alignedStart = workload.deduplicated.requests.find(_.startTsMillis.isDefined).get.startTsMillis.get
    val streamingPutsBySeries = workload.puts
      .filter(_.dataset.endsWith("_STREAMING"))
      .groupBy { put =>
        val tileKey = ai.chronon.api.TilingUtils.deserializeTileKey(put.keyBytes)
        put.dataset -> tileKey.keyBytes.asScala.map(_.toByte).toVector
      }
    streamingPutsBySeries should have size workload.uniqueStreamingRequests
    all(streamingPutsBySeries.values.map(_.exists(_.tsMillis.exists(_ < alignedStart)))) shouldBe true
    all(streamingPutsBySeries.values.map(_.exists(_.tsMillis.exists(_ >= alignedStart)))) shouldBe true
  }
}
