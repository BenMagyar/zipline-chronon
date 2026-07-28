package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisFetcherReadWorkload.{Candidate, Context, HourlyTiled, LastValue, Snapshot}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
    workload.logical.groupByRequests shouldBe 2500
    workload.deduplicated.groupByRequests shouldBe 1275
  }

  it should "materialize the expected payload and tile counts" in {
    val workload = RedisFetcherReadWorkload.build()

    workload.puts should have size 5026
    workload.deduplicated.expectedTimedValues shouldBe 5026L
    workload.deduplicated.expectedPayloadBytes shouldBe 224504L
    workload.logical.expectedTimedValues shouldBe 8750L
    workload.logical.expectedPayloadBytes shouldBe 239400L
  }
}
