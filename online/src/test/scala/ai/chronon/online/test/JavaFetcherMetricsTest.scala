package ai.chronon.online

import ai.chronon.online.metrics.Metrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class JavaFetcherMetricsTest extends AnyFlatSpec with Matchers {
  it should "emit batch metrics once per distinct request name and reuse their contexts" in {
    val emissions = ArrayBuffer.empty[(Metrics.Context, String, Long)]
    val instrumenter = new JavaFetcher.MetricsInstrumenter(new JavaFetcher.DistributionRecorder {
      override def record(context: Metrics.Context, metricName: String, value: Long): Unit =
        emissions += ((context, metricName, value))
    })
    val duplicateNames = List.fill(50)("candidate_join").asJava
    val startTs = System.currentTimeMillis()

    instrumenter.instrument(duplicateNames, false, "java.request_conversion.latency.millis", startTs)
    instrumenter.instrument(duplicateNames, false, "java.response_conversion.latency.millis", startTs)
    instrumenter.instrument(duplicateNames, false, "java.overall.latency.millis", startTs)

    emissions.map(_._2) should contain theSameElementsInOrderAs Seq(
      "java.request_conversion.latency.millis",
      "java.response_conversion.latency.millis",
      "java.overall.latency.millis"
    )
    emissions.map(_._1).forall(_ eq emissions.head._1) shouldBe true
  }

  it should "emit separate group by metrics for distinct request names" in {
    val emissions = ArrayBuffer.empty[Metrics.Context]
    val instrumenter = new JavaFetcher.MetricsInstrumenter(new JavaFetcher.DistributionRecorder {
      override def record(context: Metrics.Context, metricName: String, value: Long): Unit =
        emissions += context
    })

    instrumenter.instrument(List("first_group_by", "first_group_by", "second_group_by").asJava,
                            true,
                            "java.overall.latency.millis",
                            System.currentTimeMillis())

    emissions should have size 2
    emissions.map(_.groupBy).toSet shouldBe Set("first_group_by", "second_group_by")
  }

  it should "bound metrics context caches" in {
    val instrumenter = new JavaFetcher.MetricsInstrumenter(new JavaFetcher.DistributionRecorder {
      override def record(context: Metrics.Context, metricName: String, value: Long): Unit = ()
    })
    val names = (0 to JavaFetcher.MetricsInstrumenter.MAX_CONTEXT_CACHE_ENTRIES).map(index => s"name_$index").asJava

    instrumenter.instrument(names, false, "java.overall.latency.millis", System.currentTimeMillis())
    instrumenter.instrument(names, true, "java.overall.latency.millis", System.currentTimeMillis())

    instrumenter.cachedJoinContextCount() shouldBe JavaFetcher.MetricsInstrumenter.MAX_CONTEXT_CACHE_ENTRIES
    instrumenter.cachedGroupByContextCount() shouldBe JavaFetcher.MetricsInstrumenter.MAX_CONTEXT_CACHE_ENTRIES
  }
}
