package ai.chronon.online.metrics

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OtelMetricsReporterTemporalityTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val TemporalityKey = OtelMetricsReporter.MetricsTemporalityKey
  private var savedTemporality: Option[String] = None

  override def beforeEach(): Unit = {
    savedTemporality = Option(System.getProperty(TemporalityKey))
    System.clearProperty(TemporalityKey)
  }

  override def afterEach(): Unit = {
    savedTemporality match {
      case Some(value) => System.setProperty(TemporalityKey, value)
      case None        => System.clearProperty(TemporalityKey)
    }
  }

  "metrics temporality" should "default to cumulative" in {
    OtelMetricsReporter.getMetricsTemporality shouldBe OtelMetricsReporter.MetricsTemporalityCumulative
  }

  it should "accept and normalize cumulative and delta" in {
    System.setProperty(TemporalityKey, " CUMULATIVE ")
    OtelMetricsReporter.getMetricsTemporality shouldBe OtelMetricsReporter.MetricsTemporalityCumulative

    System.setProperty(TemporalityKey, " DELTA ")
    OtelMetricsReporter.getMetricsTemporality shouldBe OtelMetricsReporter.MetricsTemporalityDelta
  }

  it should "reject unsupported values" in {
    System.setProperty(TemporalityKey, "delat")

    val error = intercept[IllegalArgumentException] {
      OtelMetricsReporter.getMetricsTemporality
    }

    error.getMessage should include(TemporalityKey)
    error.getMessage should include("delat")
    error.getMessage should include(OtelMetricsReporter.MetricsTemporalityCumulative)
    error.getMessage should include(OtelMetricsReporter.MetricsTemporalityDelta)
  }
}
