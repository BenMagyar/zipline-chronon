package ai.chronon.online.metrics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger

class HotPathMetricsSamplerTest extends AnyFlatSpec with Matchers {

  "HotPathMetricsSampler" should "short circuit the zero and one boundaries" in {
    val randomCalls = new AtomicInteger()
    val nextDouble = () => {
      randomCalls.incrementAndGet()
      0.5
    }

    HotPathMetricsSampler.deterministic(0.0, nextDouble).shouldSample() shouldBe false
    HotPathMetricsSampler.deterministic(1.0, nextDouble).shouldSample() shouldBe true
    randomCalls.get() shouldBe 0
  }

  it should "use one random decision at an intermediate rate" in {
    val values = Iterator(0.09, 0.1, 0.75)
    val sampler = HotPathMetricsSampler.deterministic(0.1, () => values.next())

    sampler.shouldSample() shouldBe true
    sampler.shouldSample() shouldBe false
    sampler.shouldSample() shouldBe false
  }

  it should "reject invalid rates" in {
    an[IllegalArgumentException] should be thrownBy HotPathMetricsSampler.deterministic(-0.1, () => 0.0)
    an[IllegalArgumentException] should be thrownBy HotPathMetricsSampler.deterministic(1.1, () => 0.0)
    an[IllegalArgumentException] should be thrownBy HotPathMetricsSampler.deterministic(Double.NaN, () => 0.0)
  }

  it should "parse the deployment property before serving traffic" in {
    HotPathMetricsSampler.fromProperty(None, () => 0.0).sampleRate shouldBe 1.0
    HotPathMetricsSampler.fromProperty(Some(" 0.1 "), () => 0.0).sampleRate shouldBe 0.1

    an[IllegalArgumentException] should be thrownBy HotPathMetricsSampler.fromProperty(Some("often"), () => 0.0)
    an[IllegalArgumentException] should be thrownBy HotPathMetricsSampler.fromProperty(Some("1.1"), () => 0.0)
  }
}
