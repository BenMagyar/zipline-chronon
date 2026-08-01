package ai.chronon.online.metrics

import java.util.concurrent.ThreadLocalRandom
import scala.util.Try

/** Samples high-multiplicity serving histograms as one coherent response bundle. Counters and errors do not use this
  * sampler and remain exact.
  */
private[online] final class HotPathMetricsSampler private[metrics] (val sampleRate: Double, nextDouble: () => Double) {
  require(
    java.lang.Double.isFinite(sampleRate) && sampleRate >= 0.0 && sampleRate <= 1.0,
    s"${HotPathMetricsSampler.SampleRateProperty} must be between 0.0 and 1.0, got $sampleRate"
  )

  def shouldSample(): Boolean =
    sampleRate >= 1.0 || (sampleRate > 0.0 && nextDouble() < sampleRate)
}

private[online] object HotPathMetricsSampler {
  val SampleRateProperty = "ai.chronon.metrics.hot_path.sample.rate"

  lazy val global: HotPathMetricsSampler = fromProperty(Option(System.getProperty(SampleRateProperty)))

  /** Force configuration parsing while the fetcher is being constructed rather than on the first live response. */
  def initialize(): Unit = {
    global
    ()
  }

  private[metrics] def fromProperty(
      rawValue: Option[String],
      nextDouble: () => Double = () => ThreadLocalRandom.current().nextDouble()): HotPathMetricsSampler = {
    val sampleRate = rawValue match {
      case None => 1.0
      case Some(raw) =>
        Try(raw.trim.toDouble).getOrElse(
          throw new IllegalArgumentException(s"$SampleRateProperty must be a number between 0.0 and 1.0, got '$raw'")
        )
    }
    new HotPathMetricsSampler(sampleRate, nextDouble)
  }

  private[metrics] def deterministic(sampleRate: Double, nextDouble: () => Double): HotPathMetricsSampler =
    new HotPathMetricsSampler(sampleRate, nextDouble)
}
