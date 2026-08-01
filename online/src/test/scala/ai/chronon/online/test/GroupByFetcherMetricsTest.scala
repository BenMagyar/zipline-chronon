package ai.chronon.online

import ai.chronon.online.fetcher.BatchedMetricsCounter
import ai.chronon.online.metrics.Metrics
import org.mockito.Mockito.{never, times, verify}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

class GroupByFetcherMetricsTest extends AnyFlatSpec with MockitoSugar {

  it should "aggregate additive counters by context and flush them once" in {
    val first = mock[Metrics.Context]
    val second = mock[Metrics.Context]
    val counter = new BatchedMetricsCounter("group_by.degraded_counter.count")

    counter.record(first, 1L)
    counter.record(first, 2L)
    counter.record(second, 4L)
    counter.record(second, 0L)
    counter.flush()
    counter.flush()

    verify(first, times(1)).count("group_by.degraded_counter.count", 3L)
    verify(second, times(1)).count("group_by.degraded_counter.count", 4L)
    verify(first, never()).count("group_by.degraded_counter.count", 1L)
  }

  it should "avoid emitting zero-only batches" in {
    val context = mock[Metrics.Context]
    val counter = new BatchedMetricsCounter("group_by.degraded_counter.count")

    counter.record(context, 0L)
    counter.flush()

    verify(context, never()).count("group_by.degraded_counter.count", 0L)
  }
}
