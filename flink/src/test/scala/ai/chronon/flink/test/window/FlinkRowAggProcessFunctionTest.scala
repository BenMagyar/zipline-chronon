package ai.chronon.flink.test.window

import ai.chronon.api.{Constants, DataType, DoubleType, IntType, LongType, StringType}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.test.FlinkTestUtils
import ai.chronon.flink.types.{TimestampedIR, TimestampedTile}
import ai.chronon.flink.window.{FlinkRowAggProcessFunction, FlinkRowAggregationFunction}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.groups.OperatorMetricGroup
import org.apache.flink.metrics.{Counter, Histogram, MetricGroup}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.mockito.ArgumentMatchers.{any, anyLong, eq => eqTo}
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util

class FlinkRowAggProcessFunctionTest extends AnyFlatSpec with Matchers with MockitoSugar {

  private val groupBy = FlinkTestUtils.makeGroupBy(Seq("id"))
  private val inputSchema: Seq[(String, DataType)] = Seq(
    "id" -> StringType,
    "int_val" -> IntType,
    "double_val" -> DoubleType,
    Constants.TimeColumn -> LongType
  )

  private case class MetricsFixture(
      function: FlinkRowAggProcessFunction,
      incrementalHistogram: Histogram,
      completeHistogram: Histogram
  )

  private def buildMetricsFixture(): MetricsFixture = {
    val function = new FlinkRowAggProcessFunction(groupBy, inputSchema)
    val runtimeContext = mock[RuntimeContext]
    val operatorMetricGroup = mock[OperatorMetricGroup]
    val chrononMetricGroup = mock[MetricGroup]
    val featureGroupMetricGroup = mock[MetricGroup]
    val incrementalMetricGroup = mock[MetricGroup]
    val completeMetricGroup = mock[MetricGroup]
    val counter = mock[Counter]
    val rowAggregationHistogram = mock[Histogram]
    val rowTileConversionHistogram = mock[Histogram]
    val incrementalHistogram = mock[Histogram]
    val completeHistogram = mock[Histogram]

    when(runtimeContext.getMetricGroup).thenReturn(operatorMetricGroup)
    when(operatorMetricGroup.addGroup("chronon")).thenReturn(chrononMetricGroup)
    when(chrononMetricGroup.addGroup("feature_group", groupBy.getMetaData.getName))
      .thenReturn(featureGroupMetricGroup)
    when(featureGroupMetricGroup.counter("tiling_process_function_error")).thenReturn(counter)
    when(featureGroupMetricGroup.counter("event_processing_error")).thenReturn(counter)
    when(featureGroupMetricGroup.histogram(eqTo("row_aggregation_time"), any[Histogram]()))
      .thenReturn(rowAggregationHistogram)
    when(featureGroupMetricGroup.histogram(eqTo("row_tile_conversion_time"), any[Histogram]()))
      .thenReturn(rowTileConversionHistogram)
    when(featureGroupMetricGroup.addGroup("tile_status", "incremental")).thenReturn(incrementalMetricGroup)
    when(featureGroupMetricGroup.addGroup("tile_status", "complete")).thenReturn(completeMetricGroup)
    when(incrementalMetricGroup.histogram(eqTo("flink_ingress_to_tile_emission_time"), any[Histogram]()))
      .thenReturn(incrementalHistogram)
    when(completeMetricGroup.histogram(eqTo("flink_ingress_to_tile_emission_time"), any[Histogram]()))
      .thenReturn(completeHistogram)

    function.setRuntimeContext(runtimeContext)
    function.open(new Configuration())
    MetricsFixture(function, incrementalHistogram, completeHistogram)
  }

  private def timestampedIr(): TimestampedIR = {
    val aggregationFunction = new FlinkRowAggregationFunction(groupBy, inputSchema)
    val accumulator = aggregationFunction.createAccumulator()
    aggregationFunction.add(
      ProjectedEvent(
        Map(
          "id" -> "id1",
          "int_val" -> 1,
          "double_val" -> 1.5,
          Constants.TimeColumn -> 500L
        ),
        System.currentTimeMillis() - 1000L
      ),
      accumulator
    )
  }

  private def process(fixture: MetricsFixture, currentWatermark: Long): Unit = {
    type WindowProcessFunction =
      ProcessWindowFunction[TimestampedIR, TimestampedTile, util.List[Any], TimeWindow]

    val context = mock[WindowProcessFunction#Context]
    val collector = mock[Collector[TimestampedTile]]
    when(context.window).thenReturn(new TimeWindow(0L, 1000L))
    when(context.currentWatermark).thenReturn(currentWatermark)

    fixture.function.process(
      util.Arrays.asList[Any]("id1"),
      context,
      util.Arrays.asList(timestampedIr()),
      collector
    )

    verify(collector).collect(any[TimestampedTile]())
  }

  "FlinkRowAggProcessFunction" should "record incremental ingress-to-tile latency" in {
    val fixture = buildMetricsFixture()

    process(fixture, currentWatermark = 500L)

    verify(fixture.incrementalHistogram).update(anyLong())
    verify(fixture.completeHistogram, never()).update(anyLong())
  }

  it should "record complete ingress-to-tile latency" in {
    val fixture = buildMetricsFixture()

    process(fixture, currentWatermark = 1000L)

    verify(fixture.completeHistogram).update(anyLong())
    verify(fixture.incrementalHistogram, never()).update(anyLong())
  }
}
