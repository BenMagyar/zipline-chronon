package ai.chronon.flink.window

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Constants
import ai.chronon.api.DataType
import ai.chronon.api.GroupBy
import ai.chronon.api.Row
import ai.chronon.api.ScalaJavaConversions.IteratorOps
import ai.chronon.flink.{ERROR, FlinkLogging, INFO}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.TimestampedIR
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.online.TileCodec
import ai.chronon.online.serde.ArrayRow
import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.lang
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** Wrapper Flink aggregator around Chronon's RowAggregator. Relies on Flink to pass in
  * the correct set of events for the tile. As the aggregates produced by this function
  * are used on the serving side along with other pre-aggregates, we don't 'finalize' the
  * Chronon RowAggregator and instead return the intermediate representation.
  *
  * (This cannot be a RichAggregateFunction because Flink does not support Rich functions in windows.)
  */
class FlinkRowAggregationFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)],
    enableDebug: Boolean = false
) extends AggregateFunction[ProjectedEvent, TimestampedIR, TimestampedIR] {
  @transient private[flink] var rowAggregator: RowAggregator = _
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val valueColumns: Array[String] = inputSchema.map(_._1).toArray // column order matters
  private val timeColumnAlias: String = Constants.TimeColumn

  private val isMutation: Boolean = {
    Option(groupBy.getSources).exists(
      _.iterator().toScala
        .exists(source => source.isSetEntities && source.getEntities.isSetMutationTopic)
    )
  }

  private val reversalIndex = {
    val result = inputSchema.indexWhere(_._1 == Constants.ReversalColumn)

    if (isMutation)
      require(result >= 0,
              s"Please specify source.query.reversal_column for CDC sources, only found, ${inputSchema.map(_._1)}")

    result
  }

  /*
   * Initialize the transient rowAggregator.
   * Running this method is an idempotent operation:
   *   1. The initialized RowAggregator is always the same given a `groupBy` and `inputSchema`.
   *   2. The RowAggregator itself doens't hold state; Flink keeps track of the state of the IRs.
   */
  private def initializeRowAggregator(): Unit =
    rowAggregator = TileCodec.buildRowAggregator(groupBy, inputSchema)

  override def createAccumulator(): TimestampedIR = {
    initializeRowAggregator()
    new TimestampedIR(rowAggregator.init, None, None, None)
  }

  override def add(
      event: ProjectedEvent,
      accumulatorIr: TimestampedIR
  ): TimestampedIR = {

    val startAggregationTime = System.currentTimeMillis()

    val element = event.fields
    // Most times, the time column is a Long, but it could be a Double.
    val tsMills = Try(element(timeColumnAlias).asInstanceOf[Long])
      .getOrElse(element(timeColumnAlias).asInstanceOf[Double].toLong)
    val row = toChrononRow(element, tsMills)

    // Given that the rowAggregator is transient, it may be null when a job is restored from a checkpoint
    if (rowAggregator == null) {
      logger.info(
        f"The Flink RowAggregator was null for groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills"
      )
      initializeRowAggregator()
    }

    if (enableDebug) {
      logger.info(
        f"Flink pre-aggregates BEFORE adding new element: accumulatorIr=[${accumulatorIr.ir
            .mkString(", ")}] groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills element=$element"
      )
    }

    val partialAggregates = Try {
      val isDelete = isMutation && row.getAs[Boolean](reversalIndex)

      if (isDelete) {
        rowAggregator.delete(accumulatorIr.ir, row)
      } else {
        rowAggregator.update(accumulatorIr.ir, row)
      }

    }
    val rowAggrTime = System.currentTimeMillis() - startAggregationTime

    partialAggregates match {
      case Success(v) => {
        if (enableDebug) {
          logger.info(
            f"Flink pre-aggregates AFTER adding new element [${v.mkString(", ")}] " +
              f"groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills element=$element"
          )
        }
        new TimestampedIR(v, Some(tsMills), Some(event.startProcessingTimeMillis), Some(rowAggrTime))
      }
      case Failure(e) =>
        logger.error(
          "Flink error calculating partial row aggregate. " +
            s"groupBy=${groupBy.getMetaData.getName} tsMills=$tsMills element=$element",
          e
        )
        throw e
    }
  }

  // Note we return intermediate results here as the results of this
  // aggregator are used on the serving side along with other pre-aggregates
  override def getResult(accumulatorIr: TimestampedIR): TimestampedIR =
    accumulatorIr

  override def merge(aIr: TimestampedIR, bIr: TimestampedIR): TimestampedIR = {
    def mergeOptionalTs(
        aTs: Option[Long],
        bTs: Option[Long]
    ): Option[Long] =
      aTs
        .flatMap(aL => bTs.map(bL => Math.max(aL, bL)))
        .orElse(aTs.orElse(bTs))

    new TimestampedIR(
      rowAggregator.merge(aIr.ir, bIr.ir),
      mergeOptionalTs(aIr.latestTsMillis, bIr.latestTsMillis),
      mergeOptionalTs(aIr.startProcessingTime, bIr.startProcessingTime),
      mergeOptionalTs(aIr.rowAggrTime, bIr.rowAggrTime)
    )
  }

  def toChrononRow(value: Map[String, Any], tsMills: Long): Row = {
    // The row values need to be in the same order as the input schema columns
    // The reason they are out of order in the first place is because the CatalystUtil does not return values in the
    // same order as the schema
    val values: Array[Any] = valueColumns.map(value(_))
    new ArrayRow(values, tsMills)
  }
}

// This process function is only meant to be used downstream of the ChrononFlinkAggregationFunction
class FlinkRowAggProcessFunction(
    groupBy: GroupBy,
    inputSchema: Seq[(String, DataType)],
    enableDebug: Boolean = false
) extends ProcessWindowFunction[TimestampedIR, TimestampedTile, java.util.List[Any], TimeWindow]
    with FlinkLogging {

  @transient private[flink] var tileCodec: TileCodec = _

  @transient private var rowProcessingErrorCounter: Counter = _
  // Shared metric for errors across the entire Flink app.
  @transient private var eventProcessingErrorCounter: Counter = _
  @transient private var rowAggrTimeHistogram: Histogram = _
  @transient private var rowTileConvTimeHistogram: Histogram = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    tileCodec = new TileCodec(groupBy, inputSchema)

    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupBy.getMetaData.getName)
    rowProcessingErrorCounter = metricsGroup.counter("tiling_process_function_error")
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")
    rowAggrTimeHistogram = metricsGroup.histogram(
      "row_aggregation_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )
    rowTileConvTimeHistogram = metricsGroup.histogram(
      "row_tile_conversion_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )
  }

  /** Process events emitted from the aggregate function.
    * Output format: (keys, encoded tile IR, timestamp of the event being processed)
    */
  override def process(
      keys: java.util.List[Any],
      context: ProcessWindowFunction[TimestampedIR, TimestampedTile, java.util.List[Any], TimeWindow]#Context,
      elements: lang.Iterable[TimestampedIR],
      out: Collector[TimestampedTile]): Unit = {
    val startTime = System.currentTimeMillis()

    val windowEnd = context.window.getEnd
    val irEntry = elements.iterator.next()
    val isComplete = context.currentWatermark >= windowEnd

    val tileBytes = Try {
      val irBytes = tileCodec.makeTileIr(irEntry.ir, isComplete)
      if (enableDebug) {
        val irStr = irEntry.ir.mkString(", ")
        log(
          INFO,
          s"""
             |Flink RowAggProcessFunction created tile IR
             |keys=${keys.iterator().toScala.mkString(", ")},
             |groupBy=${groupBy.getMetaData.getName},
             |tileBytes=${java.util.Base64.getEncoder.encodeToString(irBytes)},
             |timestamp=${irEntry.latestTsMillis},
             |startProcessingTime=${irEntry.startProcessingTime},
             |ir=$irStr,
             |windowEnd=$windowEnd,
             |tileAvroSchema=${tileCodec.tileAvroSchema},
             |""".stripMargin
        )
      }
      irBytes
    }

    irEntry.rowAggrTime.foreach(rowAggrTime => rowAggrTimeHistogram.update(rowAggrTime))
    rowTileConvTimeHistogram.update(System.currentTimeMillis() - startTime)

    tileBytes match {
      case Success(v) => {
        // The timestamp should never be None here.
        out.collect(new TimestampedTile(keys, v, irEntry.latestTsMillis.get, irEntry.startProcessingTime.get))
      }
      case Failure(e) =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. Alerts should be set up on this metric.
        logThrottled(ERROR, "tile_ir_error", "Flink process error making tile IR", e)
        eventProcessingErrorCounter.inc()
        rowProcessingErrorCounter.inc()
    }
  }

}
