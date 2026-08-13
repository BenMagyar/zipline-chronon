package ai.chronon.flink

import ai.chronon.api.DataModel
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.api.DataType
import scala.concurrent.duration.DurationInt
import ai.chronon.flink.FlinkJob.watermarkStrategy
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.FlinkSource
import ai.chronon.flink.types.{AvroCodecOutput, TimestampedTile, WriteResponse}
import ai.chronon.flink.window.{
  AlwaysFireOnElementTrigger,
  BufferedProcessingTimeTrigger,
  FlinkRowAggProcessFunction,
  FlinkRowAggregationFunction,
  KeySelectorBuilder
}
import ai.chronon.online.{GroupByServingInfoParsed, TopicInfo}
import org.apache.flink.streaming.api.datastream.{DataStream, SingleOutputStreamOperator}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, WindowAssigner}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.OutputTag

object FlinkGroupByStreamingJob {
  // Default allowed lateness for entity (CDC/mutation) sources — mutations can arrive significantly late.
  val DefaultEntityAllowedLatenessSeconds: Long = 2.days.toSeconds
}

/** Flink job that processes a single streaming GroupBy and writes out the results (in the form of pre-aggregated tiles) to the KV store.
  *
  * @param eventSrc - Provider of a Flink Datastream[ ProjectedEvent ] for the given topic and groupBy. The event
  *                    consists of a field Map as well as metadata columns such as processing start time (to track
  *                    metrics). The Map contains projected columns from the source data based on projections and filters
  *                    in the GroupBy.
  * @param sinkFn - Async Flink writer function to help us write to the KV store
  * @param groupByServingInfoParsed - The GroupBy we are working with
  * @param parallelism - Parallelism to use for the Flink job
  * @param enableDebug - If enabled will log additional debug info per processed event
  */
class FlinkGroupByStreamingJob(eventSrc: FlinkSource[ProjectedEvent],
                               inputSchema: Seq[(String, DataType)],
                               sinkFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
                               val groupByServingInfoParsed: GroupByServingInfoParsed,
                               parallelism: Int,
                               props: Map[String, String],
                               topicInfo: TopicInfo,
                               enableDebug: Boolean = false)
    extends BaseFlinkJob {

  val groupByName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  logger.info(f"Creating Flink GroupBy streaming job. groupByName=${groupByName}")

  if (groupByServingInfoParsed.groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid groupBy: $groupByName. No streaming source"
    )
  }

  private val kvStoreCapacity = FlinkUtils
    .getProperty("kv_concurrency", props, topicInfo)
    .map(_.toInt)
    .getOrElse(AsyncKVStoreWriter.kvStoreConcurrency)

  // The source of our Flink application is a  topic
  val topic: String = groupByServingInfoParsed.groupBy.streamingSource.get.topic

  /** The "untiled" version of the Flink app.
    *
    *  At a high level, the operators are structured as follows:
    *    source -> Spark expression eval -> Avro conversion -> KV store writer
    *    source - Reads objects of type T (specific case class, Thrift / Proto) from a  topic
    *   Spark expression eval - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input data
    *   Avro conversion - Converts the Spark expr eval output to a form that can be written out to the KV store
    *      (PutRequest object)
    *   KV store writer - Writes the PutRequest objects to the KV store using the AsyncDataStream API
    *
    *  In this untiled version, there are no shuffles and thus this ends up being a single node in the Flink DAG
    *  (with the above 4 operators and parallelism as injected by the user).
    */
  def runGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {

    logger.info(
      f"Running Flink job for groupByName=${groupByName}, Topic=${topic}. " +
        "Tiling is disabled.")

    // we expect parallelism on the source stream to be set by the source provider
    val sourceSparkProjectedStream: DataStream[ProjectedEvent] =
      eventSrc
        .getDataStream(topic, groupByName)(env, parallelism)
        .uid(s"source-$groupByName")
        .name(s"Source for $groupByName")

    val sparkExprEvalDSWithWatermarks: DataStream[ProjectedEvent] = sourceSparkProjectedStream
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"spark-expr-eval-timestamps-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val putRecordDS: DataStream[AvroCodecOutput] = sparkExprEvalDSWithWatermarks
      .flatMap(AvroCodecFn(groupByServingInfoParsed))
      .uid(s"avro-conversion-$groupByName")
      .name(s"Avro conversion for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    AsyncKVStoreWriter.withUnorderedWaits(
      putRecordDS,
      sinkFn,
      groupByName,
      capacity = kvStoreCapacity
    )
  }

  /** The "tiled" version of the Flink app.
    *
    * The operators are structured as follows:
    *  1.  source - Reads objects of type T (specific case class, Thrift / Proto) from a  topic
    *  2. Spark expression eval - Evaluates the Spark SQL expression in the GroupBy and projects and filters the input
    *      data
    *  3. Window/tiling - This window aggregates incoming events, keeps track of the IRs, and sends them forward so
    *      they are written out to the KV store
    *  4. Avro conversion - Finishes converting the output of the window (the IRs) to a form that can be written out
    *      to the KV store (PutRequest object)
    *  5. KV store writer - Writes the PutRequest objects to the KV store using the AsyncDataStream API
    *
    *  The window causes a split in the Flink DAG, so there are two nodes, (1+2) and (3+4+5).
    */
  override def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      f"Running Flink job for groupByName=${groupByName}, Topic=${topic}. " +
        "Tiling is enabled.")

    val tilingWindowGrid = groupByServingInfoParsed.streamingTileGrid
    val tilingWindowSizeInMillis = tilingWindowGrid.spanMillis

    // we expect parallelism on the source stream to be set by the source provider
    val sourceSparkProjectedStream: DataStream[ProjectedEvent] =
      eventSrc
        .getDataStream(topic, groupByName)(env, parallelism)
        .uid(s"source-$groupByName")
        .name(s"Source for $groupByName")

    val sparkExprEvalDSAndWatermarks: DataStream[ProjectedEvent] = sourceSparkProjectedStream
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"spark-expr-eval-timestamps-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val window = TumblingEventTimeWindows
      .of(Time.milliseconds(tilingWindowSizeInMillis), Time.milliseconds(tilingWindowGrid.offsetMillis))
      .asInstanceOf[WindowAssigner[ProjectedEvent, TimeWindow]]

    // We default to the AlwaysFireOnElementTrigger which will cause the window to "FIRE" on every element.
    // An alternative is the BufferedProcessingTimeTrigger (trigger=buffered in topic info
    // or properties) which will buffer writes and only "FIRE" every X milliseconds per GroupBy & key.
    val trigger = getTrigger()

    // allowedLateness keeps window state open after the watermark passes the window end,
    // allowing late events to still be processed. Configurable via allowed_lateness_seconds property.
    // Default: 0 (disabled).
    val allowedLatenessMs = getAllowedLatenessMs()

    // We use Flink "Side Outputs" to track any late events that aren't computed.
    val tilingLateEventsTag = new OutputTag[ProjectedEvent]("tiling-late-events") {}

    // The tiling operator works the following way:
    // 1. Input: Spark expression eval (previous operator)
    // 2. Key by the entity key(s) defined in the groupby
    // 3. Window by a tumbling window
    // 4. Use our custom trigger that will "FIRE" on every element
    // 5. the AggregationFunction merges each incoming element with the current IRs which are kept in state
    //    - Each time a "FIRE" is triggered (i.e. on every event), getResult() is called and the current IRs are emitted
    // 6. A process window function does additional processing each time the AggregationFunction emits results
    //    - The only purpose of this window function is to mark tiles as closed so we can do client-side caching in SFS
    // 7. Output: TimestampedTile, containing the current IRs (Avro encoded) and the timestamp of the current element

    val tilingDS: SingleOutputStreamOperator[TimestampedTile] =
      sparkExprEvalDSAndWatermarks
        .keyBy(KeySelectorBuilder.build(groupByServingInfoParsed.groupBy))
        .window(window)
        .allowedLateness(Time.milliseconds(allowedLatenessMs))
        .trigger(trigger)
        .sideOutputLateData(tilingLateEventsTag)
        .aggregate(
          // See Flink's "ProcessWindowFunction with Incremental Aggregation"
          new FlinkRowAggregationFunction(groupByServingInfoParsed.groupBy, inputSchema, enableDebug),
          new FlinkRowAggProcessFunction(groupByServingInfoParsed.groupBy, inputSchema, enableDebug)
        )
        .uid(s"tiling-01-$groupByName")
        .name(s"Tiling for $groupByName")
        .setParallelism(sourceSparkProjectedStream.getParallelism)

    // Track late events
    tilingDS
      .getSideOutput(tilingLateEventsTag)
      .flatMap(new LateEventCounter(groupByName))
      .uid(s"tiling-side-output-01-$groupByName")
      .name(s"Tiling Side Output Late Data for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val putRecordDS: DataStream[AvroCodecOutput] = tilingDS
      .flatMap(TiledAvroCodecFn(groupByServingInfoParsed, tilingWindowSizeInMillis, enableDebug))
      .uid(s"avro-conversion-01-$groupByName")
      .name(s"Avro conversion for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    AsyncKVStoreWriter.withUnorderedWaits(
      putRecordDS,
      sinkFn,
      groupByName,
      capacity = kvStoreCapacity
    )
  }

  private def getTrigger(): Trigger[ProjectedEvent, TimeWindow] = {
    FlinkUtils
      .getProperty("trigger", props, topicInfo)
      .map {
        case "always_fire" => new AlwaysFireOnElementTrigger()
        case "buffered"    => new BufferedProcessingTimeTrigger(100L)
        case t =>
          throw new IllegalArgumentException(s"Unsupported trigger type: $t. Supported: 'always_fire', 'buffered'")
      }
      .getOrElse(new AlwaysFireOnElementTrigger())
  }

  private[flink] def getAllowedLatenessMs(): Long = {
    val defaultSeconds = groupByServingInfoParsed.groupBy.dataModel match {
      case DataModel.ENTITIES => FlinkGroupByStreamingJob.DefaultEntityAllowedLatenessSeconds
      case _                  => 0L
    }
    FlinkUtils.getAllowedLatenessMs(props, topicInfo, defaultSeconds)
  }
}
