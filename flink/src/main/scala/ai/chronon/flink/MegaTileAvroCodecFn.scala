package ai.chronon.flink

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{TilingUtils, TsUtils}
import ai.chronon.api.{StructType => ChrononStructType}
import ai.chronon.flink.types.{AvroCodecOutput, TimestampedTile}
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.serde.AvroConversions
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.Counter
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

/** Converts mega tile output (TimestampedTile) to KV PutRequests (AvroCodecOutput).
  * Key: TileKey(streamingDataset, entityKeyBytes, DayMillis, dayStart).
  * Value: mega tile IR bytes (passthrough from MegaTileProcessFunction).
  */
case class MegaTileAvroCodecFn(groupByServingInfoParsed: GroupByServingInfoParsed, enableDebug: Boolean = false)
    extends RichFlatMapFunction[TimestampedTile, AvroCodecOutput] {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  @transient private var avroConversionErrorCounter: Counter = _
  @transient private var eventProcessingErrorCounter: Counter = _

  private val DayMillis: Long = 24 * 3600 * 1000L
  private lazy val streamingDataset: String = groupByServingInfoParsed.groupBy.streamingDataset
  private lazy val keyColumns: Array[String] =
    SparkExpressionEval.buildKeyValueEventTimeColumns(groupByServingInfoParsed.groupBy)._1
  private lazy val keyToBytes: Any => Array[Byte] = {
    val keyZSchema: ChrononStructType = groupByServingInfoParsed.keyChrononSchema
    AvroConversions.encodeBytes(keyZSchema,
                                {
                                  case x: Map[_, _] if x.keys.forall(_.isInstanceOf[String]) =>
                                    x.toArray.flatMap { case (key, value) => Array(key, value) }
                                })
  }

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupByServingInfoParsed.groupBy.getMetaData.getName)
    avroConversionErrorCounter = metricsGroup.counter("avro_conversion_errors")
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")
  }

  override def close(): Unit = super.close()

  override def flatMap(value: TimestampedTile, out: Collector[AvroCodecOutput]): Unit =
    try {
      out.collect(avroConvertMegaTileToPutRequest(value))
    } catch {
      case e: Exception =>
        logger.error("Error converting mega tile to PutRequest", e)
        eventProcessingErrorCounter.inc()
        avroConversionErrorCounter.inc()
    }

  def avroConvertMegaTileToPutRequest(in: TimestampedTile): AvroCodecOutput = {
    val tsMills = in.latestTsMillis
    val entityKeyBytes = keyToBytes(in.keys.toArray)

    val dayStart = TsUtils.round(tsMills, DayMillis)
    val tileKey = TilingUtils.buildTileKey(streamingDataset, entityKeyBytes, Some(DayMillis), Some(dayStart))
    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)

    if (enableDebug) {
      logger.info(
        s"Mega tile PutRequest: groupBy=${groupByServingInfoParsed.groupBy.getMetaData.getName} " +
          s"tsMills=$tsMills dayStart=$dayStart valueBytes=${in.tileBytes.length} bytes")
    }

    new AvroCodecOutput(tileKeyBytes, in.tileBytes, streamingDataset, tsMills, in.startProcessingTime)
  }
}
