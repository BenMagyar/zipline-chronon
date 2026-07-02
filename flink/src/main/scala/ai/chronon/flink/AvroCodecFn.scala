package ai.chronon.flink

import ai.chronon.api.DataModel
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Extensions.WindowUtils
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.TilingUtils
import ai.chronon.api.{StructType => ChrononStructType}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.types.AvroCodecOutput
import ai.chronon.flink.types.TimestampedTile
import ai.chronon.online.serde.AvroConversions
import ai.chronon.online.GroupByServingInfoParsed
import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.util.Collector

/** Base class for the Avro conversion Flink operator.
  *
  * Subclasses should override the RichFlatMapFunction methods (flatMap) and groupByServingInfoParsed.
  *
  * @tparam IN The input data type which contains the data to be avro-converted to bytes.
  * @tparam OUT The output data type (generally a PutRequest).
  */
sealed abstract class BaseAvroCodecFn[IN, OUT] extends RichFlatMapFunction[IN, OUT] with FlinkLogging {
  def groupByServingInfoParsed: GroupByServingInfoParsed

  @transient protected var avroConversionErrorCounter: Counter = _
  // Shared metric for errors across the entire Flink app.
  @transient protected var eventProcessingErrorCounter: Counter = _

  protected lazy val streamingDataset: String = groupByServingInfoParsed.groupBy.streamingDataset

  protected lazy val (keyToBytes, valueToBytes): (Any => Array[Byte], Any => Array[Byte]) =
    getKVSerializers(groupByServingInfoParsed)
  protected lazy val (keyColumns, valueColumns, eventTimeColumn) =
    SparkExpressionEval.buildKeyValueEventTimeColumns(groupByServingInfoParsed.groupBy)

  protected lazy val extraneousRecord: Any => Array[Any] = {
    case x: Map[_, _] if x.keys.forall(_.isInstanceOf[String]) =>
      x.toArray.flatMap { case (key, value) => Array(key, value) }
  }

  private lazy val getKVSerializers = (
    groupByServingInfoParsed: GroupByServingInfoParsed
  ) => {
    val keyZSchema: ChrononStructType = groupByServingInfoParsed.keyChrononSchema
    val valueZSchema: ChrononStructType = groupByServingInfoParsed.groupBy.dataModel match {
      case DataModel.EVENTS   => groupByServingInfoParsed.valueChrononSchema
      case DataModel.ENTITIES => groupByServingInfoParsed.mutationValueChrononSchema
    }

    (
      AvroConversions.encodeBytes(keyZSchema, extraneousRecord),
      AvroConversions.encodeBytes(valueZSchema, extraneousRecord)
    )
  }
}

/** A Flink function that is responsible for converting the Spark expr eval output and converting that to a form
  * that can be written out to the KV store (PutRequest object)
  * @param groupByServingInfoParsed The GroupBy we are working with
  */
case class AvroCodecFn(groupByServingInfoParsed: GroupByServingInfoParsed)
    extends BaseAvroCodecFn[ProjectedEvent, AvroCodecOutput] {

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupByServingInfoParsed.groupBy.getMetaData.getName)
    avroConversionErrorCounter = metricsGroup.counter("avro_conversion_errors")
  }

  override def close(): Unit = super.close()

  override def flatMap(value: ProjectedEvent, out: Collector[AvroCodecOutput]): Unit =
    try {
      out.collect(avroConvertMapToPutRequest(value))
    } catch {
      case e: Exception =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. Alerts should be set up on this metric.
        logThrottled(ERROR, "avro_codec_error", "Error converting to Avro bytes", e)
        eventProcessingErrorCounter.inc()
        avroConversionErrorCounter.inc()
    }

  def avroConvertMapToPutRequest(value: ProjectedEvent): AvroCodecOutput = {
    val in = value.fields
    val tsMills = in(eventTimeColumn).asInstanceOf[Long]
    val keyBytes = keyToBytes(keyColumns.map(in(_)))
    val valueBytes = valueToBytes(valueColumns.map(in(_)))
    new AvroCodecOutput(keyBytes, valueBytes, streamingDataset, tsMills, value.startProcessingTimeMillis)
  }
}

/** A Flink function that is responsible for converting an array of pre-aggregates (aka a tile) to a form
  * that can be written out to the KV store (PutRequest object).
  *
  * @param groupByServingInfoParsed The GroupBy we are working with
  * @param tilingWindowSizeMs The size of the tiling window in milliseconds
  */
case class TiledAvroCodecFn(groupByServingInfoParsed: GroupByServingInfoParsed,
                            tilingWindowSizeMs: Long,
                            enableDebug: Boolean = false)
    extends BaseAvroCodecFn[TimestampedTile, AvroCodecOutput] {

  @transient private var tileAvroCodecTimeHistogram: Histogram = _

  override def open(configuration: Configuration): Unit = {
    super.open(configuration)
    val metricsGroup = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", groupByServingInfoParsed.groupBy.getMetaData.getName)
    avroConversionErrorCounter = metricsGroup.counter("avro_conversion_errors")
    eventProcessingErrorCounter = metricsGroup.counter("event_processing_error")
    tileAvroCodecTimeHistogram = metricsGroup.histogram(
      "tile_avro_codec_time",
      new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir()))
    )
  }
  override def close(): Unit = super.close()

  override def flatMap(value: TimestampedTile, out: Collector[AvroCodecOutput]): Unit =
    try {
      out.collect(avroConvertTileToPutRequest(value))
    } catch {
      case e: Exception =>
        // To improve availability, we don't rethrow the exception. We just drop the event
        // and track the errors in a metric. Alerts should be set up on this metric.
        logThrottled(ERROR, "avro_codec_error", "Error converting to Avro bytes", e)
        eventProcessingErrorCounter.inc()
        avroConversionErrorCounter.inc()
    }

  def avroConvertTileToPutRequest(in: TimestampedTile): AvroCodecOutput = {
    val startTime = System.currentTimeMillis()

    val tsMills = in.latestTsMillis

    // 'keys' is a map of (key name in schema -> key value), e.g. Map("card_number" -> "4242-4242-4242-4242")
    // We convert to AnyRef because Chronon expects an AnyRef (for scala <> java interoperability reasons).
    val keys: Map[String, AnyRef] = keyColumns.zip(in.keys.toScala.map(_.asInstanceOf[AnyRef])).toMap
    val entityKeyBytes = keyToBytes(in.keys.toArray)

    val tileStart = WindowUtils.windowStartMillis(tsMills, tilingWindowSizeMs)
    val tileKey = TilingUtils.buildTileKey(streamingDataset, entityKeyBytes, Some(tilingWindowSizeMs), Some(tileStart))

    val valueBytes = in.tileBytes

    if (enableDebug) {
      log(
        INFO,
        s"""
          |Avro converting tile to PutRequest - tile=${in}
          |groupBy=${groupByServingInfoParsed.groupBy.getMetaData.getName} tsMills=$tsMills keys=$keys
          |keyBytes=${java.util.Base64.getEncoder.encodeToString(entityKeyBytes)}
          |valueBytes=${java.util.Base64.getEncoder.encodeToString(valueBytes)}
          |startProcessingTime=${in.startProcessingTime}
          |streamingDataset=$streamingDataset""".stripMargin
      )
    }

    val tileKeyBytes = TilingUtils.serializeTileKey(tileKey)
    val codecOutput = new AvroCodecOutput(tileKeyBytes, valueBytes, streamingDataset, tsMills, in.startProcessingTime)
    tileAvroCodecTimeHistogram.update(System.currentTimeMillis() - startTime)
    codecOutput
  }
}
