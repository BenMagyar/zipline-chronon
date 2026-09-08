package ai.chronon.online

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{DataType, GroupBy, StructType}
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import org.apache.avro.generic.GenericData

/** Encodes/decodes the windowed mega tile IR (Array[Any]) to/from bytes.
  * Unlike TileCodec which uses the unwindowed base aggregator schema,
  * MegaTileCodec uses the windowed aggregator schema — one IR slot per (agg, window) pair.
  *
  * Also provides encodeBaseIr/decodeBaseIr for encoding per-tile base (unwindowed) IRs.
  * These are used by Flink state persistence where tiles store base IRs, not windowed IRs.
  */
class MegaTileCodec(groupBy: GroupBy, inputSchema: Seq[(String, DataType)]) {

  // Windowed: one slot per (agg, window) pair — for mega tile entries
  val rowAggregator: RowAggregator = TileCodec.buildWindowedRowAggregator(groupBy, inputSchema)
  val irSchema: StructType = StructType.from(s"${groupBy.getMetaData.cleanName}_MEGA_TILE_IR", rowAggregator.irSchema)
  val avroSchema: String = AvroConversions.fromChrononSchema(irSchema).toString()

  private val encodeFn: Any => Array[Byte] = AvroConversions.encodeBytes(irSchema, null)

  @transient private lazy val rowConverter: Any => Array[Any] =
    AvroConversions.genericRecordToChrononRowConverter(irSchema)

  def encode(ir: Array[Any]): Array[Byte] = encodeFn(rowAggregator.normalize(ir))

  // AvroCodec.of returns a per-thread cached codec keyed by schema string, so this avoids
  // cross-thread decoder reuse without reparsing the schema on every decode call.
  private def avroCodec: AvroCodec = AvroCodec.of(avroSchema)

  def decode(bytes: Array[Byte]): Array[Any] = {
    val record = avroCodec.decode(bytes).asInstanceOf[GenericData.Record]
    val ir = rowConverter(record)
    rowAggregator.denormalize(ir)
  }

  // Base (unwindowed): one slot per aggregation bucket — for individual tile state
  val baseRowAggregator: RowAggregator = TileCodec.buildRowAggregator(groupBy, inputSchema)
  private val baseIrSchema: StructType =
    StructType.from(s"${groupBy.getMetaData.cleanName}_BASE_TILE_IR", baseRowAggregator.irSchema)
  private val baseAvroSchema: String = AvroConversions.fromChrononSchema(baseIrSchema).toString()
  private val baseEncodeFn: Any => Array[Byte] = AvroConversions.encodeBytes(baseIrSchema, null)

  @transient private lazy val baseRowConverter: Any => Array[Any] =
    AvroConversions.genericRecordToChrononRowConverter(baseIrSchema)

  def encodeBaseIr(ir: Array[Any]): Array[Byte] = baseEncodeFn(baseRowAggregator.normalize(ir))

  // Same per-thread cache behavior as avroCodec above.
  private def baseAvroCodec: AvroCodec = AvroCodec.of(baseAvroSchema)

  def decodeBaseIr(bytes: Array[Byte]): Array[Any] = {
    val record = baseAvroCodec.decode(bytes).asInstanceOf[GenericData.Record]
    val ir = baseRowConverter(record)
    baseRowAggregator.denormalize(ir)
  }
}
