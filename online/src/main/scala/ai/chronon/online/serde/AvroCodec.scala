/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online.serde

import ai.chronon.api.{DataType, Row, StructType}
import ai.chronon.api.ScalaJavaConversions._
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io._
import com.linkedin.avro.fastserde.{FastGenericDatumReader, FastGenericDatumWriter, FastSerdeCache}
import org.slf4j.LoggerFactory

import java.util.concurrent.{ConcurrentHashMap, TimeUnit, TimeoutException}

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConverters._

class AvroCodec(val schemaStr: String, val writerSchemaStr: Option[String] = None) extends Serializable {
  @transient private lazy val parser = new Schema.Parser()
  @transient lazy val schema: Schema = parser.parse(schemaStr)

  // we reuse a lot of intermediate
  // lazy vals so that spark can serialize & ship the codec to executors
  @transient private lazy val datumWriter = new FastGenericDatumWriter[GenericRecord](schema)
  @transient private lazy val datumReader = writerSchemaStr match {
    case Some(wStr) =>
      // Writer schema differs from reader schema — use Avro's resolution logic to handle
      // schema evolution (fills defaults for missing fields, ignores unknown fields)
      new FastGenericDatumReader[GenericRecord](new Schema.Parser().parse(wStr), schema)
    case None =>
      new FastGenericDatumReader[GenericRecord](schema)
  }

  @transient private lazy val outputStream = new ByteArrayOutputStream()
  @transient private var jsonEncoder: JsonEncoder = null
  val fieldNames: Array[String] = schema.getFields.toScala.map(_.name()).toArray
  @transient lazy val chrononSchema: DataType = AvroConversions.toChrononSchema(schema)

  @transient private var binaryEncoder: BinaryEncoder = null
  @transient private var decoder: BinaryDecoder = null
  @transient lazy val schemaElems: Array[Field] = schema.getFields.toScala.toArray
  @transient lazy val toChrononRowFunc: Any => Array[Any] =
    AvroConversions.genericRecordToChrononRowConverter(chrononSchema.asInstanceOf[StructType])

  /** Materialize the lazy, thread-confined state used by serving reads after process-global FastSerde generation. */
  private[online] def prepareForRead(): Unit = {
    datumReader
    toChrononRowFunc
    ()
  }

  /** Materialize the lazy, thread-confined state used by key and V2 response writes. */
  private[online] def prepareForWrite(): Unit = {
    datumWriter
    schemaElems
    outputStream
    ()
  }

  /** Exercise the generated reader and writer on this thread, rather than only materializing their lazy wrappers.
    * Startup uses a schema-valid synthetic value because live sample bytes are not available for every serving schema.
    */
  private[online] def exerciseForWrite(): Unit = {
    val record = AvroCodec.syntheticValue(schema, 0, Set.empty).asInstanceOf[GenericRecord]
    encodeBinary(record)
    ()
  }

  private[online] def exerciseForRead(writerSchema: String): Unit = {
    val writer = new Schema.Parser().parse(writerSchema)
    val record = AvroCodec.syntheticValue(writer, 0, Set.empty).asInstanceOf[GenericRecord]
    val output = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get.binaryEncoder(output, null)
    new GenericDatumWriter[GenericRecord](writer).write(record, encoder)
    encoder.flush()
    decodeRow(output.toByteArray)
    ()
  }

  def encode(valueMap: Map[String, AnyRef]): Array[Byte] = {
    val record = new GenericData.Record(schema)
    schemaElems.foreach { field =>
      record.put(field.name(), AvroConversions.toAvroValue(valueMap.get(field.name()).orNull, field.schema()))
    }
    encodeBinary(record)
  }

  def encode(row: Row): Array[Byte] = {
    val record = new GenericData.Record(schema)
    for (i <- 0 until row.length) {
      record.put(i, row.get(i))
    }
    encodeBinary(record)
  }

  def encodeBinary(record: GenericRecord): Array[Byte] = {
    binaryEncoder = EncoderFactory.get.binaryEncoder(outputStream, binaryEncoder)
    encodeRecord(record, binaryEncoder)
  }

  def encodeRecord(record: GenericRecord, reusableEncoder: Encoder): Array[Byte] = {
    outputStream.reset()
    datumWriter.write(record, reusableEncoder)
    reusableEncoder.flush()
    outputStream.flush()
    outputStream.toByteArray
  }

  def encodeJson(record: GenericRecord): String = {
    jsonEncoder = EncoderFactory.get.jsonEncoder(schema, outputStream)
    new String(encodeRecord(record, jsonEncoder))
  }

  def decode(bytes: Array[Byte]): GenericRecord = {

    if (bytes == null) return null
    val inputStream = new SeekableByteArrayInput(bytes)
    inputStream.reset()
    decoder = DecoderFactory.get.directBinaryDecoder(inputStream, decoder)
    datumReader.read(null, decoder)
  }

  def decodeRow(bytes: Array[Byte]): Array[Any] = toChrononRowFunc(decode(bytes))

  def decodeRow(bytes: Array[Byte], millis: Long, mutation: Boolean = false): ArrayRow =
    new ArrayRow(decodeRow(bytes), millis, mutation)

  def decodeArray(bytes: Array[Byte]): Array[Any] = {
    if (bytes == null) return null
    toChrononRowFunc(decode(bytes))
  }

  def decodeMap(bytes: Array[Byte]): Map[String, AnyRef] = {
    if (bytes == null) return null

    val decoded = decodeArray(bytes)
    fieldNames.iterator.zip(decoded.iterator.map(_.asInstanceOf[AnyRef])).toMap
  }
}

/** Consumed by row aggregator after decoding.
  * Mutations follow the same schema as input for value indices. However there are two main differences.
  *  * ts and reversal columns are required for computation
  *  * Mutation ts takes on the role of ts.
  * Since the schema is the same with the sole difference of the added columns, we add these columns on the tail
  * of the Array and extract them accordingly.
  * i.e. for mutations: reversal index = ArrayRow.length - (Constants.MutationAvroColumns.length - (index of reversal in Constants.MutationAvroColumns)
  */
class ArrayRow(values: Array[Any], millis: Long, mutation: Boolean = false) extends Row {
  override def get(index: Int): Any = values(index)

  override def ts: Long = if (mutation) values(values.length - 2).asInstanceOf[Long] else millis

  override def isBefore: Boolean = if (mutation) values(values.length - 1).asInstanceOf[Boolean] else false

  override def mutationTs: Long = millis

  override val length: Int = values.length
}

object AvroCodec {
  final case class FastSerdePreparation(readerCount: Int,
                                        writerCount: Int,
                                        fallbackReaderCount: Int,
                                        fallbackWriterCount: Int)

  private val logger = LoggerFactory.getLogger(getClass)

  // creating new codecs is expensive - so we want to do it once per process
  // but at the same-time we want to avoid contention across threads - hence thread-local
  private val codecMap: ConcurrentHashMap[String, ThreadLocal[AvroCodec]] =
    new ConcurrentHashMap[String, ThreadLocal[AvroCodec]]

  def ofThreaded(readerSchemaStr: String, writerSchemaStr: Option[String] = None): ThreadLocal[AvroCodec] = {
    val cacheKey = writerSchemaStr.fold(readerSchemaStr)(w => s"$w|$readerSchemaStr")
    codecMap.computeIfAbsent(cacheKey,
                             _ =>
                               new ThreadLocal[AvroCodec] {
                                 override def initialValue(): AvroCodec =
                                   new AvroCodec(readerSchemaStr, writerSchemaStr)
                               })
  }

  def of(schemaStr: String): AvroCodec = ofThreaded(schemaStr).get()

  /** Finish process-global FastSerde generation for known serving schemas before a process accepts traffic.
    *
    * FastSerde's generated-class cache is process-global even though AvroCodec instances are thread-local. A
    * completed generic fallback is valid; the important startup invariant is that no compiler work remains queued
    * after this method returns. Callers may separately initialize each worker's thread-local AvroCodec state.
    */
  def prepareFastSerde(readerSchemas: Iterable[(String, Option[String])],
                       writerSchemas: Iterable[String],
                       timeoutMillis: Long): FastSerdePreparation = {
    require(timeoutMillis > 0L, s"FastSerde preparation timeout must be positive, got $timeoutMillis")

    val distinctReaders = readerSchemas.toVector.distinct.map { case (readerSchemaStr, writerSchemaStr) =>
      val readerSchema = new Schema.Parser().parse(readerSchemaStr)
      val writerSchema = writerSchemaStr.map(value => new Schema.Parser().parse(value)).getOrElse(readerSchema)
      writerSchema -> readerSchema
    }
    val distinctWriters = writerSchemas.toVector.distinct.map(value => new Schema.Parser().parse(value))
    val cache = FastSerdeCache.getDefaultInstance
    val deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)

    // Use the synchronous cache lookups to install one cold placeholder per schema. The async deserializer API starts
    // another generation task when a normal read has already installed an in-progress placeholder, which can double
    // the compiler backlog. Re-reading these cache entries observes the generated implementation without rescheduling.
    var deserializers = distinctReaders.map { case (writerSchema, readerSchema) =>
      cache.getFastGenericDeserializer(writerSchema, readerSchema)
    }
    var serializers = distinctWriters.map(schema => cache.getFastGenericSerializer(schema))
    while (
      deserializers.exists(deserializer => !deserializer.hasDynamicClassGenerationDone()) ||
      serializers.exists(serializer => !serializer.hasDynamicClassGenerationDone())
    ) {
      val remaining = remainingNanos(deadlineNanos)
      TimeUnit.NANOSECONDS.sleep(math.min(remaining, TimeUnit.MILLISECONDS.toNanos(10L)))
      deserializers = distinctReaders.map { case (writerSchema, readerSchema) =>
        cache.getFastGenericDeserializer(writerSchema, readerSchema)
      }
      serializers = distinctWriters.map(schema => cache.getFastGenericSerializer(schema))
    }

    val fallbackReaders = deserializers.count(deserializer => !deserializer.isBackedByGeneratedClass())
    val fallbackWriters = serializers.count(serializer => !serializer.isBackedByGeneratedClass())
    if (fallbackReaders > 0 || fallbackWriters > 0) {
      logger.warn(
        s"FastSerde preparation completed with generic fallback: readers=$fallbackReaders/${deserializers.size}, " +
          s"writers=$fallbackWriters/${serializers.size}")
    }

    FastSerdePreparation(deserializers.size, serializers.size, fallbackReaders, fallbackWriters)
  }

  private def remainingNanos(deadlineNanos: Long): Long = {
    val remaining = deadlineNanos - System.nanoTime()
    if (remaining <= 0L) throw new TimeoutException("Timed out waiting for FastSerde generation")
    remaining
  }

  private def syntheticValue(schema: Schema, depth: Int, recordStack: Set[String]): AnyRef = {
    require(depth < 32, s"Synthetic Avro warmup exceeded maximum schema depth at ${schema.getFullName}")
    schema.getType match {
      case Schema.Type.NULL    => null
      case Schema.Type.BOOLEAN => java.lang.Boolean.TRUE
      case Schema.Type.INT     => java.lang.Integer.valueOf(1)
      case Schema.Type.LONG    => java.lang.Long.valueOf(1L)
      case Schema.Type.FLOAT   => java.lang.Float.valueOf(1.25f)
      case Schema.Type.DOUBLE  => java.lang.Double.valueOf(1.25d)
      case Schema.Type.STRING  => "chronon-warm"
      case Schema.Type.BYTES   => ByteBuffer.wrap(Array[Byte](1, 2, 3))
      case Schema.Type.ENUM    => new GenericData.EnumSymbol(schema, schema.getEnumSymbols.get(0))
      case Schema.Type.FIXED   => new GenericData.Fixed(schema, Array.fill[Byte](schema.getFixedSize)(1))
      case Schema.Type.ARRAY =>
        val values = new util.ArrayList[AnyRef](1)
        values.add(syntheticValue(schema.getElementType, depth + 1, recordStack))
        values
      case Schema.Type.MAP =>
        val values = new util.HashMap[String, AnyRef](1)
        values.put("chronon-warm", syntheticValue(schema.getValueType, depth + 1, recordStack))
        values
      case Schema.Type.UNION =>
        val branches = schema.getTypes
        val branch = branches.iterator().asScala.find(_.getType != Schema.Type.NULL).getOrElse(branches.get(0))
        syntheticValue(branch, depth + 1, recordStack)
      case Schema.Type.RECORD =>
        val fullName = schema.getFullName
        if (recordStack(fullName)) null
        else {
          val record = new GenericData.Record(schema)
          val nextStack = recordStack + fullName
          schema.getFields.iterator().asScala.foreach { field =>
            record.put(field.name(), syntheticValue(field.schema(), depth + 1, nextStack))
          }
          record
        }
    }
  }
}
