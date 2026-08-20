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
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import com.linkedin.avro.fastserde.FastGenericDatumReader
import com.linkedin.avro.fastserde.FastGenericDatumWriter
import com.linkedin.avro.fastserde.FastSerdeCache
import java.util.concurrent.ConcurrentHashMap

import java.io.ByteArrayOutputStream
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

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

  private val PollIntervalMillis = 50L

  /** Best-effort triggers FastSerde class generation (see FastSerdeCache) for writerCodecs' schemas
    * (serializer direction) and readerCodecs' schemas (deserializer direction), without needing any real
    * data - operates on each codec's Schema directly rather than calling its encode/decode, so there's
    * nothing to construct or fail on non-nullable fields, and no risk of touching a live, shared codec's
    * mutable encode/decode buffers.
    *
    * The triggers above fire synchronously (cheap - just a cache registration each), but waitForCompileMillis,
    * if positive, needs the returned Future to settle before every touched schema's build attempt has
    * finished - hasDynamicClassGenerationDone flips true once FastSerdeCache's background build finishes,
    * whether it lands on a real generated class or falls back to the vanilla Avro path after a compile
    * failure - or the deadline elapses, whichever comes first. This never blocks a calling thread while
    * waiting: the poll between checks is scheduled via CompletableFuture.delayedExecutor (a shared, lightweight
    * JDK timer), not Thread.sleep, so a caller can fire many of these concurrently without needing one thread
    * per in-flight wait. waitForCompileMillis = 0 (default) returns an already-completed Future - nothing to
    * wait for.
    *
    * cache defaults to the real, process-wide FastSerdeCache singleton; pass an isolated instance instead to
    * avoid cross-test schema-fingerprint collisions or to inspect exactly what got touched.
    */
  def warmUp(writerCodecs: Seq[AvroCodec],
             readerCodecs: Seq[AvroCodec],
             waitForCompileMillis: Long = 0L,
             cache: FastSerdeCache = FastSerdeCache.getDefaultInstance()): Future[Unit] = {
    writerCodecs.foreach(codec => Try(cache.getFastGenericSerializer(codec.schema)))
    readerCodecs.foreach(codec => Try(cache.getFastGenericDeserializer(codec.schema, codec.schema)))

    if (waitForCompileMillis <= 0) {
      Future.successful(())
    } else {
      waitUntilCompiled(writerCodecs, readerCodecs, cache, System.currentTimeMillis() + waitForCompileMillis)
    }
  }

  // Non-blocking: each "poll" is a callback scheduled after PollIntervalMillis on a shared JDK timer
  // (CompletableFuture.delayedExecutor), not a sleeping thread - so waiting on N of these concurrently costs
  // N lightweight scheduled callbacks, not N held threads.
  private def waitUntilCompiled(writerCodecs: Seq[AvroCodec],
                                readerCodecs: Seq[AvroCodec],
                                cache: FastSerdeCache,
                                deadlineMillis: Long): Future[Unit] = {
    val ready = writerCodecs.forall(codec => isSerializerCompiled(cache, codec.schema)) &&
      readerCodecs.forall(codec => isDeserializerCompiled(cache, codec.schema))

    if (ready || System.currentTimeMillis() >= deadlineMillis) {
      Future.successful(())
    } else {
      implicit val delayedEc: ExecutionContext =
        ExecutionContext.fromExecutor(CompletableFuture.delayedExecutor(PollIntervalMillis, TimeUnit.MILLISECONDS))
      Future(()).flatMap(_ => waitUntilCompiled(writerCodecs, readerCodecs, cache, deadlineMillis))
    }
  }

  // Defaults to "ready" on any lookup failure so a transient error can't turn into an unbounded-feeling wait -
  // the outer deadline in warmUp is still the only thing that can end the loop early either way.
  private def isSerializerCompiled(cache: FastSerdeCache, schema: Schema): Boolean =
    Try(cache.getFastGenericSerializer(schema).hasDynamicClassGenerationDone).getOrElse(true)

  private def isDeserializerCompiled(cache: FastSerdeCache, schema: Schema): Boolean =
    Try(cache.getFastGenericDeserializer(schema, schema).hasDynamicClassGenerationDone).getOrElse(true)
}
