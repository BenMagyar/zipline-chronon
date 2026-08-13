package ai.chronon.online.test

import ai.chronon.api._
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import com.linkedin.avro.fastserde.{FastGenericDatumReader, FastSerdeCache}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit

class LargeSchemaFastSerdeTest extends AnyFlatSpec with Matchers {

  "FastGenericDatumReader" should "handle schemas with many distinct record-typed fields" in {
    // Reproduces JVM "code too large" error in fastserde constructor when
    // a schema has many fields, each with a unique record type in a union.
    // Each unique record type creates a separate schema var in the generated
    // constructor, which can exceed the 64KB JVM method bytecode limit.
    val n = 1000
    val subFieldCount = 5

    val chrononFields = (0 until n).map { i =>
      val innerFields = (0 until subFieldCount).map { j =>
        StructField(s"sub_$j", StringType)
      }.toArray
      StructField(s"field_$i", StructType(s"InnerRecord_$i", innerFields))
    }.toArray

    val outerSchema = StructType("LargeTestSchema", chrononFields)
    val avroSchema = AvroConversions.fromChrononSchema(outerSchema)

    // Preparation uses the same process-global cache as every thread-local AvroCodec and must not return while this
    // large reader or writer is still compiling.
    val preparation = AvroCodec.prepareFastSerde(
      Seq(avroSchema.toString -> Option.empty[String]),
      Seq(avroSchema.toString),
      TimeUnit.MINUTES.toMillis(2L))
    preparation.readerCount shouldBe 1
    preparation.writerCount shouldBe 1
    preparation.fallbackReaderCount shouldBe 0
    preparation.fallbackWriterCount shouldBe 0

    // This is the line that fails without the constructor splitting fix — FastSerde generates a Java class whose
    // constructor exceeds 64KB. It must now observe the generated implementation immediately.
    val reader = new FastGenericDatumReader[GenericData.Record](avroSchema)
    val generatedReader = reader.getFastDeserializer.get(2L, TimeUnit.MINUTES)
    generatedReader.hasDynamicClassGenerationDone() shouldBe true
    generatedReader.isBackedByGeneratedClass() shouldBe true

    // Verify round-trip through AvroCodec and that both sides use generated implementations.
    val codec = new AvroCodec(avroSchema.toString)
    val record = new GenericData.Record(avroSchema)
    (0 until n).foreach { i =>
      val innerSchema = avroSchema.getField(s"field_$i").schema().getTypes.get(1)
      val inner = new GenericData.Record(innerSchema)
      (0 until subFieldCount).foreach { j =>
        inner.put(s"sub_$j", s"val_${i}_$j")
      }
      record.put(s"field_$i", inner)
    }

    val encoded = codec.encodeBinary(record)
    val decoded = codec.decode(encoded)

    val inner0 = decoded.get("field_0").asInstanceOf[GenericData.Record]
    inner0.get("sub_0").toString shouldBe "val_0_0"
    val inner999 = decoded.get("field_999").asInstanceOf[GenericData.Record]
    inner999.get("sub_4").toString shouldBe "val_999_4"

    val cache = FastSerdeCache.getDefaultInstance
    var serializer = cache.getFastGenericSerializer(avroSchema)
    val deadlineNanos = System.nanoTime() + TimeUnit.MINUTES.toNanos(2L)
    while (!serializer.hasDynamicClassGenerationDone() && System.nanoTime() < deadlineNanos) {
      Thread.sleep(10L)
      serializer = cache.getFastGenericSerializer(avroSchema)
    }

    serializer.hasDynamicClassGenerationDone() shouldBe true
    serializer.isBackedByGeneratedClass() shouldBe true
  }
}
