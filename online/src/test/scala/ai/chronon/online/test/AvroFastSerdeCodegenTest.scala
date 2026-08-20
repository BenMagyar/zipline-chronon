package ai.chronon.online.test

import ai.chronon.api.{StringType, StructField, StructType}
import ai.chronon.online.serde.AvroConversions
import com.linkedin.avro.fastserde.FastSerdeCache
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Regression test: the vendored online/avro-fastserde-0.4.39-SNAPSHOT.jar (added in #1622 to fix
// "code too large" for big schemas) generates code that references
// com.linkedin.avroutil1.compatibility.backports.SpecificRecordBaseExt, a class that only exists from
// helper-all:0.4.39 onward. online/package.mill's helper-all pin was left at 0.4.25 (the version the
// pre-vendoring Maven-resolved avro-fastserde artifact used to pull in transitively), so every fast
// codegen attempt since #1622 hit NoClassDefFoundError.
//
// That failure was invisible in production: FastSerdeCache.getFastGenericSerializer/Deserializer (the
// methods actually used on the serving path) return an interim reflection-based GenericDatumReader/Writer
// immediately and compile in the background, catching only Exception - not the LinkageError this throws -
// so the cache silently kept serving the slow fallback forever, with no error logged. Round-tripping data
// through that path (see LargeSchemaFastSerdeTest) still succeeds, so it doesn't catch this either.
//
// buildFastGenericSerializer/Deserializer compile synchronously and throw on failure, which is what
// surfaces the break here.
class AvroFastSerdeCodegenTest extends AnyFlatSpec with Matchers {

  private val schema = AvroConversions.fromChrononSchema(
    StructType("AvroFastSerdeCodegenSmokeSchema", Array(StructField("id", StringType)))
  )

  it should "generate and link a real fast generic serializer, not just the reflection fallback" in {
    val serializer = FastSerdeCache.getDefaultInstance().buildFastGenericSerializer(schema)
    serializer.isBackedByGeneratedClass shouldBe true
  }

  it should "generate and link a real fast generic deserializer, not just the reflection fallback" in {
    val deserializer = FastSerdeCache.getDefaultInstance().buildFastGenericDeserializer(schema, schema)
    deserializer.isBackedByGeneratedClass shouldBe true
  }
}
