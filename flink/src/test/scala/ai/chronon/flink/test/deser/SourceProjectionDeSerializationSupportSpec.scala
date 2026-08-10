package ai.chronon.flink.test.deser

import ai.chronon.api.ScalaJavaConversions.ListOps
import ai.chronon.flink.deser.{DeserializationSchemaBuilder, ProjectedEvent, SourceProjectionDeserializationSchema}
import ai.chronon.flink.test.UserAvroSchema
import ai.chronon.online.serde.SparkConversions
import org.apache.flink.api.common.functions.util.ListCollector
import org.scalatest.flatspec.AnyFlatSpec

import java.util

class SourceProjectionDeSerializationSupportSpec extends AnyFlatSpec {
  import AvroObjectCreator._

  it should "project and let through avro data" in {
    val schemaStr = UserAvroSchema.schema.toString(true)
    val groupBy =
      makeGroupBy(
        Map("id" -> "id", "username" -> "username", "isActive" -> "isActive", "ts" -> "lastLoginTimestamp"),
        Seq("id == 12345", "isActive == true")
      )

    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)

    val avroSerdeProvider = new InMemoryAvroDeserializationSchemaProvider(UserAvroSchema.schema)
    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(avroSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyRecordBytes(schemaStr)
    deserSchema.deserialize(recordBytes, listCollector)

    // sanity check projected schemas is what we expect
    val sourceProjectionDeserSchema = deserSchema.asInstanceOf[SourceProjectionDeserializationSchema]
    val projectedSchema = sourceProjectionDeserSchema.projectedSchema
    assert(projectedSchema.map(_._1).toSet == Set("id", "username", "isActive", "ts"))

    // now check the types of projected data matching up with types in source schems
    val projectedSparkSchema = SparkConversions.fromChrononSchema(projectedSchema)
    val schema = deserSchema.sourceEventEncoder.schema

    // check ts out of band as the field name changes
    assert(projectedSparkSchema("ts").dataType == schema("lastLoginTimestamp").dataType)
    // check other fields
    val fieldsToCheck = Set("id", "username", "isActive")
    fieldsToCheck.map { name =>
      val sourceField = schema(name)
      val projectedField = projectedSparkSchema(name)
      assert(sourceField.dataType == projectedField.dataType, s"Field $name has different types")
    }

    // sanity check result data
    assert(resultList.size() == 1)
    val projectedEvent = resultList.toScala.head
    assert(projectedEvent.fields.nonEmpty)
    assert(projectedEvent.fields("id") == 12345)

    val expectedIngressTime =
      projectedEvent.startProcessingTimeMillis - projectedEvent.fields("ts").asInstanceOf[Long]
    val ingressTimeHistogram = sourceProjectionDeserSchema.eventTimeToFlinkIngressTimeHistogram
    assert(ingressTimeHistogram.getCount == 1)
    assert(ingressTimeHistogram.getStatistics.getMin == expectedIngressTime)
  }

  it should "project and filter avro data" in {
    val schemaStr = UserAvroSchema.schema.toString(true)
    val groupBy =
      makeGroupBy(
        Map("id" -> "id", "username" -> "username", "isActive" -> "isActive"),
        Seq("id == 45678", "isActive == true")
      )
    val avroSerdeProvider = new InMemoryAvroDeserializationSchemaProvider(UserAvroSchema.schema)
    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(avroSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyRecordBytes(schemaStr)

    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)
    deserSchema.deserialize(recordBytes, listCollector)

    // sanity check result data
    assert(resultList.isEmpty)
    assert(
      deserSchema
        .asInstanceOf[SourceProjectionDeserializationSchema]
        .eventTimeToFlinkIngressTimeHistogram
        .getCount == 0)
  }

  it should "skip avro data that can't be deserialized" in {

    val schemaStr = UserAvroSchema.schema.toString(true)
    val groupBy =
      makeGroupBy(
        Map("id" -> "id", "username" -> "username", "isActive" -> "isActive"),
        Seq("id == 45678", "isActive == true")
      )

    val avroSerdeProvider = new InMemoryAvroDeserializationSchemaProvider(UserAvroSchema.schema)
    val deserSchema = DeserializationSchemaBuilder.buildSourceProjectionDeserSchema(avroSerdeProvider, groupBy)
    deserSchema.open(new DummyInitializationContext)

    val recordBytes = createDummyRecordBytes(schemaStr)

    // corrupt the record bytes
    recordBytes(0) = 0

    val resultList = new util.ArrayList[ProjectedEvent]()
    val listCollector = new ListCollector(resultList)

    deserSchema.deserialize(recordBytes, listCollector)
    assert(resultList.isEmpty)
    assert(
      deserSchema
        .asInstanceOf[SourceProjectionDeserializationSchema]
        .eventTimeToFlinkIngressTimeHistogram
        .getCount == 0)
  }
}
