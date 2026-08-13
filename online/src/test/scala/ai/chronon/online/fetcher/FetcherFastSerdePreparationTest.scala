package ai.chronon.online.fetcher

import ai.chronon.api._
import ai.chronon.online.GroupByServingInfoParsed
import ai.chronon.online.serde.AvroConversions
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FetcherFastSerdePreparationTest extends AnyFlatSpec with Matchers {

  private val keySchema =
    AvroConversions.fromChrononSchema(StructType("serde_key", Array(StructField("id", StringType)))).toString
  private val inputSchema = AvroConversions
    .fromChrononSchema(
      StructType("serde_input", Array(StructField("id", StringType), StructField("value", LongType))))
    .toString
  private val selectedSchema =
    AvroConversions.fromChrononSchema(StructType("serde_selected", Array(StructField("value", LongType)))).toString

  private def servingInfo(name: String,
                          accuracy: Accuracy,
                          aggregated: Boolean,
                          streaming: Boolean,
                          entities: Boolean = false): GroupByServingInfoParsed = {
    val query = Builders.Query(selects = Map("id" -> "id", "value" -> "value"))
    val source =
      if (entities)
        Builders.Source.entities(query,
                                 snapshotTable = s"$name.snapshot",
                                 mutationTable = s"$name.mutations",
                                 mutationTopic = if (streaming) s"$name.topic" else null)
      else
        Builders.Source.events(query,
                               table = s"$name.events",
                               topic = if (streaming) s"$name.topic" else null)
    val aggregations =
      if (aggregated) Seq(Builders.Aggregation(Operation.SUM, "value")) else null
    val groupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = name),
      sources = Seq(source),
      keyColumns = Seq("id"),
      aggregations = aggregations,
      accuracy = accuracy)

    new GroupByServingInfoParsed(
      new GroupByServingInfo()
        .setGroupBy(groupBy)
        .setKeyAvroSchema(keySchema)
        .setInputAvroSchema(inputSchema)
        .setSelectedAvroSchema(selectedSchema)
        .setBatchEndTs(0L))
  }

  "fastSerdeReaderSchemas" should "select the same snapshot and temporal schemas as response decoding" in {
    val snapshot = servingInfo("serde_snapshot", Accuracy.SNAPSHOT, aggregated = true, streaming = false)
    Fetcher.fastSerdeReaderSchemas(snapshot, tilingEnabled = true) shouldBe Seq(snapshot.outputAvroSchema)

    val noAggregation =
      servingInfo("serde_no_aggregation", Accuracy.TEMPORAL, aggregated = false, streaming = true)
    Fetcher.fastSerdeReaderSchemas(noAggregation, tilingEnabled = true) shouldBe Seq(noAggregation.outputAvroSchema)

    val batchOnlyTemporal =
      servingInfo("serde_batch_only_temporal", Accuracy.TEMPORAL, aggregated = true, streaming = false)
    Fetcher.fastSerdeReaderSchemas(batchOnlyTemporal, tilingEnabled = true) shouldBe Seq(
      batchOnlyTemporal.irAvroSchema)
  }

  it should "select tile, raw event, and raw entity streaming schemas" in {
    val events = servingInfo("serde_events", Accuracy.TEMPORAL, aggregated = true, streaming = true)
    Fetcher.fastSerdeReaderSchemas(events, tilingEnabled = true) shouldBe Seq(
      events.irAvroSchema,
      events.tiledCodec.tileAvroSchema)
    Fetcher.fastSerdeReaderSchemas(events, tilingEnabled = false) shouldBe Seq(
      events.irAvroSchema,
      events.valueAvroSchema)

    val entities =
      servingInfo("serde_entities", Accuracy.TEMPORAL, aggregated = true, streaming = true, entities = true)
    Fetcher.fastSerdeReaderSchemas(entities, tilingEnabled = false) shouldBe Seq(
      entities.irAvroSchema,
      entities.mutationValueAvroSchema)
  }
}
