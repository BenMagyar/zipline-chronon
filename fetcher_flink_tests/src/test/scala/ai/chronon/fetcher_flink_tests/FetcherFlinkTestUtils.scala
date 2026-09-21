package ai.chronon.fetcher_flink_tests

import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.flink.{FlinkGroupByStreamingJob, SparkExpressionEval, SparkExpressionEvalFn}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.FlinkSource
import ai.chronon.flink.test.{CollectSink, MockAsyncKVStoreWriter}
import ai.chronon.flink.types.WriteResponse
import ai.chronon.flink.test.FlinkTestUtils
import ai.chronon.online.{GroupByServingInfoParsed, KVStore, TopicInfo}
import ai.chronon.online.serde.SparkConversions
import ai.chronon.spark.Extensions._
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.MockApi
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.spark.sql.{Encoders, Row, SparkSession}

// Case class whose field names match the GroupBy query's column references:
//   selects: listing_id←listing, rating←rating
//   timeColumn: ts, mutationTimeColumn: mutation_time, reversalColumn: is_before_reversal
case class ListingMutationEvent(listing: Long,
                                rating: Int,
                                ts: Long,
                                mutation_time: Long,
                                is_before_reversal: Boolean)

// Flink source for ListingMutationEvent. No watermarks assigned here — runTiledGroupByJob
// reassigns watermarks downstream via FlinkJob.watermarkStrategy (extracting ts from ProjectedEvent).
class ListingMutationSource(events: Seq[ListingMutationEvent],
                             sparkExprEvalFn: SparkExpressionEvalFn[ListingMutationEvent])
    extends FlinkSource[ProjectedEvent] {

  implicit val parallelism: Int = 1

  override def getDataStream(topic: String, groupName: String)(
      env: StreamExecutionEnvironment,
      parallelism: Int): SingleOutputStreamOperator[ProjectedEvent] =
    env
      .fromCollection(events.toJava)
      .flatMap(sparkExprEvalFn)
      .map(e => ProjectedEvent(e, System.currentTimeMillis()))
}

object FetcherFlinkTestUtils {

  private def toTs(s: String): Long = TsUtils.datetimeToTs(s)

  val endDs    = "2021-04-10"
  val batchEnd = "2021-04-09"

  val snapshotSchema: StructType = StructType(
    "listing_ratings_snapshot_fetcher",
    Array(StructField("listing", LongType),
          StructField("ts",      LongType),
          StructField("rating",  IntType),
          StructField("ds",      StringType))
  )

  val mutationSchema: StructType = StructType(
    "listing_ratings_mutations_fetcher",
    snapshotSchema.fields ++ Seq(
      StructField("mutation_time",      LongType),
      StructField("is_before_reversal", BooleanType)
    )
  )

  val eventSchema: StructType = StructType(
    "listing_events_fetcher",
    Array(StructField("listing_id", LongType),
          StructField("ts",         LongType),
          StructField("ds",         StringType))
  )

  // Mutation events for ds=2021-04-10, fed to Flink.
  // Query timestamps below are chosen to fall after all mutations for each key so the
  // tiled and non-tiled paths agree (no query straddles mid-tile mutation boundaries):
  //   listing_id=1:   mutations complete by 02:30 → query at 03:10
  //   listing_id=595: mutations complete by 23:30 → query at 23:45
  val flinkMutationEvents: Seq[ListingMutationEvent] = Seq(
    ListingMutationEvent(1L,                  5, toTs("2021-04-10 00:30:00"), toTs("2021-04-10 00:30:00"), is_before_reversal = false),
    ListingMutationEvent(1L,                  5, toTs("2021-04-10 00:30:00"), toTs("2021-04-10 02:30:00"), is_before_reversal = true),
    ListingMutationEvent(595125622443733822L,  4, toTs("2021-04-10 10:00:00"), toTs("2021-04-10 10:00:00"), is_before_reversal = false),
    ListingMutationEvent(595125622443733822L,  4, toTs("2021-04-10 10:00:00"), toTs("2021-04-10 23:30:00"), is_before_reversal = true),
    ListingMutationEvent(595125622443733822L,  3, toTs("2021-04-10 10:00:00"), toTs("2021-04-10 23:30:00"), is_before_reversal = false)
  )

  val queryRows: Seq[Row] = Seq(
    Row(595125622443733822L, toTs("2021-04-10 23:45:00"), "2021-04-10"),
    Row(1L,                  toTs("2021-04-10 03:10:00"), "2021-04-10")
  )

  def makeGroupBy(ns: String): GroupBy =
    Builders.GroupBy(
      sources = Seq(
        Builders.Source.entities(
          snapshotTable    = s"$ns.${snapshotSchema.name}",
          mutationTable    = s"$ns.${mutationSchema.name}",
          mutationTopic    = "listing_mutations",
          query = Builders.Query(
            selects            = Map("listing_id" -> "listing", "rating" -> "rating"),
            timeColumn         = "ts",
            mutationTimeColumn = "mutation_time",
            reversalColumn     = "is_before_reversal",
            startPartition     = "2021-04-08",
            endPartition       = endDs
          )
        )
      ),
      keyColumns   = Seq("listing_id"),
      aggregations = Seq(
        Builders.Aggregation(Operation.SUM,     inputColumn = "rating", windows = null),
        Builders.Aggregation(Operation.AVERAGE, inputColumn = "rating",
                             windows = Seq(new Window(1, TimeUnit.DAYS)))
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(
        name = "integration_test.entity_tiled_gb", namespace = ns, team = "chronon")
    )

  def writeSparkTables(ns: String)(implicit spark: SparkSession, tu: TableUtils): Unit = {
    val snapshotRows: Seq[Row] = Seq(
      Row(1L,                  toTs("2021-04-04 00:30:00"), 4, "2021-04-08"),
      Row(1L,                  toTs("2021-04-04 12:30:00"), 4, "2021-04-08"),
      Row(1L,                  toTs("2021-04-05 00:30:00"), 4, "2021-04-08"),
      Row(1L,                  toTs("2021-04-08 02:30:00"), 4, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-04 01:40:00"), 3, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-05 03:40:00"), 3, "2021-04-08"),
      Row(595125622443733822L, toTs("2021-04-06 03:45:00"), 4, "2021-04-08"),
      Row(1L,                  toTs("2021-04-04 00:30:00"), 4, "2021-04-09"),
      Row(1L,                  toTs("2021-04-04 12:30:00"), 4, "2021-04-09"),
      Row(1L,                  toTs("2021-04-05 00:30:00"), 4, "2021-04-09"),
      Row(1L,                  toTs("2021-04-08 02:30:00"), 4, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-04 01:40:00"), 3, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-05 03:40:00"), 3, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-06 03:45:00"), 4, "2021-04-09"),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 5, "2021-04-09")
    )

    val mutationRows: Seq[Row] = Seq(
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 05:45:00"), false),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 2, "2021-04-09", toTs("2021-04-09 07:00:00"), true),
      Row(595125622443733822L, toTs("2021-04-09 05:45:00"), 5, "2021-04-09", toTs("2021-04-09 07:00:00"), false),
      Row(1L,                  toTs("2021-04-10 00:30:00"), 5, "2021-04-10", toTs("2021-04-10 00:30:00"), false),
      Row(1L,                  toTs("2021-04-10 00:30:00"), 5, "2021-04-10", toTs("2021-04-10 02:30:00"), true),
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 10:00:00"), false),
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 4, "2021-04-10", toTs("2021-04-10 23:30:00"), true),
      Row(595125622443733822L, toTs("2021-04-10 10:00:00"), 3, "2021-04-10", toTs("2021-04-10 23:30:00"), false)
    )

    spark.createDataFrame(snapshotRows.toJava, SparkConversions.fromChrononSchema(snapshotSchema))
         .save(s"$ns.${snapshotSchema.name}")
    spark.createDataFrame(mutationRows.toJava, SparkConversions.fromChrononSchema(mutationSchema))
         .save(s"$ns.${mutationSchema.name}")
  }

  // Chronon-typed adapters over FlinkTestUtils.makeTestGroupByServingInfoParsed for tests that don't want to
  // hand-roll Spark StructTypes.
  def makeTestGroupByServingInfoParsed(groupBy: GroupBy,
                                       inputSchema: Seq[(String, DataType)]): GroupByServingInfoParsed = {
    val sparkSchema = SparkConversions.fromChrononSchema(inputSchema)
    FlinkTestUtils.makeTestGroupByServingInfoParsed(groupBy, sparkSchema, sparkSchema)
  }

  def makeTestGroupByServingInfoParsed(groupBy: GroupBy,
                                       inputSchema: Seq[(String, DataType)],
                                       outputSchemaSpark: org.apache.spark.sql.types.StructType): GroupByServingInfoParsed =
    FlinkTestUtils.makeTestGroupByServingInfoParsed(
      groupBy, SparkConversions.fromChrononSchema(inputSchema), outputSchemaSpark)

  def toSparkStructType(inputSchema: Seq[(String, DataType)]): org.apache.spark.sql.types.StructType =
    SparkConversions.fromChrononSchema(inputSchema)

  /** Runs FlinkGroupByStreamingJob in tiled mode over flinkMutationEvents, returns WriteResponses. */
  def runFlinkTiledJob(groupBy: GroupBy, mockApi: MockApi): Seq[WriteResponse] = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val encoder          = Encoders.product[ListingMutationEvent]
    val query            = SparkExpressionEval.queryFromGroupBy(groupBy)
    val sparkExprEvalFn  = new SparkExpressionEvalFn[ListingMutationEvent](
      encoder, query, groupBy.metaData.name, groupBy.dataModel)
    val outputSchema     = new SparkExpressionEval(encoder, query, groupBy.metaData.name, groupBy.dataModel)
      .getOutputSchema
    val inputSchemaTypes = outputSchema.fields.map(f => (f.name, SparkConversions.toChrononType(f.name, f.dataType)))

    val source       = new ListingMutationSource(flinkMutationEvents, sparkExprEvalFn)
    val gbServingInfo = mockApi.buildFetcher().metadataStore.getGroupByServingInfo(groupBy.metaData.name).get

    val flinkMockApi = org.mockito.Mockito.mock(
      classOf[ai.chronon.online.Api],
      org.mockito.Mockito.withSettings().serializable()
    )
    val writerFn = new MockAsyncKVStoreWriter(
      Seq.fill(flinkMutationEvents.size)(true), flinkMockApi, groupBy.metaData.name)

    val job = new FlinkGroupByStreamingJob(
      source, inputSchemaTypes, writerFn, gbServingInfo,
      parallelism = 2, props = Map.empty, topicInfo = TopicInfo.parse("kafka://listing_mutations")
    )

    job.runTiledGroupByJob(env).addSink(new CollectSink)
    env.execute("FetcherFlinkEntitySourceTest")

    CollectSink.values.toScala.toSeq
  }

  /** Converts Flink WriteResponses into KVStore PutRequests compatible with InMemoryKvStore.
    *
    * Flink writes TileKeys with tileStartTs set (specific window start). Production KV stores
    * support range scans on (entityKey, tileSizeMs), so tileStart is part of the stored key
    * but is stripped on the read side. InMemoryKvStore uses exact-key HashMap lookup, so we
    * must write with tileStartTs=None to match what the fetcher's read key looks like.
    *
    * AlwaysFireOnElementTrigger emits a cumulative tile after every event per (key, window).
    * We group by (dataset, entityKeyBytes, tileStart) to identify distinct window buckets and
    * keep the last WriteResponse per bucket — the net result after all events in that window.
    */
  def tileResponsesToPutRequests(writeResponses: Seq[WriteResponse]): Seq[KVStore.PutRequest] = {
    val latestPerWindowBucket = writeResponses
      .map { wr =>
        val tileKey   = TilingUtils.deserializeTileKey(wr.keyBytes)
        val bucketKey = (wr.dataset, tileKey.getKeyBytes.toScala.toSeq, tileKey.getTileStartTimestampMillis)
        bucketKey -> wr
      }
      .foldLeft(Map.empty[(String, Seq[java.lang.Byte], Long), WriteResponse]) {
        case (acc, (k, wr)) => acc.updated(k, wr)
      }

    latestPerWindowBucket.values.map { wr =>
      val tileKey = TilingUtils.deserializeTileKey(wr.keyBytes)
      tileKey.unsetTileStartTimestampMillis()
      KVStore.PutRequest(TilingUtils.serializeTileKey(tileKey), wr.valueBytes, wr.dataset, Some(wr.tsMillis))
    }.toSeq
  }
}
