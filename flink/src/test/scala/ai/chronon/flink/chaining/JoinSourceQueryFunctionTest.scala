package ai.chronon.flink.chaining

import ai.chronon.api.{Accuracy, Builders, DoubleType, GroupByServingInfo, IntType, LongType, Operation, StringType, StructField, StructType}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.online.metrics.TTLCache
import ai.chronon.online.serde.AvroConversions
import ai.chronon.online.{Api, GroupByServingInfoParsed, JoinCodec}
import ai.chronon.online.fetcher.{FetchContext, Fetcher, MetadataStore}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.configuration.Configuration
import org.apache.flink.metrics.{Counter, Histogram, MetricGroup}
import org.apache.flink.metrics.groups.OperatorMetricGroup
import org.apache.flink.util.Collector
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.mutable.ListBuffer
import scala.util.{Success, Try}

class JoinSourceQueryFunctionTest extends AnyFlatSpec with Matchers with MockitoSugar {

  val inputSchema = Seq(
    ("user_id", StringType),
    ("price", DoubleType),
    ("timestamp", LongType)
  )

  private def setupFunctionWithMockedMetrics(function: JoinSourceQueryFunction): Unit = {
    // Mock the runtime context and metrics
    val mockRuntimeContext = mock[RuntimeContext]
    val mockOperatorMetricGroup = mock[OperatorMetricGroup]
    val mockSubGroup = mock[MetricGroup]
    val mockCounter = mock[Counter]
    val mockHistogram = mock[Histogram]
    
    // Mock the metric group chain
    when(mockRuntimeContext.getMetricGroup).thenReturn(mockOperatorMetricGroup)
    when(mockOperatorMetricGroup.addGroup("chronon")).thenReturn(mockSubGroup)
    when(mockSubGroup.addGroup(anyString(), anyString())).thenReturn(mockSubGroup)
    when(mockSubGroup.counter(anyString())).thenReturn(mockCounter)
    when(mockSubGroup.histogram(anyString(), any())).thenReturn(mockHistogram)
    
    function.setRuntimeContext(mockRuntimeContext)
  }

  "JoinSourceQueryFunction" should "apply SQL transformations correctly" in {
    // Create a join source with SQL query
    val parentJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map("user_id" -> "user_id", "price" -> "price"),
          timeColumn = "timestamp"
        ),
        table = "test.events",
        topic = "kafka://test-topic"
      ),
      joinParts = Seq(),
      metaData = Builders.MetaData(name = "test.parent_join")
    )

    val joinSource = Builders.Source.joinSource(
      join = parentJoin,
      query = Builders.Query(
        selects = Map(
          "user_id" -> "user_id",
          "doubled_price" -> "price * 2",
          "price_category" -> "CASE WHEN price > 100 THEN 'expensive' ELSE 'affordable' END"
        ),
        timeColumn = "timestamp"
      )
    ).getJoinSource

    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher] 
    val mockMetadataStore = mock[ai.chronon.online.fetcher.MetadataStore]
    val mockJoinCodec = mock[JoinCodec]
    
    when(mockApi.buildFetcher(debug = false)).thenReturn(mockFetcher)
    when(mockFetcher.metadataStore).thenReturn(mockMetadataStore)
    when(mockMetadataStore.buildJoinCodec(parentJoin, refreshOnFail = false)).thenReturn(mockJoinCodec)
    
    // Mock join codec schema (enriched fields from join)
    val joinValueSchema = ai.chronon.api.StructType("join_enriched", Array(
      ai.chronon.api.StructField("user_category", StringType),
      ai.chronon.api.StructField("user_score", DoubleType)
    ))
    when(mockJoinCodec.valueSchema).thenReturn(joinValueSchema)

    val function = new JoinSourceQueryFunction(joinSource, inputSchema, groupByName = "testGB", mockApi, enableDebug = false)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())

    // Create enriched event (after join processing)
    val enrichedFields = Map(
      "user_id" -> "123",
      "price" -> 150.0,
      "timestamp" -> 1000L,
      "user_category" -> "premium", // From join
      "user_score" -> 85.5        // From join
    )
    val enrichedEvent = ProjectedEvent(enrichedFields, 500L)

    // Collect outputs
    val outputs = ListBuffer[ProjectedEvent]()
    val collector = new Collector[ProjectedEvent] {
      override def collect(record: ProjectedEvent): Unit = outputs += record
      override def close(): Unit = {}
    }

    // Execute transformation
    function.flatMap(enrichedEvent, collector)

    // Verify transformation results
    outputs should have size 1
    val result = outputs.head
    
    result.startProcessingTimeMillis should be(500L)
    
    val resultFields = result.fields
    resultFields("user_id") should be("123")
    resultFields("doubled_price") should be(300.0) // 150 * 2
    resultFields("price_category") should be("expensive") // price > 100
  }


  it should "handle query errors gracefully" in {
    val parentJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(),
        table = "test.events", 
        topic = "kafka://test-topic"
      ),
      joinParts = Seq(),
      metaData = Builders.MetaData(name = "test.parent_join")
    )

    val joinSource = Builders.Source.joinSource(
      join = parentJoin,
      query = Builders.Query(
        selects = Map("user_id" -> "user_id", "price_doubled" -> "price * 2"),
        timeColumn = "timestamp"
      )
    ).getJoinSource

    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    val mockMetadataStore = mock[ai.chronon.online.fetcher.MetadataStore]
    val mockJoinCodec = mock[JoinCodec]
    
    when(mockApi.buildFetcher()).thenReturn(mockFetcher)
    when(mockFetcher.metadataStore).thenReturn(mockMetadataStore)
    when(mockMetadataStore.buildJoinCodec(parentJoin, refreshOnFail = false)).thenReturn(mockJoinCodec)
    
    val joinValueSchema = ai.chronon.api.StructType("join_enriched", Array(
      ai.chronon.api.StructField("user_category", StringType)
    ))
    when(mockJoinCodec.valueSchema).thenReturn(joinValueSchema)

    val function = new JoinSourceQueryFunction(joinSource, inputSchema, groupByName = "testGB", mockApi, enableDebug = false)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())

    // Create enriched event with invalid data that will cause an exception when cast to wrong type
    val enrichedFields = Map(
      "user_id" -> "123",
      "price" -> "invalid_number", // Throws a ClassCastException when processed as Double
      "timestamp" -> 1000L,
      "user_category" -> "premium"
    )
    val enrichedEvent = ProjectedEvent(enrichedFields, 500L)

    // Collect outputs
    val outputs = ListBuffer[ProjectedEvent]()
    val collector = new Collector[ProjectedEvent] {
      override def collect(record: ProjectedEvent): Unit = outputs += record
      override def close(): Unit = {}
    }

    // Execute (should handle error gracefully and swallow event)
    function.flatMap(enrichedEvent, collector)

    // Should have swallowed the event on error - no outputs
    outputs.isEmpty shouldBe true
  }

  it should "build join schema correctly" in {
    // Test that the schema building combines left source + join codec schemas
    val parentJoin = Builders.Join(
      left = Builders.Source.events(
        query = Builders.Query(),
        table = "test.events",
        topic = "kafka://test-topic"
      ),
      joinParts = Seq(),
      metaData = Builders.MetaData(name = "test.parent_join")
    )

    val joinSource = Builders.Source.joinSource(
      join = parentJoin,
      query = Builders.Query(
        selects = Map("user_id" -> "user_id", "enriched_field" -> "enriched_field"),
        timeColumn = "timestamp"
      )
    ).getJoinSource

    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    val mockMetadataStore = mock[ai.chronon.online.fetcher.MetadataStore]
    val mockJoinCodec = mock[JoinCodec]
    
    when(mockApi.buildFetcher(debug = false)).thenReturn(mockFetcher)
    when(mockFetcher.metadataStore).thenReturn(mockMetadataStore)
    when(mockMetadataStore.buildJoinCodec(parentJoin, refreshOnFail = false)).thenReturn(mockJoinCodec)
    
    // Mock join codec value schema (fields added by join)
    val joinValueSchema = ai.chronon.api.StructType("join_enriched", Array(
      ai.chronon.api.StructField("enriched_field", StringType),
      ai.chronon.api.StructField("another_enriched", IntType)
    ))
    when(mockJoinCodec.valueSchema).thenReturn(joinValueSchema)

    val function = new JoinSourceQueryFunction(joinSource, inputSchema, groupByName = "testGB", mockApi, enableDebug = false)
    setupFunctionWithMockedMetrics(function)
    
    // Opening the function should trigger schema building without errors
    function.open(new Configuration())
    
    verify(mockJoinCodec, org.mockito.Mockito.atLeast(1)).valueSchema // Should have accessed the join schema
  }

  it should "build a chained join codec from an already projected nested left key" in {
    val productGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "unit_test.product_hydrate.latest_value", online = true, version = 4),
      sources = Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map(
              "product_id" -> "CAST(data.id AS BIGINT)",
              "seller_id" -> "data.user_id"
            ),
            wheres = Seq("data.active_status = 'active'"),
            timeColumn = "event_timestamp",
            startPartition = "2026-06-09"
          ),
          table = "unit_test.product_hydrate",
          topic = "product_hydrate.v3"
        )),
      keyColumns = Seq("product_id"),
      aggregations = Seq(Builders.Aggregation(Operation.LAST, "seller_id")),
      accuracy = Accuracy.TEMPORAL
    )
    val productJoin = Builders.Join(
      metaData = Builders.MetaData(name = "unit_test.product_views.hydrated", online = true, version = 8),
      left = Builders.Source.events(
        query = Builders.Query(
          selects = Map(
            "user_id" -> "data.baseEvent.userId",
            "product_id" -> "CAST(data.productId AS BIGINT)"
          ),
          timeColumn = "event_timestamp",
          startPartition = "2026-06-09"
        ),
        table = "unit_test.product_views",
        topic = "product_views_by_product_id.v2"
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = productGroupBy))
    )
    val joinSource = Builders.Source.joinSource(
      join = productJoin,
      query = Builders.Query(
        selects = Map(
          "user_id" -> "user_id",
          "product_id" -> "product_id"
        ),
        timeColumn = "ts"
      )
    ).getJoinSource

    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(productGroupBy)
    groupByServingInfo.setKeyAvroSchema(
      AvroConversions.fromChrononSchema(StructType("Key", Array(StructField("product_id", LongType)))).toString)
    groupByServingInfo.setInputAvroSchema(
      AvroConversions
        .fromChrononSchema(
          StructType(
            "Input",
            Array(
              StructField("product_id", LongType),
              StructField("seller_id", StringType)
            )))
        .toString)
    groupByServingInfo.setSelectedAvroSchema(
      AvroConversions.fromChrononSchema(StructType("Selected", Array(StructField("seller_id", StringType)))).toString)
    groupByServingInfo.setBatchEndTs(0L)
    val servingInfo = new GroupByServingInfoParsed(groupByServingInfo)

    val metadataStore = spy[MetadataStore](new MetadataStore(FetchContext(new TestKVStore())))
    val servingInfoCache = mock[TTLCache[String, Try[GroupByServingInfoParsed]]]
    when(metadataStore.getGroupByServingInfo).thenReturn(servingInfoCache)
    when(servingInfoCache.apply(productGroupBy.metaData.name)).thenReturn(Success(servingInfo))

    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    when(mockApi.buildFetcher(debug = false)).thenReturn(mockFetcher)
    when(mockFetcher.metadataStore).thenReturn(metadataStore)

    val projectedInputSchema = Seq(
      "user_id" -> StringType,
      "product_id" -> LongType,
      "ts" -> LongType
    )

    noException should be thrownBy JoinSourceQueryFunction.buildJoinSchema(
      projectedInputSchema,
      joinSource,
      mockApi,
      enableDebug = false
    )
  }
}
