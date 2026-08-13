package ai.chronon.flink.chaining

import ai.chronon.api._
import ai.chronon.api.Extensions._
import ai.chronon.flink.test.{CollectSink, FlinkTestUtils, MockAsyncKVStoreWriter}
import ai.chronon.flink.SparkExpressionEvalFn
import ai.chronon.online.serde.SparkConversions
import ai.chronon.online.Extensions.StructTypeOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.{Api, GroupByServingInfoParsed, TopicInfo}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._


class ChainedGroupByJobIntegrationTest extends AnyFlatSpec with BeforeAndAfter with Matchers {
  import JoinTestUtils._

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(1)
      .build)

  before {
    flinkCluster.before()
    CollectSink.values.clear()
  }

  after {
    flinkCluster.after()
    CollectSink.values.clear()
  }

  it should "run full FlinkJoinSourceJob pipeline with proper test implementations" in {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // Clear previous test results
    CollectSink.values.clear()

    // Test input events
    val elements = Seq(
      JoinTestEvent("user1", "listing1", 50.0, 1699366993123L),
      JoinTestEvent("user2", "listing2", 150.0, 1699366993124L),
      JoinTestEvent("user3", "listing3", 25.0, 1699366993125L)
    )

    val testApi = new TestApi()

    val groupBy = buildJoinSourceTerminalGroupBy()
    groupBy.getAggregations.get(0).setWindows(Seq(new Window(365, TimeUnit.DAYS)).toJava)
    val (joinSourceJob, servingInfo) = buildFlinkJoinSourceJob(groupBy, elements, testApi)
    servingInfo.groupByServingInfo
      .setPartitionInterval(WindowUtils.Day)
      .setPartitionOffset(WindowUtils.Hour)

    // Run the actual FlinkJoinSourceJob pipeline with our test implementations
    val jobDataStream = joinSourceJob.runTiledGroupByJob(env)
    jobDataStream.addSink(new CollectSink)

    // Execute the pipeline
    println(s"Starting Flink execution...")
    env.execute("FlinkJoinSourceJob Integration Test")

    // Verify outputs
    val outputs = CollectSink.values.asScala
    outputs should not be empty

    // Verify number of output events - should be as many as input events
    outputs.size should be >= elements.size

    // Verify basic structure of all responses
    outputs.foreach { response =>
      response should not be null
      response.tsMillis should be > 0L
      response.valueBytes should not be null
      response.valueBytes.length should be > 0
      response.status should be (true)
      response.dataset should not be null
    }

    val tileKeys = outputs.map(response => TilingUtils.deserializeTileKey(response.keyBytes))
    tileKeys.map(_.getTileSizeMillis).toSet shouldBe Set(WindowUtils.Day.millis)
    tileKeys.foreach { tileKey =>
      Math.floorMod(tileKey.getTileStartTimestampMillis - WindowUtils.Hour.millis, WindowUtils.Day.millis) shouldBe 0L
    }

    // check that the timestamps of the written out events match the input events
    // we use a Set as we can have elements out of order given we have multiple tasks
    val timestamps = outputs.map(_.tsMillis).toSet
    val expectedTimestamps = elements.map(_.created).toSet
    timestamps.intersect(expectedTimestamps) should not be empty

    // Extract and verify user IDs (keys contain dataset name + user_id in binary format)
    val responseKeys = outputs.map(r => new String(r.keyBytes)).toSet
    val expectedUsers = elements.map(_.user_id).toSet
    
    // Check that each expected user ID appears somewhere in at least one response key
    expectedUsers.foreach { expectedUser =>
      responseKeys.exists(_.contains(expectedUser)) should be (true)
    }
  }

  private def buildFlinkJoinSourceJob(groupBy: GroupBy, elements: Seq[JoinTestEvent], api: Api): (ChainedGroupByJob, GroupByServingInfoParsed) = {
    val joinSource = groupBy.streamingSource.get.getJoinSource
    val parentJoin = joinSource.getJoin
    
    val leftSourceQuery = parentJoin.getLeft.query
    val leftSourceGroupByName = s"left_source_${parentJoin.getMetaData.getName}"
    
    val sparkExpressionEvalFn = new SparkExpressionEvalFn(Encoders.product[JoinTestEvent], leftSourceQuery, leftSourceGroupByName)
    val source = new MockJoinSource(elements, sparkExpressionEvalFn)

    // Schema after applying projection on the join's input topic
    val inputSchemaDataTypes = Seq(
      ("user_id", ai.chronon.api.StringType),
      ("listing_id", ai.chronon.api.StringType),
      ("price_discounted", ai.chronon.api.DoubleType),
      ("ts", ai.chronon.api.LongType)
    )

    // we don't use the output schema in the Tiling implementation so we pass a dummy one
    val groupByServingInfoParsed =
      FlinkTestUtils.makeTestGroupByServingInfoParsed(
        groupBy,
        SparkConversions.fromChrononSchema(inputSchemaDataTypes),
        SparkConversions.fromChrononSchema(inputSchemaDataTypes)
      )

    // Extract topic info from the join source
    val leftSource = joinSource.getJoin.getLeft
    val topicUri = leftSource.topic
    val topicInfo = TopicInfo.parse(topicUri)
    
    val writerFn = new MockAsyncKVStoreWriter(Seq(true), api, groupBy.metaData.name)
    val propsWithKafka = Map(
      "bootstrap" -> "localhost:9092",
      "trigger" -> "always_fire"  // Explicitly use same trigger as working FlinkJobEventIntegrationTest
    ) 
    
    val flinkJob = new ChainedGroupByJob(
      eventSrc = source,
      inputSchema = inputSchemaDataTypes,
      sinkFn = writerFn,
      groupByServingInfoParsed = groupByServingInfoParsed,
      parallelism = 2,
      props = propsWithKafka,
      topicInfo = topicInfo,
      api = api,
      enableDebug = true
    )
    
    (flinkJob, groupByServingInfoParsed)
  }
}
