package ai.chronon.fetcher_flink_tests

import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.{FlagStore, FlagStoreConstants}
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SparkTestBase}
import ai.chronon.flink.test.CollectSink
import ai.chronon.flink.chaining.ModelTransformsSourceFlinkJob
import ai.chronon.flink.SparkExpressionEvalFn
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.FlinkSource
import ai.chronon.flink.test.MockAsyncKVStoreWriter
import ai.chronon.flink.types.WriteResponse
import ai.chronon.api.{EndpointConfig, ModelBackend}
import ai.chronon.online.{GroupByServingInfoParsed, ModelJobStatus, ModelOperation, ModelPlatform, ModelPlatformProvider, PredictRequest, PredictResponse, TopicInfo, TrainingRequest, DeployModelRequest, KVStore}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.{Encoders, Row}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.Executors
import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration => ScalaDuration, SECONDS}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Success

/**
  * End-to-end integration test for a streaming GroupBy whose source is a ModelTransforms.
  * (The offline batch analogue is ModelTransformsJobTest; note the offline GroupByUpload path
  * does not yet aggregate over model outputs — flagged as a follow-up in the ModelTransforms
  * streaming plan — so this test validates the streaming-tile → KV → fetch path.)
  *
  *  1. Flink:  run ModelTransformsSourceFlinkJob (tiled) over test events with a mock model platform,
  *             capturing tile WriteResponses.
  *  2. Bridge: put tile bytes into InMemoryKvStore (same store the fetcher reads).
  *  3. Fetch:  real fetchGroupBys over those tiles, asserting model output + passthrough aggregation.
  */
class FetcherFlinkModelTransformsTest extends SparkTestBase with Matchers with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(1)
      .build
  )

  before {
    flinkCluster.before()
    CollectSink.values.clear()
  }

  after {
    flinkCluster.after()
    CollectSink.values.clear()
  }

  // Test data: 3 users, each with one event. Model returns a per-user score so we can
  // assert exact aggregated values through the tile → fetch round trip.
  val testEvents: Seq[ModelTransformsStreamEvent] = Seq(
    ModelTransformsStreamEvent("user1", 1699366993123L),
    ModelTransformsStreamEvent("user2", 1699366993124L),
    ModelTransformsStreamEvent("user3", 1699366993125L)
  )

  val modelScores: Map[String, Double] = Map(
    "user1" -> 0.85,
    "user2" -> 0.65,
    "user3" -> 0.5
  )

  it should "model transforms tiled online fetch returns aggregated model outputs" in {
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

    val kvStoreFunc     = () => OnlineUtils.buildInMemoryKVStore("FetcherFlinkModelTransformsTest")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi         = new ModelTransformsMockApi(kvStoreFunc)

    val groupBy      = makeModelTransformsGroupBy()
    val mtConf       = groupBy.sources.get(0).getModelTransforms
    val groupByName  = groupBy.metaData.getName

    // -----------------------------------------------------------------------
    // Step 1: Seed metadata (serving info + model transforms conf) into the KV store
    // -----------------------------------------------------------------------
    // Input schema after the inner source's Spark projection + the model output column that the
    // aggregation consumes (selectedAvroSchema drives the tiling codec on both write and read sides).
    val aggInputColumns = new GroupByOps(groupBy).aggregationInputs.toSeq
    val inputSchema = Seq(
      ("user_id", StringType),
      (Constants.TimeColumn, LongType)
    )
    val servingInfoParsed =
      FetcherFlinkTestUtils.makeTestGroupByServingInfoParsed(
        groupBy, inputSchema, FetcherFlinkTestUtils.toSparkStructType(inputSchema ++ Seq((aggInputColumns.head, DoubleType))))
    // The streaming tiles carry all data; set the batch watermark below the earliest event time so
    // the fetcher's tile filter (tVal.millis >= batchEndTsMillis) accepts all tiles.
    servingInfoParsed.groupByServingInfo
      .setBatchEndTs(testEvents.map(_.created).min - 1)
    servingInfoParsed.groupByServingInfo
      .setBatchEndDate("2023-11-06")

    // GroupByServingInfo under the batch dataset (what fetchGroupBys looks up)
    val groupByOps = new GroupByOps(groupBy)
    val batchDataset = groupByOps.batchDataset
    val streamingDataset = groupByOps.streamingDataset
    inMemoryKvStore.create(batchDataset)
    Await.result(inMemoryKvStore.multiPut(Seq(
      KVStore.PutRequest(
        Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
        ai.chronon.api.ThriftJsonCodec.toJsonStr(servingInfoParsed.groupByServingInfo).getBytes(Constants.UTF8),
        batchDataset,
        Some(System.currentTimeMillis()))
    )), ScalaDuration(30, SECONDS))

    inMemoryKvStore.create(streamingDataset)
    inMemoryKvStore.create(MetadataDataset)
    Await.result(
      new MetadataStore(FetchContext(inMemoryKvStore)).putModelTransformsConf(mtConf),
      ScalaDuration(30, SECONDS))

    // -----------------------------------------------------------------------
    // Step 2: Run the Flink ModelTransforms tiled job, capture tile WriteResponses
    // -----------------------------------------------------------------------
    val writeResponses = runModelTransformsFlinkJob(groupBy, mtConf, mockApi)
    writeResponses should not be empty
    writeResponses.forall(_.status) shouldBe true

    // -----------------------------------------------------------------------
    // Step 3: Bridge — put Flink tile bytes into InMemoryKvStore
    // -----------------------------------------------------------------------
    inMemoryKvStore.multiPut(FetcherFlinkTestUtils.tileResponsesToPutRequests(writeResponses))

    // -----------------------------------------------------------------------
    // Step 4: Fetch via the real fetcher with tiling enabled
    // -----------------------------------------------------------------------
    val tilingMockApi = new ModelTransformsMockApi(kvStoreFunc)
    tilingMockApi.setFlagStore(new FlagStore {
      override def isSet(flagName: String, attrs: java.util.Map[String, String]): java.lang.Boolean =
        flagName == FlagStoreConstants.TILING_ENABLED
    })
    val fetcher = tilingMockApi.buildFetcher(debug = true)

    // Query 1ms after each event: the tile version timestamp is the event time, and the tiled
    // aggregator only merges tiles strictly older than the query time.
    val requests = testEvents.map { event =>
      Request(groupByName, Map("user_id" -> event.user_id), Some(event.created + 1))
    }
    val responses = Await.result(fetcher.fetchGroupBys(requests), ScalaDuration(60, SECONDS))

    // -----------------------------------------------------------------------
    // Step 5: Verify model outputs + passthrough come back through the tiles
    // -----------------------------------------------------------------------
    responses.length shouldBe requests.length
    responses.foreach { response =>
      withClue(s"fetch failed for ${response.request.keys}: ${response.values.failed.map(_.getMessage).getOrElse("")}") {
        response.values.isSuccess shouldBe true
      }
      val values = response.values.get
      val userId = response.request.keys("user_id").asInstanceOf[String]

      // The aggregated response contains the aggregation outputs (model output columns).
      // Passthrough fields only appear in the per-event model fetch path (covered by
      // ModelTransformsAsyncFunctionTest); they are not aggregate outputs.
      withClue(s"score mismatch for user $userId in: $values") {
        val scoreOpt = values.find(_._1.contains("score_model__score")).map(_._2)
        scoreOpt shouldBe defined
        scoreOpt.get.asInstanceOf[Double] shouldBe modelScores(userId) +- 1e-9
      }
    }
  }

  // -------------------------------------------------------------------------

  val flinkMockApi: ai.chronon.online.Api = org.mockito.Mockito.mock(
    classOf[ai.chronon.online.Api],
    org.mockito.Mockito.withSettings().serializable()
  )

  /** Runs ModelTransformsSourceFlinkJob in tiled mode over testEvents, returns WriteResponses. */
  private def runModelTransformsFlinkJob(groupBy: GroupBy,
                                         mtConf: ModelTransforms,
                                         mockApi: MockApi): Seq[WriteResponse] = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val innerSource = mtConf.sources.get(0)
    require(innerSource.isSetEvents, "test only supports Events inner source")
    val deserQuery = innerSource.getEvents.query

    val encoder         = Encoders.product[ModelTransformsStreamEvent]
    val sparkExprEvalFn = new SparkExpressionEvalFn[ModelTransformsStreamEvent](
      encoder, deserQuery, groupBy.metaData.getName)
    val source = new ModelTransformsEventSource(testEvents, sparkExprEvalFn)

    val inputSchemaTypes = Seq(
      ("user_id", StringType),
      (Constants.TimeColumn, LongType)
    )
    // The tiling codec keys off the serving info's selected schema, which must include the
    // aggregation inputs (model output columns). Input schema + agg input columns.
    val aggInputColumns = new GroupByOps(groupBy).aggregationInputs.toSeq
    val outputSchemaTypes = inputSchemaTypes ++ Seq((aggInputColumns.head, DoubleType))
    val servingInfo = FetcherFlinkTestUtils.makeTestGroupByServingInfoParsed(
      groupBy, inputSchemaTypes, FetcherFlinkTestUtils.toSparkStructType(outputSchemaTypes))

    val writerFn = new MockAsyncKVStoreWriter(
      Seq.fill(100)(true), flinkMockApi, groupBy.metaData.getName)

    val mtPassthroughFields = Option(mtConf.passthroughFields).map(_.asScala.toSeq).getOrElse(Seq.empty)
    val mtModels = Option(mtConf.models).map(_.asScala.toSeq).getOrElse(Seq.empty)
    val job = new ModelTransformsSourceFlinkJob(
      eventSrc = source,
      inputSchema = inputSchemaTypes,
      sinkFn = writerFn,
      groupByServingInfoParsed = servingInfo,
      modelTransformsName = mtConf.metaData.name,
      passthroughFields = mtPassthroughFields,
      models = mtModels,
      innerSource = innerSource,
      parallelism = 2,
      props = Map("trigger" -> "always_fire"),
      topicInfo = TopicInfo.parse("kafka://model_transforms_stream"),
      api = mockApi,
      enableDebug = true,
      // Test skips the metadata-store lookup by passing the MT explicitly (prod default is None).
      explicitConfOverride = Some(mtConf)
    )

    job.runTiledGroupByJob(env).addSink(new CollectSink)
    env.execute("FetcherFlinkModelTransformsTest")

    CollectSink.values.toScala.toSeq
  }

  def makeModelTransformsGroupBy(): GroupBy = {
    val model = new Model()
    model.setValueSchema(DataType.toTDataType(
      StructType.from("score_model_output", Array(("score", DoubleType: DataType)))))
    model.setMetaData(Builders.MetaData(name = "score_model"))
    val inferenceSpec = new InferenceSpec()
    inferenceSpec.setModelBackend(ModelBackend.VertexAI)
    model.setInferenceSpec(inferenceSpec)

    val mt = new ModelTransforms()
    mt.setSources(Seq(
      Builders.Source.events(
        query = Builders.Query(
          selects = Map("user_id" -> "user_id"),
          timeColumn = "created"
        ),
        table = "test_mt.user_events",
        topic = "kafka://model_transforms_stream"
      )
    ).toJava)
    mt.setModels(Seq(model).toJava)
    mt.setPassthroughFields(Seq("user_id").toJava)
    mt.setMetaData(Builders.MetaData(name = "test_mt.mt_streaming_source"))

    val mtSource = new Source()
    mtSource.setModelTransforms(mt)

    Builders.GroupBy(
      sources = Seq(mtSource),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(Operation.LAST, inputColumn = "score_model__score")
      ),
      accuracy = Accuracy.TEMPORAL,
      metaData = Builders.MetaData(name = "integration_test.mt_streaming_gb", namespace = "test_mt", team = "chronon")
    )
  }
}

case class ModelTransformsStreamEvent(user_id: String, created: Long)

class ModelTransformsEventSource(events: Seq[ModelTransformsStreamEvent],
                                 sparkExprEvalFn: SparkExpressionEvalFn[ModelTransformsStreamEvent])
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

/** MockApi with a model platform returning deterministic per-user scores. */
class ModelTransformsMockApi(kvStore: () => KVStore) extends MockApi(kvStore, "test_mt") {
  override def generateModelPlatformProvider: ModelPlatformProvider = new ModelTransformsTestPlatformProvider
}

class ModelTransformsTestPlatformProvider(scores: Map[String, Double] =
                                            Map("user1" -> 0.85, "user2" -> 0.65, "user3" -> 0.5))
    extends ModelPlatformProvider {
  override def getPlatform(modelBackend: ModelBackend, backendParams: Map[String, String]): ModelPlatform =
    new ModelTransformsTestPlatform(scores)
}

class ModelTransformsTestPlatform(scores: Map[String, Double]) extends ModelPlatform {
  override def predict(predictRequest: PredictRequest): Future[PredictResponse] = {
    val outputs = predictRequest.inputRequests.map { inputRequest =>
      val userId = inputRequest.get("user_id").map(_.toString).getOrElse("unknown")
      Map("score" -> java.lang.Double.valueOf(scores.getOrElse(userId, 0.0)))
    }
    Future.successful(PredictResponse(predictRequest, Success(outputs)))
  }
  override def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] = ???
  override def createEndpoint(endpointConfig: EndpointConfig): Future[String] = ???
  override def deployModel(deployModelRequest: DeployModelRequest): Future[String] = ???
  override def getJobStatus(operation: ModelOperation, id: String): Future[ModelJobStatus] = ???
}
