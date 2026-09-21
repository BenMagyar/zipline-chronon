package ai.chronon.flink.chaining

import ai.chronon.api
import ai.chronon.api.{Builders, Constants, DataType, StructType}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api._
import ai.chronon.flink.{FlinkJob, SparkExpressionEvalFn}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.FlinkSource
import ai.chronon.flink.test.{CollectSink, FlinkTestUtils, MockAsyncKVStoreWriter}
import ai.chronon.api.{EndpointConfig, JobStatusType, ModelBackend}
import ai.chronon.online.{Api, GroupByServingInfoParsed, ModelJobStatus, ModelOperation, ModelPlatform, ModelPlatformProvider, PredictRequest, PredictResponse, TopicInfo, TrainingRequest, DeployModelRequest}
import ai.chronon.online.fetcher.{Fetcher, MetadataStore}
import ai.chronon.online.serde.SparkConversions
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.spark.sql.Encoders
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Success

class ModelTransformsSourceFlinkJobIntegrationTest extends AnyFlatSpec with BeforeAndAfter with Matchers {
  import ModelTransformsTestUtils._

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(8)
      .setNumberTaskManagers(1)
      .build)

  before {
    flinkCluster.before()
    CollectSink.values.clear()
    ModelTransformsSourceGate.reset()
    ModelTransformsTestRegistry.clear()
  }

  after {
    flinkCluster.after()
    CollectSink.values.clear()
    ModelTransformsTestRegistry.clear()
  }

  /** Which tail mode we exercise on ModelTransformsSourceFlinkJob. */
  private sealed trait RunMode
  private case object Tiled extends RunMode
  private case object MegaTiled extends RunMode
  private case object GigaTiled extends RunMode

  /** `ExplicitConf` passes Some(mt) to the async fn; `KvLookup` passes None (prod wiring), and a stubbed
    * fetcher serves the conf from the test registry to stand in for the metadata store.
    */
  private sealed trait MtConfSource
  private case object ExplicitConf extends MtConfSource
  private case object KvLookup extends MtConfSource

  /** Mega/giga gate emissions on processing-time timers, so fixed historical timestamps rarely produce
    * observable output within a test's wall-clock window. Rebase everything to "now" instead.
    */
  private def rebaseToNow(events: Seq[ModelTransformsTestEvent]): Seq[ModelTransformsTestEvent] = {
    val now = System.currentTimeMillis()
    val latest = events.map(_.created).max
    events.map(e => e.copy(created = now - (latest - e.created)))
  }

  private def runModelTransformsJob(modelTransformsConf: api.ModelTransforms,
                                    groupBy: api.GroupBy,
                                    elements: Seq[ModelTransformsTestEvent],
                                    deserQuery: Query,
                                    mode: RunMode = Tiled,
                                    mtConfSource: MtConfSource = ExplicitConf): Unit = {
    implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val testApi = new ModelTransformsTestApi()

    val sparkExpressionEvalFn =
      new SparkExpressionEvalFn(Encoders.product[ModelTransformsTestEvent], deserQuery, groupBy.metaData.getName)
    val source = mode match {
      case Tiled =>
        new MockModelTransformsSource(elements, sparkExpressionEvalFn)
      case MegaTiled | GigaTiled =>
        // Bounded fromCollection finishes before processing-time timers fire; use the live-watermark
        // pattern from GigaTilePushFlinkIntegrationTest so the sink actually sees output.
        new LiveWatermarkedModelTransformsSource(rebaseToNow(elements), sparkExpressionEvalFn)
    }

    // Schema after the inner source's projection on the topic.
    val inputSchemaDataTypes = Seq(
      ("user_id", StringType),
      (Constants.TimeColumn, LongType)
    )

    // Giga's outputCodec needs batchEndDate/dateFormat set on the serving info before the aggregator
    // is built; route through the giga-friendly builder for that mode.
    val groupByServingInfoParsed = mode match {
      case GigaTiled =>
        makePushGroupByServingInfoParsed(
          groupBy,
          SparkConversions.fromChrononSchema(inputSchemaDataTypes),
          SparkConversions.fromChrononSchema(inputSchemaDataTypes)
        )
      case _ =>
        FlinkTestUtils.makeTestGroupByServingInfoParsed(
          groupBy,
          SparkConversions.fromChrononSchema(inputSchemaDataTypes),
          SparkConversions.fromChrononSchema(inputSchemaDataTypes)
        )
    }

    val topicInfo = TopicInfo.parse(
      "kafka://test-mt-topic/serde=custom/provider_class=ai.chronon.flink.deser.MockCustomSchemaProvider/schema_name=item_event")

    val writerFn = new MockAsyncKVStoreWriter(Seq(true), testApi, groupBy.metaData.name)
    val props = Map(
      "bootstrap" -> "localhost:9092",
      "trigger" -> "always_fire"
    )

    val asyncFetchConf = mtConfSource match {
      case ExplicitConf => Some(modelTransformsConf)
      case KvLookup =>
        // Stand in for metadataStore.getModelTransformsConf(name).
        ModelTransformsTestRegistry.register(modelTransformsConf)
        None
    }
    val passthroughFields =
      Option(modelTransformsConf.passthroughFields).map(_.asScala.toSeq).getOrElse(Seq.empty)
    val models = Option(modelTransformsConf.models).map(_.asScala.toSeq).getOrElse(Seq.empty)
    val flinkJob = new ModelTransformsSourceFlinkJob(
      eventSrc = source,
      inputSchema = inputSchemaDataTypes,
      sinkFn = writerFn,
      groupByServingInfoParsed = groupByServingInfoParsed,
      modelTransformsName = modelTransformsConf.metaData.name,
      passthroughFields = passthroughFields,
      models = models,
      innerSource = modelTransformsConf.sources.get(0),
      parallelism = 2,
      props = props,
      topicInfo = topicInfo,
      api = testApi,
      enableDebug = true,
      explicitConfOverride = asyncFetchConf
    )

    val jobDataStream = mode match {
      case Tiled     => flinkJob.runTiledGroupByJob(env)
      case MegaTiled => flinkJob.runMegaTiledGroupByJob(env)
      case GigaTiled => flinkJob.runGigaTiledGroupByJob(env)
    }
    // Live-watermark sources block until at least one write is observed — else env.execute never returns.
    val sinkPolicy: SinkPolicy = mode match {
      case Tiled                 => AcceptAll
      case MegaTiled | GigaTiled => ReleaseSourceOnFirst
    }
    jobDataStream.addSink(new GatedCollectSink(sinkPolicy))

    env.execute(s"ModelTransformsSourceFlinkJob Integration Test [$mode]")

    val outputs = CollectSink.values.asScala
    outputs should not be empty
    outputs.foreach { response =>
      response should not be null
      response.tsMillis should be > 0L
      response.valueBytes should not be null
      response.valueBytes.length should be > 0
      response.status should be(true)
      response.dataset should not be null
    }
  }

  it should "run the ModelTransforms event-source pipeline end to end (tiled)" in {
    val (mt, groupBy) = buildModelTransformsEventSourceGroupBy()
    runModelTransformsJob(mt, groupBy, testEvents, mt.sources.get(0).getEvents.query)
  }

  it should "run the tiled ModelTransforms pipeline end to end when the MT conf is resolved by name (KV-lookup path)" in {
    // Prod-shaped wiring: FlinkJob passes explicitConfOverride=None → async fn resolves by name.
    val (mt, groupBy) = buildModelTransformsEventSourceGroupBy()
    runModelTransformsJob(mt, groupBy, testEvents, mt.sources.get(0).getEvents.query, mtConfSource = KvLookup)
  }

  it should "run the ModelTransforms join-source pipeline end to end (tiled)" in {
    val (mt, groupBy) = buildModelTransformsJoinSourceGroupBy()
    // The deser query for a JoinSource inner is the join's left source query
    runModelTransformsJob(mt, groupBy, testEvents, mt.sources.get(0).getJoinSource.getJoin.getLeft.getEvents.query)
  }

  it should "run the ModelTransforms event-source pipeline end to end (mega tiled)" in {
    val (mt, groupBy) = buildModelTransformsEventSourceGroupBy()
    groupBy.setOnlineStrategy(OnlineStrategy.STREAMING_MEGATILES)
    runModelTransformsJob(mt, groupBy, testEvents, mt.sources.get(0).getEvents.query, MegaTiled)
  }

  it should "run the ModelTransforms join-source pipeline end to end (mega tiled)" in {
    val (mt, groupBy) = buildModelTransformsJoinSourceGroupBy()
    groupBy.setOnlineStrategy(OnlineStrategy.STREAMING_MEGATILES)
    runModelTransformsJob(
      mt,
      groupBy,
      testEvents,
      mt.sources.get(0).getJoinSource.getJoin.getLeft.getEvents.query,
      MegaTiled
    )
  }

  it should "run the ModelTransforms event-source pipeline end to end (giga tiled push)" in {
    val (mt, groupBy) = buildModelTransformsEventSourceGroupBy()
    groupBy.setOnlineStrategy(OnlineStrategy.PUSH)
    runModelTransformsJob(mt, groupBy, testEvents, mt.sources.get(0).getEvents.query, GigaTiled)
  }

  it should "run the ModelTransforms join-source pipeline end to end (giga tiled push)" in {
    val (mt, groupBy) = buildModelTransformsJoinSourceGroupBy()
    groupBy.setOnlineStrategy(OnlineStrategy.PUSH)
    runModelTransformsJob(
      mt,
      groupBy,
      testEvents,
      mt.sources.get(0).getJoinSource.getJoin.getLeft.getEvents.query,
      GigaTiled
    )
  }

  /** Push (giga) tail requires batchEndDate/dateFormat before the aggregator is built (needed by
    * outputCodec → batchEndTsMillis). Mirrors GigaTilePushFlinkIntegrationTest's helper.
    */
  private def makePushGroupByServingInfoParsed(
      groupBy: GroupBy,
      inputSchema: org.apache.spark.sql.types.StructType,
      outputSchema: org.apache.spark.sql.types.StructType): GroupByServingInfoParsed = {
    import ai.chronon.online.Extensions.StructTypeOps
    val servingInfo = new GroupByServingInfo()
    servingInfo.setGroupBy(groupBy)
    servingInfo.setBatchEndDate("2023-11-08")
    servingInfo.setDateFormat("yyyy-MM-dd")
    servingInfo.setInputAvroSchema(inputSchema.toAvroSchema("Input").toString(true))
    servingInfo.setKeyAvroSchema(
      org.apache.spark.sql.types
        .StructType(groupBy.keyColumns.toScala.map(col => outputSchema.fields.find(_.name == col).get))
        .toAvroSchema("Key")
        .toString(true))
    val aggInputCols = groupBy.aggregations.toScala.map(_.inputColumn).toList
    servingInfo.setSelectedAvroSchema(
      org.apache.spark.sql.types
        .StructType(outputSchema.fields.filter(f => aggInputCols.contains(f.name)))
        .toAvroSchema("Value")
        .toString(true))
    new GroupByServingInfoParsed(servingInfo)
  }
}

object ModelTransformsTestUtils {

  case class ModelTransformsTestEvent(user_id: String, amount: Double, created: Long)

  val testEvents: Seq[ModelTransformsTestEvent] = Seq(
    ModelTransformsTestEvent("user1", 50.0, 1699366993123L),
    ModelTransformsTestEvent("user2", 150.0, 1699366993124L),
    ModelTransformsTestEvent("user3", 25.0, 1699366993125L)
  )

  class MockModelPlatform extends ModelPlatform {
    override def predict(predictRequest: PredictRequest): Future[PredictResponse] = {
      val outputs = predictRequest.inputRequests.map { _ =>
        Map[String, AnyRef]("score" -> java.lang.Double.valueOf(0.5))
      }
      Future.successful(PredictResponse(predictRequest, Success(outputs)))
    }
    override def submitTrainingJob(trainingRequest: TrainingRequest): Future[String] = Future.successful("t")
    override def createEndpoint(endpointConfig: EndpointConfig): Future[String] = Future.successful("e")
    override def deployModel(deployModelRequest: DeployModelRequest): Future[String] = Future.successful("d")
    override def getJobStatus(operation: ModelOperation, id: String): Future[ModelJobStatus] =
      Future.successful(ModelJobStatus(JobStatusType.UNKNOWN, "n/a"))
  }

  class MockModelPlatformProvider extends ModelPlatformProvider {
    override def getPlatform(modelBackend: ModelBackend, backendParams: Map[String, String]): ModelPlatform =
      new MockModelPlatform
  }

  /** Stand-in for a metadata-store-backed MT lookup. The stubbed fetcher below reads from this when
    * the caller passes None. Concurrent map because Flink drives asyncInvoke from multiple threads.
    */
  object ModelTransformsTestRegistry {
    private val registry = new java.util.concurrent.ConcurrentHashMap[String, api.ModelTransforms]()
    def register(mt: api.ModelTransforms): Unit = registry.put(mt.metaData.name, mt)
    def get(name: String): Option[api.ModelTransforms] = Option(registry.get(name))
    def clear(): Unit = registry.clear()
  }

  class ModelTransformsTestApi extends TestApi {
    override def generateModelPlatformProvider: ModelPlatformProvider = new MockModelPlatformProvider

    // Custom Fetcher (JoinTestUtils' TestFetcher has no model platform provider). fetchJoin is stubbed
    // because the real path reads the join conf from KV, which the test store doesn't serve; the
    // model-transforms path stays real.
    override def buildFetcher(debug: Boolean,
                              callerName: String,
                              disableErrorThrows: Boolean,
                              joinConfTtlMillis: Long,
                              joinCodecTtlMillis: Long): Fetcher = {
      new Fetcher(
        kvStore = genKvStore,
        metaDataSet = "test_metadata",
        timeoutMillis = 5000L,
        logFunc = null,
        debug = false,
        externalSourceRegistry = null,
        modelPlatformProvider = generateModelPlatformProvider,
        callerName = "test",
        flagStore = null,
        disableErrorThrows = true
      ) with Serializable {
        override val metadataStore: MetadataStore = new TestMetadataStore()

        override def fetchJoin(requests: Seq[Fetcher.Request],
                               joinConf: Option[Join] = None): Future[Seq[Fetcher.Response]] =
          Future.successful(requests.map { request =>
            Fetcher.Response(request, Success(Map("price_discounted_last" -> java.lang.Double.valueOf(10.0))))
          })

        // None branch (prod wiring): serve from the test registry instead of touching the KV store.
        override def fetchModelTransforms(requests: scala.Seq[Fetcher.Request],
                                          modelTransformsConf: Option[api.ModelTransforms] = None)
            : Future[scala.Seq[Fetcher.Response]] = {
          modelTransformsConf match {
            case Some(_) => super.fetchModelTransforms(requests, modelTransformsConf)
            case None =>
              requests.headOption
                .flatMap(req => ModelTransformsTestRegistry.get(req.name))
                .map(mt => super.fetchModelTransforms(requests, Some(mt)))
                .getOrElse(super.fetchModelTransforms(requests, None))
          }
        }
      }
    }
  }

  /** Bounded collection source — used by the tiled path where `always_fire` emits every element. */
  class MockModelTransformsSource(mockEvents: Seq[ModelTransformsTestEvent],
                                  sparkExprEvalFn: SparkExpressionEvalFn[ModelTransformsTestEvent])
      extends FlinkSource[ProjectedEvent] {
    implicit val parallelism: Int = 1
    override def getDataStream(topic: String, groupName: String)(
        env: StreamExecutionEnvironment,
        parallelism: Int): SingleOutputStreamOperator[ProjectedEvent] = {
      val watermarkStrategy = WatermarkStrategy
        .forBoundedOutOfOrderness[ModelTransformsTestEvent](java.time.Duration.ofSeconds(5))
        .withTimestampAssigner(
          new org.apache.flink.api.common.eventtime.SerializableTimestampAssigner[ModelTransformsTestEvent] {
            override def extractTimestamp(event: ModelTransformsTestEvent, ts: Long): Long = event.created
          })
      env
        .fromCollection(mockEvents.asJava)
        .assignTimestampsAndWatermarks(watermarkStrategy)
        .flatMap(sparkExprEvalFn)
        .map(e => ProjectedEvent(e, System.currentTimeMillis()))
    }
  }

  /** Live-watermarked source for mega/giga — emits a wall-clock watermark before records and blocks
    * until the sink observes output. Mirrors LiveWatermarkedE2EEventSource; without it, processing-
    * time timers never fire on the bounded fixture.
    */
  class LiveWatermarkedModelTransformsSource(mockEvents: Seq[ModelTransformsTestEvent],
                                             sparkExprEvalFn: SparkExpressionEvalFn[ModelTransformsTestEvent])
      extends FlinkSource[ProjectedEvent] {
    implicit val parallelism: Int = 1
    override def getDataStream(topic: String, groupName: String)(
        env: StreamExecutionEnvironment,
        parallelism: Int): SingleOutputStreamOperator[ProjectedEvent] = {
      env
        .addSource(new LiveWatermarkedEventSource(mockEvents))
        .flatMap(sparkExprEvalFn)
        .map(e => ProjectedEvent(e, System.currentTimeMillis()))
    }
  }

  class LiveWatermarkedEventSource(elements: Seq[ModelTransformsTestEvent])
      extends SourceFunction[ModelTransformsTestEvent] {
    @volatile private var running = true

    private def boundedOutOfOrdernessWatermark(maxEventTime: Long): Long = {
      val delta = FlinkJob.AllowedOutOfOrderness.toMillis + 1L
      if (maxEventTime < Long.MinValue + delta) Long.MinValue else maxEventTime - delta
    }

    override def run(ctx: SourceFunction.SourceContext[ModelTransformsTestEvent]): Unit = {
      val lock = ctx.getCheckpointLock
      elements.headOption.foreach { first =>
        val initialWatermark = boundedOutOfOrdernessWatermark(first.created)
        lock.synchronized(ctx.emitWatermark(new Watermark(initialWatermark)))
      }

      var maxEventTime = Long.MinValue
      elements.iterator.takeWhile(_ => running).foreach { event =>
        lock.synchronized {
          maxEventTime = math.max(maxEventTime, event.created)
          ctx.collectWithTimestamp(event, event.created)
          ctx.emitWatermark(new Watermark(boundedOutOfOrdernessWatermark(maxEventTime)))
        }
      }

      if (running && !ModelTransformsSourceGate.awaitOutput()) {
        throw new IllegalStateException("Timed out waiting for pipeline output")
      }
    }

    override def cancel(): Unit = running = false
  }

  private def buildModel(modelName: String, valueFields: Seq[(String, DataType)]): Model = {
    val model = new Model()
    val fields = valueFields.map { case (name, typ) => StructField(name, typ) }.toArray
    model.setValueSchema(DataType.toTDataType(StructType.from(modelName, fields.map(f => (f.name, f.fieldType)))))
    model.setMetaData(Builders.MetaData(name = modelName))
    val inferenceSpec = new InferenceSpec()
    inferenceSpec.setModelBackend(ModelBackend.VertexAI)
    model.setInferenceSpec(inferenceSpec)
    model
  }

  def buildModelTransformsEventSourceGroupBy(): (api.ModelTransforms, api.GroupBy) = {
    val model = buildModel("score_model", Seq(("score", DoubleType)))
    val mt = new api.ModelTransforms()
    mt.setSources(
      Seq(
        Builders.Source.events(
          query = Builders.Query(
            selects = Map("user_id" -> "user_id"),
            timeColumn = "created"
          ),
          table = "test.user_events",
          topic = "kafka://test-mt-topic/serde=custom/provider_class=ai.chronon.flink.deser.MockCustomSchemaProvider/schema_name=item_event"
        )
      ).toJava)
    mt.setModels(Seq(model).toJava)
    mt.setPassthroughFields(Seq("user_id").toJava)
    mt.setMetaData(Builders.MetaData(name = "test.mt_event_source"))

    val groupBy = Builders.GroupBy(
      sources = Seq(wrapModelTransforms(mt)),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.LAST,
          inputColumn = "score_model__score"
        )
      ),
      metaData = Builders.MetaData(name = "test.mt_event_gb"),
      accuracy = Accuracy.TEMPORAL
    )
    (mt, groupBy)
  }

  def buildModelTransformsJoinSourceGroupBy(): (api.ModelTransforms, api.GroupBy) = {
    val model = buildModel("score_model", Seq(("score", DoubleType)))
    val mt = new api.ModelTransforms()
    mt.setMetaData(Builders.MetaData(name = "test.mt_join_source"))
    mt.setModels(Seq(model).toJava)
    mt.setPassthroughFields(Seq("user_id").toJava)
    mt.setSources(
      Seq(
        Builders.Source.joinSource(
          join = Builders.Join(
            left = Builders.Source.events(
              query = Builders.Query(
                selects = Map("user_id" -> "user_id"),
                timeColumn = "created"
              ),
              table = "test.user_events",
              topic = "kafka://test-mt-join-topic/serde=custom/provider_class=ai.chronon.flink.deser.MockCustomSchemaProvider/schema_name=item_event"
            ),
            joinParts = Seq.empty,
            metaData = Builders.MetaData(name = "test.mt_parent_join")
          ),
          query = Builders.Query()
        )
      ).toJava)

    val groupBy = Builders.GroupBy(
      sources = Seq(wrapModelTransforms(mt)),
      keyColumns = Seq("user_id"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.LAST,
          inputColumn = "score_model__score"
        )
      ),
      metaData = Builders.MetaData(name = "test.mt_join_source"),
      accuracy = Accuracy.TEMPORAL
    )
    (mt, groupBy)
  }

  def wrapModelTransforms(mt: api.ModelTransforms): Source = {
    val source = new Source()
    source.setModelTransforms(mt)
    source
  }

  /** Tiled just collects; mega/giga also release the gate so the live-watermark source can exit. */
  sealed trait SinkPolicy
  case object AcceptAll extends SinkPolicy
  case object ReleaseSourceOnFirst extends SinkPolicy

  class GatedCollectSink(policy: SinkPolicy)
      extends org.apache.flink.streaming.api.functions.sink.SinkFunction[
        ai.chronon.flink.types.WriteResponse] {
    override def invoke(
        value: ai.chronon.flink.types.WriteResponse,
        context: org.apache.flink.streaming.api.functions.sink.SinkFunction.Context): Unit = {
      CollectSink.values.add(value)
      policy match {
        case AcceptAll            => ()
        case ReleaseSourceOnFirst => ModelTransformsSourceGate.outputObserved()
      }
    }
  }
}

/** Source blocks on `awaitOutput()`; sink calls `outputObserved()` after the first write. */
object ModelTransformsSourceGate {
  @volatile private var outputReady = new CountDownLatch(1)

  def reset(): Unit = {
    outputReady = new CountDownLatch(1)
  }

  def outputObserved(): Unit = outputReady.countDown()

  def awaitOutput(): Boolean = outputReady.await(30, TimeUnit.SECONDS)
}
