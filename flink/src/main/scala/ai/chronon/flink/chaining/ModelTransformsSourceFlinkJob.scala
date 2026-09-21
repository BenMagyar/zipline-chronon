package ai.chronon.flink.chaining

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{DataType, Model, ModelTransforms, Source}
import ai.chronon.flink.{AsyncKVStoreWriter, BaseFlinkJob, FlinkUtils}
import ai.chronon.flink.FlinkJob.watermarkStrategy
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.source.{BatchIrSourceBuilder, FlinkSource}
import ai.chronon.flink.types.{AvroCodecOutput, WriteResponse}
import ai.chronon.flink.window.{
  AlwaysFireOnElementTrigger,
  BufferedProcessingTimeTrigger,
  GigaTileProcessFunction,
  MegaTileEmissionPolicy
}
import ai.chronon.online.fetcher.ModelTransformsSchemaUtil
import ai.chronon.online.{Api, GroupByServingInfoParsed, TopicInfo}

import java.util.concurrent.TimeUnit
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStream}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/** Streaming Flink job for a GroupBy whose source is a ModelTransforms.
  *
  * Pipeline: read the inner-source topic → watermark → optional join enrichment (when the inner is a
  * JoinSource) → optional join-source query transform → async model inference via
  * [[ModelTransformsAsyncFunction]] → shared tiled / mega-tiled / giga-tiled aggregation tail → KV write.
  * On any enrichment / query / model error the event is dropped and logged so a bad row can't poison
  * the aggregator.
  *
  * The ctor takes only the model-side data (`modelTransformsName`, `passthroughFields`, `models`) rather
  * than the full ModelTransforms — those three feed operator uids, logs, and
  * `ModelTransformsSchemaUtil.computePostModelSchema`, which is everything the class needs at wiring time.
  *
  * `explicitConfOverride = None` (production) makes the async fn resolve the MT conf by name via
  * `MetadataStore.getModelTransformsConf` at request time (mirrors how [[JoinEnrichmentAsyncFunction]]
  * resolves join confs). `Some(mt)` skips the lookup — MiniCluster integration tests use it to avoid
  * seeding a metadata store.
  */
class ModelTransformsSourceFlinkJob(eventSrc: FlinkSource[ProjectedEvent],
                                    inputSchema: Seq[(String, DataType)],
                                    sinkFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
                                    val groupByServingInfoParsed: GroupByServingInfoParsed,
                                    val modelTransformsName: String,
                                    val passthroughFields: Seq[String],
                                    val models: Seq[Model],
                                    val innerSource: Source,
                                    parallelism: Int,
                                    props: Map[String, String],
                                    topicInfo: TopicInfo,
                                    api: Api,
                                    enableDebug: Boolean = false,
                                    explicitConfOverride: Option[ModelTransforms] = None)
    extends BaseFlinkJob {

  val groupByName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  logger.info(f"Creating Flink ModelTransforms streaming job. groupByName=${groupByName}")

  val topic: String = topicInfo.name

  private val groupByConf = groupByServingInfoParsed.groupBy

  require(modelTransformsName != null && modelTransformsName.nonEmpty,
          s"modelTransformsName must be non-empty for groupBy: $groupByName")
  require(
    innerSource != null && (innerSource.isSetJoinSource || innerSource.isSetEvents),
    s"Only JoinSource or EventSource inner sources are currently supported for ModelTransforms streaming. " +
      s"Found: ${Option(innerSource).map(_.getSetField).orNull} for groupBy: $groupByName"
  )
  if (innerSource.isSetJoinSource) {
    require(
      innerSource.getJoinSource.getJoin.getLeft.isSetEvents,
      s"Only Events-based left sources are currently supported for ModelTransforms JoinSource inner. " +
        s"Found: ${innerSource.getJoinSource.getJoin.getLeft.getSetField} for groupBy: $groupByName"
    )
  }

  val keyColumns: Array[String] = groupByConf.keyColumns.toScala.toArray
  val valueColumns: Array[String] = groupByConf.aggregationInputs
  val eventTimeColumn = ai.chronon.api.Constants.TimeColumn

  // Two async hops (join enrichment → model inference) each get their own timeout / capacity so a slow
  // model can't starve enrichment. Fall back to the shared async_* props to stay compatible with older
  // job configs that only set the un-suffixed keys.
  private val defaultAsyncTimeout: String =
    FlinkUtils.getProperty("async_timeout_ms", props, topicInfo).getOrElse("5000")
  private val defaultAsyncCapacity: String =
    FlinkUtils.getProperty("async_capacity", props, topicInfo).getOrElse("100")

  private val joinAsyncTimeout: Long =
    FlinkUtils.getProperty("join_enrichment_async_timeout_ms", props, topicInfo).getOrElse(defaultAsyncTimeout).toLong
  private val joinAsyncCapacity: Int =
    FlinkUtils.getProperty("join_enrichment_async_capacity", props, topicInfo).getOrElse(defaultAsyncCapacity).toInt

  private val modelAsyncTimeout: Long =
    FlinkUtils.getProperty("model_async_timeout_ms", props, topicInfo).getOrElse(defaultAsyncTimeout).toLong
  private val modelAsyncCapacity: Int =
    FlinkUtils.getProperty("model_async_capacity", props, topicInfo).getOrElse(defaultAsyncCapacity).toInt

  private val kvStoreCapacity = FlinkUtils
    .getProperty("kv_concurrency", props, topicInfo)
    .map(_.toInt)
    .getOrElse(AsyncKVStoreWriter.kvStoreConcurrency)
  private val bufferingOutputTimeMillis =
    FlinkUtils.getNonNegativeLongProperty("buffering_output_time_millis", props, topicInfo)
  private val bufferingOutputJitterMillis =
    FlinkUtils.getNonNegativeLongProperty("buffering_output_jitter_millis", props, topicInfo)
  private val bufferingOutputPolicy =
    FlinkUtils
      .getProperty("buffering_output_policy", props, topicInfo)
      .map(MegaTileEmissionPolicy.fromString)
      .getOrElse(MegaTileEmissionPolicy.Default)
  private val firstSeenKeyGraceMillis =
    FlinkUtils.getNonNegativeLongProperty(GigaTileProcessFunction.FirstSeenKeyGraceMillisConfig, props, topicInfo)
  private val gigaTileBufferingOutputTimeMillis =
    FlinkUtils.gigaTileBufferingOutputTimeMillis(bufferingOutputTimeMillis, bufferingOutputPolicy)

  override def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      s"Building tiled Flink streaming job for groupBy: $groupByName with model transforms: " +
        s"$modelTransformsName using topic: $topic")
    val (processedStream, schema) = buildEnrichedStream(env)
    buildTiledTail(processedStream, schema, sinkFn, kvStoreCapacity, getTrigger(), getAllowedLatenessMs(), enableDebug)
  }

  private def getTrigger(): Trigger[ProjectedEvent, TimeWindow] =
    FlinkUtils.getProperty("trigger", props, topicInfo).getOrElse("always_fire") match {
      case "always_fire" => new AlwaysFireOnElementTrigger()
      case "buffered"    => new BufferedProcessingTimeTrigger(100L)
      case t =>
        throw new IllegalArgumentException(s"Unsupported trigger type: $t. Supported: 'always_fire', 'buffered'")
    }

  private def getAllowedLatenessMs(): Long = FlinkUtils.getAllowedLatenessMs(props, topicInfo)

  override def runMegaTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      s"Building mega tiled Flink streaming job for groupBy: $groupByName with model transforms: " +
        s"$modelTransformsName using topic: $topic")
    val (processedStream, schema) = buildEnrichedStream(env)
    buildMegaTiledTail(processedStream,
                       schema,
                       parallelism,
                       sinkFn,
                       kvStoreCapacity,
                       enableDebug,
                       bufferingOutputTimeMillis,
                       bufferingOutputJitterMillis,
                       bufferingOutputPolicy)
  }

  override def runGigaTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = {
    logger.info(
      s"Building giga tiled (push) Flink streaming job for groupBy: $groupByName with model transforms: " +
        s"$modelTransformsName using topic: $topic")
    val (processedStream, schema) = buildEnrichedStream(env)
    val batchIrStream = BatchIrSourceBuilder.build(env,
                                                   groupByServingInfoParsed,
                                                   props,
                                                   requireConfiguredBatchSource = firstSeenKeyGraceMillis > 0L)
    buildGigaTiledTail(processedStream,
                       batchIrStream,
                       schema,
                       parallelism,
                       sinkFn,
                       kvStoreCapacity,
                       enableDebug,
                       gigaTileBufferingOutputTimeMillis,
                       firstSeenKeyGraceMillis)
  }

  private def buildEnrichedStream(
      env: StreamExecutionEnvironment): (DataStream[ProjectedEvent], Seq[(String, DataType)]) = {
    val sourceSparkProjectedStream: DataStream[ProjectedEvent] = eventSrc
      .getDataStream(topic, groupByName)(env, parallelism)
      .uid(s"model-transforms-source-$groupByName")
      .name(s"ModelTransforms Source for $groupByName")

    val watermarkedStream = sourceSparkProjectedStream
      .assignTimestampsAndWatermarks(watermarkStrategy)
      .uid(s"model-transforms-watermarks-$groupByName")
      .name(s"Spark expression eval with timestamps for $groupByName")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val isJoinSourceInner = innerSource.isSetJoinSource
    val joinSource = if (isJoinSourceInner) innerSource.getJoinSource else null

    val enrichedStream =
      if (isJoinSourceInner) {
        val enrichmentFunction = new JoinEnrichmentAsyncFunction(
          joinSource.join.metaData.getName,
          groupByName,
          api,
          enableDebug
        )
        AsyncDataStream
          .unorderedWait(
            watermarkedStream,
            enrichmentFunction,
            joinAsyncTimeout,
            TimeUnit.MILLISECONDS,
            joinAsyncCapacity
          )
          .uid(s"join-enrichment-$groupByName")
          .name(s"Async Join Enrichment for $groupByName")
          .setParallelism(sourceSparkProjectedStream.getParallelism)
      } else {
        watermarkedStream
      }

    val hasJoinQuery = isJoinSourceInner && joinSource.query != null && joinSource.query.selects != null &&
      !joinSource.query.selects.isEmpty
    val queriedStream =
      if (hasJoinQuery) {
        logger.info("Applying join source query transformations for ModelTransforms inner source")
        val queryFunction = new JoinSourceQueryFunction(joinSource, inputSchema, groupByName, api, enableDebug)
        enrichedStream
          .flatMap(queryFunction)
          .uid(s"join-source-query-$groupByName")
          .name(s"Join Source Query for $groupByName")
          .setParallelism(sourceSparkProjectedStream.getParallelism)
      } else {
        if (isJoinSourceInner)
          logger.info("No join source query transformations to apply - using enriched stream directly")
        enrichedStream
      }

    val preModelSchema = computePreModelSchema(inputSchema, isJoinSourceInner, hasJoinQuery)

    val modelFn =
      new ModelTransformsAsyncFunction(modelTransformsName, explicitConfOverride, groupByName, api, enableDebug)
    val modelStream = AsyncDataStream
      .unorderedWait(
        queriedStream,
        modelFn,
        modelAsyncTimeout,
        TimeUnit.MILLISECONDS,
        modelAsyncCapacity
      )
      .uid(s"model-transforms-$groupByName")
      .name(s"Async Model Inference for $modelTransformsName ($groupByName)")
      .setParallelism(sourceSparkProjectedStream.getParallelism)

    val postTransformationSchema =
      ModelTransformsSchemaUtil.computePostModelSchema(preModelSchema, passthroughFields, models)
    (modelStream, postTransformationSchema)
  }

  /** Schema that model inference + passthrough see (post-enrichment, post-query). */
  private def computePreModelSchema(originalInputSchema: Seq[(String, DataType)],
                                    isJoinSourceInner: Boolean,
                                    hasJoinQuery: Boolean): Seq[(String, DataType)] = {
    if (!isJoinSourceInner) {
      originalInputSchema
    } else if (hasJoinQuery) {
      // Query defines the exact output schema.
      JoinSourceQueryFunction
        .buildCatalystUtil(innerSource.getJoinSource, originalInputSchema, api, enableDebug)
        .outputSchema
    } else {
      // No query transforms: join schema (enrichment inclusive) is the pre-model schema.
      val joinSchema =
        JoinSourceQueryFunction.buildJoinSchema(originalInputSchema, innerSource.getJoinSource, api, enableDebug)
      joinSchema.fields.map(f => (f.name, f.fieldType)).toSeq
    }
  }
}
