package ai.chronon.flink.chaining

import ai.chronon.api.{Constants, DataType, JoinSource, StructField, StructType}
import ai.chronon.flink.{ERROR, FlinkLogging, INFO}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.online.{Api, CatalystUtil, JoinCodec}
import ai.chronon.online.serde.SparkConversions
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._

/** Flink function for applying join source query transformations to enriched events.
  *
  * This function takes ProjectedEvent objects (after join enrichment) and applies
  * the joinSource.query SQL expressions to produce the final processed events
  * for aggregation and storage.
  *
  * @param joinSource The JoinSource configuration containing the query to apply
  * @param inputSchema Schema of the left source (before enrichment)
  * @param api API implementation for join codec access
  * @param enableDebug Whether to enable debug logging
  */
class JoinSourceQueryFunction(joinSource: JoinSource,
                              inputSchema: Seq[(String, DataType)],
                              groupByName: String,
                              api: Api,
                              enableDebug: Boolean)
    extends RichFlatMapFunction[ProjectedEvent, ProjectedEvent]
    with FlinkLogging {
  @transient private var catalystUtil: CatalystUtil = _
  @transient private var successCounter: Counter = _
  @transient private var errorCounter: Counter = _
  @transient private var queryLatencyHistogram: Histogram = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    log(INFO, "Initializing CatalystUtil for join source query evaluation")

    val result = JoinSourceQueryFunction.buildCatalystUtil(joinSource, inputSchema, api, enableDebug)
    catalystUtil = result.catalystUtil

    val group = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("join_source", joinSource.join.metaData.getName)
      .addGroup("group_by", groupByName)

    successCounter = group.counter("query_eval.successes")
    errorCounter = group.counter("query_eval.errors")
    queryLatencyHistogram = group.histogram(
      "query_eval_latency",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new com.codahale.metrics.ExponentiallyDecayingReservoir())
      )
    )

    log(INFO, s"Initialized CatalystUtil with join schema")
  }

  override def flatMap(enrichedEvent: ProjectedEvent, out: Collector[ProjectedEvent]): Unit = {
    val startTime = System.currentTimeMillis()
    try {
      val queryResults = catalystUtil.performSql(enrichedEvent.fields)

      queryLatencyHistogram.update(System.currentTimeMillis() - startTime)
      successCounter.inc()

      if (enableDebug) {
        log(INFO, s"Join source query input: ${enrichedEvent.fields}")
        log(INFO, s"Join source query results: $queryResults")
      }

      // Output each result as a ProjectedEvent
      queryResults.foreach { resultFields =>
        val resultEvent = ProjectedEvent(resultFields, enrichedEvent.startProcessingTimeMillis)
        out.collect(resultEvent)
      }
    } catch {
      case ex: Exception =>
        // we swallow the event on error
        errorCounter.inc()
        queryLatencyHistogram.update(System.currentTimeMillis() - startTime)
        logThrottled(ERROR,
                     "join_query_error",
                     s"Error applying join source query to event: ${enrichedEvent.fields}",
                     ex)
    }
  }
}

case class JoinSourceQueryResult(
    catalystUtil: CatalystUtil,
    joinSchema: StructType,
    outputSchema: Seq[(String, DataType)]
)

object JoinSourceQueryFunction {
  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def buildCatalystUtil(
      joinSource: JoinSource,
      inputSchema: Seq[(String, DataType)],
      api: Api,
      enableDebug: Boolean
  ): JoinSourceQueryResult = {

    // Build join schema (leftSourceSchema + joinCodec.valueSchema)
    val joinSchema = buildJoinSchema(inputSchema, joinSource, api, enableDebug)

    // Handle time column mapping like SparkExpressionEval.buildQueryTransformsAndFilters
    val timeColumn = Option(joinSource.query.timeColumn).getOrElse(Constants.TimeColumn)
    val rawSelects = joinSource.query.selects.asScala.toMap

    val timeColumnMapping = Map(Constants.TimeColumn -> timeColumn) // Add ts -> timeColumn mapping
    val selectsWithTimeColumn = (rawSelects ++ timeColumnMapping).toSeq
    val wheres = Option(joinSource.query.wheres).map(_.asScala.toSeq).getOrElse(Seq.empty)

    // Create CatalystUtil instance
    val catalystUtil = new CatalystUtil(joinSchema, selectsWithTimeColumn, wheres)

    // Get the output schema from Catalyst and convert to Chronon format
    val outputSparkSchema = catalystUtil.getOutputSparkSchema
    val outputSchema = outputSparkSchema.fields.map { field =>
      val chrononType = SparkConversions.toChrononType(field.name, field.dataType)
      (field.name, chrononType)
    }.toSeq

    logger.info(s"""
         |Building CatalystUtil for join source query:
         |Selects with time column: ${selectsWithTimeColumn}
         |Wheres: ${wheres}
         |Time column mapping: ${Constants.TimeColumn} -> ${timeColumn}
         |Output schema: ${outputSchema.map { case (name, dataType) => s"$name: $dataType" }.mkString(", ")}
         |""".stripMargin)

    JoinSourceQueryResult(catalystUtil, joinSchema, outputSchema)
  }

  /** Build the join schema following JoinSourceRunner.buildSchemas approach:
    * joinSchema = leftSourceSchema ++ joinCodec.valueSchema
    */
  def buildJoinSchema(
      inputSchema: Seq[(String, DataType)],
      joinSource: JoinSource,
      api: Api,
      enableDebug: Boolean
  ): StructType = {
    // leftSourceSchema: Convert inputSchema to Chronon StructType
    val leftSourceFields = inputSchema.map { case (name, dataType) =>
      StructField(name, dataType)
    }.toArray
    val leftSourceSchema = StructType("left_source", leftSourceFields)

    // joinCodec.valueSchema: Get schema of enriched fields from upstream join
    val joinCodec: JoinCodec = api
      .buildFetcher(debug = enableDebug)
      .metadataStore
      .buildJoinCodec(joinSource.getJoin, refreshOnFail = false)

    // joinSchema = leftSourceSchema ++ joinCodec.valueSchema
    val joinFields = leftSourceSchema.fields ++ joinCodec.valueSchema.fields
    val joinSchema = StructType("join_enriched", joinFields)

    logger.info(s"""
         |Schema building for join source query:
         |leftSourceSchema (${leftSourceFields.length} fields):
         |  ${leftSourceFields.map(f => s"${f.name}: ${f.fieldType}").mkString(", ")}
         |joinCodec.valueSchema (${joinCodec.valueSchema.fields.length} fields):
         |  ${joinCodec.valueSchema.fields.map(f => s"${f.name}: ${f.fieldType}").mkString(", ")}
         |joinSchema (${joinFields.length} fields):
         |  ${joinFields.map(f => s"${f.name}: ${f.fieldType}").mkString(", ")}
         |""".stripMargin)

    joinSchema
  }
}
