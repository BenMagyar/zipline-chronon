package ai.chronon.flink.validation

import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.flink.validation.SparkExprEvalComparisonFn.compareResultRows
import ai.chronon.flink.{SparkExpressionEval, SparkExpressionEvalFn}
import ai.chronon.online.fetcher.MetadataStore
import ai.chronon.online.{GroupByServingInfoParsed, TopicInfo}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.apache.spark.sql.{Encoder, Row}
import org.slf4j.LoggerFactory

import java.lang
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.flink.FlinkJob.CheckPointInterval
import ai.chronon.flink.deser.{DeserializationSchemaBuilder, FlinkSerDeProvider}
import ai.chronon.flink.source.{FlinkSource, FlinkSourceProvider}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode

import scala.concurrent.duration.DurationInt

case class EventRecord(recordId: String, event: Row)
case class ValidationStats(totalRecords: Int,
                           totalMatches: Int,
                           totalMismatches: Int,
                           catalystRowCount: Int,
                           sparkDfRowCount: Int,
                           mismatches: Seq[ComparisonResult]) {
  override def toString: String = {
    s"""
         |Total Records: $totalRecords
         |Total Matches: $totalMatches
         |Total Mismatches: $totalMismatches
         |Total Catalyst Rows: $catalystRowCount
         |Total Spark DF Rows: $sparkDfRowCount
         |Mismatch examples (limited to 100):
         |${mismatches.mkString("\n")}
         |""".stripMargin
  }
}

/** A Flink window function that compares the results of Catalyst and Spark DataFrame evaluation for a given set of records.
  */
class SparkDFVsCatalystComparisonFn(sparkExpressionEvalFn: SparkExpressionEvalFn[Row])
    extends RichAllWindowFunction[EventRecord, ValidationStats, GlobalWindow] {

  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    sparkExpressionEvalFn.setRuntimeContext(this.getRuntimeContext)
    try {
      sparkExpressionEvalFn.open(parameters)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to initialize SparkExpressionEval function - ${e.getMessage}")
        throw new RuntimeException(
          s"Failed to initialize SparkExpressionEval function. " +
            s"Make sure the Spark SQL expressions are valid (e.g. column names match the source event schema).")
    }
  }

  override def apply(window: GlobalWindow, input: lang.Iterable[EventRecord], out: Collector[ValidationStats]): Unit = {
    val inputRecords = input.toScala.toSeq.map(r => (r.recordId, r.event))
    logger.info(s"Kicking off Spark Sql vs CU comparison for ${inputRecords.size} records")
    val catalystResults = sparkExpressionEvalFn.runCatalystBulk(inputRecords)
    logger.info("Finished Catalyst evaluation")
    val sparkSQLResults = sparkExpressionEvalFn.runSparkSQLBulk(inputRecords)
    logger.info("Finished Spark SQL evaluation")
    val comparisonResults = inputRecords.map(_._1).map { recordId =>
      val catalystResult = catalystResults(recordId)
      val sparkSQLResult = sparkSQLResults(recordId)
      compareResultRows(recordId, catalystResult.toSeq, sparkSQLResult.toSeq)
    }

    val total = comparisonResults.size
    val matching = comparisonResults.count(_.isMatch)
    val mismatches = comparisonResults.filterNot(_.isMatch)
    val cuOutputRowCount = catalystResults.values.map(_.size).sum
    val sparkOutputRowCount = sparkSQLResults.values.map(_.size).sum
    logger.info("Wrapped up comparison. Emitted stats")
    // limit to 100 mismatches to avoid flooding the logs
    out.collect(
      ValidationStats(total, matching, mismatches.size, cuOutputRowCount, sparkOutputRowCount, mismatches.take(100)))
  }
}

class ValidationFlinkJob(eventSrc: FlinkSource[Row],
                         groupByServingInfoParsed: GroupByServingInfoParsed,
                         encoder: Encoder[Row],
                         parallelism: Int,
                         validationRows: Int) {

  private[this] val logger = LoggerFactory.getLogger(getClass)

  val groupByName: String = groupByServingInfoParsed.groupBy.getMetaData.getName
  logger.info(f"Creating Flink job. groupByName=${groupByName}")

  if (groupByServingInfoParsed.groupBy.streamingSource.isEmpty) {
    throw new IllegalArgumentException(
      s"Invalid groupBy: $groupByName. No streaming source"
    )
  }

  // The source of our Flink application is a  topic
  val topic: String = groupByServingInfoParsed.groupBy.streamingSource.get.topic

  def runValidationJob(env: StreamExecutionEnvironment): DataStream[ValidationStats] = {

    logger.info(s"Running Validation job for groupByName=$groupByName, Topic=$topic")

    val sourceStream: DataStream[Row] =
      eventSrc
        .getDataStream(topic, groupByName)(env, parallelism)
        .uid(s"source-$groupByName")
        .name(s"Source for $groupByName")

    // add a unique record ID to every record - this is needed to correlate results from the two operators as we can have
    // 0 to n records per input event.
    val sourceStreamWithId: DataStream[EventRecord] = sourceStream
      .map(new MapFunction[Row, EventRecord] {
        override def map(e: Row): EventRecord = EventRecord(java.util.UUID.randomUUID().toString, e)
      })
      .uid(s"source-with-id-$groupByName")
      .name(s"Source with ID for $groupByName")
      .setParallelism(sourceStream.getParallelism) // Use same parallelism as previous operator

    val query = SparkExpressionEval.queryFromGroupBy(groupByServingInfoParsed.groupBy)
    sourceStreamWithId
      .countWindowAll(validationRows)
      .apply(new SparkDFVsCatalystComparisonFn(
        new SparkExpressionEvalFn[Row](encoder, query, groupByName, groupByServingInfoParsed.groupBy.dataModel)))
      .returns(TypeInformation.of(classOf[ValidationStats]))
      .uid(s"validation-stats-$groupByName")
      .name(s"Validation stats for $groupByName")
      .setParallelism(1)
  }
}

object ValidationFlinkJob {
  def run(metadataStore: MetadataStore,
          props: Map[String, String],
          groupByName: String,
          validateRows: Int): Seq[ValidationStats] = {

    val maybeServingInfo = metadataStore.getGroupByServingInfo(groupByName)
    val validationJob: ValidationFlinkJob = maybeServingInfo
      .map { servingInfo =>
        val topicUri = servingInfo.groupBy.streamingSource.get.topic
        val topicInfo = TopicInfo.parse(topicUri)

        val schemaProvider = FlinkSerDeProvider.build(topicInfo)

        val deserializationSchema =
          DeserializationSchemaBuilder.buildSourceIdentityDeserSchema(schemaProvider, servingInfo.groupBy)

        val source = FlinkSourceProvider.build(props, deserializationSchema, topicInfo)

        // keep //ism low as we just need a small set of rows to compare against
        new ValidationFlinkJob(
          eventSrc = source,
          groupByServingInfoParsed = servingInfo,
          encoder = deserializationSchema.sourceEventEncoder,
          parallelism = 1,
          validationRows = validateRows
        )
      }
      .recover { case e: Exception =>
        throw new IllegalArgumentException(s"Unable to lookup serving info for GroupBy: '$groupByName'", e)
      }
      .get

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig
      .enableForceKryo() // use kryo for complex types that Flink's default ser system doesn't support (e.g case classes)
    env.getConfig.enableGenericTypes() // more permissive type checks
    env.enableCheckpointing(CheckPointInterval.toMillis, CheckpointingMode.AT_LEAST_ONCE)
    // cap to 3 restarts in the validation job to surface errors quickly
    env.setRestartStrategy(
      RestartStrategies.fixedDelayRestart(
        3,
        30.seconds.toMillis
      ))

    val jobDatastream = validationJob.runValidationJob(env)

    // Our Flink Kafka source is set up to run in an unbounded fashion by default. We retrieve one ValidationStats object
    // corresponding to the 'validateRows' number of records from the source and terminate the job.
    val resultStatsList = jobDatastream.executeAndCollect(1)
    resultStatsList.toScala.foreach { stats =>
      println(s"**** Validation stats for $groupByName **** \n$stats")
    }

    resultStatsList.toScala
  }
}
