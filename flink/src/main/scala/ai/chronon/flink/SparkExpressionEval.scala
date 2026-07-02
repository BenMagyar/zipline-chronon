package ai.chronon.flink

import ai.chronon.api.{Constants, DataModel, GroupBy, Query, StructType => ChrononStructType}
import ai.chronon.api.Extensions.{GroupByOps, MetadataOps, SourceOps}
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.online.CatalystUtil
import ai.chronon.online.serde.SparkConversions
import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Histogram, MetricGroup}
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters._

/** Core utility class for Spark expression evaluation that can be reused across different Flink operators.
  * This evaluator is instantiated for a given EventType (specific case class object, Thrift / Proto object).
  * Based on the selects and where clauses in the Query, this function projects and filters the input data and
  * emits a Map which contains the relevant fields & values that are needed to compute the aggregated values.
  * This class is meant to be used in Flink operators (e.g. DeserializationSchema, RichMapFunctions) to run Spark expression evals.
  *
  * @param encoder Spark Encoder for the input event
  * @param query The Query to evaluate
  * @param groupByName Name for metrics and schema identification
  * @tparam EventType The type of the input event.
  */
class SparkExpressionEval[EventType](encoder: Encoder[EventType],
                                     query: Query,
                                     groupByName: String,
                                     dataModel: DataModel = DataModel.EVENTS)
    extends Serializable
    with FlinkLogging {

  import SparkExpressionEval._

  private val (transforms, filters) = buildQueryTransformsAndFilters(query, dataModel)

  // Chronon's CatalystUtil expects a Chronon `StructType` so we convert the
  // Encoder[T]'s schema to one.
  val chrononSchema: ChrononStructType =
    ChrononStructType.from(
      groupByName,
      SparkConversions.toChrononSchema(encoder.schema)
    )

  @transient private var catalystUtil: CatalystUtil = _

  // Metrics
  @transient private var exprEvalTimeHistogram: Histogram = _
  @transient private var rowSerTimeHistogram: Histogram = _
  @transient private var exprEvalSuccessCounter: Counter = _
  @transient private var exprEvalErrorCounter: Counter = _

  def initialize(metricsGroup: MetricGroup): Unit = {
    exprEvalTimeHistogram = metricsGroup.histogram(
      "spark_expr_eval_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )

    rowSerTimeHistogram = metricsGroup.histogram(
      "spark_row_ser_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )

    exprEvalSuccessCounter = metricsGroup.counter("spark_expr_eval_success")
    exprEvalErrorCounter = metricsGroup.counter("spark_expr_eval_errors")

    // Initialize CatalystUtil without acquiring session reference
    val setups = Option(query.setups).map(_.asScala.toSeq).getOrElse(Seq.empty)
    catalystUtil = new CatalystUtil(chrononSchema, transforms, filters, setups)
  }

  def performSql(row: InternalRow): Seq[Map[String, Any]] = {
    val exprEvalStart = System.currentTimeMillis()
    val result = catalystUtil.performSql(row)
    exprEvalTimeHistogram.update(System.currentTimeMillis() - exprEvalStart)
    exprEvalSuccessCounter.inc()

    result
  }

  def performSql(row: Array[Any]): Seq[Map[String, Any]] = {
    val internalRow = catalystUtil.inputArrEncoder(row).asInstanceOf[InternalRow]
    performSql(internalRow)
  }

  def evaluateExpressions(inputEvent: EventType,
                          rowSerializer: ExpressionEncoder.Serializer[EventType]): Seq[Map[String, Any]] = {
    try {
      val start = System.currentTimeMillis()
      val row: InternalRow = rowSerializer(inputEvent)
      rowSerTimeHistogram.update(System.currentTimeMillis() - start)

      performSql(row)
    } catch {
      case e: Exception =>
        logThrottled(ERROR, "spark_expr_error", "Error evaluating Spark expression", e)
        exprEvalErrorCounter.inc()
        Seq.empty
    }
  }

  def getOutputSchema: StructType = {
    val setups = Option(query.setups).map(_.asScala.toSeq).getOrElse(Seq.empty)
    new CatalystUtil(chrononSchema, transforms, filters, setups).getOutputSparkSchema
  }

  def close(): Unit = {
    if (catalystUtil != null) {
      // Don't close the shared session - let it remain open for other components
      catalystUtil = null
    }
  }

  // Utility method to help with result validation. This method is used to match results of the core catalyst util based
  // eval against Spark DF based eval. To do the Spark Df based eval, we:
  // 1. Create a df with the events + record_id tacked on
  // 2. Apply the projections and filters based on how we've set up the CatalystUtil instance based on the input groupBy.
  // 3. Collect the results and group them by record_id
  def runSparkSQLBulk(idToRecords: Seq[(String, Row)]): Map[String, Seq[Map[String, Any]]] = {

    val idField = StructField("__record_id", StringType, false)
    val fullSchema = StructType(idField +: encoder.schema.fields)
    val fullRows = idToRecords.map { case (id, row) =>
      // Create a new Row with id as the first field, followed by all fields from the original row
      Row.fromSeq(id +: row.toSeq)
    }

    val rowsRdd = CatalystUtil.session.sparkContext.parallelize(fullRows.toSeq)

    val eventDfs = CatalystUtil.session
      .createDataFrame(rowsRdd, fullSchema)

    // Apply filtering if needed
    val filteredDf = catalystUtil.whereClauseOpt match {
      case Some(whereClause) => eventDfs.where(whereClause)
      case None              => eventDfs
    }

    // Apply projections while preserving the index
    val projectedDf = filteredDf.selectExpr(
      // Include the index column and all the select clauses
      Array("__record_id") ++ catalystUtil.selectClauses: _*
    )

    // Collect the results
    val results = projectedDf.collect()

    // Group results by record ID
    val resultsByRecordId = results.groupBy(row => row.getString(0))

    // Map back to the original record order
    idToRecords.map { record =>
      val recordId = record._1
      val resultRows = resultsByRecordId.getOrElse(recordId, Array.empty)

      val maps = resultRows.map { row =>
        val columnNames = projectedDf.columns.tail // Skip the record ID column
        columnNames.zipWithIndex.map { case (colName, i) =>
          (colName, row.get(i + 1)) // +1 to skip the record ID column
        }.toMap
      }.toSeq

      (recordId, maps)
    }.toMap
  }

  // Utility method to help with result validation. This method is used to match results of the core catalyst util based
  // eval against Spark DF based eval. This method iterates over the input records and hits the catalyst performSql method
  // to collect results.
  def runCatalystBulk(records: Seq[(String, EventType)],
                      rowSerializer: ExpressionEncoder.Serializer[EventType]): Map[String, Seq[Map[String, Any]]] = {
    records.map { record =>
      val recordId = record._1
      val row = rowSerializer(record._2)
      val maybeRow = catalystUtil.performSql(row)
      (recordId, maybeRow)
    }.toMap
  }
}

object SparkExpressionEval {
  def queryFromGroupBy(gb: GroupBy): Query = {
    require(gb.streamingSource.isDefined, s"Streaming source is missing in GroupBy: ${gb.metaData.cleanName}")
    val query = gb.streamingSource.get.query
    require(query != null, s"Streaming query is missing in GroupBy: ${gb.metaData.cleanName}")
    query
  }

  def buildQueryTransformsAndFilters(query: Query,
                                     dataModel: DataModel = DataModel.EVENTS): (Seq[(String, String)], Seq[String]) = {
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    val reversalColumn = Option(query.reversalColumn).getOrElse(Constants.ReversalColumn)
    val mutationTimeColumn = Option(query.mutationTimeColumn).getOrElse(Constants.MutationTimeColumn)
    val selects = Option(query.selects).map(_.toScala).getOrElse(Map.empty[String, String])

    val transforms: Seq[(String, String)] = dataModel match {
      case DataModel.EVENTS =>
        (selects ++ Map(Constants.TimeColumn -> timeColumn)).toSeq
      case DataModel.ENTITIES =>
        (selects ++ Map(
          Constants.TimeColumn -> timeColumn,
          Constants.ReversalColumn -> reversalColumn,
          Constants.MutationTimeColumn -> mutationTimeColumn
        )).toSeq
    }

    val timeFilters = dataModel match {
      case DataModel.ENTITIES => Seq(s"${Constants.MutationTimeColumn} is NOT NULL", s"$timeColumn is NOT NULL")
      case DataModel.EVENTS   => Seq(s"$timeColumn is NOT NULL")
    }

    val baseFilters: Seq[String] = Option(query.getWheres)
      .map(_.toScala.toSeq)
      .getOrElse(Seq.empty[String])

    val filters: Seq[String] = baseFilters ++ timeFilters

    (transforms, filters)
  }

  def buildKeyValueEventTimeColumns(gb: GroupBy): (Array[String], Array[String], String) = {
    val keyColumns = gb.keyColumns.asScala.toArray
    val (additionalColumns, eventTimeColumn) = gb.dataModel match {
      case DataModel.EVENTS   => Seq.empty -> Constants.TimeColumn
      case DataModel.ENTITIES => Constants.MutationAvroColumns -> Constants.MutationTimeColumn
    }
    val valueColumns = gb.aggregationInputs ++ additionalColumns
    (keyColumns, valueColumns, eventTimeColumn)
  }
}
