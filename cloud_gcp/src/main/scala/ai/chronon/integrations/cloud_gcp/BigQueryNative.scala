package ai.chronon.integrations.cloud_gcp

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.catalog.{Format, TableUtils}
import com.google.cloud.bigquery._
import org.apache.spark.sql.functions.{col, date_format, to_date}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.ZoneOffset
import scala.util.{Failure, Success, Try}

case class PartitionColumnNotFoundException(message: String) extends UnsupportedOperationException(message)

case object BigQueryNative extends Format {

  private val bqFormat = "bigquery"
  private lazy val bqOptions = BigQueryOptions.getDefaultInstance
  private lazy val bigQueryClient: BigQuery = bqOptions.getService

  override def table(tableName: String, partitionFilters: String)(implicit sparkSession: SparkSession): DataFrame = {
    throw new UnsupportedOperationException(
      "Direct table reads are not supported for BigQueryNative format. Use a stagingQuery with EngineType.BIGQUERY to export the data first.")
  }

  override def createTable(tableName: String,
                           schema: StructType,
                           partitionColumns: List[String],
                           tableProperties: Map[String, String],
                           semanticHash: scala.Option[String])(implicit sparkSession: SparkSession): Unit = {
    throw new UnsupportedOperationException("Table creation is not supported for BigQueryNative format.")
  }

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] = {
    val tableIdentifier = SparkBQUtils.toTableId(tableName)
    val definitionOpt = scala
      .Option(bigQueryClient.getTable(tableIdentifier))
      .map((table) => table.getDefinition.asInstanceOf[TableDefinition])

    definitionOpt match {
      case None => List.empty
      case Some(view: ViewDefinition) => {
        if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
          throw new NotImplementedError("subPartitionsFilter is not supported on this format.")
        }
        import sparkSession.implicits._

        val tableIdentifier = SparkBQUtils.toTableId(tableName)
        val providedProject = scala.Option(tableIdentifier.getProject).getOrElse(bqOptions.getProjectId)
        val partitionWheres = if (partitionFilters.nonEmpty) s"WHERE ${partitionFilters}" else partitionFilters

        val bqPartSQL =
          s"""
             |select distinct ${partitionColumn} FROM ${tableName} ${partitionWheres}
             |""".stripMargin

        val partVals = sparkSession.read
          .format(bqFormat)
          .option("project", providedProject)
          // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
          // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
          .option("viewsEnabled", true)
          .option("materializationDataset", tableIdentifier.getDataset)
          .load(bqPartSQL)

        partVals.as[String].collect().toList
      }
      case Some(std: StandardTableDefinition) =>
        super.primaryPartitions(tableName, partitionColumn, partitionFilters, subPartitionsFilter)
      case Some(other) =>
        throw new IllegalArgumentException(
          s"Table ${tableName} is not a view or standard table. It is of type ${other.getClass.getName}."
        )
    }
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    import sparkSession.implicits._
    val tableIdentifier = SparkBQUtils.toTableId(tableName)
    val providedProject = scala.Option(tableIdentifier.getProject).getOrElse(bqOptions.getProjectId)
    val table = tableIdentifier.getTable
    val database =
      scala
        .Option(tableIdentifier.getDataset)
        .getOrElse(throw new IllegalArgumentException(s"database required for table: ${tableName}"))

    // See: https://cloud.google.com/bigquery/docs/information-schema-columns
    val partColsSql =
      s"""
           |SELECT column_name FROM `${providedProject}.${database}.INFORMATION_SCHEMA.COLUMNS`
           |WHERE table_name = '${table}' AND is_partitioning_column = 'YES'
           |
           |""".stripMargin

    val partitionCol = sparkSession.read
      .format(bqFormat)
      .option("project", providedProject)
      // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
      // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
      .option("viewsEnabled", true)
      .option("materializationDataset", database)
      .load(partColsSql)
      .as[String]
      .collect
      .headOption
      .getOrElse(throw PartitionColumnNotFoundException(s"No partition column for table ${tableName} found."))

    // See: https://cloud.google.com/bigquery/docs/information-schema-partitions
    val partValsSql =
      s"""
           |SELECT partition_id FROM `${providedProject}.${database}.INFORMATION_SCHEMA.PARTITIONS`
           |WHERE table_name = '${table}'
           |
           |""".stripMargin

    // TODO: remove temporary hack. this is done because the existing raw data is in the date format yyyy-MM-dd
    //  but partition values in bigquery's INFORMATION_SCHEMA.PARTITIONS are in yyyyMMdd format.
    //  moving forward, for bigquery gcp we should default to storing raw data in yyyyMMdd format.
    val partitionFormat = TableUtils(sparkSession).partitionFormat

    val partitionInfoDf = sparkSession.read
      .format(bqFormat)
      .option("project", providedProject)
      // See: https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/434#issuecomment-886156191
      // and: https://cloud.google.com/bigquery/docs/information-schema-intro#limitations
      .option("viewsEnabled", true)
      .option("materializationDataset", database)
      .load(partValsSql)

    val unfilteredDf = partitionInfoDf
      .select(
        date_format(
          to_date(
            col("partition_id"),
            "yyyyMMdd" // Note: this "yyyyMMdd" format is hardcoded but we need to change it to be something else.
          ),
          partitionFormat)
          .as(partitionCol))
      .na // Should filter out '__NULL__' and '__UNPARTITIONED__'. See: https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables
      .drop()

    val partitionVals = (if (partitionFilters.isEmpty) {
                           unfilteredDf
                         } else {
                           unfilteredDf.where(partitionFilters)
                         })
      .as[String]
      .collect
      .toList

    partitionVals.map((p) => Map(partitionCol -> p))

  }

  override def supportSubPartitionsFilter: Boolean = false

  override def maxTimestampDate(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): scala.Option[String] = {
    Try {
      val tableIdentifier = SparkBQUtils.toTableId(tableName)
      val providedProject = scala.Option(tableIdentifier.getProject).getOrElse(bqOptions.getProjectId)
      val database = scala
        .Option(tableIdentifier.getDataset)
        .getOrElse(throw new IllegalArgumentException(s"database required for table: ${tableName}"))

      val sql = s"SELECT CAST(MAX(${timestampColumn}) AS DATE) AS max_val FROM ${tableName}"

      val result = sparkSession.read
        .format(bqFormat)
        .option("project", providedProject)
        .option("viewsEnabled", true)
        .option("materializationDataset", database)
        .load(sql)
        .collect()
        .headOption

      result.flatMap { row =>
        if (row.isNullAt(0)) None
        else {
          val utcMillis = row.getDate(0).toLocalDate.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli
          Some(partitionSpec.at(utcMillis))
        }
      }
    } match {
      case Success(result) => result
      case Failure(e) =>
        logger.warn(s"Failed to get max timestamp date for $tableName: ${e.getMessage}")
        None
    }
  }

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    Try {
      val tableIdentifier = SparkBQUtils.toTableId(tableName)
      val providedProject = scala.Option(tableIdentifier.getProject).getOrElse(bqOptions.getProjectId)
      val database = scala
        .Option(tableIdentifier.getDataset)
        .getOrElse(throw new IllegalArgumentException(s"database required for table: ${tableName}"))

      val sql =
        s"""SELECT CAST(MIN(${timestampColumn}) AS DATE) AS min_val,
           |       CAST(MAX(${timestampColumn}) AS DATE) AS max_val
           |FROM ${tableName}""".stripMargin

      val result = sparkSession.read
        .format(bqFormat)
        .option("project", providedProject)
        .option("viewsEnabled", true)
        .option("materializationDataset", database)
        .load(sql)
        .collect()
        .headOption

      result
        .flatMap { row =>
          if (row.isNullAt(0) || row.isNullAt(1)) None
          else {
            val minMillis = row.getDate(0).toLocalDate.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli
            val maxMillis = row.getDate(1).toLocalDate.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli
            Some(partitionSpec.expandRange(partitionSpec.at(minMillis), partitionSpec.at(maxMillis)))
          }
        }
        .getOrElse(List.empty)
    } match {
      case Success(partitions) => partitions
      case Failure(e) =>
        logger.warn(s"Failed to get virtual partitions for $tableName: ${e.getMessage}")
        List.empty
    }
  }
}
