package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.{col, count, date_format, from_json, lit, min, max, when}
import org.apache.spark.sql.types.{MapType, StringType, StructField, StructType}

import scala.util.{Failure, Success, Try}

// Compiled against delta-spark 3.3.2 to match EMR 7.12.0. DeltaLog.update() signature changes
// across Delta versions (e.g. 2 params in 3.2, 3 params in 3.3), so compiling against an older
// version will cause NoSuchMethodError at runtime if the EMR-bundled Delta jar has a newer signature.
case object DeltaLake extends Format {

  private[catalog] case class StatsDateRange(start: String, end: String) {
    def virtualPartitions(partitionSpec: PartitionSpec): List[String] =
      partitionSpec.expandRange(start, end)

    def firstAvailablePartition: String = start

    def lastAvailablePartition(partitionSpec: PartitionSpec, isStringColumn: Boolean): String =
      if (isStringColumn) end else partitionSpec.before(end)
  }

  override def tableTypeString: String = "delta"

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] =
    super.primaryPartitions(tableName, partitionColumn, partitionFilters, subPartitionsFilter)

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {

    // delta lake doesn't support the `SHOW PARTITIONS <tableName>` syntax - https://github.com/delta-io/delta/issues/996
    // there's alternative ways to retrieve partitions using the DeltaLog abstraction which is what we have to lean into
    // below first pull table location as that is what we need to pass to the delta log
    val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
    val tablePath = describeResult.select("location").head().getString(0)

    val snapshot = DeltaLog.forTable(sparkSession, tablePath).update()
    val snapshotPartitionsDf = snapshot.allFiles.toDF().select("partitionValues")

    val partitions = snapshotPartitionsDf.collect().map(r => r.getAs[Map[String, String]](0))
    partitions.toList

  }

  override def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    statsDateRange(tableName, timestampColumn, partitionSpec) match {
      case Some(range) => range.virtualPartitions(partitionSpec)
      case None        => super.virtualPartitions(tableName, timestampColumn, partitionSpec)
    }
  }

  override def firstAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(
      implicit sparkSession: SparkSession): Option[String] =
    statsDateRange(tableName, partitionColumn, partitionSpec)
      .map(_.firstAvailablePartition)
      .orElse(super.firstAvailablePartition(tableName, partitionColumn, partitionSpec))

  override def lastAvailablePartition(tableName: String, partitionColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] =
    statsDateRange(tableName, partitionColumn, partitionSpec)
      .map(_.lastAvailablePartition(partitionSpec, isStringColumn(tableName, partitionColumn)))
      .orElse(super.lastAvailablePartition(tableName, partitionColumn, partitionSpec))

  private def isStringColumn(tableName: String, columnName: String)(implicit sparkSession: SparkSession): Boolean =
    Try(sparkSession.read.table(tableName).schema(columnName).dataType == StringType).getOrElse(false)

  private[catalog] def statsDateRange(tableName: String, columnName: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[StatsDateRange] = {
    import sparkSession.implicits._

    Try {
      val describeResult = sparkSession.sql(s"DESCRIBE DETAIL $tableName")
      val tablePath = describeResult.select("location").head().getString(0)
      val activeFiles = DeltaLog.forTable(sparkSession, tablePath).update().allFiles.toDF()

      val statsSchema = StructType(
        Seq(
          StructField("minValues", MapType(StringType, StringType), nullable = true),
          StructField("maxValues", MapType(StringType, StringType), nullable = true)
        ))

      val stats = activeFiles
        .select(from_json(col("stats"), statsSchema).as("stats"))
        .select(
          col("stats.minValues").getItem(columnName).as("min_value"),
          col("stats.maxValues").getItem(columnName).as("max_value")
        )

      val boundaries = stats
        .agg(
          count(lit(1)).as("fileCount"),
          count(when(col("min_value").isNull || col("max_value").isNull, lit(1))).as("missingCount"),
          date_format(min(col("min_value").cast("timestamp")), partitionSpec.format).as("start"),
          date_format(max(col("max_value").cast("timestamp")), partitionSpec.format).as("end")
        )
        .collect()
        .headOption

      boundaries.flatMap { row =>
        val fileCount = row.getAs[Long]("fileCount")
        val missingCount = row.getAs[Long]("missingCount")
        val start = row.getAs[String]("start")
        val end = row.getAs[String]("end")

        if (fileCount > 0 && missingCount == 0 && start != null && end != null) {
          Some(StatsDateRange(start = start, end = end))
        } else {
          None
        }
      }
    } match {
      case Success(result) =>
        if (result.isDefined) {
          logger.info(s"Resolved Delta log stats boundaries for $tableName.$columnName: ${result.get}")
        } else {
          logger.info(s"Delta log stats were incomplete for $tableName.$columnName; falling back to table scan")
        }
        result
      case Failure(e) =>
        logger.warn(
          s"Failed to resolve Delta log stats boundaries for $tableName.$columnName: ${Option(e.getMessage).getOrElse("(no message)")}")
        None
    }
  }

  override def supportSubPartitionsFilter: Boolean = true
}
