package ai.chronon.spark.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType

import scala.util.{Failure, Success, Try}

case object Iceberg extends Format {

  override def tableTypeString: String = "iceberg"

  override def tableProperties: Map[String, String] = {
    Map(
      "commit.retry.num-retries" -> "20", // default = 4
      "commit.retry.min-wait-ms" -> (10 * 1000).toString,
      "commit.retry.max-wait-ms" -> (600 * 1000).toString,
      "commit.status-check.num-retries" -> "20", // default = 3
      "commit.status-check.min-wait-ms" -> (10 * 1000).toString, // default = 1000
      "commit.status-check.max-wait-ms" -> (600 * 1000).toString,
      "write.merge.isolation-level" -> "snapshot",
      "format-version" -> "2"
    )
  }

  override def primaryPartitions(tableName: String,
                                 partitionColumn: String,
                                 partitionFilters: String,
                                 subPartitionsFilter: Map[String, String])(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    Try(getIcebergPartitions(tableName, partitionColumn)) match {
      case Success(p) => p
      case Failure(e) if Option(e.getMessage).exists(_.contains("TABLE_OR_VIEW_NOT_FOUND")) =>
        logger.warn(s"Failed to get partitions for $tableName: ${e.getMessage}")
        List.empty
      case Failure(e) =>
        logger.warn(
          s"Failed to get partitions for $tableName: ${e.getClass.getSimpleName}: ${Option(e.getMessage).getOrElse("(no message)")}")
        List.empty
    }
  }

  override def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]] = {
    val partitionsDf = sparkSession.table(s"${qualifyWithCatalog(tableName)}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    val partitionColumnNames = partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames

    partitionsDf
      .select(col("partition"))
      .collect()
      .map { row =>
        val partitionData = row.getStruct(0)
        partitionColumnNames.flatMap { colName =>
          Option(partitionData.getAs[Any](colName)).map(colName -> _.toString)
        }.toMap
      }
      .toList
      .distinct
  }

  private def getIcebergPartitions(tableName: String, partitionColumn: String)(implicit
      sparkSession: SparkSession): List[String] = {

    val partitionsDf = sparkSession.table(s"${qualifyWithCatalog(tableName)}.partitions")

    val index = partitionsDf.schema.fieldIndex("partition")
    if (partitionsDf.schema(index).dataType.asInstanceOf[StructType].fieldNames.contains("hr")) {
      // Hour filter is currently buggy in iceberg. https://github.com/apache/iceberg/issues/4718
      // so we collect and then filter.
      partitionsDf
        .select(col(s"partition.$partitionColumn"), col("partition.hr"))
        .collect()
        .filter(_.get(1) == null)
        .flatMap(row => Option(row.getString(0)))
        .toList
    } else {
      partitionsDf
        .select(col(s"partition.$partitionColumn").cast("string"))
        .collect()
        .flatMap(row => Option(row.getString(0)))
        .toList
    }
  }

  private[catalog] def qualifyWithCatalog(tableName: String)(implicit sparkSession: SparkSession): String = {
    val resolved = Format.resolveTableName(tableName)
    s"${QuotingUtils.quoteIdentifier(resolved.catalog)}.${QuotingUtils.quoteIdentifier(resolved.namespace)}.${QuotingUtils.quoteIdentifier(resolved.table)}"
  }

  override def supportSubPartitionsFilter: Boolean = false
}
