package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.util.QuotingUtils
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.functions.{col, date_format, min, max}
import org.apache.spark.sql.types.{DateType, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

trait Format {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  def tableProperties: Map[String, String] = Map.empty[String, String]

  def tableTypeString: String = ""

  def createTable(tableName: String,
                  schema: StructType,
                  partitionColumns: List[String],
                  providedProperties: Map[String, String],
                  semanticHash: Option[String] = None)(implicit sparkSession: SparkSession): Unit = {
    val (creationName, quotedOriginal) = semanticHash match {
      case Some(hash) =>
        val parts = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName).toList
        val hashedParts = parts.init :+ s"${parts.last}_$hash"
        (hashedParts.map(QuotingUtils.quoteIdentifier).mkString("."), tableName)
      case None => (tableName, tableName)
    }
    sparkSession.sql(
      CreationUtils
        .createTableSql(creationName, schema, partitionColumns, providedProperties, tableTypeString))
    if (semanticHash.isDefined) {
      try {
        sparkSession.sql(Format.renameTableSql(creationName, tableName))
      } catch {
        case _: TableAlreadyExistsException =>
          // Another writer already created the target table — safe to clean up our intermediate table
          logger.info(s"Table $quotedOriginal already exists, dropping intermediate table $creationName")
          sparkSession.sql(s"DROP TABLE IF EXISTS $creationName")
        case e: Exception =>
          logger.error(
            s"Failed to rename $creationName to $quotedOriginal. Orphan table $creationName may need manual cleanup.",
            e)
          throw e
      }
    }
  }

  def table(tableName: String, partitionFilters: String)(implicit sparkSession: SparkSession): DataFrame = {

    val df = sparkSession.read.table(tableName)

    if (partitionFilters.isEmpty) {
      df
    } else {
      df.where(partitionFilters)
    }

  }

  // Return the primary partitions (based on the 'partitionColumn') filtered down by sub-partition filters if provided
  // If subpartition filters are supplied and the format doesn't support it, we throw an error
  def primaryPartitions(tableName: String,
                        partitionColumn: String,
                        partitionFilters: String,
                        subPartitionsFilter: Map[String, String] = Map.empty)(implicit
      sparkSession: SparkSession): List[String] = {

    if (!supportSubPartitionsFilter && subPartitionsFilter.nonEmpty) {
      throw new NotImplementedError("subPartitionsFilter is not supported on this format")
    }

    val partitionSeq = Try(partitions(tableName, partitionFilters)(sparkSession)) match {
      case Success(p) => p
      case Failure(e) =>
        logger.warn(s"Failed to get partitions for $tableName: ${e.getMessage}")
        List.empty
    }

    partitionSeq.flatMap { partitionMap =>
      if (
        subPartitionsFilter.forall { case (k, v) =>
          partitionMap.get(k).contains(v)
        }
      ) {
        partitionMap.get(partitionColumn)
      } else {
        None
      }
    }
  }

  // Return a sequence for partitions where each partition entry consists of a map of partition keys to values
  // e.g. Seq(
  //         Map("ds" -> "2023-04-01", "hr" -> "12"),
  //         Map("ds" -> "2023-04-01", "hr" -> "13")
  //         Map("ds" -> "2023-04-02", "hr" -> "00")
  //      )
  def partitions(tableName: String, partitionFilters: String)(implicit
      sparkSession: SparkSession): List[Map[String, String]]

  // Does this format support sub partitions filters
  def supportSubPartitionsFilter: Boolean

  // Returns the max date present in a timestamp/date column, formatted as a partition date string.
  // Used by sensors to check if data covers a required date range.
  def maxTimestampDate(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): Option[String] = {
    import sparkSession.implicits._
    Try {
      val df = sparkSession.read.table(tableName)
      df.select(date_format(max(col(timestampColumn)).cast(DateType), partitionSpec.format).as("max_date"))
        .as[String]
        .collect()
        .headOption
        .flatMap(v => Option(v))
    } match {
      case Success(result) => result
      case Failure(e) =>
        logger.warn(s"Failed to get max timestamp date for $tableName: ${e.getMessage}")
        None
    }
  }

  // Derive virtual partitions from MIN/MAX of a timestamp/date column.
  // Returns all dates between min and max as formatted date strings.
  def virtualPartitions(tableName: String, timestampColumn: String, partitionSpec: PartitionSpec)(implicit
      sparkSession: SparkSession): List[String] = {
    import sparkSession.implicits._
    Try {
      val df = sparkSession.read.table(tableName)
      // Cast to date first (truncates to date in session timezone), then compute min/max
      // Compute min/max first, then cast — allows Spark to use Parquet file-level statistics
      val result = df
        .select(
          date_format(min(col(timestampColumn)).cast(DateType), partitionSpec.format).as("min_date"),
          date_format(max(col(timestampColumn)).cast(DateType), partitionSpec.format).as("max_date")
        )
        .as[(String, String)]
        .collect()
        .headOption

      result
        .flatMap { case (minDate, maxDate) =>
          if (minDate == null || maxDate == null) None
          else Some(partitionSpec.expandRange(minDate, maxDate))
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

case class ResolvedTableName(catalog: String, namespace: String, table: String) {
  def toIdentifier: Identifier = Identifier.of(Array(namespace), table)
}

object Format {

  def parseHiveStylePartition(pstring: String): List[(String, String)] = {
    pstring
      .split("/")
      .map { part =>
        val p = part.split("=", 2)
        p(0) -> p(1)
      }
      .toList
  }

  def resolveTableName(tableName: String)(implicit sparkSession: SparkSession): ResolvedTableName = {
    val parsed = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    def defaultCatalog: String = sparkSession.conf.get("spark.sql.defaultCatalog", "spark_catalog")
    parsed.toList match {
      case catalog :: namespace :: table :: Nil => ResolvedTableName(catalog, namespace, table)
      case namespace :: table :: Nil            => ResolvedTableName(defaultCatalog, namespace, table)
      case table :: Nil =>
        ResolvedTableName(defaultCatalog, sparkSession.catalog.currentDatabase, table)
      case _ => throw new IllegalStateException(s"Invalid table naming convention specified: ${tableName}")
    }
  }

  // Lightweight version that avoids triggering catalog initialization
  def getCatalog(inputTableName: String)(implicit sparkSession: SparkSession): String = {
    val parsed = sparkSession.sessionState.sqlParser.parseMultipartIdentifier(inputTableName)
    def defaultCatalog: String = sparkSession.conf.get("spark.sql.defaultCatalog", "spark_catalog")
    parsed.toList match {
      case catalog :: _ :: _ :: Nil => catalog
      case _ :: _ :: Nil            => defaultCatalog
      case _ :: Nil                 => defaultCatalog
      case _ => throw new IllegalStateException(s"Invalid table naming convention specified: ${inputTableName}")
    }
  }

  def renameTableSql(srcTable: String, destTable: String)(implicit sparkSession: SparkSession): String = {
    val srcResolved = resolveTableName(srcTable)
    val destResolved = resolveTableName(destTable)
    val normalizedDest = if (srcResolved.catalog == destResolved.catalog) {
      s"${QuotingUtils.quoteIdentifier(destResolved.namespace)}.${QuotingUtils.quoteIdentifier(destResolved.table)}"
    } else {
      s"${QuotingUtils.quoteIdentifier(destResolved.catalog)}.${QuotingUtils.quoteIdentifier(destResolved.namespace)}.${QuotingUtils.quoteIdentifier(destResolved.table)}"
    }
    s"ALTER TABLE $srcTable RENAME TO $normalizedDest"
  }

}
