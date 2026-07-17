package ai.chronon.spark.catalog

import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Success, Try}

/** Default format provider implementation based on default Chronon supported open source library versions.
  */
class DefaultFormatProvider(val sparkSession: SparkSession) extends FormatProvider {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  // delta-spark is compile-only, so format detection must not link Delta runtime classes.
  private val DeltaTableV2ClassName = "org.apache.spark.sql.delta.catalog.DeltaTableV2"

  // Checks the format of a given table if it exists.
  override def readFormat(tableName: String): Option[Format] = {
    Option(if (isIcebergTable(tableName)) {
      Iceberg
    } else if (isDeltaTable(tableName)) {
      DeltaLake
    } else if (sparkSession.catalog.tableExists(tableName)) {
      Hive
    } else { null })
  }

  protected def isIcebergTable(tableName: String): Boolean = {
    val resolved = Format.resolveTableName(tableName)(sparkSession)
    val catalog = sparkSession.sessionState.catalogManager.catalog(resolved.catalog)

    catalog match {
      case sparkCatalog: SparkCatalog =>
        Try(sparkCatalog.loadTable(resolved.toIdentifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"IcebergCheck: Detected iceberg formatted table $tableName.")
            true
          case _ =>
            logger.info(s"IcebergCheck: Checked table $tableName is not iceberg format.")
            false
        }
      case tableCatalog: TableCatalog =>
        Try(tableCatalog.loadTable(resolved.toIdentifier)) match {
          case Success(_: SparkTable) =>
            logger.info(s"IcebergCheck: Detected iceberg formatted table $tableName.")
            true
          case _ =>
            logger.info(s"IcebergCheck: Checked table $tableName is not iceberg format.")
            false
        }
      case _ =>
        logger.info(s"IcebergCheck: Checked table $tableName is not iceberg format.")
        false
    }
  }

  private def isDeltaTable(tableName: String): Boolean =
    Try {
      val resolved = Format.resolveTableName(tableName)(sparkSession)
      val catalog = sparkSession.sessionState.catalogManager.catalog(resolved.catalog).asInstanceOf[TableCatalog]
      val table = catalog.loadTable(resolved.toIdentifier)
      val provider =
        Option(table.properties()).flatMap(properties => Option(properties.get(TableCatalog.PROP_PROVIDER)))
      val isDeltaTableV2 = Iterator
        .iterate[Class[_]](table.getClass)(_.getSuperclass)
        .takeWhile(_ != null)
        .exists(_.getName == DeltaTableV2ClassName)

      isDeltaTableV2 || provider.exists(_.equalsIgnoreCase("delta"))
    }.getOrElse(false)
}
