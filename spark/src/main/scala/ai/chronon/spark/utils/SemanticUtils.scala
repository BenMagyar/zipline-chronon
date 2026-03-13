package ai.chronon.spark.utils

import ai.chronon.api.Constants
import ai.chronon.spark.catalog.{CreationUtils, TableUtils}
import org.slf4j.{Logger, LoggerFactory}

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

class SemanticUtils(tableUtils: TableUtils) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // tries to archive to a "reuse" table,
  // if a reuse table is already present, it first moves the reuse table to "shelf" table - suffixed with timestamp
  private def archiveForReuse(outputTable: String,
                              reuseTableOpt: Option[String] = None,
                              shelfTableOpt: Option[String] = None): String = {

    val archiveTimestampFormatter = DateTimeFormatter
      .ofPattern("yyyyMMddHHmmss")
      .withZone(ZoneOffset.UTC)

    val nowSecondsStr = archiveTimestampFormatter.format(Instant.now())

    val reuseTable = reuseTableOpt.getOrElse(outputTable + Constants.archiveReuseTableSuffix)
    val shelfTable = shelfTableOpt.getOrElse(outputTable + "_archive_" + nowSecondsStr)

    if (tableUtils.tableReachable(reuseTable)) {
      tableUtils.renameTable(reuseTable, shelfTable)
    }

    if (tableUtils.tableReachable(outputTable)) {
      tableUtils.renameTable(outputTable, reuseTable)
    }

    logger.info(s"Archived table $outputTable to $reuseTable")
    reuseTable

  }

  def checkSemanticHashAndArchive(outputTable: String, incomingSemanticHash: String): Option[String] = {
    try {

      // Check if table exists first
      if (!tableUtils.tableReachable(outputTable)) {
        logger.info(s"Table $outputTable does not exist, nothing to archive")
        return None
      }

      val existingSemanticHashOpt = getSemanticHash(outputTable)

      existingSemanticHashOpt match {
        case Some(existingHash) if existingHash != incomingSemanticHash =>
          logger.info(
            s"Semantic hash has changed for table $outputTable. " +
              s"Existing: $existingHash, New: $incomingSemanticHash. " +
              s"Going to archive the table."
          )
          val archived = archiveForReuse(outputTable)
          Some(archived)

        case Some(_) =>
          // Hash matches, no archival needed
          None

        case None =>
          // Hash missing — likely a legacy table or first run with semantic hashing.
          // Don't archive; schema compatibility is enforced during write.
          // The hash will be set after the job succeeds.
          logger.info(
            s"No semantic hash found for table $outputTable. " +
              s"Skipping archival — hash will be set after successful job completion."
          )
          None
      }

    } catch {
      case ex: Exception =>
        logger.error(
          s"Failed to check-semantic-hash and archive-on-change for table $outputTable:",
          ex
        )

        throw ex

    }
  }

  private def getSemanticHash(outputTable: String): Option[String] = {

    val tableProps = tableUtils
      .getTableProperties(outputTable)

    logger.info(s"Table $outputTable has properties: $tableProps")

    tableProps.flatMap(_.get(Constants.SemanticHashKey))

  }

  def setSemanticHash(outputTable: String, semanticHash: String): Unit = {

    val existingHashOpt = getSemanticHash(outputTable)

    if (existingHashOpt.contains(semanticHash)) {

      logger.info(s"Table $outputTable already has the desired semantic hash - $semanticHash")

    } else {
      if (existingHashOpt.isEmpty) {

        logger.info(s"No semantic hash exists for table $outputTable. Setting it to - $semanticHash")

        val alterStmt = CreationUtils.alterTablePropertiesSql(
          outputTable,
          Map(Constants.SemanticHashKey -> semanticHash)
        )

        tableUtils.sql(alterStmt)

      } else {
        throw new IllegalStateException(
          s"Cannot update the existing semantic hash ${existingHashOpt.get} of $outputTable to $semanticHash. " +
            s"The table should have been archived.")
      }
    }

  }

}
