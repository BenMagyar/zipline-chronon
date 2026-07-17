package ai.chronon.spark

import com.amazon.ion.IonType
import com.amazon.ion.system.IonBinaryWriterBuilder
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, ZoneOffset}
import java.util.UUID
import scala.util.control.NonFatal

/** Configuration keys used for Ion upload paths. */
object IonPathConfig {
  val UploadFormatKey = "spark.chronon.table_write.upload.format"
  val UploadLocationKey = "spark.chronon.table_write.upload.location"
  val PartitionColumnKey = "spark.chronon.partition.column"
  val IonWriterPartitionsKey = "spark.chronon.table_writer.ion_writer.partitions"
  val IonWriterTimeoutKey = "spark.chronon.table_writer.ion_writer.timeout_ms"
  val DefaultPartitionColumn = "ds"
}

case class IonWriteResult(
    rowCount: Long,
    keyBytes: Long,
    valueBytes: Long
)

object IonWriter {
  private val logger = LoggerFactory.getLogger(getClass)

  def write(df: DataFrame,
            dataSetName: String,
            partitionColumn: String,
            partitionValue: String,
            rootPath: Option[String] = None): IonWriteResult = {
    val serializableConf = new SerializableConfiguration(df.sparkSession.sparkContext.hadoopConfiguration)
    val schema = df.schema

    val partitionPath = resolvePartitionPath(dataSetName, partitionColumn, partitionValue, rootPath)

    // Clean up any existing files in the partition path to ensure idempotency
    cleanupPartitionPath(partitionPath, serializableConf)

    val requiredColumns = Seq("key_bytes", "value_bytes", partitionColumn)
    val missingColumns = requiredColumns.filterNot(schema.fieldNames.contains)
    if (missingColumns.nonEmpty) {
      throw new IllegalArgumentException(
        s"DataFrame schema for Ion upload is missing required column(s): ${missingColumns.mkString(", ")}")
    }

    val keyIdx = schema.fieldIndex("key_bytes")
    val valueIdx = schema.fieldIndex("value_bytes")
    val tsIdx = schema.fieldIndex(partitionColumn)
    val writeDf = configuredPartitions(df) match {
      case Some(partitions) =>
        logger.info(s"Repartitioning Ion upload for $dataSetName partition $partitionValue to $partitions partition(s)")
        df.repartition(partitions)
      case None => df
    }

    val written = writeDf.rdd
      .mapPartitionsWithIndex((partitionId, iter) =>
        if (!iter.hasNext) Iterator.empty
        else {
          val unique = UUID.randomUUID().toString
          val filePath = new Path(partitionPath, s"part-$partitionId-$unique.ion")
          val fs = FileSystem.get(filePath.toUri, serializableConf.value)
          fs.mkdirs(partitionPath)
          val out = fs.create(filePath, true)
          val writer = IonBinaryWriterBuilder.standard().build(out)

          var rowCount = 0L
          var keyBytesTotal = 0L
          var valueBytesTotal = 0L

          try {
            iter.foreach { row =>
              writer.stepIn(IonType.STRUCT)
              writer.setFieldName("Item")
              writer.stepIn(IonType.STRUCT)
              if (!row.isNullAt(keyIdx)) {
                val bytes = row.getAs[Array[Byte]](keyIdx)
                writer.setFieldName("keyBytes")
                writer.writeBlob(bytes)
                keyBytesTotal += bytes.length
              }
              if (!row.isNullAt(valueIdx)) {
                val bytes = row.getAs[Array[Byte]](valueIdx)
                writer.setFieldName("valueBytes")
                writer.writeBlob(bytes)
                valueBytesTotal += bytes.length
              }
              if (!row.isNullAt(tsIdx)) {
                writer.setFieldName("ts")
                val millis = toMillis(row.get(tsIdx))
                writer.writeDecimal(millis)
              }
              writer.stepOut()
              writer.stepOut()
              rowCount += 1
            }
            writer.finish()
          } catch {
            case NonFatal(e) =>
              logger.error(s"Failed writing Ion file at $filePath", e)
              throw e
          } finally {
            writer.close()
            out.close()
          }
          Iterator.single((rowCount, keyBytesTotal, valueBytesTotal))
        })
      .collect()

    val totalRows = written.map(_._1).sum
    if (totalRows == 0L) {
      throw new RuntimeException("Ion upload produced zero rows.")
    }

    val totalKeyBytes = written.map(_._2).sum
    val totalValueBytes = written.map(_._3).sum
    logger.info(
      s"Wrote Ion files for partition $partitionValue at $partitionPath rows=$totalRows key_bytes=$totalKeyBytes value_bytes=$totalValueBytes"
    )
    IonWriteResult(totalRows, totalKeyBytes, totalValueBytes)
  }

  def resolvePartitionPath(dataSetName: String,
                           partitionColumn: String,
                           partitionValue: String,
                           rootPath: Option[String]): Path = {
    val root = validateRootPath(rootPath)
    new Path(new Path(root, dataSetName), s"$partitionColumn=$partitionValue")
  }

  private def cleanupPartitionPath(partitionPath: Path, serializableConf: SerializableConfiguration): Unit = {
    val fs = FileSystem.get(partitionPath.toUri, serializableConf.value)
    if (fs.exists(partitionPath)) {
      val files = fs.listStatus(partitionPath)
      val deletedCount = files.count { fileStatus =>
        val deleted = fs.delete(fileStatus.getPath, fileStatus.isDirectory)
        if (!deleted) {
          logger.warn(s"Failed to delete: ${fileStatus.getPath}")
        }
        deleted
      }
      logger.info(s"Cleaned up $deletedCount existing file(s) from $partitionPath")
    }
  }

  /** Validates and normalizes rootPath. Must be s3:// or file:// format. */
  def validateRootPath(rootPath: Option[String]): String = {
    val trimmed = rootPath.map(_.trim.stripSuffix("/")).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(
        s"Location path is required. Set '${IonPathConfig.UploadLocationKey}' in configuration.")
    }

    if (!trimmed.matches("^(s3|s3a|s3n|file):/{1,3}.*")) {
      throw new IllegalArgumentException(
        s"Root path must start with s3://, s3a://, s3n://, or file:/ for local testing but got: $trimmed")
    }
    trimmed
  }

  // The upload stamps the partition column with the partition-start epoch millis directly (a Long).
  // Date/timestamp cases remain for callers that pass a converted partition column: to_timestamp(...)
  // externalizes to java.time.Instant (java8 API on) / java.sql.Timestamp (off); to_date(...) yields
  // java.time.LocalDate / java.sql.Date. All map to the epoch-millis this writer emits.
  def toMillis(value: Any): BigDecimal = {
    value match {
      case null => throw new IllegalArgumentException("Partition column is blank; cannot write Ion timestamp")
      case millis: Long =>
        BigDecimal.valueOf(millis)
      case instant: Instant =>
        BigDecimal.valueOf(instant.toEpochMilli)
      case ts: Timestamp =>
        BigDecimal.valueOf(ts.getTime)
      case localDate: LocalDate =>
        BigDecimal.valueOf(localDate.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli)
      case date: Date =>
        // java.sql.Date.toInstant() throws UnsupportedOperationException; go through LocalDate so
        // the value is anchored to UTC midnight, matching the LocalDate case above.
        BigDecimal.valueOf(date.toLocalDate.atStartOfDay(ZoneOffset.UTC).toInstant.toEpochMilli)
      case other =>
        throw new IllegalArgumentException(s"Unsupported partition type: ${other.getClass.getName}")
    }
  }

  private def configuredPartitions(df: DataFrame): Option[Int] = {
    val conf = df.sparkSession.conf
    conf.getOption(IonPathConfig.IonWriterPartitionsKey).map(_.toInt)
  }
}
