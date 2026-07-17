package ai.chronon.spark.upload

import ai.chronon.spark.{IonPathConfig, IonWriter}
import ai.chronon.spark.utils.SparkTestBase
import com.amazon.ion.system.IonSystemBuilder
import com.amazon.ion.{IonBlob, IonDecimal, IonStruct}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.time.Instant
import java.time.LocalDate
import scala.util.Using
import scala.jdk.CollectionConverters._

class IonWriterTest extends SparkTestBase with Matchers {

  private val tmpDir = Files.createTempDirectory("ion-writer-test").toFile

  override protected def sparkConfs: Map[String, String] = Map(
    "spark.sql.warehouse.dir" -> new File(tmpDir, "warehouse").getAbsolutePath
  )

  behavior of "IonWriter"

  it should "write ion files with expected rows and fields" in {
    val partitionValue = "2025-10-17"
    val tsValue = "2025-10-17T00:00:00Z"
    val tsValueMillis = Instant.parse(tsValue).toEpochMilli
    val rootPath = Some(tmpDir.toURI.toString)
    val dataSetName = "ion-output"

    val schema = StructType(
      Seq(
        StructField("key_bytes", BinaryType, nullable = true),
        StructField("value_bytes", BinaryType, nullable = true),
        StructField("key_json", StringType, nullable = true),
        StructField("value_json", StringType, nullable = true),
        StructField("ds", DateType, nullable = false)
      )
    )

    val rows = Seq(
      Row("k1".getBytes("UTF-8"), "v1-bytes".getBytes("UTF-8"), "k1-json", """{"v":"one"}""", LocalDate.parse(partitionValue)),
      Row("k2".getBytes("UTF-8"), "v2-bytes".getBytes("UTF-8"), "k2-json", """{"v":"two"}""", LocalDate.parse(partitionValue))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, numSlices = 2), schema)
    val result = IonWriter.write(df, dataSetName, "ds", partitionValue, rootPath)

    result.rowCount shouldBe rows.size
    result.keyBytes should be > 0L
    result.valueBytes should be > 0L

    // Verify files were written by reading from the partition path
    val partitionPath = IonWriter.resolvePartitionPath(dataSetName, "ds", partitionValue, rootPath)
    val partitionDir = new File(partitionPath.toUri)
    val ionFiles = partitionDir.listFiles().filter(_.getName.endsWith(".ion"))
    ionFiles should not be empty

    val ion = IonSystemBuilder.standard().build()

    val parsed =
      ionFiles.flatMap { file =>
        val datagram = Using.resource(new FileInputStream(file)) { in =>
          ion.getLoader.load(in)
        }
        datagram.iterator().asScala.map { value =>
          val struct = value.asInstanceOf[IonStruct].get("Item").asInstanceOf[IonStruct]
          val keyBytes = Option(struct.get("keyBytes")).map(_.asInstanceOf[IonBlob].getBytes)
          val valueBytes = Option(struct.get("valueBytes")).map(_.asInstanceOf[IonBlob].getBytes)
          val ts = Option(struct.get("ts")).map(_.asInstanceOf[IonDecimal])
          (keyBytes, valueBytes, ts)
        }
      }

    parsed.size shouldBe rows.size
    parsed.map(_._1.get.toSeq).toSet should contain("k1".getBytes("UTF-8").toSeq)
    parsed.map(_._2.get.toSeq).toSet should contain("v2-bytes".getBytes("UTF-8").toSeq)
    parsed.flatMap(_._3).foreach(_.bigDecimalValue().longValueExact() shouldBe tsValueMillis)
  }

  it should "honor upload bucket when provided" in {
    val partitionValue = "2025-10-18"
    val dataSetName = "ion-output-bucket"
    val rootPath = Some(new File(tmpDir, "bucket-root").toURI.toString)

    val schema = StructType(
      Seq(
        StructField("key_bytes", BinaryType, nullable = true),
        StructField("value_bytes", BinaryType, nullable = true),
        StructField("key_json", StringType, nullable = true),
        StructField("value_json", StringType, nullable = true),
        StructField("ds", DateType, nullable = false)
      )
    )

    val rows = Seq(
      Row("k3".getBytes("UTF-8"), "v3-bytes".getBytes("UTF-8"), "k3-json", """{"v":"three"}""", LocalDate.parse(partitionValue))
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, numSlices = 1), schema)

    val result = IonWriter.write(df, dataSetName, "ds", partitionValue, rootPath)

    result.rowCount shouldBe rows.size
    result.keyBytes should be > 0L
    result.valueBytes should be > 0L

    // Verify files were written to the correct location
    val partitionPath = IonWriter.resolvePartitionPath(dataSetName, "ds", partitionValue, rootPath)
    partitionPath.toString should include(dataSetName)
    partitionPath.toString should include(s"ds=$partitionValue")
    val partitionDir = new File(partitionPath.toUri)
    partitionDir.listFiles().filter(_.getName.endsWith(".ion")) should not be empty
  }

  it should "honor configured ion writer partitions" in {
    val partitionValue = "2025-10-19"
    val dataSetName = "ion-output-partitions"
    val rootPath = Some(new File(tmpDir, "partition-root").toURI.toString)

    val schema = StructType(
      Seq(
        StructField("key_bytes", BinaryType, nullable = true),
        StructField("value_bytes", BinaryType, nullable = true),
        StructField("key_json", StringType, nullable = true),
        StructField("value_json", StringType, nullable = true),
        StructField("ds", DateType, nullable = false)
      )
    )

    val rows = (0 until 30).map { idx =>
      Row(s"k$idx".getBytes("UTF-8"),
          s"v$idx".getBytes("UTF-8"),
          s"k$idx-json",
          s"""{"v":"$idx"}""",
          LocalDate.parse(partitionValue))
    }
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, numSlices = 8), schema)

    spark.conf.set(IonPathConfig.IonWriterPartitionsKey, "3")
    try {
      val result = IonWriter.write(df, dataSetName, "ds", partitionValue, rootPath)
      result.rowCount shouldBe rows.size

      val partitionPath = IonWriter.resolvePartitionPath(dataSetName, "ds", partitionValue, rootPath)
      val partitionDir = new File(partitionPath.toUri)
      partitionDir.listFiles().filter(_.getName.endsWith(".ion")).length shouldBe 3
    } finally {
      spark.conf.unset(IonPathConfig.IonWriterPartitionsKey)
    }
  }

  it should "write ion files for sub-daily (hourly) timestamp partitions" in {
    // Reproduces the hourly online-upload failure: to_timestamp(...) on a sub-daily grid produces a
    // TimestampType column, which externalizes to java.time.Instant under java8 API and previously hit
    // "Unsupported partition type: java.time.Instant".
    val partitionValue = "2026-07-13-18-00"
    val tsValueMillis = Instant.parse("2026-07-13T18:00:00Z").toEpochMilli
    val rootPath = Some(tmpDir.toURI.toString)
    val dataSetName = "ion-output-hourly"

    val schema = StructType(
      Seq(
        StructField("key_bytes", BinaryType, nullable = true),
        StructField("value_bytes", BinaryType, nullable = true),
        StructField("key_json", StringType, nullable = true),
        StructField("value_json", StringType, nullable = true),
        StructField("ds", TimestampType, nullable = false)
      )
    )

    val instant = Instant.ofEpochMilli(tsValueMillis)
    val rows = Seq(
      Row("k1".getBytes("UTF-8"), "v1-bytes".getBytes("UTF-8"), "k1-json", """{"v":"one"}""", instant),
      Row("k2".getBytes("UTF-8"), "v2-bytes".getBytes("UTF-8"), "k2-json", """{"v":"two"}""", instant)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows, numSlices = 2), schema)
    val result = IonWriter.write(df, dataSetName, "ds", partitionValue, rootPath)

    result.rowCount shouldBe rows.size

    val partitionPath = IonWriter.resolvePartitionPath(dataSetName, "ds", partitionValue, rootPath)
    val partitionDir = new File(partitionPath.toUri)
    val ionFiles = partitionDir.listFiles().filter(_.getName.endsWith(".ion"))
    ionFiles should not be empty

    val ion = IonSystemBuilder.standard().build()
    val parsed = ionFiles.flatMap { file =>
      val datagram = Using.resource(new FileInputStream(file))(in => ion.getLoader.load(in))
      datagram.iterator().asScala.map { value =>
        val struct = value.asInstanceOf[IonStruct].get("Item").asInstanceOf[IonStruct]
        Option(struct.get("ts")).map(_.asInstanceOf[IonDecimal])
      }
    }

    parsed.size shouldBe rows.size
    parsed.flatten.foreach(_.bigDecimalValue().longValueExact() shouldBe tsValueMillis)
  }

  it should "convert supported partition types to epoch millis" in {
    val expected = Instant.parse("2026-07-13T18:00:00Z").toEpochMilli
    // The upload path stamps the partition-start millis directly as a Long.
    IonWriter.toMillis(expected).longValueExact() shouldBe expected
    IonWriter.toMillis(Instant.ofEpochMilli(expected)).longValueExact() shouldBe expected
    IonWriter.toMillis(new java.sql.Timestamp(expected)).longValueExact() shouldBe expected

    // Daily types anchor to UTC midnight.
    val midnightMillis = Instant.parse("2026-07-13T00:00:00Z").toEpochMilli
    IonWriter.toMillis(LocalDate.parse("2026-07-13")).longValueExact() shouldBe midnightMillis
    IonWriter.toMillis(java.sql.Date.valueOf("2026-07-13")).longValueExact() shouldBe midnightMillis
  }

  it should "reject unsupported partition types" in {
    an[IllegalArgumentException] should be thrownBy IonWriter.toMillis(null)
    an[IllegalArgumentException] should be thrownBy IonWriter.toMillis("2026-07-13")
  }

  it should "validate root path with valid schemes" in {
    IonWriter.validateRootPath(Some("s3://my-bucket/path")) shouldBe "s3://my-bucket/path"
    IonWriter.validateRootPath(Some("s3a://my-bucket")) shouldBe "s3a://my-bucket"
    IonWriter.validateRootPath(Some("file:///tmp/local")) shouldBe "file:///tmp/local"
    IonWriter.validateRootPath(Some("  s3://trimmed/  ")) shouldBe "s3://trimmed"
  }

  it should "reject invalid root paths" in {
    an[IllegalArgumentException] should be thrownBy IonWriter.validateRootPath(None)
    an[IllegalArgumentException] should be thrownBy IonWriter.validateRootPath(Some(""))
    an[IllegalArgumentException] should be thrownBy IonWriter.validateRootPath(Some("  "))
    an[IllegalArgumentException] should be thrownBy IonWriter.validateRootPath(Some("no-scheme-bucket"))
  }

  it should "resolve partition path correctly" in {
    val bucketUri = new File(tmpDir, "bucket-resolve").toURI.toString
    val path = IonWriter.resolvePartitionPath("my-dataset", "ds", "2025-01-15", Some(bucketUri))
    path.toString should include("my-dataset")
    path.toString should include("ds=2025-01-15")
  }
}
