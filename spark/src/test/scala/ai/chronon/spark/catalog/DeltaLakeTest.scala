package ai.chronon.spark.catalog

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.submission.SparkSessionBuilder
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

class DeltaLakeTest extends AnyFlatSpec with BeforeAndAfterAll {

  private implicit lazy val spark: SparkSession =
    SparkSessionBuilder.build(
      "DeltaLakeTest",
      local = true,
      additionalConfig = Some(
        Map(
          "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
          "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ))
    )

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  it should "derive virtual partitions from Delta log stats for a timestamp column" in {
    val dbName = s"delta_stats_${System.nanoTime()}"
    val tableName = s"$dbName.time_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (TIMESTAMP '2024-01-01 12:00:00', 'user1'),
          (TIMESTAMP '2024-01-03 12:00:00', 'user2')
      """)

      DeltaLake.statsDateRange(tableName, "created_at", PartitionSpec.daily) shouldBe
        Some(DeltaLake.StatsDateRange(start = "2024-01-01", end = "2024-01-03"))
      DeltaLake.virtualPartitions(tableName, "created_at", PartitionSpec.daily) shouldBe
        List("2024-01-01", "2024-01-02", "2024-01-03")
      DeltaLake.firstAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-01-01")
      DeltaLake.lastAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-01-02")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "derive virtual partitions from Delta log stats for a clustered timestamp column" in {
    val dbName = s"delta_clustered_stats_${System.nanoTime()}"
    val tableName = s"$dbName.time_clustered_with_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
        CLUSTER BY (created_at)
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (TIMESTAMP '2024-03-01 12:00:00', 'user1'),
          (TIMESTAMP '2024-03-03 12:00:00', 'user2')
      """)

      DeltaLake.statsDateRange(tableName, "created_at", PartitionSpec.daily) shouldBe
        Some(DeltaLake.StatsDateRange(start = "2024-03-01", end = "2024-03-03"))
      DeltaLake.virtualPartitions(tableName, "created_at", PartitionSpec.daily) shouldBe
        List("2024-03-01", "2024-03-02", "2024-03-03")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  it should "fall back to scanning when Delta log stats do not cover the timestamp column" in {
    val dbName = s"delta_stats_fallback_${System.nanoTime()}"
    val tableName = s"$dbName.time_missing_stats"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    try {
      spark.sql(s"""
        CREATE TABLE $tableName (
          indexed_id INT,
          created_at TIMESTAMP,
          user_id STRING
        ) USING DELTA
        TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1')
      """)
      spark.sql(s"""
        INSERT INTO $tableName VALUES
          (1, TIMESTAMP '2024-02-01 12:00:00', 'user1'),
          (2, TIMESTAMP '2024-02-03 12:00:00', 'user2')
      """)

      DeltaLake.statsDateRange(tableName, "created_at", PartitionSpec.daily) shouldBe None
      DeltaLake.virtualPartitions(tableName, "created_at", PartitionSpec.daily) shouldBe
        List("2024-02-01", "2024-02-02", "2024-02-03")
      DeltaLake.firstAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-02-01")
      DeltaLake.lastAvailablePartition(tableName, "created_at", PartitionSpec.daily) shouldBe Some("2024-02-02")
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }
}
