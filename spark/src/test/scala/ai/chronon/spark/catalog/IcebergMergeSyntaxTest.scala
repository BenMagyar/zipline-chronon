package ai.chronon.spark.catalog

import ai.chronon.spark.utils.SparkTestBase
import org.scalatest.matchers.should.Matchers

class IcebergMergeSyntaxTest extends SparkTestBase with Matchers {

  override def sparkConfs: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.JavaSerializer",
    "spark.sql.extensions" -> (
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
      "ai.chronon.spark.extensions.ChrononMergeExtension"
    )
  )

  "ChrononMergeExtension" should "support WHEN NOT MATCHED BY SOURCE with a condition" in {
    val tableName = "default.merge_nmbs_cond_test"
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"""
        CREATE TABLE $tableName (id INT, value STRING, ds STRING) USING iceberg
        TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')
      """)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-01'), (3, 'c', '2024-01-02')")

      spark.sql("CREATE OR REPLACE TEMP VIEW merge_source AS SELECT 10 as id, 'new' as value, '2024-01-01' as ds")

      // Spark 4 grammar requires NOT MATCHED [BY TARGET] clauses before NOT MATCHED BY SOURCE.
      spark.sql(s"""
        MERGE INTO $tableName AS target
        USING merge_source AS source
        ON FALSE
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE AND target.ds = '2024-01-01' THEN DELETE
      """)

      val result = spark.table(tableName).orderBy("ds", "id").collect()
      result.length shouldBe 2

      val day01 = result.filter(_.getAs[String]("ds") == "2024-01-01")
      day01.length shouldBe 1
      day01.head.getAs[Int]("id") shouldBe 10

      val day02 = result.filter(_.getAs[String]("ds") == "2024-01-02")
      day02.length shouldBe 1
      day02.head.getAs[Int]("id") shouldBe 3
    } finally {
      spark.catalog.dropTempView("merge_source")
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  it should "support WHEN NOT MATCHED BY SOURCE without a condition (deletes all unmatched)" in {
    val tableName = "default.merge_nmbs_nocond_test"
    try {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"""
        CREATE TABLE $tableName (id INT, value STRING, ds STRING) USING iceberg
        TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')
      """)
      spark.sql(s"INSERT INTO $tableName VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02')")

      spark.sql("CREATE OR REPLACE TEMP VIEW merge_source_nocond AS SELECT 10 as id, 'new' as value, '2024-01-03' as ds")

      // No condition on NOT MATCHED BY SOURCE — all existing target rows should be deleted
      spark.sql(s"""
        MERGE INTO $tableName AS target
        USING merge_source_nocond AS source
        ON FALSE
        WHEN NOT MATCHED THEN INSERT *
        WHEN NOT MATCHED BY SOURCE THEN DELETE
      """)

      val result = spark.table(tableName).orderBy("ds").collect()
      // All old rows deleted, only the new row inserted
      result.length shouldBe 1
      result.head.getAs[Int]("id") shouldBe 10
      result.head.getAs[String]("ds") shouldBe "2024-01-03"
    } finally {
      spark.catalog.dropTempView("merge_source_nocond")
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }
}
