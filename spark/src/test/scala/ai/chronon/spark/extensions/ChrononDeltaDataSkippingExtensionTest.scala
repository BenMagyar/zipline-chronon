package ai.chronon.spark.extensions

import ai.chronon.spark.utils.SparkTestBase
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.FileSourceScanExec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class ChrononDeltaDataSkippingExtensionTest extends SparkTestBase with Matchers {

  override def sparkConfs: Map[String, String] = Map(
    "spark.serializer" -> "org.apache.spark.serializer.JavaSerializer",
    "spark.sql.extensions" -> (
      "ai.chronon.spark.extensions.ChrononDeltaDataSkippingExtension," +
        "io.delta.sql.DeltaSparkSessionExtension," +
        "ai.chronon.spark.extensions.ChrononDeltaFixExtension"
    ),
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  )

  "Delta data skipping" should "prune files when one side of an AND has missing stats" in {
    val deltaDir = Files.createTempDirectory("chronon-delta-partial-stats")
    val deltaPath = deltaDir.toString
    val defaultStatsColumnsKey = "spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols"
    val previousDefaultStatsColumns = spark.conf.getOption(defaultStatsColumnsKey)

    try {
      spark.conf.set(defaultStatsColumnsKey, "1")
      import spark.implicits._
      Seq((1, 10)).toDF("a", "b").coalesce(1).write.format("delta").save(deltaPath)

      spark.sql(s"""
        ALTER TABLE delta.`$deltaPath`
        SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '2')
      """)
      Seq((2, 20)).toDF("a", "b").coalesce(1).write.format("delta").mode("append").save(deltaPath)

      val pruned = spark.read.format("delta").load(deltaPath).where("a < 0 AND b < 100")
      selectedFileCount(pruned) shouldBe 0
      pruned.collect() shouldBe empty

      val partiallyPruned = spark.read.format("delta").load(deltaPath).where("a > 0 AND b < 15")
      selectedFileCount(partiallyPruned) shouldBe 1
      partiallyPruned.collect().map(_.getAs[Int]("a")) should contain theSameElementsAs Seq(1)

      val retained = spark.read.format("delta").load(deltaPath).where("a > 0 AND b < 100")
      selectedFileCount(retained) shouldBe 2
      retained.collect().map(_.getAs[Int]("a")) should contain theSameElementsAs Seq(1, 2)

      val nested = spark.read.format("delta").load(deltaPath).where("a < 0 AND (b < 100 OR b > 1000)")
      selectedFileCount(nested) shouldBe 0
      nested.collect() shouldBe empty

      val nestedOr = spark.read
        .format("delta")
        .load(deltaPath)
        .where("(a < 0 AND b < 100) OR (a > 10 AND b > 1000)")
      selectedFileCount(nestedOr) shouldBe 0
      nestedOr.collect() shouldBe empty
    } finally {
      previousDefaultStatsColumns.fold(spark.conf.unset(defaultStatsColumnsKey))(
        spark.conf.set(defaultStatsColumnsKey, _))
      FileUtils.deleteDirectory(deltaDir.toFile)
    }
  }

  private def selectedFileCount(query: org.apache.spark.sql.DataFrame): Int =
    query.queryExecution.executedPlan
      .collect { case scan: FileSourceScanExec => scan.relation.location.inputFiles }
      .flatten
      .distinct
      .size
}
