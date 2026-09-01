package ai.chronon.spark

import ai.chronon.api.PartitionSpec
import ai.chronon.spark.Extensions._
import ai.chronon.spark.utils.SparkTestBase
import org.scalatest.matchers.should.Matchers

class ExtensionsTest extends SparkTestBase with Matchers {

  "withPartitionBasedTimestamp" should "only accept values matching the partition format" in {
    import spark.implicits._

    val compactDailySpec = PartitionSpec("ds", "yyyyMMdd", PartitionSpec.daily.spanMillis)
    val result = Seq("20240101", "2024-01-01")
      .toDF("ds")
      .withPartitionBasedTimestamp("ts", spec = compactDailySpec)
      .orderBy("ds")
      .collect()

    result(0).getAs[String]("ds") shouldBe "2024-01-01"
    result(0).isNullAt(result(0).fieldIndex("ts")) shouldBe true
    result(1).getAs[String]("ds") shouldBe "20240101"
    result(1).getAs[Long]("ts") shouldBe compactDailySpec.epochMillis("20240101")
  }

  "translatePartitionSpec" should "floor same-format partition values to the target grid" in {
    import spark.implicits._

    val fifteenMinuteSpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 15 * 60 * 1000)
    val hourlySpec = PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 60 * 60 * 1000)

    val result = Seq("2024-01-01-00-00", "2024-01-01-00-15", "2024-01-01-00-45")
      .toDF("ds")
      .translatePartitionSpec(fifteenMinuteSpec, hourlySpec)
      .as[String]
      .collect()
      .toSeq

    result should contain theSameElementsInOrderAs Seq(
      "2024-01-01-00-00",
      "2024-01-01-00-00",
      "2024-01-01-00-00"
    )
  }
}
