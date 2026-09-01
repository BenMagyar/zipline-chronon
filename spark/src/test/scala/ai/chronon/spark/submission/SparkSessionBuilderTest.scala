package ai.chronon.spark.submission

import ai.chronon.spark.utils.SparkTestBase
import org.scalatest.matchers.should.Matchers

class SparkSessionBuilderTest extends SparkTestBase with Matchers {

  "SparkSessionBuilder" should "preserve non-ANSI SQL semantics by default" in {
    spark.conf.get("spark.sql.ansi.enabled") shouldBe "false"
  }
}
