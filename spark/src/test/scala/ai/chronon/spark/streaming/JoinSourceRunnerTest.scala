package ai.chronon.spark.streaming

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinSourceRunnerTest extends AnyFlatSpec with Matchers {

  "JoinSourceRunner.mergeJoinSchemas" should "replace a shadowed left field with the join response field" in {
    val leftSourceSchema = StructType(
      Seq(
        StructField("listing_id", LongType),
        StructField("transition_from", StringType)
      ))
    val joinValueSchema = StructType(
      Seq(
        StructField("transition_from", IntegerType),
        StructField("listing_score", LongType)
      ))

    val merged = JoinSourceRunner.mergeJoinSchemas(leftSourceSchema, joinValueSchema)

    merged.fields.count(_.name == "transition_from") shouldBe 1
    merged("transition_from").dataType shouldBe IntegerType
    merged.fieldNames should contain theSameElementsInOrderAs Seq("listing_id", "transition_from", "listing_score")
  }
}
