package ai.chronon.online.fetcher

import ai.chronon.api
import ai.chronon.api.{Builders, Constants, DataType, LongType, StringType, DoubleType, StructField, StructType}
import ai.chronon.api.ScalaJavaConversions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ModelTransformsSchemaUtilTest extends AnyFlatSpec with Matchers {

  private val preModelSchema: Seq[(String, api.DataType)] = Seq(
    ("user_id", StringType),
    (Constants.TimeColumn, LongType),
    ("price", DoubleType)
  )

  private def mtWith(models: Seq[api.Model], passthrough: Seq[String] = null): api.ModelTransforms = {
    val mt = new api.ModelTransforms()
    mt.setSources(Seq(Builders.Source.events(query = Builders.Query(), table = "test.events")).toJava)
    if (models.nonEmpty) mt.setModels(models.toJava)
    if (passthrough != null) mt.setPassthroughFields(passthrough.toJava)
    mt.setMetaData(Builders.MetaData(name = "test_model_transforms"))
    mt
  }

  private def modelWith(valueFields: Seq[(String, api.DataType)],
                        outputMapping: java.util.Map[String, String] = null): api.Model = {
    val model = new api.Model()
    val fields = valueFields.map { case (name, typ) => StructField(name, typ) }.toArray
    model.setValueSchema(DataType.toTDataType(StructType("model_value", fields)))
    model.setMetaData(Builders.MetaData(name = "test model"))
    if (outputMapping != null) model.setOutputMapping(outputMapping)
    model
  }

  "computePostModelSchema" should "return just the passthrough intersect when there are no models" in {
    val mt = mtWith(models = Seq.empty, passthrough = Seq("user_id", "nonexistent"))
    val result = ModelTransformsSchemaUtil.computePostModelSchema(preModelSchema, mt)

    // TimeColumn always included; "nonexistent" dropped by intersection
    result.map(_._1) should contain theSameElementsAs Seq("user_id", Constants.TimeColumn)
    result.find(_._1 == "user_id").get._2 shouldBe StringType
  }

  it should "emit all raw value schema fields prefixed when there is no outputMapping" in {
    val model = modelWith(Seq(("score", DoubleType), ("label", StringType)))
    val mt = mtWith(models = Seq(model), passthrough = Seq("user_id"))
    val result = ModelTransformsSchemaUtil.computePostModelSchema(preModelSchema, mt)

    val names = result.map(_._1)
    names should contain theSameElementsInOrderAs Seq("user_id", Constants.TimeColumn, "test_model__score",
      "test_model__label")
    result.find(_._1 == "test_model__score").get._2 shouldBe DoubleType
    result.find(_._1 == "test_model__label").get._2 shouldBe StringType
  }

  it should "emit only mapped fields when outputMapping is set" in {
    val mapping = new java.util.HashMap[String, String]()
    // The output mapping SQL runs against the model's value schema prefixed with <modelName>__, so the
    // expression must reference the prefixed column (matches ModelTransformsFetcher.applySqlMapping).
    mapping.put("p_score", "test_model__score * 2")
    val model = modelWith(Seq(("score", DoubleType)), outputMapping = mapping)
    val mt = mtWith(models = Seq(model), passthrough = Seq("user_id"))
    val result = ModelTransformsSchemaUtil.computePostModelSchema(preModelSchema, mt)

    val names = result.map(_._1)
    // Only the mapped key is emitted (prefixed with model name), not the raw value schema field
    names.filter(_.startsWith("test_model__")) shouldBe Seq("test_model__p_score")
  }

  it should "handle multiple models" in {
    val modelA = modelWith(Seq(("score", DoubleType)))
    val modelB = new api.Model()
    modelB.setValueSchema(DataType.toTDataType(StructType("model_b_value", Array(StructField("flag", StringType)))))
    modelB.setMetaData(Builders.MetaData(name = "model_b"))
    val mt = mtWith(models = Seq(modelA, modelB), passthrough = Seq("user_id"))
    val result = ModelTransformsSchemaUtil.computePostModelSchema(preModelSchema, mt)

    val names = result.map(_._1)
    names should contain allOf ("test_model__score", "model_b__flag")
    names.filterNot(Seq("user_id", Constants.TimeColumn).contains) should have size 2
  }
}