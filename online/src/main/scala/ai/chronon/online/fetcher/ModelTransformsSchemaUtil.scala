package ai.chronon.online.fetcher

import ai.chronon.api.Constants
import ai.chronon.api.Extensions.MetadataOps
import ai.chronon.api.{DataType, ModelTransforms, StructType}
import ai.chronon.online.PooledCatalystUtil

import scala.collection.JavaConverters._

/** Pure (SparkSession-free) schema derivation for a ModelTransforms source. Used by the Flink streaming job
  * to compute the post-model-inference schema fed into tiling/aggregation without requiring a Spark cluster.
  * Mirrors the naming and shape rules of the batch counterpart ModelTransformsJob (computeFinalFieldsToKeep)
  * and the online ModelTransformsFetcher (computePrefixedValueSchema / applySqlMapping).
  */
object ModelTransformsSchemaUtil {

  /** Wrapper that decomposes the MT and delegates. */
  def computePostModelSchema(preModelSchema: Seq[(String, DataType)], mt: ModelTransforms): Seq[(String, DataType)] = {
    val passthrough = Option(mt.passthroughFields).map(_.asScala.toSeq).getOrElse(Seq.empty)
    val models = Option(mt.models).map(_.asScala.toSeq).getOrElse(Seq.empty)
    computePostModelSchema(preModelSchema, passthrough, models)
  }

  /** @param preModelSchema fields present on the event before model inference (source fields + optional
    *                       join enrichment / join-source query outputs)
    * @return passthrough fields (from passthroughFields ∩ preModelSchema, plus Constants.TimeColumn),
    *         followed by one entry per model output field (`<modelName>__<field>`)
    */
  def computePostModelSchema(preModelSchema: Seq[(String, DataType)],
                             passthroughFields: Seq[String],
                             models: Seq[ai.chronon.api.Model]): Seq[(String, DataType)] = {
    val passthroughNames = passthroughFields.toSet + Constants.TimeColumn
    val passthrough = preModelSchema.filter { case (name, _) => passthroughNames.contains(name) }

    val modelOutputFields = models.flatMap { model =>
      val modelName = model.metaData.cleanName

      Option(model.outputMapping) match {
        case Some(mapping) if !mapping.isEmpty =>
          // SQL mapping input schema is the value schema with each field prefixed <modelName>__
          // (parity with ModelTransformsFetcher.applySqlMapping).
          val prefixedValueSchema = computePrefixedValueSchema(model, modelName)
          val pooledCatalyst = new PooledCatalystUtil(mapping.asScala.toSeq, prefixedValueSchema)
          pooledCatalyst.outputChrononSchema.map { case (key, dataType) =>
            s"${modelName}__$key" -> dataType
          }.toSeq

        case _ =>
          // No mapping: emit each raw value-schema field, prefixed.
          optionStructFields(model, modelName).map { case (name, dataType) =>
            s"${modelName}__$name" -> dataType
          }
      }
    }

    passthrough ++ modelOutputFields
  }

  private def computePrefixedValueSchema(model: ai.chronon.api.Model, modelName: String): StructType = {
    val valueSchema = Option(model.valueSchema).map(DataType.fromTDataType)
    valueSchema match {
      case Some(structType: StructType) =>
        val prefixedFields = structType.fields.map { field =>
          field.copy(name = s"${modelName}__${field.name}")
        }
        StructType(s"${structType.name}_prefixed", prefixedFields)
      case other =>
        throw new IllegalArgumentException(
          s"Model ${model.metaData.name} valueSchema must be a StructType for schema derivation, got: $other")
    }
  }

  private def optionStructFields(model: ai.chronon.api.Model, modelName: String): Seq[(String, DataType)] = {
    val valueSchema = Option(model.valueSchema).map(DataType.fromTDataType)
    valueSchema match {
      case Some(structType: StructType) => structType.fields.map(f => f.name -> f.fieldType).toSeq
      case other =>
        throw new IllegalArgumentException(
          s"Model ${model.metaData.name} valueSchema must be a StructType for schema derivation, got: $other")
    }
  }
}
