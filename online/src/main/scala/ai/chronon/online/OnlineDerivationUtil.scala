package ai.chronon.online

import ai.chronon.api.{Derivation, TsUtils}
import ai.chronon.api.Extensions.DerivationOps
import ai.chronon.api.LongType
import ai.chronon.api.StringType
import ai.chronon.api.StructField
import ai.chronon.api.StructType
import ai.chronon.online.fetcher.Fetcher

object OnlineDerivationUtil {
  type DerivationFunc = (Map[String, Any], Map[String, Any]) => Map[String, Any]

  val timeFields: Array[StructField] = Array(
    StructField("ts", LongType),
    StructField("ds", StringType)
  )

  // remove value fields of groupBys that have failed with exceptions
  // and reintroduce the exceptions back
  private[online] def reintroduceExceptions(derived: Map[String, Any],
                                            preDerivation: Map[String, Any]): Map[String, Any] = {
    val exceptions: Map[String, Any] =
      preDerivation.iterator.filter(_._1.endsWith(FetcherUtil.FeatureExceptionSuffix)).toMap
    if (exceptions.isEmpty) {
      return derived
    }
    val exceptionParts: Array[String] =
      exceptions.keys.map(_.dropRight(FetcherUtil.FeatureExceptionSuffix.length)).toArray
    derived.filterKeys(key => !exceptionParts.exists(key.startsWith)).toMap ++ exceptions
  }

  def buildRenameOnlyDerivationFunction(derivationsScala: List[Derivation]): DerivationFunc = {
    { case (_: Map[String, Any], values: Any) =>
      reintroduceExceptions(derivationsScala.applyRenameOnlyDerivation(
                              Option(values).getOrElse(Map.empty[String, Any]).asInstanceOf[Map[String, Any]]),
                            values)
    }
  }

  private def buildDerivationFunctionWithSql(
      catalystUtil: PooledCatalystUtil
  ): DerivationFunc = {
    { case (keys: Map[String, Any], values: Map[String, Any]) =>
      reintroduceExceptions(catalystUtil.performSql(keys ++ values).headOption.orNull, values)
    }
  }

  def buildDerivationFunction(
      derivationsScala: List[Derivation],
      keySchema: StructType,
      baseValueSchema: StructType,
      setups: Seq[String] = Seq.empty
  ): DerivationFunc = {

    if (derivationsScala.isEmpty) {

      { case (_, values: Map[String, Any]) => values }

    } else if (derivationsScala.areDerivationsRenameOnly) {

      buildRenameOnlyDerivationFunction(derivationsScala)

    } else {

      val catalystUtil = buildCatalystUtil(derivationsScala, keySchema, baseValueSchema, setups)
      buildDerivationFunctionWithSql(catalystUtil)

    }
  }

  def applyDeriveFunc(
      deriveFunc: DerivationFunc,
      request: Fetcher.Request,
      baseMap: Map[String, AnyRef]
  ): Map[String, AnyRef] = {

    val requestTs = request.atMillis.getOrElse(System.currentTimeMillis())
    val requestDs = TsUtils.toStr(requestTs).substring(0, 10)

    // used for derivation based on ts/ds
    val tsDsMap: Map[String, AnyRef] = {
      Map("ts" -> (requestTs).asInstanceOf[AnyRef], "ds" -> (requestDs).asInstanceOf[AnyRef])
    }

    val derivedMap: Map[String, AnyRef] = deriveFunc(request.keys, Option(baseMap).getOrElse(Map.empty) ++ tsDsMap)
      .mapValues(_.asInstanceOf[AnyRef])
      .toMap

    val derivedMapCleaned = derivedMap -- tsDsMap.keys
    derivedMapCleaned
  }

  private def buildCatalystUtil(
      derivationsScala: List[Derivation],
      keySchema: StructType,
      baseValueSchema: StructType,
      setups: Seq[String] = Seq.empty
  ): PooledCatalystUtil = {
    val baseExpressions = if (derivationsScala.derivationsContainStar) {
      baseValueSchema
        .filterNot { derivationsScala.derivationExpressionSet contains _.name }
        .map(sf => sf.name -> sf.name)
    } else { Seq.empty }
    val expressions = baseExpressions ++ derivationsScala.derivationsWithoutStar.map { d => d.name -> d.expression }
    new PooledCatalystUtil(expressions, StructType("all", (keySchema ++ baseValueSchema).toArray ++ timeFields), setups)
  }

  def buildDerivedFields(
      derivationsScala: List[Derivation],
      keySchema: StructType,
      baseValueSchema: StructType,
      setups: Seq[String] = Seq.empty
  ): Seq[StructField] = {
    if (derivationsScala.areDerivationsRenameOnly) {
      val baseExpressions = if (derivationsScala.derivationsContainStar) {
        baseValueSchema.filterNot { derivationsScala.derivationExpressionSet contains _.name }
      } else {
        Seq.empty
      }
      val expressions: Seq[StructField] = baseExpressions ++ derivationsScala.derivationsWithoutStar.map { d =>
        {
          if (baseValueSchema.typeOf(d.expression).isEmpty) {
            throw new IllegalArgumentException(
              s"Failed to run expression ${d.expression} for ${d.name}. Please ensure the derivation is " +
                "correct.")
          } else {
            StructField(d.name, baseValueSchema.typeOf(d.expression).get)
          }
        }
      }
      expressions
    } else {
      val catalystUtil = buildCatalystUtil(derivationsScala, keySchema, baseValueSchema, setups)
      catalystUtil.outputChrononSchema.map(tup => StructField(tup._1, tup._2))
    }
  }
}
