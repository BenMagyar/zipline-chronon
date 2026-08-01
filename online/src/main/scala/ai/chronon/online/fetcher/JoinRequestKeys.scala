package ai.chronon.online.fetcher

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.online.fetcher.Fetcher.Request
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

import scala.collection.mutable
import scala.util.Try

/** Translates a Join request's keys into the keys needed by each underlying GroupBy fetch.
  *
  * GroupBys are fetched by their source keys, but a Join can define left-side selects that derive those keys from the
  * raw request. For example, a Join may receive `query`, select `query_normalized = lower(query)`, and then map
  * `query_normalized` to a GroupBy key. In that case the online Join fetch path should accept `query` from callers,
  * derive `query_normalized` inside the fetcher, and use the derived value only when building the GroupBy request.
  *
  * The helper keeps that flow local to Join fetching:
  *   - `buildKeyMapping` is run while building the JoinCodec so SQL parsing, input-schema construction, and Catalyst
  *     setup are attempted once and cached with the Join metadata instead of repeated per request. A Catalyst setup
  *     failure is surfaced only if a request actually needs key derivation.
  *   - `requestKeyFields` uses the GroupBy key schema for direct Join keys. For selected Join keys it reports the raw
  *     request keys needed for derivation, plus the derived key aliases themselves so existing direct-key callers can
  *     still be logged against the Join key schema.
  *   - `valueInfoLeftKeys` reports only the raw request keys for selected Join keys, so fetchJoinSchema does not imply
  *     that callers must provide derived keys.
  *   - `missingRequestKeys` validates against those raw inputs, while still accepting a derived key if the caller
  *     provides it directly.
  *   - `KeyMapping.leftKeys` runs the cached Join left-select transform when needed, then combines derived and directly
  *     supplied left keys before JoinPartFetcher maps them to right-side GroupBy keys.
  */
private[online] object JoinRequestKeys {

  /** Request-only portion of a Join part's key mapping.
    *
    * Unlike [[KeyMapping]], this does not depend on GroupBy serving schemas and can therefore be prepared alongside
    * the Join metadata. In particular, selected SQL expressions are parsed once here instead of once per candidate
    * and Join part in `JoinPartFetcher`.
    */
  final class RequestPlan private[JoinRequestKeys] (private val leftKeys: Array[String],
                                                    private val selectedLeftKeySet: Set[String],
                                                    private val selfReferencingLeftKeys: Set[String],
                                                    private[fetcher] val rawInputsByLeftKey: Map[String, Seq[String]],
                                                    private[fetcher] val selectedLeftKeys: Seq[(String, String)]) {

    def missingRequestKeys(request: Request): Seq[String] = {
      var missing: mutable.LinkedHashSet[String] = null
      var index = 0
      while (index < leftKeys.length) {
        val leftKey = leftKeys(index)
        if (!request.keys.contains(leftKey)) {
          val requiredKeys =
            if (selectedLeftKeySet.contains(leftKey)) rawInputsByLeftKey(leftKey)
            else Seq(leftKey)
          requiredKeys.foreach { requestKey =>
            if (!request.keys.contains(requestKey)) {
              if (missing == null) missing = mutable.LinkedHashSet.empty[String]
              missing += requestKey
            }
          }
        }
        index += 1
      }
      if (missing == null) Seq.empty else missing.toSeq
    }

    def needsDerivation(request: Request): Boolean = {
      var index = 0
      while (index < leftKeys.length) {
        val leftKey = leftKeys(index)
        if (
          selectedLeftKeySet.contains(leftKey) &&
          (!request.keys.contains(leftKey) || selfReferencingLeftKeys.contains(leftKey))
        ) {
          return true
        }
        index += 1
      }
      false
    }

    private[fetcher] def derives(leftKey: String): Boolean = selectedLeftKeySet.contains(leftKey)
  }

  case class KeyMapping(leftToRight: Map[String, String],
                        requestKeyFields: Iterable[StructField],
                        rawInputsByLeftKey: Map[String, Seq[String]],
                        selectedLeftKeys: Seq[(String, String)],
                        catalystUtil: Option[Try[PooledCatalystUtil]]) {
    private val selectedExpressions = selectedLeftKeys.toMap

    private def shouldDeriveLeftKey(request: Request, leftKey: String): Boolean =
      selectedExpressions.contains(leftKey) &&
        (!request.keys.contains(leftKey) || rawInputsByLeftKey.getOrElse(leftKey, Seq.empty).contains(leftKey))

    def missingRequestKeys(request: Request): Seq[String] =
      leftToRight.keys.toSeq.flatMap { leftKey =>
        if (request.keys.contains(leftKey)) {
          Seq.empty
        } else {
          rawInputsByLeftKey
            .get(leftKey)
            .filter(_ => selectedExpressions.contains(leftKey))
            .getOrElse(Seq(leftKey))
            .filterNot(request.keys.contains)
        }
      }.distinct

    def leftKeys(request: Request): Map[String, AnyRef] = {
      val directLeftKeys = leftToRight.keys.collect {
        case leftKey if request.keys.contains(leftKey) && !shouldDeriveLeftKey(request, leftKey) =>
          leftKey -> request.keys(leftKey)
      }.toMap

      val keysToDerive = selectedLeftKeys.filter { case (leftKey, _) => shouldDeriveLeftKey(request, leftKey) }
      val derivedLeftKeys =
        if (keysToDerive.isEmpty) {
          Map.empty[String, Any]
        } else {
          val derivedValues = catalystUtil
            .map(_.get.performSql(request.keys).headOption.getOrElse(Map.empty))
            .getOrElse(Map.empty)
          keysToDerive.map { case (leftKey, _) =>
            leftKey -> derivedValues.getOrElse(leftKey, null)
          }.toMap
        }

      (directLeftKeys ++ derivedLeftKeys).map { case (key, value) => key -> value.asInstanceOf[AnyRef] }
    }
  }

  private def partKey(parts: Seq[String]): String =
    parts.map(part => s"${part.length}:$part").mkString("|")

  private def partKeyParts(join: Join, joinPart: JoinPartOps): Seq[String] =
    Seq(
      joinPart.groupBy.metaData.getName,
      Option(joinPart.prefix).getOrElse("")
    ) ++
      joinPart.leftToRight.toSeq.sortBy(_._1).map { case (left, right) => s"key:$left=$right" } ++
      leftSelects(join).toSeq.sortBy(_._1).map { case (name, expr) => s"select:$name=$expr" } ++
      leftSetups(join).map(setup => s"setup:$setup")

  def partKey(join: Join, joinPart: JoinPartOps): String =
    partKey(partKeyParts(join, joinPart))

  def partKey(join: Join, joinPart: JoinPartOps, servingInfo: GroupByServingInfoParsed): String =
    partKey(partKeyParts(join, joinPart) ++ Seq(servingInfo.keyAvroSchema, servingInfo.inputAvroSchema))

  private def leftSelects(join: Join): Map[String, String] =
    Option(join.left)
      .flatMap(source => Option(source.query))
      .flatMap(query => Option(query.getQuerySelects))
      .getOrElse(Map.empty)

  private def leftSetups(join: Join): Seq[String] =
    Option(join.left)
      .flatMap(source => Option(source.query))
      .map(_.setupsSeq)
      .getOrElse(Seq.empty)

  private def selectExpression(join: Join, leftKey: String): Option[String] =
    leftSelects(join).get(leftKey).filter(_ != leftKey)

  def prepare(join: Join, joinPart: JoinPartOps): RequestPlan = {
    val leftKeys = joinPart.leftToRight.keys.toArray
    val selectedLeftKeys = leftKeys.toSeq.flatMap { leftKey =>
      selectExpression(join, leftKey).map(leftKey -> _)
    }
    val rawInputsByLeftKey = leftKeys.iterator.map { leftKey =>
      val inputs = selectedLeftKeys
        .find(_._1 == leftKey)
        .map { case (_, expression) => rawInputs(expression) }
        .getOrElse(Seq(leftKey))
      leftKey -> inputs
    }.toMap
    val selectedLeftKeySet = selectedLeftKeys.iterator.map(_._1).toSet
    val selfReferencingLeftKeys = selectedLeftKeySet.filter { leftKey =>
      rawInputsByLeftKey(leftKey).contains(leftKey)
    }

    new RequestPlan(
      leftKeys,
      selectedLeftKeySet,
      selfReferencingLeftKeys,
      rawInputsByLeftKey,
      selectedLeftKeys
    )
  }

  private[fetcher] def rawInputs(expression: String): Seq[String] =
    CatalystSqlParser
      .parseExpression(expression)
      .collect { case attr: UnresolvedAttribute =>
        attr.nameParts.head
      }
      .distinct

  private def rawInputType(servingInfo: GroupByServingInfoParsed,
                           requestKey: String,
                           fallbackType: DataType): DataType =
    Try(servingInfo.inputChrononSchema.typeOf(requestKey)).toOption.flatten.getOrElse(fallbackType)

  def valueInfoLeftKeys(join: Join, joinPart: JoinPartOps): Iterable[String] =
    joinPart.leftToRight.keys.toSeq.flatMap { leftKey =>
      selectExpression(join, leftKey).map(rawInputs).getOrElse(Seq(leftKey))
    }.distinct

  private def requestKeyFields(joinPart: JoinPartOps,
                               servingInfo: GroupByServingInfoParsed,
                               requestPlan: RequestPlan): Iterable[StructField] = {
    val keySchema = servingInfo.keyCodec.chrononSchema.asInstanceOf[StructType]
    val fieldsByRightKey = keySchema.fields.map(field => field.name -> field).toMap
    val fieldsByRequestKey = mutable.LinkedHashMap.empty[String, StructField]

    joinPart.leftToRight.foreach { case (leftKey, rightKey) =>
      val fieldType = fieldsByRightKey
        .getOrElse(
          rightKey,
          throw new IllegalArgumentException(
            s"Join part ${joinPart.fullPrefix} maps left key $leftKey to right key $rightKey, " +
              s"but $rightKey is not present in GroupBy key schema ${keySchema.fields.map(_.name).mkString(", ")}")
        )
        .fieldType
      val derivesLeftKey = requestPlan.derives(leftKey)
      val requestKeys = requestPlan.rawInputsByLeftKey.getOrElse(leftKey, Seq(leftKey))
      requestKeys.foreach { requestKey =>
        if (!fieldsByRequestKey.contains(requestKey)) {
          val requestFieldType =
            if (derivesLeftKey) rawInputType(servingInfo, requestKey, fieldType) else fieldType
          fieldsByRequestKey.put(requestKey, StructField(requestKey, requestFieldType))
        }
      }
      if (!fieldsByRequestKey.contains(leftKey)) {
        fieldsByRequestKey.put(leftKey, StructField(leftKey, fieldType))
      }
    }

    fieldsByRequestKey.values
  }

  def buildKeyMapping(join: Join, joinPart: JoinPartOps, servingInfo: GroupByServingInfoParsed): KeyMapping =
    buildKeyMapping(join, joinPart, servingInfo, prepare(join, joinPart))

  private[fetcher] def buildKeyMapping(join: Join,
                                       joinPart: JoinPartOps,
                                       servingInfo: GroupByServingInfoParsed,
                                       requestPlan: RequestPlan): KeyMapping = {
    val keyFields = requestKeyFields(joinPart, servingInfo, requestPlan)
    val catalystUtil =
      if (requestPlan.selectedLeftKeys.isEmpty) None
      else
        Some(
          Try(
            new PooledCatalystUtil(requestPlan.selectedLeftKeys,
                                   StructType("JoinRequest", keyFields.toArray),
                                   leftSetups(join))))

    KeyMapping(joinPart.leftToRight,
               keyFields,
               requestPlan.rawInputsByLeftKey,
               requestPlan.selectedLeftKeys,
               catalystUtil)
  }

  def missingRequestKeys(request: Request, join: Join, joinPart: JoinPartOps): Seq[String] =
    prepare(join, joinPart).missingRequestKeys(request)

  def needsDerivation(request: Request, join: Join, joinPart: JoinPartOps): Boolean =
    prepare(join, joinPart).needsDerivation(request)

  def deriveLeftKeys(request: Request,
                     join: Join,
                     joinPart: JoinPartOps,
                     servingInfo: GroupByServingInfoParsed): Map[String, AnyRef] = {
    buildKeyMapping(join, joinPart, servingInfo).leftKeys(request)
  }

  private[fetcher] def deriveLeftKeys(request: Request,
                                      join: Join,
                                      joinPart: JoinPartOps,
                                      servingInfo: GroupByServingInfoParsed,
                                      requestPlan: RequestPlan): Map[String, AnyRef] = {
    buildKeyMapping(join, joinPart, servingInfo, requestPlan).leftKeys(request)
  }
}
