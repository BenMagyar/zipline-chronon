/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online.fetcher

import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.online.fetcher.Fetcher.{ColumnSpec, PrefixedRequest, Request, Response}
import ai.chronon.online.fetcher.FetcherCache.BatchResponses
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/** Translates a Join request's keys into the keys needed by each underlying GroupBy fetch.
  *
  * GroupBys are fetched by their source keys, but a Join can define left-side selects that derive those keys from the
  * raw request. For example, a Join may receive `query`, select `query_normalized = lower(query)`, and then map
  * `query_normalized` to a GroupBy key. In that case the online Join fetch path should accept `query` from callers,
  * derive `query_normalized` inside the fetcher, and use the derived value only when building the GroupBy request.
  *
  * The helper keeps that flow local to Join fetching:
  *   - `requestKeyFields` reports the raw request keys needed to derive selected Join keys, plus the derived key aliases
  *     themselves so existing direct-key callers can still be logged against the Join key schema.
  *   - `valueInfoLeftKeys` reports only the raw request keys for selected Join keys, so fetchJoinSchema does not imply
  *     that callers must provide derived keys.
  *   - `missingRequestKeys` validates against those raw inputs, while still accepting a derived key if the caller
  *     provides it directly.
  *   - `deriveLeftKeys` runs the Join left selects with the request keys, then combines derived and directly supplied
  *     left keys before JoinPartFetcher maps them to right-side GroupBy keys.
  */
private[fetcher] object JoinRequestKeys {

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

  def requestKeyFields(join: Join,
                       joinPart: JoinPartOps,
                       servingInfo: GroupByServingInfoParsed): Iterable[StructField] = {
    val keySchema = servingInfo.keyCodec.chrononSchema.asInstanceOf[StructType]
    val fieldsByRightKey = keySchema.fields.map(field => field.name -> field).toMap
    val fieldsByRequestKey = mutable.LinkedHashMap.empty[String, StructField]

    joinPart.leftToRight.foreach { case (leftKey, rightKey) =>
      val fieldType = fieldsByRightKey(rightKey).fieldType
      val requestKeys = selectExpression(join, leftKey).map(rawInputs).getOrElse(Seq(leftKey))
      requestKeys.foreach { requestKey =>
        if (!fieldsByRequestKey.contains(requestKey)) {
          fieldsByRequestKey.put(requestKey, StructField(requestKey, rawInputType(servingInfo, requestKey, fieldType)))
        }
      }
      if (!fieldsByRequestKey.contains(leftKey)) {
        fieldsByRequestKey.put(leftKey, StructField(leftKey, fieldType))
      }
    }

    fieldsByRequestKey.values
  }

  def missingRequestKeys(request: Request, join: Join, joinPart: JoinPartOps): Seq[String] =
    joinPart.leftToRight.keys.toSeq.flatMap { leftKey =>
      if (request.keys.contains(leftKey)) {
        Seq.empty
      } else {
        selectExpression(join, leftKey).map(rawInputs).getOrElse(Seq(leftKey)).filterNot(request.keys.contains)
      }
    }.distinct

  private def shouldDeriveLeftKey(request: Request, join: Join, leftKey: String): Boolean =
    selectExpression(join, leftKey).exists { expression =>
      !request.keys.contains(leftKey) || rawInputs(expression).contains(leftKey)
    }

  def needsDerivation(request: Request, join: Join, joinPart: JoinPartOps): Boolean =
    joinPart.leftToRight.keys.exists(shouldDeriveLeftKey(request, join, _))

  def deriveLeftKeys(request: Request,
                     join: Join,
                     joinPart: JoinPartOps,
                     servingInfo: GroupByServingInfoParsed): Map[String, AnyRef] = {
    val directLeftKeys = joinPart.leftToRight.keys.collect {
      case leftKey if request.keys.contains(leftKey) && !shouldDeriveLeftKey(request, join, leftKey) =>
        leftKey -> request.keys(leftKey)
    }.toMap

    val selectedLeftKeys = joinPart.leftToRight.keys.toSeq.flatMap { leftKey =>
      if (shouldDeriveLeftKey(request, join, leftKey)) selectExpression(join, leftKey).map(leftKey -> _) else None
    }

    val derivedLeftKeys =
      if (selectedLeftKeys.isEmpty) {
        Map.empty[String, Any]
      } else {
        val inputSchema = StructType("JoinRequest", requestKeyFields(join, joinPart, servingInfo).toArray)
        val derivedValues = new PooledCatalystUtil(selectedLeftKeys, inputSchema, leftSetups(join))
          .performSql(request.keys)
          .headOption
          .getOrElse(Map.empty)
        selectedLeftKeys.map { case (leftKey, _) =>
          leftKey -> derivedValues.getOrElse(leftKey, null)
        }.toMap
      }

    (directLeftKeys ++ derivedLeftKeys).map { case (key, value) => key -> value.asInstanceOf[AnyRef] }
  }
}

class JoinPartFetcher(fetchContext: FetchContext, metadataStore: MetadataStore) {

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private[online] val groupByFetcher = new GroupByFetcher(fetchContext, metadataStore)
  private implicit val executionContext: ExecutionContext = fetchContext.getOrCreateExecutionContext

  def fetchGroupBys(requests: Seq[Request]): Future[Seq[Response]] = {
    groupByFetcher.fetchGroupBys(requests)
  }

  // ----- START -----
  // floated up to makes tests easy
  def fetchColumns(specs: Seq[ColumnSpec]): Future[Map[ColumnSpec, Response]] = {
    groupByFetcher.fetchColumns(specs)
  }

  def getServingInfo(existing: GroupByServingInfoParsed, batchResponses: BatchResponses): GroupByServingInfoParsed = {
    groupByFetcher.getServingInfo(existing, batchResponses)
  }

  def isCacheSizeConfigured: Boolean = {
    groupByFetcher.isCacheSizeConfigured
  }
  // ---- END ----

  // prioritize passed in joinOverrides over the ones in metadata store
  // used in stream-enrichment and in staging testing
  def fetchJoins(requests: Seq[Request], joinConf: Option[Join] = None): Future[Seq[Response]] = {
    val startTimeMs = System.currentTimeMillis()
    // convert join requests to groupBy requests
    val joinDecomposed: Seq[(Request, Try[Seq[Either[PrefixedRequest, KeyMissingException]]])] =
      requests.map { request =>
        // use passed-in join or fetch one
        val joinTry: Try[JoinOps] = if (joinConf.isEmpty) {
          val joinConfTry = metadataStore.getJoinConf(request.name)
          if (joinConfTry.isFailure) {
            metadataStore.getJoinConf.refresh(request.name)
          }
          joinConfTry
        } else {
          logger.debug(s"Using passed in join configuration: ${joinConf.get.metaData.getName}")
          Success(JoinOps(joinConf.get))
        }

        var joinContext: Option[metrics.Metrics.Context] = None

        val decomposedTry = joinTry.map { join =>
          import ai.chronon.online.metrics
          joinContext = Some(metrics.Metrics.Context(metrics.Metrics.Environment.JoinFetching, join.join))
          joinContext.get.increment("join_request.count")

          join.joinPartOps.map { part =>
            import ai.chronon.online.metrics
            val joinContextInner = metrics.Metrics.Context(joinContext.get, part)
            val missingKeys = JoinRequestKeys.missingRequestKeys(request, join.join, part)

            if (missingKeys.nonEmpty) {
              Right(KeyMissingException(part.fullPrefix, missingKeys.toSeq, request.keys))
            } else {
              val leftKeys =
                if (JoinRequestKeys.needsDerivation(request, join.join, part)) {
                  val servingInfo = metadataStore.getGroupByServingInfo(part.groupBy.metaData.getName).get
                  JoinRequestKeys.deriveLeftKeys(request, join.join, part, servingInfo)
                } else {
                  part.leftToRight.keys.map(leftKey => leftKey -> request.keys(leftKey)).toMap
                }
              val rightKeys = part.leftToRight.map { case (leftKey, rightKey) => rightKey -> leftKeys(leftKey) }
              Left(
                PrefixedRequest(
                  part.columnPrefix,
                  Request(part.groupBy.getMetaData.getName, rightKeys, request.atMillis, Some(joinContextInner))))
            }

          }
        }
        request.copy(context = joinContext) -> decomposedTry
      }

    val groupByRequests = joinDecomposed.flatMap { case (_, gbTry) =>
      gbTry match {
        case Failure(_)        => Iterator.empty
        case Success(requests) => requests.iterator.flatMap(_.left.toOption).map(_.request)
      }
    }

    val groupByResponsesFuture = groupByFetcher.fetchGroupBys(groupByRequests)

    // re-attach groupBy responses to join
    groupByResponsesFuture
      .map { groupByResponses =>
        val responseMap = groupByResponses.iterator.map { response => response.request -> response.values }.toMap
        val responses = joinDecomposed.iterator.map { case (joinRequest, decomposedRequestsTry) =>
          val joinValuesTry = decomposedRequestsTry.map { groupByRequestsWithPrefix =>
            groupByRequestsWithPrefix.iterator.flatMap {

              case Right(keyMissingException) =>
                Map(
                  keyMissingException.requestName + FetcherUtil.FeatureExceptionSuffix -> keyMissingException.getMessage)

              case Left(PrefixedRequest(prefix, groupByRequest)) =>
                parseGroupByResponse(prefix, groupByRequest, responseMap)
            }.toMap

          }
          joinValuesTry match {
            case Failure(ex) => joinRequest.context.foreach(_.incrementException(ex))
            case Success(responseMap) =>
              joinRequest.context.foreach { ctx =>
                ctx.distribution("response.keys.count", responseMap.size)
                Fetcher.logFeatureNullRates(responseMap, ctx)
              }
          }
          joinRequest.context.foreach { ctx =>
            ctx.distribution("internal.latency.millis", System.currentTimeMillis() - startTimeMs)
            ctx.increment("internal.request.count")
          }
          Response(joinRequest, joinValuesTry)
        }.toSeq
        responses
      }
  }

  def parseGroupByResponse(prefix: String,
                           groupByRequest: Request,
                           responseMap: Map[Request, Try[Map[String, AnyRef]]]): Map[String, AnyRef] = {
    // Group bys with all null keys won't be requested from the KV store and we don't expect a response.
    val isRequiredRequest = groupByRequest.keys.values.exists(_ != null) || groupByRequest.keys.isEmpty

    val response: Try[Map[String, AnyRef]] = responseMap.get(groupByRequest) match {
      case Some(value) => value
      case None =>
        if (isRequiredRequest)
          Failure(new IllegalStateException(s"Couldn't find a groupBy response for $groupByRequest in response map"))
        else Success(null)
    }

    response
      .map { valueMap =>
        if (valueMap != null) {
          valueMap.map { case (aggName, aggValue) => prefix + aggName -> aggValue }
        } else {
          Map.empty[String, AnyRef]
        }
      }
      // prefix feature names
      .recover { // capture exception as a key
        case ex: Throwable =>
          if (fetchContext.debug || Math.random() < 0.001) {
            println(s"Failed to fetch $groupByRequest with \n${ex.traceString}")
          }
          Map(prefix + "exception" -> ex.traceString)
      }
      .get
  }
}
