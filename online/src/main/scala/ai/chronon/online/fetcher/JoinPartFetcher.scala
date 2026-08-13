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
import ai.chronon.online.fetcher.Fetcher.{ColumnSpec, Request, Response}
import ai.chronon.online.fetcher.FetcherCache.BatchResponses
import ai.chronon.online.metrics.Metrics
import org.slf4j.{Logger, LoggerFactory}

import java.util.function.BiFunction
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

object JoinPartFetcher {
  private val PreparedJoinMetricsCacheMaxEntries = 10000
}

class JoinPartFetcher(fetchContext: FetchContext, metadataStore: MetadataStore) extends ThrottledLogging {

  import JoinPartFetcher._

  private final class PreparedJoinMetrics(val mutableJoinSnapshot: Join,
                                          val joinOps: JoinOps,
                                          val joinContext: Metrics.Context,
                                          val parts: Seq[PreparedJoinPart])

  private final class PreparedJoinPart(join: Join, val joinPartOps: JoinPartOps, val metricsContext: Metrics.Context) {
    // Parsing SQL expressions can fail. Cache the Try rather than a throwing lazy initializer: Scala retries failed
    // lazy vals, which would otherwise reparse the same invalid expression for every candidate in the request. The
    // accessor still throws only after fetchJoins has attached the prepared join metrics context.
    private lazy val requestPlanResult = Try(JoinRequestKeys.prepare(join, joinPartOps))
    def requestPlan: JoinRequestKeys.RequestPlan = requestPlanResult.get
    lazy val unqualifiedPartKey: String = JoinRequestKeys.partKey(join, joinPartOps)
    private final class QualifiedPartKey(val servingInfo: GroupByServingInfoParsed, val value: String)
    @volatile private var cachedQualifiedPartKey: QualifiedPartKey = _
    private val prefix = joinPartOps.columnPrefix
    private lazy val prefixedFeatureNames = {
      val responseFeatureNames = Try {
        val groupBy = joinPartOps.groupBy
        val baseFeatureNames = groupBy.valueColumns.toSeq
        if (!groupBy.hasDerivations) {
          baseFeatureNames
        } else {
          val wildcardFeatureNames =
            if (groupBy.derivationsContainStar)
              baseFeatureNames.filterNot(groupBy.derivationExpressionSet.contains)
            else Seq.empty
          wildcardFeatureNames ++ groupBy.derivationsWithoutStar.map(_.name)
        }
      }.getOrElse(Seq.empty)

      (responseFeatureNames.iterator ++ Iterator("exception", s"derivation${FetcherUtil.FeatureExceptionSuffix}"))
        .map(featureName => featureName -> (prefix + featureName))
        .toMap
    }

    def prefixedFeatureName(featureName: String): String =
      prefixedFeatureNames.getOrElse(featureName, prefix + featureName)

    def qualifiedPartKey(servingInfo: GroupByServingInfoParsed): String = {
      val cached = cachedQualifiedPartKey
      if ((cached ne null) && (cached.servingInfo eq servingInfo)) {
        cached.value
      } else {
        this.synchronized {
          val current = cachedQualifiedPartKey
          if ((current ne null) && (current.servingInfo eq servingInfo)) {
            current.value
          } else {
            val key = JoinRequestKeys.partKey(join, joinPartOps, servingInfo)
            cachedQualifiedPartKey = new QualifiedPartKey(servingInfo, key)
            key
          }
        }
      }
    }
  }

  private final class PreparedPrefixedRequest(val part: PreparedJoinPart, val request: Request)

  private final class ResolvedDerivedKeys(val servingInfo: Try[GroupByServingInfoParsed],
                                          val keyMapping: Option[JoinRequestKeys.KeyMapping])

  private val preparedJoinMetricsByName =
    LRUCache[String, PreparedJoinMetrics]("join_part_prepared_metrics", PreparedJoinMetricsCacheMaxEntries)

  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private[online] val groupByFetcher = new GroupByFetcher(fetchContext, metadataStore)
  private implicit val executionContext: ExecutionContext = fetchContext.getOrCreateExecutionContext

  private def preparedJoinMetrics(joinName: String,
                                  joinOps: JoinOps,
                                  validateMutableOverride: Boolean): PreparedJoinMetrics = {
    def matches(prepared: PreparedJoinMetrics): Boolean =
      if (validateMutableOverride) {
        prepared.mutableJoinSnapshot != null && prepared.mutableJoinSnapshot == joinOps.join
      } else {
        // MetadataStore refreshes publish a new Join object, making identity the mutation-free production fast path.
        prepared.joinOps.join.eq(joinOps.join)
      }

    val cached = preparedJoinMetricsByName.getIfPresent(joinName)
    if (cached != null && matches(cached)) {
      cached
    } else {
      // Only metadata refreshes and first access take the per-name cache update lock. The normal request path above
      // is a concurrent cache lookup, which matters when many candidate batches target the same join.
      preparedJoinMetricsByName
        .asMap()
        .compute(
          joinName,
          new BiFunction[String, PreparedJoinMetrics, PreparedJoinMetrics] {
            override def apply(ignored: String, current: PreparedJoinMetrics): PreparedJoinMetrics = {
              if (current != null && matches(current)) {
                current
              } else {
                val joinContext = Metrics.Context(Metrics.Environment.JoinFetching, joinOps.join)
                val parts =
                  joinOps.joinPartOps.map(part =>
                    new PreparedJoinPart(joinOps.join, part, Metrics.Context(joinContext, part)))
                // Caller-provided Thrift overrides are mutable. Snapshot those so in-place changes cannot reuse stale
                // key or output-prefix plans. Metadata-owned joins have an immutable publication contract and use
                // identity above, avoiding a 43-part structural scan on every production request.
                val mutableJoinSnapshot = if (validateMutableOverride) joinOps.join.deepCopy() else null
                new PreparedJoinMetrics(mutableJoinSnapshot, joinOps, joinContext, parts)
              }
            }
          }
        )
    }
  }

  def fetchGroupBys(requests: Seq[Request]): Future[Seq[Response]] =
    fetchGroupBys(requests, allowBatchIrCache = true)

  def fetchGroupBys(requests: Seq[Request], allowBatchIrCache: Boolean): Future[Seq[Response]] = {
    groupByFetcher.fetchGroupBys(requests, allowBatchIrCache)
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
  def fetchJoins(requests: Seq[Request],
                 joinConf: Option[Join] = None,
                 joinCodecForName: String => Option[Try[JoinCodec]] = _ => None): Future[Seq[Response]] =
    fetchJoins(requests, joinConf, joinCodecForName, allowBatchIrCache = true, deduplicateGroupByRequests = true)

  def fetchJoins(requests: Seq[Request],
                 joinConf: Option[Join],
                 joinCodecForName: String => Option[Try[JoinCodec]],
                 allowBatchIrCache: Boolean,
                 deduplicateGroupByRequests: Boolean): Future[Seq[Response]] = {
    val startTimeMs = System.currentTimeMillis()
    val overrideJoinOps = joinConf.map { join =>
      logger.debug(s"Using passed in join configuration: ${join.metaData.getName}")
      Success(JoinOps(join))
    }
    val joinOpsByName = mutable.HashMap.empty[String, Try[JoinOps]]
    val preparedMetricsByName = mutable.HashMap.empty[String, PreparedJoinMetrics]
    // A derived part uses the same JoinCodec and serving metadata for every candidate in this outer request. Resolve
    // its schema-qualified mapping once rather than rebuilding the large part key (left selects + setups) per row.
    // Keep this cache request-scoped so metadata and codec refreshes are visible on the next fetchJoins call.
    val resolvedDerivedKeysByPart = mutable.HashMap.empty[PreparedJoinPart, ResolvedDerivedKeys]

    def resolveDerivedKeys(requestName: String, join: JoinOps, preparedPart: PreparedJoinPart): ResolvedDerivedKeys =
      resolvedDerivedKeysByPart.get(preparedPart).getOrElse {
        val part = preparedPart.joinPartOps
        val servingInfo = metadataStore.getGroupByServingInfo(part.groupBy.metaData.getName)
        val keyMapping = joinCodecForName(requestName).flatMap(_.toOption).flatMap { codec =>
          servingInfo.toOption
            .flatMap(info => codec.joinPartKeyMappings.get(preparedPart.qualifiedPartKey(info)))
            .orElse {
              if (servingInfo.isFailure) {
                codec.joinPartKeyMappings.get(preparedPart.unqualifiedPartKey)
              } else {
                None
              }
            }
        }
        val resolved = new ResolvedDerivedKeys(servingInfo, keyMapping)
        // A cold serving-info Failure is deliberately stamped expired by TTLCache so a later candidate can observe an
        // asynchronous recovery. Do not turn that transient failure into an all-candidate request failure. A cached
        // JoinCodec mapping remains usable without the current serving info, so it is safe to memoize that case.
        if (servingInfo.isSuccess || keyMapping.isDefined) resolvedDerivedKeysByPart.put(preparedPart, resolved)
        resolved
      }

    // convert join requests to groupBy requests
    val joinDecomposed: Seq[(Request, Try[Seq[Either[PreparedPrefixedRequest, KeyMissingException]]])] =
      requests.map { request =>
        // use passed-in join or fetch one
        val joinTry: Try[JoinOps] = overrideJoinOps.getOrElse {
          joinOpsByName.getOrElseUpdate(
            request.name, {
              val joinConfTry = metadataStore.getJoinConf(request.name)
              if (joinConfTry.isFailure) {
                metadataStore.getJoinConf.refresh(request.name)
              }
              joinConfTry
            }
          )
        }

        var joinContext: Option[metrics.Metrics.Context] = None

        val decomposedTry = joinTry.map { join =>
          val preparedMetrics =
            preparedMetricsByName.getOrElseUpdate(
              request.name,
              preparedJoinMetrics(request.name, join, validateMutableOverride = joinConf.isDefined))
          joinContext = Some(preparedMetrics.joinContext)
          preparedMetrics.joinContext.increment("join_request.count")

          preparedMetrics.parts.map { preparedPart =>
            val part = preparedPart.joinPartOps
            val needsDerivation = preparedPart.requestPlan.needsDerivation(request)
            lazy val derivedKeys = resolveDerivedKeys(request.name, join, preparedPart)
            val staticMissingKeys = preparedPart.requestPlan.missingRequestKeys(request)
            val missingKeys =
              if (staticMissingKeys.nonEmpty || !needsDerivation) {
                staticMissingKeys
              } else {
                derivedKeys.keyMapping.map(_.missingRequestKeys(request)).getOrElse(staticMissingKeys)
              }

            if (missingKeys.nonEmpty) {
              Right(KeyMissingException(part.fullPrefix, missingKeys.toSeq, request.keys))
            } else {
              val leftKeys =
                if (needsDerivation) {
                  derivedKeys.keyMapping.map(_.leftKeys(request)).getOrElse {
                    val servingInfo = derivedKeys.servingInfo.get
                    JoinRequestKeys.deriveLeftKeys(request, join.join, part, servingInfo, preparedPart.requestPlan)
                  }
                } else {
                  part.leftToRight.keys.map(leftKey => leftKey -> request.keys(leftKey)).toMap
                }
              val rightKeys = part.leftToRight.map { case (leftKey, rightKey) => rightKey -> leftKeys(leftKey) }
              Left(
                new PreparedPrefixedRequest(preparedPart,
                                            Request(part.groupBy.getMetaData.getName,
                                                    rightKeys,
                                                    request.atMillis,
                                                    Some(preparedPart.metricsContext))))
            }

          }
        }
        request.copy(context = joinContext) -> decomposedTry
      }

    // Deduplicate GroupBy requests by (name, keys, atMillis) before fetching.
    // Context is per-join-row and must not influence deduplication or the response lookup key.
    // We keep one representative request (with its original context) per unique logical key so
    // that GroupBy-level metrics continue to fire with the correct join+groupBy tags.
    type GroupByKey = (String, Map[String, AnyRef], Option[Long])
    def groupByKey(r: Request): GroupByKey = (r.name, r.keys, r.atMillis)

    val allGroupByRequests = joinDecomposed.flatMap { case (_, gbTry) =>
      gbTry match {
        case Failure(_)        => Iterator.empty
        case Success(requests) => requests.iterator.flatMap(_.left.toOption).map(_.request)
      }
    }

    val groupByRequests: Seq[Request] =
      if (deduplicateGroupByRequests) {
        // One representative request per unique (name, keys, atMillis) — first occurrence wins. Startup repeats outer
        // rows to exercise response fan-out but retains authored timestamps, so its normal dedup also lands here.
        val uniqueGroupByRequests = mutable.LinkedHashMap.empty[GroupByKey, Request]
        allGroupByRequests.foreach(request => uniqueGroupByRequests.getOrElseUpdate(groupByKey(request), request))
        uniqueGroupByRequests.values.toSeq
      } else {
        // Retain this explicit opt-out for separately compiled callers; the fetcher warmup does not use it.
        allGroupByRequests.toVector
      }

    val groupByResponsesFuture =
      if (allowBatchIrCache) fetchGroupBys(groupByRequests)
      else fetchGroupBys(groupByRequests, allowBatchIrCache = false)

    // re-attach groupBy responses to join
    groupByResponsesFuture
      .map { groupByResponses =>
        // Key the response map by (name, keys, atMillis) so the fan-out lookup ignores context.
        val responseMap: Map[GroupByKey, Try[Map[String, AnyRef]]] =
          groupByResponses.iterator.map { response => groupByKey(response.request) -> response.values }.toMap
        val featureMetrics = new Fetcher.FeatureMetricsAccumulator
        val responses = joinDecomposed.iterator.map { case (joinRequest, decomposedRequestsTry) =>
          val joinValuesTry = decomposedRequestsTry.map { groupByRequestsWithPrefix =>
            var result = Map.empty[String, AnyRef]
            groupByRequestsWithPrefix.foreach {

              case Right(keyMissingException) =>
                result = result.updated(keyMissingException.requestName + FetcherUtil.FeatureExceptionSuffix,
                                        keyMissingException.getMessage)

              case Left(preparedRequest) =>
                result =
                  appendGroupByResponse(preparedRequest.part, groupByKey(preparedRequest.request), responseMap, result)
            }
            result

          }
          joinValuesTry match {
            case Failure(ex) => joinRequest.context.foreach(_.incrementException(ex))
            case Success(responseMap) =>
              joinRequest.context.foreach { ctx =>
                ctx.distribution("response.keys.count", responseMap.size)
                featureMetrics.add(responseMap, ctx)
              }
          }
          joinRequest.context.foreach { ctx =>
            ctx.distribution("internal.latency.millis", System.currentTimeMillis() - startTimeMs)
            ctx.increment("internal.request.count")
          }
          Response(joinRequest, joinValuesTry)
        }.toSeq
        featureMetrics.flush()
        responses
      }
  }

  private def appendGroupByResponse(
      part: PreparedJoinPart,
      key: (String, Map[String, AnyRef], Option[Long]),
      responseMap: Map[(String, Map[String, AnyRef], Option[Long]), Try[Map[String, AnyRef]]],
      result: Map[String, AnyRef]
  ): Map[String, AnyRef] = {
    groupByResponse(key, responseMap) match {
      case Success(valueMap) if valueMap != null =>
        var updated = result
        try {
          valueMap.foreach { case (featureName, featureValue) =>
            updated = updated.updated(part.prefixedFeatureName(featureName), featureValue)
          }
          updated
        } catch {
          // The previous parseGroupByResponse contract converted response-map transformation failures into a
          // part-scoped exception. Persistent maps let us discard any partial updates without an intermediate map.
          case NonFatal(ex) => appendGroupByFailure(part, key, ex, result)
        }
      case Success(_)  => result // A response with all-null request keys is intentionally absent.
      case Failure(ex) => appendGroupByFailure(part, key, ex, result)
    }
  }

  private def appendGroupByFailure(part: PreparedJoinPart,
                                   key: (String, Map[String, AnyRef], Option[Long]),
                                   ex: Throwable,
                                   result: Map[String, AnyRef]): Map[String, AnyRef] = {
    val prefix = part.joinPartOps.columnPrefix
    logThrottled(ERROR, s"join_part_response_failure_${prefix}_${ex.getClass.getName}", s"Failed to fetch $key", ex)
    result.updated(part.prefixedFeatureName("exception"), ex.toString)
  }

  private def groupByResponse(
      key: (String, Map[String, AnyRef], Option[Long]),
      responseMap: Map[(String, Map[String, AnyRef], Option[Long]), Try[Map[String, AnyRef]]]
  ): Try[Map[String, AnyRef]] = {
    // Group bys with all null keys won't be requested from the KV store and we don't expect a response.
    val isRequiredRequest = key._2.values.exists(_ != null) || key._2.isEmpty

    responseMap.get(key) match {
      case Some(value) => value
      case None =>
        if (isRequiredRequest)
          Failure(new IllegalStateException(s"Couldn't find a groupBy response for $key in response map"))
        else Success(null)
    }
  }

  def parseGroupByResponse(
      prefix: String,
      key: (String, Map[String, AnyRef], Option[Long]),
      responseMap: Map[(String, Map[String, AnyRef], Option[Long]), Try[Map[String, AnyRef]]]
  ): Map[String, AnyRef] = {
    groupByResponse(key, responseMap)
      .map { valueMap =>
        if (valueMap != null) {
          valueMap.map { case (aggName, aggValue) => prefix + aggName -> aggValue }
        } else {
          Map.empty[String, AnyRef]
        }
      }
      .recover { case ex: Throwable =>
        logThrottled(ERROR, s"join_part_response_failure_${prefix}_${ex.getClass.getName}", s"Failed to fetch $key", ex)
        Map(prefix + "exception" -> ex.toString)
      }
      .get
  }
}
