package ai.chronon.flink.chaining

import ai.chronon.api.Constants
import ai.chronon.flink.DirectExecutionContext
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.{Api, ERROR, INFO, ThrottledLogging, WARN}
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Invokes [[Fetcher.fetchModelTransforms]] and merges model outputs into the event. On error /
  * timeout / not-found the event is swallowed (empty result) so a bad row can't poison the downstream
  * aggregation pipeline.
  *
  * `maybeModelTransformsConf = None` (production) makes the Fetcher resolve the conf by name from
  * `MetadataStore.getModelTransformsConf` — TTL-cached, refreshed from the `models/<name>` KV entry,
  * mirroring how [[JoinEnrichmentAsyncFunction]] resolves join confs. `Some(conf)` skips the lookup;
  * integration tests use it to avoid seeding a metadata store.
  */
class ModelTransformsAsyncFunction(modelTransformsName: String,
                                   maybeModelTransformsConf: Option[ai.chronon.api.ModelTransforms],
                                   groupByName: String,
                                   api: Api,
                                   enableDebug: Boolean)
    extends RichAsyncFunction[ProjectedEvent, ProjectedEvent]
    with ThrottledLogging {
  @transient private var fetcher: Fetcher = _
  @transient private var successCounter: Counter = _
  @transient private var errorCounter: Counter = _
  @transient private var notFoundCounter: Counter = _
  @transient private var modelFetchLatencyHistogram: Histogram = _

  implicit lazy val ec: ExecutionContext = ModelTransformsAsyncFunction.ExecutionContextInstance

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    log(INFO, s"Initializing Fetcher for ModelTransformsAsyncFunction: $modelTransformsName")

    // Fail fast rather than surfacing a confusing NPE deep inside a model inference call.
    require(
      api.generateModelPlatformProvider != null,
      s"Api implementation ${api.getClass.getSimpleName} returned a null ModelPlatformProvider from " +
        s"generateModelPlatformProvider. ModelTransforms streaming requires a model platform (e.g. Majordomo) " +
        s"to be configured for the Api used at Flink deploy time."
    )

    fetcher = api.buildFetcher(debug = enableDebug)

    val group = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("group_by", groupByName)
      .addGroup("model_transforms", modelTransformsName)

    successCounter = group.counter("model_fetch.successes")
    errorCounter = group.counter("model_fetch.errors")
    notFoundCounter = group.counter("model_fetch.not_found")
    modelFetchLatencyHistogram = group.histogram(
      "model_fetch_latency",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new com.codahale.metrics.ExponentiallyDecayingReservoir())
      )
    )
    val confSource = if (maybeModelTransformsConf.isDefined) "explicit" else "metadata store (by name)"
    log(INFO,
        s"ModelTransformsAsyncFunction initialized for model transforms: $modelTransformsName " +
          s"(conf source: $confSource)")
  }

  override def asyncInvoke(event: ProjectedEvent, resultFuture: ResultFuture[ProjectedEvent]): Unit = {
    val keys: Map[String, AnyRef] = event.fields.map { case (k, v) => k -> v.asInstanceOf[AnyRef] }.toMap
    // SparkExprEval always emits Constants.TimeColumn; a missing or null value is a poison row —
    // dropping avoids NPE / garbage-window assignment in the downstream tile process function.
    val atMillis: Option[Long] = event.fields.get(Constants.TimeColumn) match {
      case Some(v: Number) => Some(v.longValue)
      case _               => None
    }

    if (atMillis.isEmpty) {
      notFoundCounter.inc()
      logThrottled(WARN,
                   "model_fetch_missing_ts",
                   s"Skipping event with missing/null ${Constants.TimeColumn} for $modelTransformsName")
      resultFuture.complete(java.util.Collections.emptyList())
      return
    }

    val request = Fetcher.Request(modelTransformsName, keys, atMillis)

    if (enableDebug) {
      log(INFO, s"Model transforms request: ${request.keys}, ts: ${request.atMillis}")
    }

    val startTime = System.currentTimeMillis()

    val future = fetcher.fetchModelTransforms(Seq(request), maybeModelTransformsConf)

    future.onComplete {
      case Success(responses) if responses.nonEmpty && responses.head.values.isSuccess =>
        modelFetchLatencyHistogram.update(System.currentTimeMillis() - startTime)
        successCounter.inc()

        val responseMap = responses.head.values.get

        // Per-model failures come back as <modelName>_exception entries (see ModelTransformsFetcher).
        // Downstream aggregator drops them since they aren't in postTransformationSchema; log them here
        // so oncall isn't blind to a model returning errors on every row.
        val exceptionKeys = responseMap.keys.filter(_.endsWith("_exception"))
        if (exceptionKeys.nonEmpty) {
          logThrottled(
            WARN,
            "model_fetch_partial_error",
            s"Model transforms response for $modelTransformsName contained error entries: " +
              exceptionKeys.map(k => s"$k=${responseMap(k)}").mkString(", ")
          )
        }

        val mergedFields = event.fields ++ responseMap

        if (enableDebug) {
          log(INFO, s"Model transforms response: request=${responses.head.request.keys}, values=$responseMap")
        }

        resultFuture.complete(
          java.util.Collections.singleton(ProjectedEvent(mergedFields, event.startProcessingTimeMillis)))

      case Success(_) =>
        // empty response or failed values Try - swallow the event
        notFoundCounter.inc()
        resultFuture.complete(java.util.Collections.emptyList())

      case Failure(ex) =>
        modelFetchLatencyHistogram.update(System.currentTimeMillis() - startTime)
        errorCounter.inc()
        logThrottled(ERROR, "model_fetch_error", s"Error fetching model transforms data for $modelTransformsName", ex)
        resultFuture.complete(java.util.Collections.emptyList())
    }
  }

  override def timeout(event: ProjectedEvent, resultFuture: ResultFuture[ProjectedEvent]): Unit = {
    errorCounter.inc()
    // event.fields may contain PII / embedding vectors; MT + groupBy names are enough to correlate.
    logThrottled(WARN, "model_fetch_timeout", s"Model transforms fetch timeout for $modelTransformsName ($groupByName)")
    resultFuture.complete(java.util.Collections.emptyList())
  }
}

object ModelTransformsAsyncFunction {
  private val ExecutionContextInstance: ExecutionContext = new DirectExecutionContext
}
