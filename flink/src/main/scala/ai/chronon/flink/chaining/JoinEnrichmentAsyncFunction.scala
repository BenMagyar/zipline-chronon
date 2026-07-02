package ai.chronon.flink.chaining

import ai.chronon.flink.{DirectExecutionContext, ERROR, FlinkLogging, INFO, WARN}
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.Api
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/** Async function for performing join enrichment on streaming data.
  *
  * This function takes ProjectedEvent objects (from the left source after query application)
  * and enriches them with features from upstream joins, producing enriched ProjectedEvent objects
  * that contain both original fields and joined features.
  *
  * @param joinRequestName The name of the join to fetch (format: "joins/join_name")
  * @param api API implementation for fetcher access
  * @param enableDebug Whether to enable debug logging
  */
class JoinEnrichmentAsyncFunction(joinRequestName: String, groupByName: String, api: Api, enableDebug: Boolean)
    extends RichAsyncFunction[ProjectedEvent, ProjectedEvent]
    with FlinkLogging {
  @transient private var fetcher: Fetcher = _
  @transient private var successCounter: Counter = _
  @transient private var errorCounter: Counter = _
  @transient private var notFoundCounter: Counter = _
  @transient private var joinFetchLatencyHistogram: Histogram = _

  // The context used for the future callbacks
  implicit lazy val ec: ExecutionContext = JoinEnrichmentAsyncFunction.ExecutionContextInstance

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    log(INFO, "Initializing Fetcher for JoinEnrichmentAsyncFunction")
    fetcher = api.buildFetcher(debug = enableDebug)

    val group = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("group_by", groupByName)
      .addGroup("join_enrichment", joinRequestName)

    successCounter = group.counter("join_fetch.successes")
    errorCounter = group.counter("join_fetch.errors")
    notFoundCounter = group.counter("join_fetch.not_found")
    joinFetchLatencyHistogram = group.histogram(
      "join_fetch_latency",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new com.codahale.metrics.ExponentiallyDecayingReservoir())
      )
    )
    log(INFO, s"JoinEnrichmentAsyncFunction initialized for join: $joinRequestName")
  }

  override def asyncInvoke(event: ProjectedEvent, resultFuture: ResultFuture[ProjectedEvent]): Unit = {
    // Pass all left source field names to match Spark JoinSourceRunner approach
    val scalaKeyMap: Map[String, AnyRef] = event.fields.map { case (k, v) => k -> v.asInstanceOf[AnyRef] }.toMap
    // Create join request
    val request = Fetcher.Request(joinRequestName, scalaKeyMap)

    if (enableDebug) {
      log(INFO, s"Join request: ${request.keys}, ts: ${request.atMillis}")
    }

    // Start latency measurement
    val startTime = System.currentTimeMillis()

    // Perform async join fetch
    val future = fetcher.fetchJoin(Seq(request))

    future.onComplete {
      case Success(responses) =>
        // Record latency and increment success counter
        joinFetchLatencyHistogram.update(System.currentTimeMillis() - startTime)
        successCounter.inc()

        if (responses.nonEmpty) {
          val response = responses.head
          val responseMap = response.values.getOrElse(Map.empty[String, Any])
          val enrichedFields = event.fields ++ responseMap

          if (enableDebug) {
            log(INFO,
                s"Join response: request=${response.request.keys}, " +
                  s"ts=${response.request.atMillis}, values=${response.values}")
          }

          val enrichedEvent = ProjectedEvent(
            enrichedFields,
            event.startProcessingTimeMillis
          )
          resultFuture.complete(java.util.Collections.singleton(enrichedEvent))
        } else {
          // No join response, swallow the event and increment not found counter
          notFoundCounter.inc()
          resultFuture.complete(java.util.Collections.emptyList())
        }

      case Failure(ex) =>
        // Record latency and increment error counter
        joinFetchLatencyHistogram.update(System.currentTimeMillis() - startTime)
        errorCounter.inc()

        logThrottled(ERROR, "join_fetch_error", "Error fetching join data", ex)
        // we swallow the event on error as there might be downstream join source queries dependent on the
        // enrichment fields
        resultFuture.complete(java.util.Collections.emptyList())
    }
  }

  override def timeout(event: ProjectedEvent, resultFuture: ResultFuture[ProjectedEvent]): Unit = {
    // Increment error counter for timeout
    errorCounter.inc()

    logThrottled(WARN, "join_fetch_timeout", s"Join enrichment timeout for event: ${event.fields}")
    // we swallow the event on error as there might be downstream join source queries dependent on the
    // enrichment fields
    resultFuture.complete(java.util.Collections.emptyList())
  }
}

object JoinEnrichmentAsyncFunction {
  private val ExecutionContextInstance: ExecutionContext = new DirectExecutionContext
}
