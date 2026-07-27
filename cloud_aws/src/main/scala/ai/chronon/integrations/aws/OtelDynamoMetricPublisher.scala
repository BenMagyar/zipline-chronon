package ai.chronon.integrations.aws

import ai.chronon.online.metrics.Metrics
import software.amazon.awssdk.core.metrics.CoreMetric
import software.amazon.awssdk.http.HttpMetric
import software.amazon.awssdk.metrics.{MetricCollection, MetricPublisher, SdkMetric}

import scala.jdk.CollectionConverters._

/** Bridges AWS SDK v2 per-request metrics into Chronon's OTel metrics pipeline.
  *
  * The SDK calls publish() once per API call with a three-level collection tree:
  *   ApiCall → ApiCallAttempt (one per retry) → HttpClient (Netty pool snapshot)
  *
  * publish() is called on the Netty event loop thread, so all forwarding is done
  * inline — Metrics.Context calls are non-blocking in-memory OTel increments.
  */
class OtelDynamoMetricPublisher(
    metricsContext: Metrics.Context = Metrics.Context(Metrics.Environment.KVStore).withSuffix("dynamodb.sdk")
) extends MetricPublisher {

  override def publish(collection: MetricCollection): Unit = {
    val operation = firstValue(collection, CoreMetric.OPERATION_NAME).getOrElse("unknown")
    val opTag = Map("operation" -> operation)
    val success = firstValue(collection, CoreMetric.API_CALL_SUCCESSFUL).getOrElse(false)
    val callTag = opTag + ("success" -> success.toString)

    firstValue(collection, CoreMetric.API_CALL_DURATION).foreach { d =>
      metricsContext.distribution("api_call_duration_ms", d.toMillis, callTag)
    }
    firstValue(collection, CoreMetric.RETRY_COUNT).foreach { r =>
      metricsContext.distribution("retry_count", r.toLong, opTag)
    }
    firstValue(collection, CoreMetric.CREDENTIALS_FETCH_DURATION).foreach { d =>
      metricsContext.distribution("credentials_fetch_duration_ms", d.toMillis, opTag)
    }

    collection.childrenWithName("ApiCallAttempt").iterator().asScala.foreach { attempt =>
      firstValue(attempt, CoreMetric.SERVICE_CALL_DURATION).foreach { d =>
        metricsContext.distribution("service_call_duration_ms", d.toMillis, opTag)
      }
      firstValue(attempt, CoreMetric.SIGNING_DURATION).foreach { d =>
        metricsContext.distribution("signing_duration_ms", d.toMillis, opTag)
      }
      firstValue(attempt, CoreMetric.BACKOFF_DELAY_DURATION).foreach { d =>
        metricsContext.distribution("backoff_delay_ms", d.toMillis, opTag)
      }
      firstValue(attempt, CoreMetric.MARSHALLING_DURATION).foreach { d =>
        metricsContext.distribution("marshalling_duration_ms", d.toMillis, opTag)
      }
      firstValue(attempt, CoreMetric.ERROR_TYPE).foreach { errType =>
        metricsContext.increment("error", opTag + ("error_type" -> errType))
      }

      attempt.childrenWithName("HttpClient").iterator().asScala.foreach { http =>
        firstValue(http, HttpMetric.MAX_CONCURRENCY).foreach { v =>
          metricsContext.distribution("http.max_concurrency", v.toLong, opTag)
        }
        firstValue(http, HttpMetric.AVAILABLE_CONCURRENCY).foreach { v =>
          metricsContext.distribution("http.available_concurrency", v.toLong, opTag)
        }
        firstValue(http, HttpMetric.LEASED_CONCURRENCY).foreach { v =>
          metricsContext.distribution("http.leased_concurrency", v.toLong, opTag)
        }
        firstValue(http, HttpMetric.PENDING_CONCURRENCY_ACQUIRES).foreach { v =>
          metricsContext.distribution("http.pending_acquires", v.toLong, opTag)
        }
        firstValue(http, HttpMetric.CONCURRENCY_ACQUIRE_DURATION).foreach { d =>
          metricsContext.distribution("http.concurrency_acquire_ms", d.toMillis, opTag)
        }
      }
    }
  }

  override def close(): Unit = ()

  private def firstValue[T](collection: MetricCollection, metric: SdkMetric[T]): Option[T] =
    collection.metricValues(metric).asScala.headOption
}
