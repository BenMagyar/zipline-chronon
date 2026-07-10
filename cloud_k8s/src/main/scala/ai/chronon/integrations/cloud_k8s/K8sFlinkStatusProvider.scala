package ai.chronon.integrations.cloud_k8s

import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.LocalPortForward
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import org.slf4j.{Logger, LoggerFactory}

import java.net.{HttpURLConnection, URL}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Try

/** Cloud-agnostic Flink status provider for Kubernetes-hosted Flink deployments.
  *
  * Supports two modes:
  *  - caller provides an explicit Flink REST base URL (nginx ingress path).
  *  - caller provides namespace + deploymentName; the provider resolves the K8s-internal
  *    service URL directly, falling back to a fabric8 port-forward for local dev.
  */
class K8sFlinkStatusProvider(k8sClient: Option[KubernetesClient] = None) {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val vertxClient = {
    val vertx = Vertx.vertx()
    WebClient.create(vertx)
  }

  // One port-forward per namespace/service; reused across status-check calls.
  // Close-on-replace handles stale tunnels without manual lifecycle management.
  private val portForwardCache = new ConcurrentHashMap[String, LocalPortForward]()

  def getFlinkInternalJobId(flinkUriOpt: Option[String])(implicit
      executionContext: ExecutionContext): Future[Option[String]] =
    flinkUriOpt match {
      case Some(flinkUri) => fetchJobId(flinkUri)
      case None           => Future.successful(None)
    }

  def isFlinkJobHealthy(flinkUriOpt: Option[String])(implicit executionContext: ExecutionContext): Future[Boolean] =
    flinkUriOpt match {
      case Some(flinkUri) => isFlinkJobHealthyAtUrl(flinkUri)
      case None =>
        logger.warn("Flink URI not provided")
        Future.successful(false)
    }

  def isFlinkJobHealthy(namespace: String, deploymentName: String)(implicit ec: ExecutionContext): Future[Boolean] =
    Future(resolveServiceUrl(namespace, deploymentName)).flatMap {
      case Some(url) => isFlinkJobHealthyAtUrl(url)
      case None =>
        logger.warn(s"Could not resolve internal service URL for $deploymentName in $namespace")
        Future.successful(false)
    }

  def getFlinkInternalJobId(namespace: String, deploymentName: String)(implicit
      ec: ExecutionContext): Future[Option[String]] =
    Future(resolveServiceUrl(namespace, deploymentName)).flatMap {
      case Some(url) => fetchJobId(url)
      case None =>
        logger.warn(s"Could not resolve internal service URL for $deploymentName in $namespace")
        Future.successful(None)
    }

  private def isFlinkJobHealthyAtUrl(flinkUri: String)(implicit ec: ExecutionContext): Future[Boolean] =
    fetchJobId(flinkUri).flatMap {
      case Some(jobId) =>
        fetchCheckpointCounts(flinkUri, jobId).map {
          case Some(completedCount) =>
            val healthy = isHealthyBasedOnCheckpoints(completedCount)
            logger.info(s"Job $jobId has $completedCount completed checkpoints, isHealthy: $healthy")
            healthy
          case None =>
            logger.warn(s"Could not retrieve checkpoint counts for job $jobId")
            false
        }
      case None =>
        logger.warn(s"Could not retrieve / parse job ID from $flinkUri")
        Future.successful(false)
    }

  // Try svc.cluster.local DNS first; fall back to port-forward when k8sClient is available.
  private[cloud_k8s] def resolveServiceUrl(namespace: String, deploymentName: String): Option[String] = {
    val service = s"$deploymentName-rest"
    val directUrl = K8sFlinkSubmitter.flinkRestServiceUrl(namespace, deploymentName)
    if (isReachable(directUrl)) return Some(directUrl)
    k8sClient
      .flatMap(c => openPortForward(c, namespace, service))
      .map(pf => s"http://127.0.0.1:${pf.getLocalPort}")
  }

  // Short-timeout TCP probe to avoid blocking on unresolvable cluster-local DNS in local dev.
  private def isReachable(url: String): Boolean =
    Try {
      val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
      conn.setConnectTimeout(K8sFlinkStatusProvider.ReachabilityTimeoutMs.toInt)
      conn.setReadTimeout(K8sFlinkStatusProvider.ReachabilityTimeoutMs.toInt)
      conn.connect()
      conn.disconnect()
      true
    }.getOrElse(false)

  private def openPortForward(k8s: KubernetesClient, namespace: String, service: String): Option[LocalPortForward] = {
    val cacheKey = s"$namespace/$service"
    val existing = portForwardCache.get(cacheKey)
    if (existing != null && existing.isAlive && !existing.errorOccurred()) return Some(existing)
    Try(
      k8s
        .services()
        .inNamespace(namespace)
        .withName(service)
        .portForward(K8sFlinkStatusProvider.FlinkRestPort)).toOption
      .map { fresh =>
        val prev = portForwardCache.put(cacheKey, fresh)
        if (prev != null && prev != fresh) Try(prev.close())
        fresh
      }
  }

  private def fetchJobId(flinkUri: String)(implicit ec: ExecutionContext): Future[Option[String]] =
    fetchJobsResponse(flinkUri).map(_.flatMap(parseJobId))

  private[cloud_k8s] def parseJobId(jobsResponseJson: String): Option[String] =
    Try {
      val jsonBody = new JsonObject(jobsResponseJson)
      val jobsArray = jsonBody.getJsonArray("jobs")
      if (jobsArray != null && jobsArray.size() > 0)
        jobsArray.asScala.headOption.map(_.asInstanceOf[JsonObject].getString("id"))
      else None
    }.toOption.flatten

  private def fetchCheckpointCounts(flinkUri: String, jobId: String)(implicit
      ec: ExecutionContext): Future[Option[Int]] =
    fetchCheckpointsResponse(flinkUri, jobId).map(_.flatMap(parseCheckpointCounts))

  private[cloud_k8s] def parseCheckpointCounts(checkpointResponseJson: String): Option[Int] =
    Try {
      val jsonBody = new JsonObject(checkpointResponseJson)
      val counts = jsonBody.getJsonObject("counts")
      if (counts != null) Some(counts.getInteger("completed", 0).intValue()) else None
    }.toOption.flatten

  private[cloud_k8s] def isHealthyBasedOnCheckpoints(completedCheckpoints: Int): Boolean =
    completedCheckpoints >= 3

  protected def fetchJobsResponse(flinkUri: String): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    val endpoint = s"$flinkUri/jobs"
    vertxClient
      .getAbs(endpoint)
      .putHeader("Content-Type", "application/json")
      .putHeader("Accept", "application/json")
      .timeout(K8sFlinkStatusProvider.RequestTimeoutMs)
      .send()
      .onComplete { ar =>
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() == 200) {
            val body = response.bodyAsString()
            logger.debug(s"GET $endpoint -> ${response.statusCode()}: $body")
            promise.success(Some(body))
          } else {
            logger.warn(
              s"Failed to retrieve Flink jobs from $endpoint, status: ${response.statusCode()} ${response.statusMessage()}")
            promise.success(None)
          }
        } else {
          logger.error(s"Error retrieving Flink jobs from $endpoint", ar.cause())
          promise.success(None)
        }
      }
    promise.future
  }

  protected def fetchCheckpointsResponse(flinkUri: String, jobId: String): Future[Option[String]] = {
    val promise = Promise[Option[String]]()
    val endpoint = s"$flinkUri/jobs/$jobId/checkpoints"
    vertxClient
      .getAbs(endpoint)
      .putHeader("Content-Type", "application/json")
      .putHeader("Accept", "application/json")
      .timeout(K8sFlinkStatusProvider.RequestTimeoutMs)
      .send()
      .onComplete { ar =>
        if (ar.succeeded()) {
          val response = ar.result()
          if (response.statusCode() == 200) {
            val body = response.bodyAsString()
            logger.debug(s"GET $endpoint -> ${response.statusCode()}: $body")
            promise.success(Some(body))
          } else {
            logger.warn(
              s"Failed to retrieve checkpoints from $endpoint, status: ${response.statusCode()} ${response.statusMessage()}")
            promise.success(None)
          }
        } else {
          logger.error(s"Error retrieving checkpoints from $endpoint", ar.cause())
          promise.success(None)
        }
      }
    promise.future
  }
}

object K8sFlinkStatusProvider {
  // Per-request HTTP timeout. Keeps individual health-check calls from hanging indefinitely
  // when the Flink REST endpoint is slow or unreachable.
  val RequestTimeoutMs: Long = 10000L

  val FlinkRestPort: Int = 8081

  // Short timeout for the reachability probe so local-dev startup isn't delayed by DNS failures.
  val ReachabilityTimeoutMs: Long = 1000L
}
