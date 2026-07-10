package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.integrations.cloud_k8s.K8sFlinkSubmitter
import ai.chronon.spark.submission.JobSubmitterConstants.MaxRetainedCheckpoints
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.Instant

class EksFlinkSubmitterTest extends AnyFlatSpec {

  private val submitter: K8sFlinkSubmitter = EksFlinkSubmitter()

  private val checkpointUri = "s3://zipline-warehouse/flink-checkpoints"

  private def config(jobProperties: Map[String, String] = Map.empty): Map[String, String] =
    submitter.buildFlinkConfiguration(
      flinkCheckpointUri = checkpointUri,
      jobProperties      = jobProperties
    )

  "buildFlinkConfiguration" should "set checkpoint and backend defaults" in {
    val cfg = config()
    assertEquals(checkpointUri, cfg("state.savepoints.dir"))
    assertEquals(checkpointUri, cfg("state.checkpoints.dir"))
    assertEquals("rocksdb", cfg("state.backend.type"))
    assertEquals("true", cfg("state.backend.incremental"))
    assertEquals("filesystem", cfg("state.checkpoint-storage"))
    assertEquals("/tmp/flink-state", cfg("state.backend.rocksdb.localdir"))
    assertEquals(MaxRetainedCheckpoints, cfg("state.checkpoints.num-retained"))
  }

  it should "set IRSA credential provider and clear explicit keys" in {
    val cfg = config()
    assertEquals("", cfg("s3.access.key"))
    assertEquals("", cfg("s3.secret.key"))
    assertEquals("com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
      cfg("fs.s3a.aws.credentials.provider"))
  }

  it should "not set pipeline.jars — jars are on the classpath via FLINK_CLASSPATH env var" in {
    val cfg = config()
    assertFalse(cfg.contains("pipeline.jars"))
  }

  it should "default to 16G tier when no TM memory is specified" in {
    val cfg = config()
    assertEquals("16G", cfg("taskmanager.memory.process.size"))
    assertEquals("1", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("4G", cfg("jobmanager.memory.process.size"))
    assertEquals("256m", cfg("taskmanager.memory.network.min"))
    assertEquals("512m", cfg("taskmanager.memory.network.max"))
    assertEquals("0.5f", cfg("taskmanager.memory.managed.fraction"))
    assertEquals("512m", cfg("taskmanager.memory.jvm-metaspace.size"))
    assertEquals("256m", cfg("taskmanager.memory.task.off-heap.size"))
  }

  it should "apply 16G tier settings when explicitly specified" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "16G"))
    assertEquals("16G", cfg("taskmanager.memory.process.size"))
    assertEquals("1", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("256m", cfg("taskmanager.memory.network.min"))
    assertEquals("512m", cfg("taskmanager.memory.network.max"))
    assertEquals("0.5f", cfg("taskmanager.memory.managed.fraction"))
    assertEquals("512m", cfg("taskmanager.memory.jvm-metaspace.size"))
    assertEquals("256m", cfg("taskmanager.memory.task.off-heap.size"))
  }

  it should "use SmallTaskManager defaults for sub-16G TM memory (e.g. 8G)" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "8G"))
    assertEquals("1", cfg("taskmanager.numberOfTaskSlots"))
    // jobProperties wins — 8G flows through untouched
    assertEquals("8G", cfg("taskmanager.memory.process.size"))
    assertFalse(cfg.contains("taskmanager.memory.network.min"))
  }

  it should "apply 32G tier settings" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "32G"))
    assertEquals("32G", cfg("taskmanager.memory.process.size"))
    assertEquals("2", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("512m", cfg("taskmanager.memory.network.min"))
    assertEquals("1G", cfg("taskmanager.memory.network.max"))
    assertEquals("0.5f", cfg("taskmanager.memory.managed.fraction"))
    assertEquals("512m", cfg("taskmanager.memory.jvm-metaspace.size"))
    assertEquals("512m", cfg("taskmanager.memory.task.off-heap.size"))
  }

  it should "apply 64G tier settings" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "64G"))
    assertEquals("64G", cfg("taskmanager.memory.process.size"))
    assertEquals("4", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("1G", cfg("taskmanager.memory.network.min"))
    assertEquals("2G", cfg("taskmanager.memory.network.max"))
    assertEquals("0.5f", cfg("taskmanager.memory.managed.fraction"))
    assertEquals("512m", cfg("taskmanager.memory.jvm-metaspace.size"))
    assertEquals("1G", cfg("taskmanager.memory.task.off-heap.size"))
  }

  it should "allow jobProperties to override JM memory" in {
    val cfg = config(Map("jobmanager.memory.process.size" -> "2G"))
    assertEquals("2G", cfg("jobmanager.memory.process.size"))
  }

  it should "allow jobProperties to override any default config key" in {
    val cfg = config(Map("state.checkpoints.num-retained" -> "5"))
    assertEquals("5", cfg("state.checkpoints.num-retained"))
  }

  "sanitizeDeploymentName" should "lowercase and replace invalid chars with dashes" in {
    assertEquals("flink-my-job-123", K8sFlinkSubmitter.sanitizeDeploymentName("flink-My_Job.123"))
  }

  it should "truncate to 45 characters" in {
    val long = "flink-" + "a" * 50
    val result = K8sFlinkSubmitter.sanitizeDeploymentName(long)
    assertTrue(s"Expected <= 45 chars, got ${result.length}", result.length <= 45)
  }

  it should "strip trailing dashes after truncation" in {
    // Construct a name whose 45th character lands on a dash
    val name = "a" * 44 + "-extra"
    val result = K8sFlinkSubmitter.sanitizeDeploymentName(name)
    assertFalse(s"Expected no trailing dash, got: $result", result.endsWith("-"))
  }

  it should "strip leading and trailing dashes" in {
    assertEquals("foo-bar", K8sFlinkSubmitter.sanitizeDeploymentName("--foo-bar--"))
  }

  "parsePodTemplateLabels" should "parse comma-separated key=value pairs" in {
    val labels = EksFlinkSubmitter.parsePodTemplateLabels(Some("team=ml,tier=streaming,env=prod"))
    assertEquals("ml", labels("team"))
    assertEquals("streaming", labels("tier"))
    assertEquals("prod", labels("env"))
  }

  it should "keep keys containing dots and slashes" in {
    val labels = EksFlinkSubmitter.parsePodTemplateLabels(Some("app.kubernetes.io/component=jobmanager"))
    assertEquals("jobmanager", labels("app.kubernetes.io/component"))
  }

  it should "return an empty map when the env var is unset or blank" in {
    assertTrue(EksFlinkSubmitter.parsePodTemplateLabels(None).isEmpty)
    assertTrue(EksFlinkSubmitter.parsePodTemplateLabels(Some("   ")).isEmpty)
  }

  it should "skip entries with an empty key or value and trim whitespace" in {
    val labels = EksFlinkSubmitter.parsePodTemplateLabels(Some("=novalue, nokey , tier = streaming ,valid=ok"))
    assertEquals(Map("tier" -> "streaming", "valid" -> "ok"), labels)
  }

  it should "split on the first '=' so values may contain '='" in {
    val labels = EksFlinkSubmitter.parsePodTemplateLabels(Some("key=val=extra"))
    assertEquals("val=extra", labels("key"))
  }

  "eksAdditionalFlinkJars" should "resolve jar names against the base path" in {
    val jars = EksFlinkSubmitter.eksAdditionalFlinkJars("s3://my-bucket/libs/")
    EksFlinkSubmitter.EksOnlyAdditionalJarNames.foreach { name =>
      assertTrue(s"Expected $name in jars", jars.contains(s"s3://my-bucket/libs/$name"))
    }
  }

  it should "append a trailing slash to the base path if missing" in {
    val withSlash    = EksFlinkSubmitter.eksAdditionalFlinkJars("s3://my-bucket/libs/")
    val withoutSlash = EksFlinkSubmitter.eksAdditionalFlinkJars("s3://my-bucket/libs")
    assertEquals(withSlash.toList, withoutSlash.toList)
  }

  it should "use DefaultS3FlinkJarsBasePath as the fallback base" in {
    val base = EksFlinkSubmitter.DefaultS3FlinkJarsBasePath
    val jars = EksFlinkSubmitter.eksAdditionalFlinkJars(base)
    EksFlinkSubmitter.EksOnlyAdditionalJarNames.foreach { name =>
      assertTrue(jars.exists(_.endsWith(name)))
    }
  }

  "EksFlinkSubmitter() with ingressBaseUrl" should "not set web.base-url in flinkConfiguration" in {
    val submitterWithIngress = EksFlinkSubmitter(ingressBaseUrl = Some("https://hub.example.com"))
    val cfg = submitterWithIngress.buildFlinkConfiguration(checkpointUri, Map.empty)
    assertFalse("web.base-url is not honoured by the EMR Flink image and must not be set", cfg.contains("web.base-url"))
  }

  "EmrSubmitter.getFlinkUrl" should "return ingress URL for flink job IDs when ingressBaseUrl is set" in {
    val emr = new EmrSubmitter(
      customerId = "test",
      emrClient = null,
      ec2Client = null,
      ingressBaseUrl = Some("http://localhost:3000")
    )
    assertEquals(Some("http://localhost:3000/flink/my-deployment/"),
      emr.getFlinkUrl("flink:zipline-flink:my-deployment"))
  }

  it should "return None when ingressBaseUrl is not set" in {
    val emr = new EmrSubmitter(customerId = "test", emrClient = null, ec2Client = null)
    assertEquals(None, emr.getFlinkUrl("flink:zipline-flink:my-deployment"))
  }

  it should "return None for non-flink job IDs" in {
    val emr = new EmrSubmitter(
      customerId = "test",
      emrClient = null,
      ec2Client = null,
      ingressBaseUrl = Some("http://localhost:3000")
    )
    assertEquals(None, emr.getFlinkUrl("j-ABC123:s-XYZ"))
  }

  it should "normalize a trailing slash in ingressBaseUrl" in {
    val emr = new EmrSubmitter(
      customerId = "test",
      emrClient = null,
      ec2Client = null,
      ingressBaseUrl = Some("http://localhost:3000/")
    )
    assertEquals(Some("http://localhost:3000/flink/my-deployment/"),
      emr.getFlinkUrl("flink:zipline-flink:my-deployment"))
  }

  it should "return services proxy URL when Flink UI proxy mode is enabled" in {
    val emr = new EmrSubmitter(
      customerId = "test",
      emrClient = null,
      ec2Client = null,
      ingressBaseUrl = Some("https://hub.example.com/services/hub"),
      flinkUiProxyEnabled = true
    )
    assertEquals(Some("https://hub.example.com/services/hub/engines/flink/job/my-deployment"),
      emr.getFlinkUrl("flink:zipline-flink:my-deployment"))
  }

  "resolveStatus" should "return RUNNING when lifecycleState is STABLE" in {
    assertEquals(JobStatusType.RUNNING,
      submitter.resolveStatus("d", "STABLE", "READY", Some(Instant.now())))
  }

  it should "return FAILED when lifecycleState is FAILED" in {
    assertEquals(JobStatusType.FAILED,
      submitter.resolveStatus("d", "FAILED", "MISSING", Some(Instant.now())))
  }

  it should "return FAILED when lifecycleState is SUSPENDED" in {
    assertEquals(JobStatusType.FAILED,
      submitter.resolveStatus("d", "SUSPENDED", "READY", Some(Instant.now())))
  }

  it should "return UNKNOWN for an unrecognised lifecycleState" in {
    assertEquals(JobStatusType.UNKNOWN,
      submitter.resolveStatus("d", "SOMETHING_NEW", "READY", Some(Instant.now())))
  }

  // --- DEPLOYED/UPGRADING family ---

  it should "return PENDING when DEPLOYED with READY jmStatus and within timeout" in {
    val recentTs = Some(Instant.now())
    assertEquals(JobStatusType.PENDING,
      submitter.resolveStatus("d", "DEPLOYED", "READY", recentTs))
  }

  it should "return PENDING when UPGRADING with READY jmStatus and within timeout" in {
    val recentTs = Some(Instant.now())
    assertEquals(JobStatusType.PENDING,
      submitter.resolveStatus("d", "UPGRADING", "READY", recentTs))
  }

  it should "return FAILED immediately when jmDeploymentStatus is ERROR regardless of age" in {
    // Brand new deployment — should still fail immediately on ERROR
    val recentTs = Some(Instant.now())
    assertEquals(JobStatusType.FAILED,
      submitter.resolveStatus("d", "DEPLOYED", "ERROR", recentTs))
  }

  it should "return PENDING when jmDeploymentStatus is MISSING and deployment is new" in {
    // MISSING is normal immediately after creation — must not be treated as failure yet
    val recentTs = Some(Instant.now())
    assertEquals(JobStatusType.PENDING,
      submitter.resolveStatus("d", "DEPLOYED", "MISSING", recentTs))
  }

  it should "return FAILED when jmDeploymentStatus is MISSING and deployment has timed out" in {
    val oldTs = Some(Instant.now().minusSeconds(K8sFlinkSubmitter.DeploymentPendingTimeout.toSeconds + 60))
    assertEquals(JobStatusType.FAILED,
      submitter.resolveStatus("d", "DEPLOYED", "MISSING", oldTs))
  }

  it should "return FAILED when DEPLOYED with READY jmStatus but deployment has timed out" in {
    // Covers the unschedulable TM scenario — JM is up (READY) but TMs never join
    val oldTs = Some(Instant.now().minusSeconds(K8sFlinkSubmitter.DeploymentPendingTimeout.toSeconds + 60))
    assertEquals(JobStatusType.FAILED,
      submitter.resolveStatus("d", "DEPLOYED", "READY", oldTs))
  }

  it should "return FAILED when UPGRADING with READY jmStatus but deployment has timed out" in {
    val oldTs = Some(Instant.now().minusSeconds(K8sFlinkSubmitter.DeploymentPendingTimeout.toSeconds + 60))
    assertEquals(JobStatusType.FAILED,
      submitter.resolveStatus("d", "UPGRADING", "READY", oldTs))
  }

  it should "return PENDING when no creationTimestamp is available" in {
    // No timestamp — cannot determine timeout, conservatively stay PENDING
    assertEquals(JobStatusType.PENDING,
      submitter.resolveStatus("d", "DEPLOYED", "READY", None))
  }

  "createFlinkIngress" should "throw IllegalArgumentException when host cannot be extracted" in {
    // "not-a-valid-url" parses as a relative URI, so getHost returns null — triggering the fast-fail
    val submitterWithBadUrl = EksFlinkSubmitter(ingressBaseUrl = Some("not-a-valid-url"))
    assertThrows[IllegalArgumentException] {
      submitterWithBadUrl.createFlinkIngress(null, "my-deployment", "default", "some-uid")
    }
  }

  "statusWithCreationTime contract" should "agree with resolveStatus for STABLE state" in {
    val ts = Some(Instant.now())
    val (status, _) = (submitter.resolveStatus("d", "STABLE", "READY", ts), ts)
    assertEquals(JobStatusType.RUNNING, status)
  }

  it should "agree with resolveStatus for FAILED state" in {
    val ts = Some(Instant.now())
    val status = submitter.resolveStatus("d", "FAILED", "READY", ts)
    assertEquals(JobStatusType.FAILED, status)
  }

  it should "agree with resolveStatus: DEPLOYED within timeout stays PENDING" in {
    val ts = Some(Instant.now())
    assertEquals(JobStatusType.PENDING, submitter.resolveStatus("d", "DEPLOYED", "MISSING", ts))
  }

  it should "agree with resolveStatus: DEPLOYED past timeout becomes FAILED" in {
    val oldTs = Some(Instant.now().minusSeconds(K8sFlinkSubmitter.DeploymentPendingTimeout.toSeconds + 60))
    assertEquals(JobStatusType.FAILED, submitter.resolveStatus("d", "DEPLOYED", "MISSING", oldTs))
  }

  it should "return RUNNING status for STABLE deployment (the state where creation time matters)" in {
    val ts = Some(Instant.now().minusSeconds(30))
    val status = submitter.resolveStatus("my-dep", "STABLE", "READY", ts)
    assertEquals(JobStatusType.RUNNING, status)
  }
}
