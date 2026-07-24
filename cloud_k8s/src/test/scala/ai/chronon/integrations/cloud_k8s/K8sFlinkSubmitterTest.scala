package ai.chronon.integrations.cloud_k8s

import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants.MaxRetainedCheckpoints
import K8sFlinkSubmitter.{
  DeploymentPendingTimeout,
  InitContainerSpec
}
import org.junit.Assert.{assertEquals, assertFalse, assertNull, assertTrue}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.Instant
import java.util.Collections
import scala.jdk.CollectionConverters._

class K8sFlinkSubmitterTest extends AnyFlatSpec {

  private val checkpointUri = "gs://zipline-warehouse/flink-checkpoints"

  // Minimal no-op init container spec builder for tests that don't exercise the init container
  private val noopInitContainerSpec: Array[String] => InitContainerSpec = _ =>
    InitContainerSpec(
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.emptyList(),
      Collections.emptyList()
    )

  private def submitterWithExtra(extraConfig: Map[String, String] = Map.empty,
                                  extraJars: Array[String] = Array.empty,
                                  podLabels: Map[String, String] = Map.empty): K8sFlinkSubmitter =
    new K8sFlinkSubmitter(
      flinkImage = "test-image:latest",
      buildInitContainerSpec = noopInitContainerSpec,
      extraFlinkConfig = extraConfig,
      extraJarNames = extraJars,
      defaultJarsBasePath = "gs://test-bucket/libs/",
      podTemplateLabels = podLabels
    )

  private def config(jobProperties: Map[String, String] = Map.empty,
                     extraConfig: Map[String, String] = Map.empty): Map[String, String] =
    submitterWithExtra(extraConfig).buildFlinkConfiguration(
      flinkCheckpointUri = checkpointUri,
      jobProperties = jobProperties
    )

  // --- buildFlinkConfiguration: base defaults ---

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

  it should "not set pipeline.jars — jars are on the classpath via FLINK_CLASSPATH env var" in {
    assertFalse(config().contains("pipeline.jars"))
  }

  it should "not include cloud-specific keys by default (no extraFlinkConfig)" in {
    val cfg = config()
    assertFalse(cfg.contains("s3.access.key"))
    assertFalse(cfg.contains("fs.s3a.aws.credentials.provider"))
  }

  // --- buildFlinkConfiguration: extraFlinkConfig injection ---

  it should "include keys from extraFlinkConfig" in {
    val cfg = config(extraConfig = Map("custom.key" -> "custom.val"))
    assertEquals("custom.val", cfg("custom.key"))
  }

  it should "allow jobProperties to override extraFlinkConfig" in {
    val cfg = config(
      jobProperties = Map("state.backend.type" -> "hashmap"),
      extraConfig = Map("state.backend.type" -> "rocksdb")
    )
    assertEquals("hashmap", cfg("state.backend.type"))
  }

  it should "allow jobProperties to override base defaults" in {
    val cfg = config(Map("state.checkpoints.num-retained" -> "5"))
    assertEquals("5", cfg("state.checkpoints.num-retained"))
  }

  it should "pass through all jobProperties keys into the Flink configuration" in {
    val cfg = config(Map(
      "custom.flink.key" -> "custom-value",
      "state.checkpoints.num-retained" -> "7"
    ))
    assertEquals("custom-value", cfg("custom.flink.key"))
    assertEquals("7", cfg("state.checkpoints.num-retained"))
  }

  it should "allow jobProperties to override JM memory" in {
    val cfg = config(Map("jobmanager.memory.process.size" -> "2G"))
    assertEquals("2G", cfg("jobmanager.memory.process.size"))
  }

  // --- buildFlinkConfiguration: memory tier dispatch ---

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

  it should "use SmallTaskManager defaults for sub-16G TM memory (e.g. 8G)" in {
    val cfg = config(Map("taskmanager.memory.process.size" -> "8G"))
    assertEquals("1", cfg("taskmanager.numberOfTaskSlots"))
    assertEquals("8G", cfg("taskmanager.memory.process.size"))
    assertFalse(cfg.contains("taskmanager.memory.network.min"))
  }

  // --- sanitizeDeploymentName ---

  "sanitizeDeploymentName" should "lowercase and replace invalid chars with dashes" in {
    assertEquals("flink-my-job-123", K8sFlinkSubmitter.sanitizeDeploymentName("flink-My_Job.123"))
  }

  it should "truncate to 45 characters" in {
    val long = "flink-" + "a" * 50
    val result = K8sFlinkSubmitter.sanitizeDeploymentName(long)
    assertTrue(s"Expected <= 45 chars, got ${result.length}", result.length <= 45)
  }

  it should "strip trailing dashes after truncation" in {
    val name = "a" * 44 + "-extra"
    val result = K8sFlinkSubmitter.sanitizeDeploymentName(name)
    assertFalse(s"Expected no trailing dash, got: $result", result.endsWith("-"))
  }

  it should "strip leading and trailing dashes" in {
    assertEquals("foo-bar", K8sFlinkSubmitter.sanitizeDeploymentName("--foo-bar--"))
  }

  "flinkRestServiceUrl" should "build the internal Kubernetes service URL for the Flink REST service" in {
    assertEquals(
      "http://my-deploy-rest.zipline-flink.svc.cluster.local:8081",
      K8sFlinkSubmitter.flinkRestServiceUrl("zipline-flink", "my-deploy")
    )
  }

  "flinkUiProxyEnabledFromEnv" should "read FLINK_UI_PROXY_ENABLED from the environment map" in {
    assertTrue(
      K8sFlinkSubmitter.flinkUiProxyEnabledFromEnv(Map(K8sFlinkSubmitter.FlinkUiProxyEnabledEnvVar -> " true ")))
    assertTrue(K8sFlinkSubmitter.flinkUiProxyEnabledFromEnv(Map(K8sFlinkSubmitter.FlinkUiProxyEnabledEnvVar -> "1")))
  }

  it should "return false when FLINK_UI_PROXY_ENABLED is unset or false" in {
    assertFalse(K8sFlinkSubmitter.flinkUiProxyEnabledFromEnv(Map.empty))
    assertFalse(K8sFlinkSubmitter.flinkUiProxyEnabledFromEnv(Map(K8sFlinkSubmitter.FlinkUiProxyEnabledEnvVar -> "false")))
  }

  "shouldCreateFlinkIngress" should "return false when Flink UI proxy mode is enabled" in {
    assertFalse(K8sFlinkSubmitter.shouldCreateFlinkIngress(flinkUiProxyEnabled = true))
  }

  it should "return true when Flink UI proxy mode is disabled" in {
    assertTrue(K8sFlinkSubmitter.shouldCreateFlinkIngress(flinkUiProxyEnabled = false))
  }

  "flinkIngressPath" should "use the legacy /flink path when Flink UI proxy mode is disabled" in {
    assertEquals(
      "/flink/my-deploy(/|$)(.*)",
      K8sFlinkSubmitter.flinkIngressPath(flinkUiProxyEnabled = false, "my-deploy")
    )
  }

  "flinkUiUrl" should "return a proxy URL when Flink UI proxy mode is enabled" in {
    assertEquals(
      "https://hub.example.com/services/hub/engines/flink/job/my-deploy",
      K8sFlinkSubmitter.flinkUiUrl("https://hub.example.com/services/hub/", "zipline-flink", "my-deploy", true)
    )
  }

  it should "return the legacy /flink URL when Flink UI proxy mode is disabled" in {
    assertEquals(
      "https://hub.example.com/flink/my-deploy/",
      K8sFlinkSubmitter.flinkUiUrl("https://hub.example.com/", "zipline-flink", "my-deploy", false)
    )
  }

  // --- resolveStatus ---

  private def resolveStatus(submitter: K8sFlinkSubmitter,
                             lifecycle: String,
                             jmStatus: String,
                             ts: Option[Instant]): JobStatusType =
    submitter.resolveStatus("test-deployment", lifecycle, jmStatus, ts)

  private val s = submitterWithExtra()
  private val now = Some(Instant.now())
  private val old = Some(Instant.now().minusSeconds(DeploymentPendingTimeout.toSeconds + 60))

  "resolveStatus" should "return RUNNING when lifecycleState is STABLE" in {
    assertEquals(JobStatusType.RUNNING, resolveStatus(s, "STABLE", "READY", now))
  }

  it should "return FAILED when lifecycleState is FAILED" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "FAILED", "MISSING", now))
  }

  it should "return FAILED when lifecycleState is SUSPENDED" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "SUSPENDED", "READY", now))
  }

  it should "return UNKNOWN for an unrecognised lifecycleState" in {
    assertEquals(JobStatusType.UNKNOWN, resolveStatus(s, "SOMETHING_NEW", "READY", now))
  }

  it should "return PENDING when DEPLOYED with READY jmStatus and within timeout" in {
    assertEquals(JobStatusType.PENDING, resolveStatus(s, "DEPLOYED", "READY", now))
  }

  it should "return PENDING when UPGRADING with READY jmStatus and within timeout" in {
    assertEquals(JobStatusType.PENDING, resolveStatus(s, "UPGRADING", "READY", now))
  }

  it should "return PENDING when jmDeploymentStatus is ERROR and deployment is new (transient pod failure)" in {
    assertEquals(JobStatusType.PENDING, resolveStatus(s, "DEPLOYED", "ERROR", now))
  }

  it should "return FAILED when jmDeploymentStatus is ERROR and deployment has timed out" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "DEPLOYED", "ERROR", old))
  }

  it should "return PENDING when jmDeploymentStatus is MISSING and deployment is new" in {
    assertEquals(JobStatusType.PENDING, resolveStatus(s, "DEPLOYED", "MISSING", now))
  }

  it should "return FAILED when jmDeploymentStatus is MISSING and deployment has timed out" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "DEPLOYED", "MISSING", old))
  }

  it should "return FAILED when DEPLOYED with READY jmStatus but deployment has timed out" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "DEPLOYED", "READY", old))
  }

  it should "return FAILED when UPGRADING with READY jmStatus but deployment has timed out" in {
    assertEquals(JobStatusType.FAILED, resolveStatus(s, "UPGRADING", "READY", old))
  }

  it should "return PENDING when no creationTimestamp is available" in {
    assertEquals(JobStatusType.PENDING, resolveStatus(s, "DEPLOYED", "READY", None))
  }

  // --- buildComponentSpec: podTemplateLabels ---

  private def podMeta(submitter: K8sFlinkSubmitter,
                      labels: Map[String, String] = Map.empty): java.util.Map[String, Object] = {
    // When labels is empty here, buildComponentSpec falls back to its default (the submitter's
    // podTemplateLabels), which lets us test both the constructor-level labels and the
    // per-call override path.
    val componentSpec =
      if (labels.isEmpty) {
        submitter.buildComponentSpec(
          memory = "4G",
          cpu = 1.0,
          replicas = Some(1),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList()
        )
      } else {
        submitter.buildComponentSpec(
          memory = "4G",
          cpu = 1.0,
          replicas = Some(1),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList(),
          labels = labels
        )
      }
    componentSpec.get("podTemplate")
      .asInstanceOf[java.util.Map[String, Object]]
      .get("metadata")
      .asInstanceOf[java.util.Map[String, Object]]
  }

  "buildComponentSpec" should "not include labels in podTemplate metadata when podTemplateLabels is empty" in {
    val meta = podMeta(submitterWithExtra())
    assertNull(meta.get("labels"))
  }

  it should "include labels in podTemplate metadata when podTemplateLabels is set" in {
    val meta = podMeta(submitterWithExtra(podLabels = Map("azure.workload.identity/use" -> "true")))
    val labels = meta.get("labels").asInstanceOf[java.util.Map[String, String]]
    assertEquals("true", labels.get("azure.workload.identity/use"))
  }

  it should "include all provided podTemplateLabels" in {
    val meta = podMeta(submitterWithExtra(podLabels = Map("foo" -> "bar", "baz" -> "qux")))
    val labels = meta.get("labels").asInstanceOf[java.util.Map[String, String]]
    assertEquals("bar", labels.get("foo"))
    assertEquals("qux", labels.get("baz"))
  }

  it should "always include prometheus annotations regardless of podTemplateLabels" in {
    val meta = podMeta(submitterWithExtra(podLabels = Map("some-label" -> "val")))
    val annotations = meta.get("annotations").asInstanceOf[java.util.Map[String, String]]
    assertEquals("true", annotations.get("prometheus.io/scrape"))
  }

  it should "use the labels override when provided, ignoring constructor podTemplateLabels" in {
    val submitter = submitterWithExtra(podLabels = Map("constructor-label" -> "ignored"))
    val meta: java.util.Map[String, Object] = podMeta(submitter, Map("override-label" -> "wins"))
    val labels = meta.get("labels").asInstanceOf[java.util.Map[String, String]]
    assertEquals("wins", labels.get("override-label"))
    assertNull(labels.get("constructor-label"))
  }

  // --- buildEffectivePodLabels ---

  "buildEffectivePodLabels" should "preserve unrelated constructor labels when groupByName is None" in {
    val submitter = submitterWithExtra(podLabels = Map("foo" -> "bar"))
    assertEquals(Map("foo" -> "bar"), submitter.buildEffectivePodLabels(None))
  }

  it should "merge the GroupBy label onto constructor labels when groupByName is provided" in {
    val submitter = submitterWithExtra(podLabels = Map("foo" -> "bar"))
    val labels = submitter.buildEffectivePodLabels(Some("ranking.user.x.v1__2"))
    assertEquals("bar", labels("foo"))
    assertEquals("ranking_user_x_v1__2", labels(K8sFlinkSubmitter.JobNamePodLabel))
  }

  it should "preserve the compile-version suffix in the label value" in {
    val submitter = submitterWithExtra()
    val labels = submitter.buildEffectivePodLabels(Some("a.b.v1__7"))
    assertEquals("a_b_v1__7", labels(K8sFlinkSubmitter.JobNamePodLabel))
  }

  it should "let the GroupBy label win when constructor labels already have the same key" in {
    val submitter = submitterWithExtra(
      podLabels = Map(K8sFlinkSubmitter.JobNamePodLabel -> "stale-value")
    )
    val labels = submitter.buildEffectivePodLabels(Some("fresh.v1"))
    assertEquals("fresh_v1", labels(K8sFlinkSubmitter.JobNamePodLabel))
  }

  it should "merge extraLabels (e.g. branch, zipline-version) onto constructor labels" in {
    val submitter = submitterWithExtra(podLabels = Map("foo" -> "bar"))
    val labels = submitter.buildEffectivePodLabels(None, Map("branch" -> "nikhil-fix", "zipline-version" -> "1.18.0"))
    assertEquals("bar", labels("foo"))
    assertEquals("nikhil-fix", labels("branch"))
    assertEquals("1.18.0", labels("zipline-version"))
  }

  it should "sanitize extraLabels values that contain characters invalid in K8s label values" in {
    val submitter = submitterWithExtra()
    // Branch names commonly look like "nikhil/customer-timeout" — '/' is invalid in a label value.
    val labels = submitter.buildEffectivePodLabels(None, Map("branch" -> "nikhil/customer-timeout"))
    assertEquals("nikhil_customer-timeout", labels("branch"))
  }

  it should "drop extraLabels entries with an empty value instead of throwing" in {
    val submitter = submitterWithExtra()
    val labels = submitter.buildEffectivePodLabels(None, Map("branch" -> "", "user" -> "piyush"))
    assertEquals(Map("user" -> "piyush"), labels)
  }

  it should "let the GroupBy job-name label win over an extraLabels entry with the same key" in {
    val submitter = submitterWithExtra()
    val labels =
      submitter.buildEffectivePodLabels(Some("fresh.v1"), Map(K8sFlinkSubmitter.JobNamePodLabel -> "stale-value"))
    assertEquals("fresh_v1", labels(K8sFlinkSubmitter.JobNamePodLabel))
  }

  // --- sanitizeLabelValue ---

  "sanitizeLabelValue" should "leave an already-strict name unchanged" in {
    assertEquals("a_b_v1__2", K8sFlinkSubmitter.sanitizeLabelValue("a_b_v1__2"))
  }

  it should "replace dots with underscores so the value survives Prometheus filtering" in {
    assertEquals("a_b_v1__2", K8sFlinkSubmitter.sanitizeLabelValue("a.b.v1__2"))
  }

  it should "replace any other disallowed characters with underscore" in {
    assertEquals("a_b_c", K8sFlinkSubmitter.sanitizeLabelValue("a/b-c"))
  }

  it should "strip leading and trailing underscores after sanitization" in {
    assertEquals("a_b", K8sFlinkSubmitter.sanitizeLabelValue("..a.b--"))
  }

  it should "truncate names longer than 63 chars and append a hash for distinguishability" in {
    val long = "a" * 80
    val sanitized = K8sFlinkSubmitter.sanitizeLabelValue(long)
    assertTrue(s"length should be <= 63, got ${sanitized.length}", sanitized.length <= 63)
    assertTrue(s"should contain hash separator, got '$sanitized'", sanitized.contains("_"))
  }

  it should "produce different values for two distinct long names that share a prefix" in {
    val a = "team.entity.subentity." + ("x" * 60) + ".groupbyA.v1__1"
    val b = "team.entity.subentity." + ("x" * 60) + ".groupbyB.v1__1"
    val sa = K8sFlinkSubmitter.sanitizeLabelValue(a)
    val sb = K8sFlinkSubmitter.sanitizeLabelValue(b)
    assertTrue(s"sanitized values should differ, got '$sa' and '$sb'", sa != sb)
    assertTrue(sa.length <= 63 && sb.length <= 63)
  }

  it should "throw when input is empty or has no alphanumeric characters" in {
    assertThrows[IllegalArgumentException](K8sFlinkSubmitter.sanitizeLabelValue(""))
    assertThrows[IllegalArgumentException](K8sFlinkSubmitter.sanitizeLabelValue("---"))
  }

  it should "produce values that contain only [A-Za-z0-9_] (Prometheus-safe intersection)" in {
    val sanitized = K8sFlinkSubmitter.sanitizeLabelValue("ranking.user.last_n.v1__2")
    assertTrue(s"value should match strict pattern, got '$sanitized'", sanitized.matches("[A-Za-z0-9_]+"))
  }

  // --- sanitizeGenericLabelValue ---

  "sanitizeGenericLabelValue" should "leave dots and dashes intact, unlike sanitizeLabelValue" in {
    assertEquals("1.18.0", K8sFlinkSubmitter.sanitizeGenericLabelValue("1.18.0"))
    assertEquals("nikhil-fix", K8sFlinkSubmitter.sanitizeGenericLabelValue("nikhil-fix"))
  }

  it should "replace disallowed characters (e.g. '/' in a branch name) with underscore" in {
    assertEquals("nikhil_customer-timeout", K8sFlinkSubmitter.sanitizeGenericLabelValue("nikhil/customer-timeout"))
  }

  it should "strip leading and trailing non-alphanumeric characters after sanitization" in {
    assertEquals("a.b", K8sFlinkSubmitter.sanitizeGenericLabelValue("..a.b--"))
  }

  it should "truncate values longer than 63 chars and append a hash for distinguishability" in {
    val long = "1." + "0" * 80
    val sanitized = K8sFlinkSubmitter.sanitizeGenericLabelValue(long)
    assertTrue(s"length should be <= 63, got ${sanitized.length}", sanitized.length <= 63)
  }

  it should "return an empty string for input with no alphanumeric characters, rather than throwing" in {
    assertEquals("", K8sFlinkSubmitter.sanitizeGenericLabelValue(""))
    assertEquals("", K8sFlinkSubmitter.sanitizeGenericLabelValue("---"))
  }

  // --- buildEffectivePodLabels: sanitization ---

  it should "sanitize the GroupBy label value before merging" in {
    val submitter = submitterWithExtra()
    val long = "team.entity." + ("x" * 80)
    val labels = submitter.buildEffectivePodLabels(Some(long))
    val value = labels(K8sFlinkSubmitter.JobNamePodLabel)
    assertTrue(value.length <= 63)
  }

  // --- flinkEnvVars injection into JM/TM containers ---

  private def containerEnvVars(submitter: K8sFlinkSubmitter,
                                envVars: java.util.List[java.util.Map[String, String]]
                               ): java.util.List[java.util.Map[String, String]] = {
    val componentSpec = submitter.buildComponentSpec(
      memory = "4G",
      cpu = 1.0,
      replicas = Some(1),
      Collections.emptyList(),
      envVars,
      Collections.emptyList(),
      Collections.emptyList()
    )
    val podSpec = componentSpec.get("podTemplate")
      .asInstanceOf[java.util.Map[String, Object]]
      .get("spec")
      .asInstanceOf[java.util.Map[String, Object]]
    val containers = podSpec.get("containers")
      .asInstanceOf[java.util.List[java.util.Map[String, Object]]]
    containers.get(0).get("env")
      .asInstanceOf[java.util.List[java.util.Map[String, String]]]
  }

  it should "inject flinkEnvVars as env vars on JM and TM containers with FLINK_ prefix stripped" in {
    val submitter = submitterWithExtra()
    // buildComponentSpec receives already-processed env var maps (FLINK_ prefix already stripped by submit()).
    // Here we test that buildComponentSpec faithfully passes them through to the container spec.
    val podEnvVars: java.util.List[java.util.Map[String, String]] =
      List("SASL_JAAS_CFG" -> "sasl-value", "BOOTSTRAP_SERVERS" -> "kafka:9092").map {
        case (key, value) =>
          val m = new java.util.HashMap[String, String]()
          m.put("name", key)
          m.put("value", value)
          m: java.util.Map[String, String]
      }.asJava

    val envs = containerEnvVars(submitter, podEnvVars)

    val envMap = envs.asScala.map(e => e.get("name") -> e.get("value")).toMap
    assertEquals("sasl-value", envMap("SASL_JAAS_CFG"))
    assertEquals("kafka:9092", envMap("BOOTSTRAP_SERVERS"))
  }

  it should "preserve existing container env vars (e.g. FLINK_CLASSPATH) alongside injected pod env vars" in {
    val submitter = submitterWithExtra()
    val classpathVar = new java.util.HashMap[String, String]()
    classpathVar.put("name", "FLINK_CLASSPATH")
    classpathVar.put("value", "/opt/flink/usrlib/*")
    val saslVar = new java.util.HashMap[String, String]()
    saslVar.put("name", "SASL_JAAS_CFG")
    saslVar.put("value", "secret-value")
    val allVars = new java.util.ArrayList[java.util.Map[String, String]]()
    allVars.add(classpathVar)
    allVars.add(saslVar)

    val envs = containerEnvVars(submitter, allVars)

    val envMap = envs.asScala.map(e => e.get("name") -> e.get("value")).toMap
    assertEquals("/opt/flink/usrlib/*", envMap("FLINK_CLASSPATH"))
    assertEquals("secret-value", envMap("SASL_JAAS_CFG"))
  }

  // --- buildJobSpec: upgradeMode ---

  "buildJobSpec" should "set upgradeMode=savepoint and omit initialSavepointPath on cold start" in {
    val job = s.buildJobSpec("local:///opt/flink/usrlib/job.jar", "com.example.Main", Seq.empty, None)
    assertEquals("savepoint", job.get("upgradeMode"))
    assertNull(job.get("initialSavepointPath"))
  }

  it should "set upgradeMode=savepoint and initialSavepointPath on checkpoint resume" in {
    val chk = "s3://bucket/flink-state/checkpoints/abc123/chk-42"
    val job = s.buildJobSpec("local:///opt/flink/usrlib/job.jar", "com.example.Main", Seq.empty, Some(chk))
    assertEquals("savepoint", job.get("upgradeMode"))
    assertEquals(chk, job.get("initialSavepointPath"))
  }

  it should "never set upgradeMode=stateless" in {
    val jobCold = s.buildJobSpec("local:///opt/flink/usrlib/job.jar", "com.example.Main", Seq.empty, None)
    val jobResume = s.buildJobSpec("local:///opt/flink/usrlib/job.jar", "com.example.Main", Seq.empty, Some("s3://bucket/chk-5"))
    assertFalse("cold start should not be stateless", "stateless" == jobCold.get("upgradeMode"))
    assertFalse("checkpoint resume should not be stateless", "stateless" == jobResume.get("upgradeMode"))
  }

  // --- createFlinkIngress ---

  "createFlinkIngress" should "throw IllegalArgumentException when host cannot be extracted" in {
    val submitterWithBadUrl = new K8sFlinkSubmitter(
      flinkImage = "test-image:latest",
      buildInitContainerSpec = noopInitContainerSpec,
      defaultJarsBasePath = "gs://test-bucket/libs/",
      ingressBaseUrl = Some("not-a-valid-url")
    )
    assertThrows[IllegalArgumentException] {
      submitterWithBadUrl.createFlinkIngress(null, "my-deployment", "default", "some-uid")
    }
  }
}
