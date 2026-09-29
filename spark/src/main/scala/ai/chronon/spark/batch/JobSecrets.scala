package ai.chronon.spark.batch

import ai.chronon.api.secrets.SecretResolver
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/** Resolves `NAME_VAULT_URI` refs (Azure Key Vault, GCP Secret Manager, AWS Secrets Manager ARN)
  * inside the job at startup and fills `{NAME}` placeholders in Spark conf, so the job-submission
  * request (StartJobRun, CRDs, CloudTrail) only carries refs and placeholders.
  */
object JobSecrets {
  private val logger = LoggerFactory.getLogger(getClass)

  private val VaultUriSuffix = "_VAULT_URI"
  private val PlaceholderPattern = """\{([A-Z_][A-Z0-9_]*)\}""".r

  private val EnvFanOutPrefixes = Seq(
    "spark.executorEnv.",
    "spark.emr-serverless.driverEnv.",
    "spark.kubernetes.driverEnv.",
    "spark.yarn.appMasterEnv."
  )

  /** @param secrets values resolved here that were not already in the process env
    * @param sparkConfOverrides Spark conf entries whose placeholders were filled
    */
  case class Resolved(secrets: Map[String, String], sparkConfOverrides: Map[String, String])

  def resolve(env: Map[String, String],
              sparkConf: Map[String, String],
              resolveRefs: Map[String, String] => Map[String, String] = SecretResolver.resolveVaultUris): Resolved = {
    val resolvedEnv = resolveRefs(env)
    // Env fan-out props only matter at process launch; filling them in the driver would only
    // spread values to executor env, and their placeholders are not this job's inputs.
    val confToFill = sparkConf.filter { case (key, _) => !EnvFanOutPrefixes.exists(key.startsWith) }
    val filled = fillPlaceholders(confToFill, resolvedEnv)

    val failedRefs = unresolvedRefs(env, resolvedEnv)
    val unfilled = filled.toSeq.flatMap { case (key, value) => placeholders(value).map(_ -> key) }
    val (broken, leftover) = unfilled.partition { case (name, _) => failedRefs.contains(name) }
    if (broken.nonEmpty)
      throw new IllegalStateException(
        s"Could not resolve ${describe(broken)} from their NAME_VAULT_URI refs; see the warnings above for the cause.")
    if (leftover.nonEmpty)
      logger.warn(s"Spark conf placeholders with no value in the job env: ${describe(leftover)}")

    Resolved(
      secrets = resolvedEnv -- env.keySet,
      sparkConfOverrides = filled.filter { case (k, v) => sparkConf(k) != v }
    )
  }

  private def placeholders(value: String): Seq[String] =
    PlaceholderPattern.findAllMatchIn(value).map(_.group(1)).toSeq

  private[batch] def fillPlaceholders(conf: Map[String, String], values: Map[String, String]): Map[String, String] =
    conf.map { case (k, v) =>
      k -> PlaceholderPattern.replaceAllIn(v, m => Regex.quoteReplacement(values.getOrElse(m.group(1), m.matched)))
    }

  // Names whose non-empty NAME_VAULT_URI ref is in env but did not resolve.
  private def unresolvedRefs(env: Map[String, String], resolved: Map[String, String]): Set[String] =
    env.collect {
      case (key, uri)
          if key.endsWith(VaultUriSuffix) && uri.trim.nonEmpty && !resolved.contains(key.stripSuffix(VaultUriSuffix)) =>
        key.stripSuffix(VaultUriSuffix)
    }.toSet

  private def describe(usages: Seq[(String, String)]): String =
    usages
      .groupBy(_._1)
      .toSeq
      .sortBy(_._1)
      .map { case (name, uses) => s"{$name} (used by ${uses.map(_._2).sorted.mkString(", ")})" }
      .mkString("; ")
}
