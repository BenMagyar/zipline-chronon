package ai.chronon.api.secrets

import scala.util.matching.Regex

/** Spark's `spark.redaction.regex` doubles as the submitter-side definition of "secret":
  * anything Spark would black out in its UI must not travel in plaintext through a
  * submission API or a pod spec either.
  */
object Redaction {
  val Key = "spark.redaction.regex"
  // Spark's built-in default for spark.redaction.regex
  val DefaultRegex = "(?i)secret|password|token|access[.]key"

  val VaultUriSuffix = "_VAULT_URI"

  def regex(properties: Map[String, String]): Regex =
    properties.getOrElse(Key, DefaultRegex).r

  /** Mirrors Spark's own test: the key or the value matches. Redaction settings and
    * secret references (`_VAULT_URI`, `EMR.secret@`) are not secrets themselves even
    * though their names or values mention one.
    */
  def isSensitive(regex: Regex, key: String, value: String): Boolean =
    !key.startsWith("spark.redaction.") &&
      !key.endsWith(VaultUriSuffix) &&
      !value.startsWith("EMR.secret@") &&
      (regex.findFirstIn(key).isDefined || regex.findFirstIn(value).isDefined)
}
