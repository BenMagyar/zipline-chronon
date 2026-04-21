package ai.chronon.integrations.aws

import java.util.regex.Pattern
import scala.util.Try

private[aws] object SparkPropertyRedaction {
  private val SparkRedactionRegexKey = "spark.redaction.regex"
  private val DefaultSparkRedactionRegex = "(?i)secret|password|token|access[.]?key"
  private val RedactedValue = "<redacted>"

  def redactProperties(
      sparkProperties: Map[String, String],
      properties: Seq[(String, String)]
  ): Seq[(String, String)] = {
    val pattern = compilePattern(sparkProperties.getOrElse(SparkRedactionRegexKey, DefaultSparkRedactionRegex))
    properties.map { case (key, value) =>
      if (pattern.matcher(key).find()) key -> RedactedValue else key -> value
    }
  }

  def redactPropertiesWithFormat[T](
      sparkProperties: Map[String, String],
      properties: Seq[(String, String)]
  )(render: Seq[(String, String)] => T): T =
    render(redactProperties(sparkProperties, properties))

  private def compilePattern(regex: String): Pattern =
    Try(Pattern.compile(regex)).getOrElse(Pattern.compile(DefaultSparkRedactionRegex))
}
