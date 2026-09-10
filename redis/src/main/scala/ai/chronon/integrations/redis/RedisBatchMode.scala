package ai.chronon.integrations.redis

import java.util.Locale

/** Selects the implementation used for Redis batch writes. Reads discover the published protocol from Redis. */
sealed trait RedisBatchMode extends Product with Serializable

object RedisBatchMode {
  case object FullSnapshot extends RedisBatchMode

  final case class Incremental(conditionalWriter: ConditionalObjectWriter) extends RedisBatchMode {
    require(conditionalWriter != null, "Redis incremental batch mode requires a conditional object writer")
  }
}

/** Serializable name used by upload configuration and the persistent Redis publication-mode marker. */
sealed trait RedisBatchModeSelection extends Product with Serializable {
  def configValue: String
}

object RedisBatchModeSelection {
  case object FullSnapshot extends RedisBatchModeSelection {
    override val configValue: String = "full_snapshot"
  }

  case object Incremental extends RedisBatchModeSelection {
    override val configValue: String = "incremental"
  }

  private val supported = Seq(Incremental, FullSnapshot)

  def parse(value: String): RedisBatchModeSelection =
    value.trim.toLowerCase(Locale.ROOT) match {
      case FullSnapshot.configValue => FullSnapshot
      case Incremental.configValue  => Incremental
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported Redis batch mode '$value'; supported values: ${supported.map(_.configValue).mkString(", ")}")
    }
}
