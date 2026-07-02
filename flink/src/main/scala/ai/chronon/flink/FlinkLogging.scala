package ai.chronon.flink

import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap

sealed trait LogLevel
case object DEBUG extends LogLevel
case object INFO extends LogLevel
case object WARN extends LogLevel
case object ERROR extends LogLevel

/** Unified logging trait for Flink operators.
  *
  * Provides two methods:
  *   - `log(level, msg, t)` — immediate pass-through to SLF4J, for init/one-shot events.
  *   - `logThrottled(level, key, msg, t)` — rate-limited variant for per-message hot-path error/warn sites.
  *
  * Throttling: the first occurrence in a window is logged with full detail. Subsequent occurrences
  * within the window are silently counted. When the window expires and the next occurrence arrives,
  * the suppressed count is prepended to the message before logging.
  *
  * Convention: use `log` for INFO/DEBUG and any one-shot operational log. Use `logThrottled` for
  * any ERROR/WARN that fires per Kafka message or on a tight loop.
  */
trait FlinkLogging {

  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /** Throttle window duration. Override in the operator class if a different window is needed. */
  protected val throttleWindowMs: Long = 60000L

  // key -> (windowStartMs, suppressedCount); lazily initialized per task-manager process
  @transient private lazy val throttleState = new ConcurrentHashMap[String, ThrottleEntry]()

  def log(level: LogLevel, msg: => String, t: Throwable = null): Unit = emit(level, msg, t)

  def logThrottled(level: LogLevel, key: String, msg: => String, t: Throwable = null): Unit = {
    val now = System.currentTimeMillis()
    val entry = throttleState.get(key)
    if (entry == null) {
      throttleState.put(key, ThrottleEntry(now, 0L))
      emit(level, msg, t)
    } else if (now - entry.windowStartMs >= throttleWindowMs) {
      val prefix =
        if (entry.suppressedCount > 0)
          s"[suppressed ${entry.suppressedCount} occurrences in last ${throttleWindowMs / 1000L}s] "
        else ""
      throttleState.put(key, ThrottleEntry(now, 0L))
      emit(level, prefix + msg, t)
    } else {
      throttleState.put(key, entry.copy(suppressedCount = entry.suppressedCount + 1))
    }
  }

  // Protected so subclasses (e.g. in tests) can intercept emissions without a real SLF4J logger.
  protected def emit(level: LogLevel, msg: => String, t: Throwable): Unit = level match {
    case DEBUG => if (t != null) logger.debug(msg, t) else logger.debug(msg)
    case INFO  => if (t != null) logger.info(msg, t) else logger.info(msg)
    case WARN  => if (t != null) logger.warn(msg, t) else logger.warn(msg)
    case ERROR => if (t != null) logger.error(msg, t) else logger.error(msg)
  }
}

private case class ThrottleEntry(windowStartMs: Long, suppressedCount: Long)
