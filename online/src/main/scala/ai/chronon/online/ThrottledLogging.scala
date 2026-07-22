package ai.chronon.online

import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap

sealed trait LogLevel
case object DEBUG extends LogLevel
case object INFO extends LogLevel
case object WARN extends LogLevel
case object ERROR extends LogLevel

/** Unified logging trait for hot-path operators (fetcher, Flink, etc).
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
  * any ERROR/WARN that fires per request/message or on a tight loop.
  */
trait ThrottledLogging {

  // Named distinctly from `logger` since most mixing-in classes already declare their own SLF4J logger val.
  @transient private lazy val throttledLoggingLogger: Logger = LoggerFactory.getLogger(getClass)

  /** Throttle window duration. Override in the operator class if a different window is needed. */
  protected val throttleWindowMs: Long = 60000L

  // key -> (windowStartMs, suppressedCount); lazily initialized per task-manager process
  @transient private lazy val throttleState = new ConcurrentHashMap[String, ThrottleEntry]()

  def log(level: LogLevel, msg: => String, t: Throwable = null): Unit = emit(level, msg, t)

  def logThrottled(level: LogLevel, key: String, msg: => String, t: Throwable = null): Unit = {
    val now = System.currentTimeMillis()
    // Decided atomically inside compute (which serializes concurrent calls for the same key), then
    // read out here to actually emit - keeps the by-name `msg` unevaluated for suppressed calls.
    var emitPrefix: String = null

    throttleState.compute(
      key,
      (_, entry) => {
        if (entry == null) {
          emitPrefix = ""
          ThrottleEntry(now, 0L)
        } else if (now - entry.windowStartMs >= throttleWindowMs) {
          emitPrefix =
            if (entry.suppressedCount > 0)
              s"[suppressed ${entry.suppressedCount} occurrences in last ${throttleWindowMs / 1000L}s] "
            else ""
          ThrottleEntry(now, 0L)
        } else {
          entry.copy(suppressedCount = entry.suppressedCount + 1)
        }
      }
    )

    if (emitPrefix != null) emit(level, emitPrefix + msg, t)
  }

  // Protected so subclasses (e.g. in tests) can intercept emissions without a real SLF4J logger.
  protected def emit(level: LogLevel, msg: => String, t: Throwable): Unit = level match {
    case DEBUG => if (t != null) throttledLoggingLogger.debug(msg, t) else throttledLoggingLogger.debug(msg)
    case INFO  => if (t != null) throttledLoggingLogger.info(msg, t) else throttledLoggingLogger.info(msg)
    case WARN  => if (t != null) throttledLoggingLogger.warn(msg, t) else throttledLoggingLogger.warn(msg)
    case ERROR => if (t != null) throttledLoggingLogger.error(msg, t) else throttledLoggingLogger.error(msg)
  }
}

private case class ThrottleEntry(windowStartMs: Long, suppressedCount: Long)
