package ai.chronon.online

import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

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

  /** Global hot-log sample interval. Tests and specialized operators may override it explicitly. */
  protected val throttleWindowMs: Long = LogSampling.intervalMillis

  // key -> mutable atomic window state; lazily initialized per task-manager process
  @transient private lazy val throttleState = new ConcurrentHashMap[String, ThrottleEntry]()

  // Overridable to make the window-boundary concurrency tests deterministic.
  protected def currentTimeMillis: Long = System.currentTimeMillis()

  def log(level: LogLevel, msg: => String, t: Throwable = null): Unit = emit(level, msg, t)

  /** Returns -1 when the caller should suppress this occurrence, otherwise the approximate number of occurrences
    * suppressed since the previous admission. Hot callers can use this primitive before constructing a by-name log
    * message. Under exact window-boundary concurrency, a racing occurrence can be counted by either adjacent reminder.
    */
  protected def acquireThrottle(key: String): Long = {
    val now = currentTimeMillis
    var entry = throttleState.get(key)

    if (entry == null) {
      val candidate = new ThrottleEntry(now + throttleWindowMs)
      val raced = throttleState.putIfAbsent(key, candidate)
      if (raced == null) {
        return 0L
      }
      entry = raced
    }

    var decided = false
    var result = -1L
    while (!decided) {
      val nextEmitAt = entry.nextEmitAtMs.get()
      if (now < nextEmitAt) {
        entry.suppressedCount.incrementAndGet()
        decided = true
      } else if (entry.nextEmitAtMs.compareAndSet(nextEmitAt, now + throttleWindowMs)) {
        // Counting stays off the map lock, so a racing increment can be reported by either adjacent reminder. The
        // aggregate is preserved; the diagnostic count is intentionally approximate at the exact window boundary.
        result = entry.suppressedCount.getAndSet(0L)
        decided = true
      }
    }
    result
  }

  /** Emits an occurrence previously admitted by acquireThrottle. */
  protected def emitThrottled(level: LogLevel, suppressedCount: Long, msg: => String, t: Throwable = null): Unit = {
    val prefix =
      if (suppressedCount > 0)
        s"[suppressed $suppressedCount occurrences in last ${throttleWindowMs / 1000L}s] "
      else ""
    emit(level, prefix + msg, t)
  }

  def logThrottled(level: LogLevel, key: String, msg: => String, t: Throwable = null): Unit = {
    val suppressedCount = acquireThrottle(key)
    if (suppressedCount >= 0L) emitThrottled(level, suppressedCount, msg, t)
  }

  // Protected so subclasses (e.g. in tests) can intercept emissions without a real SLF4J logger.
  protected def emit(level: LogLevel, msg: => String, t: Throwable): Unit = level match {
    case DEBUG => if (t != null) throttledLoggingLogger.debug(msg, t) else throttledLoggingLogger.debug(msg)
    case INFO  => if (t != null) throttledLoggingLogger.info(msg, t) else throttledLoggingLogger.info(msg)
    case WARN  => if (t != null) throttledLoggingLogger.warn(msg, t) else throttledLoggingLogger.warn(msg)
    case ERROR => if (t != null) throttledLoggingLogger.error(msg, t) else throttledLoggingLogger.error(msg)
  }
}

private final class ThrottleEntry(initialNextEmitAtMs: Long) {
  val nextEmitAtMs = new AtomicLong(initialNextEmitAtMs)
  val suppressedCount = new AtomicLong(0L)
}

object LogSampling {
  val IntervalMillisProperty = "ai.chronon.logging.sample.interval.millis"
  val DefaultIntervalMillis: Long = 60000L

  private[online] def parseIntervalMillis(value: String): Long = {
    val parsed =
      try value.toLong
      catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"$IntervalMillisProperty must be a positive integer, found '$value'")
      }
    require(parsed > 0L, s"$IntervalMillisProperty must be positive, found '$value'")
    parsed
  }

  lazy val intervalMillis: Long =
    Option(System.getProperty(IntervalMillisProperty)).map(parseIntervalMillis).getOrElse(DefaultIntervalMillis)
}
