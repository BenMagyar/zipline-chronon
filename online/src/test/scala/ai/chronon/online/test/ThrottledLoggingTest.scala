package ai.chronon.online.test

import ai.chronon.online.{DEBUG, ERROR, INFO, LogLevel, LogSampling, ThrottledLogging, WARN}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable

/** Captures all emissions via the protected emit() hook so we can test throttle logic
  * without needing a real SLF4J logger or a mocking framework.
  */
class CapturingLogging(windowMs: Long, clock: () => Long = () => System.currentTimeMillis()) extends ThrottledLogging {
  override protected val throttleWindowMs: Long = windowMs
  override protected def currentTimeMillis: Long = clock()
  val emitCount: AtomicInteger = new AtomicInteger(0)
  val messages: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  def acquire(key: String): Long = acquireThrottle(key)

  override protected def emit(level: LogLevel, msg: => String, t: Throwable): Unit = {
    emitCount.incrementAndGet()
    messages.synchronized(messages += msg)
  }
}

class ThrottledLoggingTest extends AnyFlatSpec with Matchers {

  behavior of "LogSampling"

  it should "parse a positive global sample interval" in {
    LogSampling.parseIntervalMillis("2500") shouldBe 2500L
  }

  it should "reject invalid global sample intervals" in {
    an[IllegalArgumentException] should be thrownBy LogSampling.parseIntervalMillis("0")
    an[IllegalArgumentException] should be thrownBy LogSampling.parseIntervalMillis("not-a-number")
  }

  behavior of "ThrottledLogging.logThrottled"

  it should "emit the first occurrence immediately" in {
    val op = new CapturingLogging(60000L)
    op.logThrottled(ERROR, "test_key", "first error")
    op.emitCount.get() shouldBe 1
    op.messages.head shouldBe "first error"
  }

  it should "suppress subsequent occurrences within the window" in {
    val op = new CapturingLogging(60000L)
    op.logThrottled(ERROR, "test_key", "error 1")
    op.logThrottled(ERROR, "test_key", "error 2")
    op.logThrottled(ERROR, "test_key", "error 3")
    op.emitCount.get() shouldBe 1
  }

  it should "admit through the primitive path without constructing a log message" in {
    val clock = new AtomicLong(0L)
    val op = new CapturingLogging(windowMs = 1000L, clock = () => clock.get())

    op.acquire("hot_key") shouldBe 0L
    op.acquire("hot_key") shouldBe -1L
    op.acquire("hot_key") shouldBe -1L
    op.emitCount.get() shouldBe 0

    clock.set(1000L)
    op.acquire("hot_key") shouldBe 2L
  }

  it should "track different keys independently" in {
    val op = new CapturingLogging(60000L)
    op.logThrottled(ERROR, "key_a", "error a")
    op.logThrottled(ERROR, "key_b", "error b")
    op.logThrottled(ERROR, "key_a", "error a again") // suppressed
    op.emitCount.get() shouldBe 2
    op.messages should contain("error a")
    op.messages should contain("error b")
  }

  it should "emit with suppressed count prefix after the window expires" in {
    val op = new CapturingLogging(windowMs = 50L)
    op.logThrottled(ERROR, "test_key", "first")  // emitted
    op.logThrottled(ERROR, "test_key", "second") // suppressed
    op.logThrottled(ERROR, "test_key", "third")  // suppressed
    Thread.sleep(60)
    op.logThrottled(ERROR, "test_key", "fourth") // emitted with prefix
    op.emitCount.get() shouldBe 2
    op.messages.last should include("suppressed 2 occurrences")
    op.messages.last should include("fourth")
  }

  it should "not include suppressed prefix when no suppressions occurred in previous window" in {
    val op = new CapturingLogging(windowMs = 50L)
    op.logThrottled(ERROR, "test_key", "first")
    Thread.sleep(60)
    op.logThrottled(ERROR, "test_key", "second") // new window, 0 suppressed
    op.emitCount.get() shouldBe 2
    op.messages.last should not include "suppressed"
    op.messages.last shouldBe "second"
  }

  it should "emit exactly once and lose no suppressed-count updates under concurrent calls to the same key" in {
    val threadCount = 50
    val op = new CapturingLogging(windowMs = 300L)
    val pool = Executors.newFixedThreadPool(threadCount)
    val startLatch = new CountDownLatch(1)
    val doneLatch = new CountDownLatch(threadCount)

    try {
      (0 until threadCount).foreach { i =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            startLatch.await()
            op.logThrottled(ERROR, "concurrent_key", s"message $i")
            doneLatch.countDown()
          }
        })
      }
      startLatch.countDown()
      doneLatch.await(10, TimeUnit.SECONDS)

      // Exactly one of the threadCount concurrent calls should have gotten through.
      op.emitCount.get() shouldBe 1

      // The other (threadCount - 1) calls must have been counted as suppressed, with no lost updates.
      Thread.sleep(400)
      op.logThrottled(ERROR, "concurrent_key", "after window")
      op.emitCount.get() shouldBe 2
      op.messages.last should include(s"suppressed ${threadCount - 1} occurrences")
    } finally {
      pool.shutdownNow()
    }
  }

  it should "emit one periodic reminder and preserve suppression counts across a concurrent window boundary" in {
    val clock = new AtomicLong(0L)
    val op = new CapturingLogging(windowMs = 1000L, clock = () => clock.get())
    val threadCount = 64
    val pool = Executors.newFixedThreadPool(threadCount)

    def invokeConcurrently(): Unit = {
      val startLatch = new CountDownLatch(1)
      val doneLatch = new CountDownLatch(threadCount)
      (0 until threadCount).foreach { i =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            try {
              startLatch.await()
              op.logThrottled(WARN, "hot_key", s"message $i")
            } finally {
              doneLatch.countDown()
            }
          }
        })
      }
      startLatch.countDown()
      doneLatch.await(10, TimeUnit.SECONDS) shouldBe true
    }

    try {
      op.logThrottled(WARN, "hot_key", "first")
      invokeConcurrently()
      op.emitCount.get() shouldBe 1

      clock.set(1000L)
      invokeConcurrently()
      op.emitCount.get() shouldBe 2

      clock.set(2000L)
      op.logThrottled(WARN, "hot_key", "final reminder")
      op.emitCount.get() shouldBe 3

      val SuppressedCount = """\[suppressed ([0-9]+) occurrences.*""".r
      val totalReportedSuppressions = op.messages.drop(1).flatMap {
        case SuppressedCount(count) => Some(count.toLong)
        case _                      => None
      }.sum
      totalReportedSuppressions shouldBe (threadCount * 2L - 1L)
    } finally {
      pool.shutdownNow()
    }
  }

  it should "not evaluate the message string for suppressed calls" in {
    val op = new CapturingLogging(60000L)
    var evaluated = 0
    def expensiveMsg(): String = { evaluated += 1; "expensive" }
    op.logThrottled(ERROR, "test_key", expensiveMsg())  // first: evaluated
    op.logThrottled(ERROR, "test_key", expensiveMsg())  // suppressed: should NOT evaluate
    op.logThrottled(ERROR, "test_key", expensiveMsg())  // suppressed: should NOT evaluate
    // The by-name param is only evaluated when emit() is called, which only happens once
    evaluated shouldBe 1
  }

  behavior of "ThrottledLogging.log"

  it should "always emit regardless of window state" in {
    val op = new CapturingLogging(60000L)
    op.log(ERROR, "msg 1")
    op.log(ERROR, "msg 2")
    op.log(ERROR, "msg 3")
    op.emitCount.get() shouldBe 3
  }

  it should "support all log levels" in {
    val op = new CapturingLogging(60000L)
    op.log(DEBUG, "debug msg")
    op.log(INFO, "info msg")
    op.log(WARN, "warn msg")
    op.log(ERROR, "error msg")
    op.emitCount.get() shouldBe 4
  }

  it should "not interfere with logThrottled state" in {
    val op = new CapturingLogging(60000L)
    op.logThrottled(ERROR, "key", "throttled first") // emitted, window started
    op.log(ERROR, "immediate")                        // always emitted, no key
    op.logThrottled(ERROR, "key", "throttled second") // suppressed
    op.emitCount.get() shouldBe 2
  }
}
