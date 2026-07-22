package ai.chronon.online.test

import ai.chronon.online.{DEBUG, ERROR, INFO, LogLevel, ThrottledLogging, WARN}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/** Captures all emissions via the protected emit() hook so we can test throttle logic
  * without needing a real SLF4J logger or a mocking framework.
  */
class CapturingLogging(windowMs: Long) extends ThrottledLogging {
  override protected val throttleWindowMs: Long = windowMs
  val emitCount: AtomicInteger = new AtomicInteger(0)
  val messages: mutable.ListBuffer[String] = mutable.ListBuffer.empty

  override protected def emit(level: LogLevel, msg: => String, t: Throwable): Unit = {
    emitCount.incrementAndGet()
    messages.synchronized(messages += msg)
  }
}

class ThrottledLoggingTest extends AnyFlatSpec with Matchers {

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
