package ai.chronon.online.metrics

import java.io.PrintWriter
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger

import scala.jdk.CollectionConverters._

class MetricsExceptionLoggingTest extends AnyFlatSpec with Matchers {

  private final class CountingThrowable(traceWrites: AtomicInteger) extends RuntimeException("failure") {
    setStackTrace(Array(new StackTraceElement("Example", "fetch", "Example.scala", 42)))

    override def printStackTrace(writer: PrintWriter): Unit = {
      traceWrites.incrementAndGet()
      super.printStackTrace(writer)
    }
  }

  private def recordingLogger(): (Logger, ConcurrentLinkedQueue[String]) = {
    val messages = new ConcurrentLinkedQueue[String]()
    val handler = new InvocationHandler {
      override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = method.getName match {
        case "isErrorEnabled" => java.lang.Boolean.TRUE
        case "error" if args != null && args.length == 1 =>
          messages.add(args(0).asInstanceOf[String])
          null
        case "getName" => "metrics-exception-test"
        case _ if method.getReturnType == java.lang.Boolean.TYPE => java.lang.Boolean.FALSE
        case _                                                  => null
      }
    }
    val logger = Proxy
      .newProxyInstance(classOf[Logger].getClassLoader, Array(classOf[Logger]), handler)
      .asInstanceOf[Logger]
    logger -> messages
  }

  "Metrics exception logging" should "suppress stack formatting until the next interval and report the count" in {
    val nowNanos = new AtomicLong(0L)
    val limiter = new Metrics.ExceptionLogLimiter(
      intervalNanos = TimeUnit.SECONDS.toNanos(60L),
      bucketCount = 32,
      ticker = () => nowNanos.get())
    val (logger, messages) = recordingLogger()
    val traceWrites = new AtomicInteger(0)
    val exception = new CountingThrowable(traceWrites)
    val signature = Metrics.exceptionSignature(exception)

    Metrics.logException(exception, signature, logger, limiter)
    (1 to 100).foreach(_ => Metrics.logException(exception, signature, logger, limiter))

    traceWrites.get() shouldBe 1
    messages.size() shouldBe 1

    nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(60L))
    Metrics.logException(exception, signature, logger, limiter)

    traceWrites.get() shouldBe 2
    messages.size() shouldBe 2
    messages.asScala.last should include("100 exception logs suppressed in the same sampling bucket")
  }

  it should "admit only one concurrent log and carry every suppressed occurrence forward" in {
    val nowNanos = new AtomicLong(0L)
    val limiter = new Metrics.ExceptionLogLimiter(
      intervalNanos = TimeUnit.SECONDS.toNanos(60L),
      bucketCount = 32,
      ticker = () => nowNanos.get())
    val workers = 16
    val calls = 256
    val executor = Executors.newFixedThreadPool(workers)
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(calls)
    val decisions = new ConcurrentLinkedQueue[Option[Long]]()

    try {
      (1 to calls).foreach { _ =>
        executor.execute(new Runnable {
          override def run(): Unit = {
            try {
              start.await()
              decisions.add(limiter.acquire("stable-signature"))
            } finally {
              done.countDown()
            }
          }
        })
      }
      start.countDown()
      done.await(10L, TimeUnit.SECONDS) shouldBe true

      decisions.asScala.count(_.isDefined) shouldBe 1
      decisions.asScala.count(_.isEmpty) shouldBe calls - 1

      nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(60L))
      limiter.acquire("stable-signature") shouldBe Some(calls - 1L)
    } finally {
      executor.shutdownNow()
    }
  }

  it should "keep cooldowns intact under signature churn with fixed bounded state" in {
    val nowNanos = new AtomicLong(0L)
    val limiter = new Metrics.ExceptionLogLimiter(
      intervalNanos = TimeUnit.SECONDS.toNanos(60L),
      bucketCount = 1,
      ticker = () => nowNanos.get())

    limiter.capacity shouldBe 1
    limiter.acquire("signature-a") shouldBe Some(0L)
    limiter.acquire("signature-b") shouldBe None
    limiter.acquire("signature-a") shouldBe None

    nowNanos.addAndGet(TimeUnit.SECONDS.toNanos(60L))

    limiter.acquire("signature-a") shouldBe Some(2L)
  }
}
