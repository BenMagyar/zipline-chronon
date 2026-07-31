package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.GlideReadCommand.Get
import ai.chronon.integrations.redis.GlideReadResult.Value
import ai.chronon.integrations.redis.RedisKVStoreConstants.GlideExecutionDirect
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CompletableFuture, RejectedExecutionException, TimeUnit, TimeoutException}
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class GlideRedisReadClientTest extends AnyFlatSpec with Matchers {

  "GLIDE direct admission" should "bound active commands and preserve input order" in {
    val runner = new ControlledRunner
    val closes = new AtomicInteger()
    val client = testClient(runner, closes, maxInflight = 2, maxPending = 10, queueTimeoutMs = 5000)
    try {
      val result = client.execute(commands(5))
      runner.submittedCount shouldBe 2

      runner.succeed(1)
      eventually(runner.submittedCount shouldBe 3)
      runner.succeed(0)
      eventually(runner.submittedCount shouldBe 4)
      runner.succeed(2)
      eventually(runner.submittedCount shouldBe 5)
      runner.succeed(4)
      runner.succeed(3)

      val values = result.get(2, TimeUnit.SECONDS).map {
        case Success(Value(Some(bytes))) => bytes.head.toInt
        case other                       => fail(s"Unexpected direct result: $other")
      }
      values shouldBe (0 until 5)
      runner.maxActive shouldBe 2
    } finally client.close()
    closes.get() shouldBe 1
  }

  it should "reject commands beyond the total active and queued bound" in {
    val runner = new ControlledRunner
    val client = testClient(runner, new AtomicInteger(), maxInflight = 1, maxPending = 3, queueTimeoutMs = 5000)
    try {
      val result = client.execute(commands(5))
      runner.submittedCount shouldBe 1
      runner.succeed(0)
      eventually(runner.submittedCount shouldBe 2)
      runner.succeed(1)
      eventually(runner.submittedCount shouldBe 3)
      runner.succeed(2)

      val values = result.get(2, TimeUnit.SECONDS)
      values.take(3).foreach(_.isSuccess shouldBe true)
      values.drop(3).foreach { value =>
        value.failed.get shouldBe a[RejectedExecutionException]
      }
      runner.maxActive shouldBe 1
    } finally client.close()
  }

  it should "expire queued commands while an active command remains unresolved" in {
    val runner = new ControlledRunner
    val client = testClient(runner, new AtomicInteger(), maxInflight = 1, maxPending = 3, queueTimeoutMs = 50)
    try {
      val active = client.execute(commands(1))
      val queued = client.execute(commands(2, offset = 1))

      val queuedValues = queued.get(2, TimeUnit.SECONDS)
      queuedValues.foreach { value =>
        value.failed.get shouldBe a[TimeoutException]
      }
      active.isDone shouldBe false
      runner.submittedCount shouldBe 1

      runner.succeed(0)
      active.get(2, TimeUnit.SECONDS).head.isSuccess shouldBe true
    } finally client.close()
  }

  it should "fail queued and future commands when closed" in {
    val runner = new ControlledRunner
    val closes = new AtomicInteger()
    val client = testClient(runner, closes, maxInflight = 1, maxPending = 3, queueTimeoutMs = 5000)
    val active = client.execute(commands(1))
    val queued = client.execute(commands(2, offset = 1))

    client.close()
    queued.get(2, TimeUnit.SECONDS).foreach { value =>
      value.failed.get.getMessage should include("closed")
    }
    val afterClose = client.execute(commands(1, offset = 3)).get(2, TimeUnit.SECONDS)
    afterClose.head.failed.get.getMessage should include("closed")
    runner.submittedCount shouldBe 1

    runner.succeed(0)
    active.get(2, TimeUnit.SECONDS).head.isSuccess shouldBe true
    client.close()
    closes.get() shouldBe 1
  }

  private def testClient(runner: ControlledRunner,
                         closes: AtomicInteger,
                         maxInflight: Int,
                         maxPending: Int,
                         queueTimeoutMs: Int): GlideRedisReadClient =
    new GlideRedisReadClient(
      GlideExecutionDirect,
      maxInflight,
      maxPending,
      queueTimeoutMs,
      runner.run,
      () => {
        closes.incrementAndGet()
        ()
      }
    )

  private def commands(count: Int, offset: Int = 0): IndexedSeq[GlideReadCommand] =
    (offset until offset + count).map(index => Get(Array(index.toByte))).toVector

  private def eventually(assertion: => Unit): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2)
    var lastFailure: Throwable = null
    do {
      Try(assertion) match {
        case Success(_) => return
        case Failure(error) =>
          lastFailure = error
          Thread.sleep(5)
      }
    } while (System.nanoTime() < deadline)
    throw lastFailure
  }

  private final class ControlledRunner {
    private final class Submission(val command: GlideReadCommand, val result: CompletableFuture[Try[GlideReadResult]])

    private val submissions = ArrayBuffer.empty[Submission]
    private var active = 0
    private var observedMaxActive = 0

    def run(command: GlideReadCommand): CompletableFuture[Try[GlideReadResult]] = synchronized {
      val result = new CompletableFuture[Try[GlideReadResult]]()
      submissions += new Submission(command, result)
      active += 1
      observedMaxActive = math.max(observedMaxActive, active)
      result
    }

    def submittedCount: Int = synchronized(submissions.size)

    def maxActive: Int = synchronized(observedMaxActive)

    def succeed(index: Int): Unit = {
      val submission = synchronized {
        active -= 1
        submissions(index)
      }
      val value = submission.command match {
        case Get(key) => Value(Some(key.clone()))
        case other    => throw new IllegalStateException(s"Unexpected test command: $other")
      }
      submission.result.complete(Success(value)) shouldBe true
    }
  }
}
