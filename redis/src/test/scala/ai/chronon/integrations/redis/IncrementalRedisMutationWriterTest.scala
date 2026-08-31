package ai.chronon.integrations.redis

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.providers.ClusterConnectionProvider
import redis.clients.jedis.{ClusterCommandObjects, ClusterPipeline, HostAndPort, Response}
import redis.clients.jedis.exceptions.{
  JedisAskDataException,
  JedisClusterException,
  JedisConnectionException,
  JedisDataException,
  JedisMovedDataException,
  JedisNoScriptException
}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class IncrementalRedisMutationWriterTest extends AnyFlatSpec with Matchers {

  private final class RecordingPipeline
      extends ClusterPipeline(null.asInstanceOf[ClusterConnectionProvider], new ClusterCommandObjects()) {
    var closeCount = 0

    override def close(): Unit = closeCount += 1
  }

  private def response[T](value: => T): Response[T] =
    new Response[T](null) {
      override def get(): T = value
    }

  "Redis upload progress" should "deduplicate retries and speculative attempts by partition" in {
    val tracker = new IncrementalRedisMutationWriter.UploadProgressTracker
    tracker.taskStarted(taskId = 1L, partition = 0)
    tracker.taskStarted(taskId = 2L, partition = 1)
    tracker.taskProgress(taskId = 1L, completed = 400L)
    tracker.taskProgress(taskId = 2L, completed = 300L)
    tracker.completed shouldBe 700L

    tracker.taskStarted(taskId = 3L, partition = 0)
    tracker.taskProgress(taskId = 3L, completed = 200L)
    tracker.completed shouldBe 700L

    tracker.taskProgress(taskId = 3L, completed = 600L)
    tracker.taskProgress(taskId = 1L, completed = 500L)
    tracker.completed shouldBe 900L
  }

  "Redis mutation retries" should "retry only transient cluster failures" in {
    IncrementalRedisMutationWriter.retryableRedisFailure(new JedisConnectionException("connection reset")) shouldBe true
    IncrementalRedisMutationWriter.retryableRedisFailure(new JedisClusterException("CLUSTERDOWN unavailable")) shouldBe true
    IncrementalRedisMutationWriter.retryableRedisFailure(new JedisDataException("TRYAGAIN migrating")) shouldBe true
    IncrementalRedisMutationWriter.retryableRedisFailure(new JedisNoScriptException("NOSCRIPT missing")) shouldBe true
    IncrementalRedisMutationWriter.retryableRedisFailure(
      new IllegalStateException("Please close pipeline or multi block before calling this method.")) shouldBe true
    IncrementalRedisMutationWriter.retryableRedisFailure(
      new IllegalStateException("application invariant failed")) shouldBe false
    IncrementalRedisMutationWriter.retryableRedisFailure(new JedisDataException("WRONGTYPE bad input")) shouldBe false
    IncrementalRedisMutationWriter.retryableRedisFailure(new IllegalArgumentException("invalid mutation")) shouldBe false
  }

  it should "close a write pipeline once before resolving its responses" in {
    val pipeline = new RecordingPipeline
    val unsetResponse =
      new IllegalStateException("Please close pipeline or multi block before calling this method.")
    val pending = response[Any](throw unsetResponse)

    val responses = IncrementalRedisMutationWriter.completePipeline(pipeline)(Seq(pending))

    responses shouldBe Seq(pending)
    pipeline.closeCount shouldBe 1
    val failure = the[IllegalStateException] thrownBy responses.head.get()
    failure shouldBe unsetResponse
  }

  it should "directly replay only commands whose pipeline responses require it" in {
    val target = new HostAndPort("127.0.0.1", 6380)
    val moved = new JedisMovedDataException("MOVED 42 127.0.0.1:6380", target, 42)
    val successful = response(java.lang.Long.valueOf(1L))
    val redirected = response[java.lang.Long](throw moved)
    val unchanged = response(java.lang.Long.valueOf(0L))
    val replayed = ArrayBuffer.empty[String]

    val (results, failures) = IncrementalRedisMutationWriter.resolvePipelineResponses(
      Seq("successful", "redirected", "unchanged"),
      Seq(successful, redirected, unchanged)) { item =>
      replayed += item
      2L
    }

    results shouldBe Seq(1L, 2L, 0L)
    failures shouldBe Seq(moved)
    replayed shouldBe Seq("redirected")
  }

  it should "preserve a bounded whole-batch fallback when MOVED escapes response recovery" in {
    val target = new HostAndPort("127.0.0.1", 6380)
    val moved = new JedisMovedDataException("MOVED 42 127.0.0.1:6380", target, 42)

    IncrementalRedisMutationWriter.directReplayRequired(moved, pipelineMovedFailures = 1, attemptsRemaining = 3) shouldBe false
    IncrementalRedisMutationWriter.directReplayRequired(moved, pipelineMovedFailures = 2, attemptsRemaining = 2) shouldBe true
    IncrementalRedisMutationWriter.directReplayRequired(moved, pipelineMovedFailures = 1, attemptsRemaining = 1) shouldBe true
  }

  it should "selectively replay only redirect, script, and unset response failures" in {
    val unsetResponse =
      new IllegalStateException("Please close pipeline or multi block before calling this method.")
    val ask = new JedisAskDataException("ASK 42 127.0.0.1:6380", new HostAndPort("127.0.0.1", 6380), 42)

    IncrementalRedisMutationWriter.pipelineResponseDirectReplayRequired(ask) shouldBe true
    IncrementalRedisMutationWriter.pipelineResponseDirectReplayRequired(
      new JedisNoScriptException("NOSCRIPT missing")) shouldBe true
    IncrementalRedisMutationWriter.pipelineResponseDirectReplayRequired(unsetResponse) shouldBe true
    IncrementalRedisMutationWriter.pipelineResponseDirectReplayRequired(
      new JedisConnectionException("connection reset")) shouldBe false
    IncrementalRedisMutationWriter.pipelineResponseDirectReplayRequired(
      new JedisDataException("WRONGTYPE bad input")) shouldBe false
  }

  "Redis OOM retries" should "recognize only Redis maxmemory failures" in {
    val reported = new JedisDataException(
      "OOM command not allowed when used memory > 'maxmemory'. script: abc, on @user_script:22.")
    val wrapped = new RuntimeException(
      "pipeline failed",
      new JedisDataException(
        "ERR Error running script (call to f_abc): @user_script:22: -OOM command not allowed when used memory > 'maxmemory'."))

    IncrementalRedisMutationWriter.redisOutOfMemoryFailure(reported) shouldBe true
    IncrementalRedisMutationWriter.redisOutOfMemoryFailure(wrapped) shouldBe true
    IncrementalRedisMutationWriter.redisOutOfMemoryFailure(
      new JedisDataException("ERR OOM maxmemory reached")) shouldBe true
    IncrementalRedisMutationWriter.redisOutOfMemoryFailure(
      new JedisDataException("WRONGTYPE bad input")) shouldBe false
    IncrementalRedisMutationWriter.redisOutOfMemoryFailure(new OutOfMemoryError("Java heap space")) shouldBe false
    IncrementalRedisMutationWriter.retryableRedisFailure(reported) shouldBe false
  }

  it should "back off until Redis has capacity" in {
    var nowMillis = 1000L
    var attempts = 0
    val sleeps = ArrayBuffer.empty[Long]
    val events = ArrayBuffer.empty[IncrementalRedisMutationWriter.OomRetryEvent]

    val result = IncrementalRedisMutationWriter.withRedisOomRetry(
      operation = () => {
        attempts += 1
        if (attempts <= 2) throw new JedisDataException("OOM command not allowed when used memory > 'maxmemory'.")
        "written"
      },
      retryStartMillis = nowMillis,
      retryDeadlineMillis = nowMillis + 10000L,
      retryWaitMillis = attempt => attempt * 100L,
      nowMillis = () => nowMillis,
      sleepMillis = millis => {
        sleeps += millis
        nowMillis += millis
      },
      onRetry = event => {
        events += event
        ()
      }
    )

    result shouldBe "written"
    attempts shouldBe 3
    sleeps shouldBe Seq(100L, 200L)
    events.map(_.attempt) shouldBe Seq(1, 2)
    events.map(_.elapsedMillis) shouldBe Seq(0L, 100L)
  }

  it should "stop at the shared one-hour deadline" in {
    val oom = new JedisDataException("OOM command not allowed when used memory > 'maxmemory'.")
    val sevenMinutes = 7.minutes.toMillis
    val oneHour = 1.hour.toMillis
    var nowMillis = 0L
    var attempts = 0
    val sleeps = ArrayBuffer.empty[Long]

    val failure = the[RuntimeException] thrownBy {
      IncrementalRedisMutationWriter.withRedisOomRetry(
        operation = () => {
          attempts += 1
          throw oom
        },
        retryStartMillis = 0L,
        retryDeadlineMillis = oneHour,
        retryWaitMillis = _ => sevenMinutes,
        nowMillis = () => nowMillis,
        sleepMillis = millis => {
          sleeps += millis
          nowMillis += millis
        },
        onRetry = _ => ()
      )
    }

    attempts shouldBe 9
    sleeps shouldBe (Seq.fill(8)(sevenMinutes) :+ 4.minutes.toMillis)
    failure.getMessage should include("external scaling or memory release")
    failure.getCause shouldBe oom
  }

  it should "propagate non-OOM data errors without waiting" in {
    val wrongType = new JedisDataException("WRONGTYPE bad input")
    var attempts = 0
    var sleeps = 0

    val failure = the[JedisDataException] thrownBy {
      IncrementalRedisMutationWriter.withRedisOomRetry(
        operation = () => {
          attempts += 1
          throw wrongType
        },
        retryStartMillis = 0L,
        retryDeadlineMillis = 1000L,
        retryWaitMillis = _ => 10L,
        nowMillis = () => 0L,
        sleepMillis = _ => sleeps += 1,
        onRetry = _ => ()
      )
    }

    failure shouldBe wrongType
    attempts shouldBe 1
    sleeps shouldBe 0
  }

  it should "use capped exponential backoff and honor the publication lease" in {
    (1 to 8).map(IncrementalRedisMutationWriter.cappedOomRetryBackoffMillis(_, 1000L)) shouldBe
      Seq(1000L, 2000L, 4000L, 8000L, 16000L, 30000L, 30000L, 30000L)
    IncrementalRedisMutationWriter.redisOomRetryDeadline(
      startMillis = 1000L,
      timeoutMillis = RedisBatchUploadDefaults.OomRetryTimeoutMs,
      leaseDeadlineMillis = 5000L) shouldBe 5000L
  }

  "Redis upload lease waits" should "fail before sleeping beyond the publication deadline" in {
    noException should be thrownBy
      IncrementalRedisMutationWriter.requireWaitWithinLease(
        waitMillis = 99L,
        leaseDeadlineMillis = 1000L,
        nowMillis = 900L
      )

    val expired = the[IllegalArgumentException] thrownBy {
      IncrementalRedisMutationWriter.requireWaitWithinLease(
        waitMillis = 100L,
        leaseDeadlineMillis = 1000L,
        nowMillis = 900L
      )
    }
    expired.getMessage should include("cannot wait 100 ms")
  }
}
