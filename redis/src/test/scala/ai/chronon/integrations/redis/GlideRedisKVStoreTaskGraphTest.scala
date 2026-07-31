package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.GlideReadResult.Value
import ai.chronon.integrations.redis.RedisKVStoreConstants.{ConfigPipelineSyncThreads, GlideExecutionDirect}
import ai.chronon.online.KVStore.GetRequest
import org.slf4j.{Logger, LoggerFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.JedisCluster

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{
  CompletableFuture,
  CountDownLatch,
  ExecutionException,
  LinkedBlockingQueue,
  RejectedExecutionException,
  ThreadPoolExecutor,
  TimeUnit
}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

class GlideRedisKVStoreTaskGraphTest extends AnyFlatSpec with Matchers {

  "GLIDE Redis multiGet" should "release the shared executor while native IO waits and use exactly two tasks" in {
    val runner = new ControlledRunner
    withStore(runner) { (store, executor) =>
      val request = GetRequest("entity".getBytes(StandardCharsets.UTF_8), "TASK_GRAPH_BATCH")
      val payload = "payload".getBytes(StandardCharsets.UTF_8)
      val storedValue = RedisBatchUpload.encodeValue(RedisBatchUpload.Upsert, 123L, payload)
      val taskCountBefore = executor.getTaskCount
      val completedTaskCountBefore = executor.getCompletedTaskCount

      val result = store.multiGet(Seq(request))
      runner.awaitSubmission()
      eventually(executor.getCompletedTaskCount - completedTaskCountBefore shouldBe 1L)

      result.isCompleted shouldBe false
      executor.getActiveCount shouldBe 0
      executor.getQueue.isEmpty shouldBe true
      executor.getTaskCount - taskCountBefore shouldBe 1L

      runner.complete(Success(Value(Some(storedValue))))
      val responses = Await.result(result, 2.seconds)
      eventually(executor.getCompletedTaskCount - completedTaskCountBefore shouldBe 2L)

      responses.map(_.request) shouldBe Seq(request)
      responses.head.values.get should have size 1
      responses.head.values.get.head.bytes.toSeq shouldBe payload.toSeq
      responses.head.values.get.head.millis shouldBe 123L
      executor.getTaskCount - taskCountBefore shouldBe 2L
    }
  }

  it should "preserve command failures as per-request failures without adding executor continuations" in {
    val runner = new ControlledRunner
    withStore(runner) { (store, executor) =>
      val request = GetRequest("entity".getBytes(StandardCharsets.UTF_8), "TASK_GRAPH_FAILURE_BATCH")
      val expected = new IllegalStateException("native command failed")
      val taskCountBefore = executor.getTaskCount
      val completedTaskCountBefore = executor.getCompletedTaskCount

      val result = store.multiGet(Seq(request))
      runner.awaitSubmission()
      eventually(executor.getCompletedTaskCount - completedTaskCountBefore shouldBe 1L)
      runner.complete(Failure(expected))

      val responses = Await.result(result, 2.seconds)
      eventually(executor.getCompletedTaskCount - completedTaskCountBefore shouldBe 2L)

      responses should have size 1
      responses.head.values.failed.get should be theSameInstanceAs expected
      executor.getTaskCount - taskCountBefore shouldBe 2L
    }
  }

  it should "normalize an ordinary executor rejection into per-request failures" in {
    val runner = new ControlledRunner
    val expected = new RejectedExecutionException("executor is full")
    withExecutionContext(runner, new RejectingExecutionContext(expected)) { store =>
      val request = GetRequest("entity".getBytes(StandardCharsets.UTF_8), "TASK_GRAPH_REJECTION_BATCH")

      val responses = Await.result(store.multiGet(Seq(request)), 2.seconds)

      responses should have size 1
      responses.head.values.failed.get should be theSameInstanceAs expected
      runner.submittedCount shouldBe 0
    }
  }

  it should "fail the outer future when task 2 is rejected with a non-Exception throwable" in {
    val runner = new ControlledRunner
    val executor = singleThreadExecutor()
    val expected = new AssertionError("task 2 rejected")
    val executionContext = new RejectSecondExecutionContext(executor, expected)
    try {
      withExecutionContext(runner, executionContext) { store =>
        val request = GetRequest("entity".getBytes(StandardCharsets.UTF_8), "TASK_GRAPH_FATAL_BATCH")
        val storedValue = RedisBatchUpload.encodeValue(RedisBatchUpload.Upsert, 123L, Array[Byte](1))
        val result = store.multiGet(Seq(request))
        runner.awaitSubmission()
        eventually(executor.getCompletedTaskCount shouldBe 1L)

        runner.complete(Success(Value(Some(storedValue))))

        val thrown = intercept[ExecutionException](Await.result(result, 2.seconds))
        thrown.getCause should be theSameInstanceAs expected
        executionContext.executionAttempts shouldBe 2
      }
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(2, TimeUnit.SECONDS)
    }
  }

  it should "fail the outer future when failure reporting throws a second exception" in {
    val runner = new ControlledRunner
    val rejection = new RejectedExecutionException("executor is full")
    val reportingFailure = new IllegalStateException("failure reporting failed")
    withExecutionContext(runner,
                         new RejectingExecutionContext(rejection),
                         loggerFailingOnError(reportingFailure)) { store =>
      val request = GetRequest("entity".getBytes(StandardCharsets.UTF_8), "TASK_GRAPH_REPORTING_FAILURE_BATCH")

      val thrown = intercept[IllegalStateException](Await.result(store.multiGet(Seq(request)), 2.seconds))

      thrown should be theSameInstanceAs reportingFailure
      runner.submittedCount shouldBe 0
    }
  }

  private def withStore(testRunner: ControlledRunner)(test: (TaskGraphStore, ThreadPoolExecutor) => Unit): Unit = {
    val executor = singleThreadExecutor()
    try {
      withExecutionContext(testRunner, ExecutionContext.fromExecutor(executor)) { store =>
        test(store, executor)
      }
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(2, TimeUnit.SECONDS)
    }
  }

  private def withExecutionContext(testRunner: ControlledRunner,
                                   executionContext: ExecutionContext,
                                   testLogger: Logger = null)(
      test: TaskGraphStore => Unit): Unit = {
    val glideClient = new GlideRedisReadClient(
      GlideExecutionDirect,
      1,
      10,
      5000,
      testRunner.run,
      () => ()
    )
    // This focused GLIDE unit does not touch Jedis; a null keeps the test independent of a live Redis cluster.
    val store = new TaskGraphStore(null, glideClient, executionContext, testLogger)
    try test(store)
    finally {
      store.shutdownGlideReadClient()
    }
  }

  private def singleThreadExecutor(): ThreadPoolExecutor =
    new ThreadPoolExecutor(
      1,
      1,
      0L,
      TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable]()
    )

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

  private def loggerFailingOnError(error: Throwable): Logger =
    Proxy
      .newProxyInstance(
        classOf[Logger].getClassLoader,
        Array(classOf[Logger]),
        new InvocationHandler {
          override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
            if (method.getName == "error") throw error
            else if (method.getReturnType == java.lang.Boolean.TYPE) java.lang.Boolean.FALSE
            else if (method.getReturnType == java.lang.Integer.TYPE) Integer.valueOf(0)
            else null
          }
        }
      )
      .asInstanceOf[Logger]

  private final class ControlledRunner {
    private val submitted = new CountDownLatch(1)
    private val result = new AtomicReference[CompletableFuture[Try[GlideReadResult]]]()
    private val submissions = new AtomicInteger()

    def run(_command: GlideReadCommand): CompletableFuture[Try[GlideReadResult]] = {
      val commandResult = new CompletableFuture[Try[GlideReadResult]]()
      result.set(commandResult)
      submissions.incrementAndGet()
      submitted.countDown()
      commandResult
    }

    def submittedCount: Int = submissions.get()

    def awaitSubmission(): Unit =
      submitted.await(2, TimeUnit.SECONDS) shouldBe true

    def complete(value: Try[GlideReadResult]): Unit =
      result.get().complete(value) shouldBe true
  }

  private final class RejectingExecutionContext(error: Throwable) extends ExecutionContext {
    override def execute(runnable: Runnable): Unit = throw error

    override def reportFailure(cause: Throwable): Unit = ()
  }

  private final class RejectSecondExecutionContext(delegate: ThreadPoolExecutor, error: Throwable)
      extends ExecutionContext {
    private val attempts = new AtomicInteger()

    override def execute(runnable: Runnable): Unit =
      if (attempts.incrementAndGet() == 2) throw error else delegate.execute(runnable)

    override def reportFailure(cause: Throwable): Unit = ()

    def executionAttempts: Int = attempts.get()
  }

  private final class TaskGraphStore(client: JedisCluster,
                                     glideClient: GlideRedisReadClient,
                                     override implicit val executionContext: ExecutionContext,
                                     testLogger: Logger)
      extends RedisKVStoreImpl(
        client,
        Map("redis.read.metrics.enabled" -> "false", ConfigPipelineSyncThreads -> "0"),
        glideReadClient = Some(glideClient)
      ) {
    @transient override lazy val logger: Logger =
      if (testLogger == null) LoggerFactory.getLogger(getClass) else testLogger
  }
}
