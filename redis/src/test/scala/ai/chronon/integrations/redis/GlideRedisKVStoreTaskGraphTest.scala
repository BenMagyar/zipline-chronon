package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.GlideReadResult.Value
import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.integrations.redis.RedisKVStoreConstants.{
  ConfigPipelineSyncThreads,
  GlideExecutionDirect,
  PropRedisReadMaxInflightBatches,
  PropRedisReadMaxQueuedBatches,
  PropRedisReadQueueTimeoutMs
}
import ai.chronon.online.KVStore.GetRequest
import glide.api.models.exceptions.{TimeoutException => GlideTimeoutException}
import org.slf4j.{Logger, LoggerFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.JedisCluster

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{
  CompletableFuture,
  ConcurrentLinkedQueue,
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

  it should "gate ClusterBatch before planning and hold its permit through scoped consumer decoding" in {
    val runner = new ControlledBatchRunner
    val executionContext = new ManualExecutionContext
    withClusterBatchStore(runner, executionContext) { store =>
      val firstRequest = GetRequest("first".getBytes(StandardCharsets.UTF_8), "ADMISSION_BATCH")
      val secondRequest = GetRequest("second".getBytes(StandardCharsets.UTF_8), "ADMISSION_BATCH")
      var snapshotDuringConsumer: RedisReadAdmissionSnapshot = null
      val first = store.scopedMultiGet(Seq(firstRequest)) { rawFuture =>
        rawFuture.map { responses =>
          snapshotDuringConsumer = store.readAdmissionController.get.snapshot
          new String(responses.head.values.get.head.bytes, StandardCharsets.UTF_8)
        }(executionContext)
      }
      executionContext.pending shouldBe 1

      executionContext.runNext()
      runner.submittedCount shouldBe 1
      executionContext.pending shouldBe 0

      val second = store.multiGet(Seq(secondRequest))
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(1, 1)
      // The queued request has not reached planning, the shared executor, or GLIDE.
      executionContext.pending shouldBe 0
      runner.submittedCount shouldBe 1

      runner.completeNext(storedResult("first-value", 1L))
      executionContext.pending shouldBe 1
      // Native completion alone does not release admission; task 2 still owns Redis response decoding.
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(1, 1)

      executionContext.runNext()
      first.isCompleted shouldBe false
      // The raw Redis response is complete, but the scoped GroupBy/FastSerde consumer has not run yet.
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(1, 1)
      executionContext.pending shouldBe 1

      executionContext.runNext()
      Await.result(first, 2.seconds) shouldBe "first-value"
      snapshotDuringConsumer shouldBe RedisReadAdmissionSnapshot(1, 1)
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(1, 0)
      executionContext.pending shouldBe 1

      executionContext.runNext()
      runner.submittedCount shouldBe 2
      runner.completeNext(storedResult("second-value", 2L))
      executionContext.runNext()
      readValue(second) shouldBe "second-value"
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(0, 0)
    }
  }

  it should "open cooldown on a GLIDE timeout and fail active queued and new batches open" in {
    val runner = new ControlledBatchRunner
    val executionContext = new ManualExecutionContext
    withClusterBatchStore(runner, executionContext) { store =>
      val activeRequest = GetRequest("active".getBytes(StandardCharsets.UTF_8), "TIMEOUT_BATCH")
      val queuedRequest = GetRequest("queued".getBytes(StandardCharsets.UTF_8), "TIMEOUT_BATCH")
      val cooldownRequest = GetRequest("cooldown".getBytes(StandardCharsets.UTF_8), "TIMEOUT_BATCH")

      val active = store.multiGet(Seq(activeRequest))
      executionContext.runNext()
      runner.submittedCount shouldBe 1

      val queued = store.multiGet(Seq(queuedRequest))
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(1, 1)

      runner.completeNext(Vector(Failure(new GlideTimeoutException("native request timed out"))))
      // Timeout fail-open must not wait behind the shared executor's completion queue.
      missingValue(active, activeRequest)
      missingValue(queued, queuedRequest)
      executionContext.pending shouldBe 0
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(0, 0)

      val cooldown = store.multiGet(Seq(cooldownRequest))
      runner.submittedCount shouldBe 1
      missingValue(cooldown, cooldownRequest)
      runner.submittedCount shouldBe 1
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(0, 0)
    }
  }

  it should "retain a scoped permit when the consumer times out before the raw GLIDE read settles" in {
    val runner = new ControlledBatchRunner
    val executionContext = new ManualExecutionContext
    withClusterBatchStore(runner, executionContext) { store =>
      val activeRequest = GetRequest("active".getBytes(StandardCharsets.UTF_8), "CONSUMER_TIMEOUT_BATCH")
      val queuedRequest = GetRequest("queued".getBytes(StandardCharsets.UTF_8), "CONSUMER_TIMEOUT_BATCH")
      val consumerPromise = scala.concurrent.Promise[String]()

      val active = store.scopedMultiGet(Seq(activeRequest))(_ => consumerPromise.future)
      executionContext.runNext()
      runner.submittedCount shouldBe 1
      val queued = store.multiGet(Seq(queuedRequest))
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(1, 1)

      consumerPromise.failure(new java.util.concurrent.TimeoutException("consumer timed out"))
      intercept[java.util.concurrent.TimeoutException](Await.result(active, 2.seconds))
      // The caller has stopped waiting, but GLIDE still owns the active lifecycle; no replacement is admitted.
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(1, 1)
      runner.submittedCount shouldBe 1

      runner.completeNext(storedResult("ignored", 1L))
      executionContext.runNext()
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(1, 0)
      executionContext.runNext()
      runner.submittedCount shouldBe 2

      runner.completeNext(storedResult("queued-value", 2L))
      executionContext.runNext()
      readValue(queued) shouldBe "queued-value"
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(0, 0)
    }
  }

  it should "fail queue-full and queue-timeout batches open as missing values" in {
    val runner = new ControlledBatchRunner
    val executionContext = new ManualExecutionContext
    withClusterBatchStore(
      runner,
      executionContext,
      Map(PropRedisReadQueueTimeoutMs -> "25")
    ) { store =>
      val activeRequest = GetRequest("active".getBytes(StandardCharsets.UTF_8), "SHED_BATCH")
      val timeoutRequest = GetRequest("timeout".getBytes(StandardCharsets.UTF_8), "SHED_BATCH")
      val fullRequest = GetRequest("full".getBytes(StandardCharsets.UTF_8), "SHED_BATCH")

      val active = store.multiGet(Seq(activeRequest))
      val timedOut = store.multiGet(Seq(timeoutRequest))
      val full = store.multiGet(Seq(fullRequest))

      val fullResponse = Await.result(full, 2.seconds).head
      fullResponse.request shouldBe fullRequest
      fullResponse.values shouldBe Success(Seq.empty)
      store.startupWarmupDegradedReadCount shouldBe 1L
      executionContext.pending shouldBe 1

      val timeoutResponse = Await.result(timedOut, 2.seconds).head
      timeoutResponse.request shouldBe timeoutRequest
      timeoutResponse.values shouldBe Success(Seq.empty)
      store.startupWarmupDegradedReadCount shouldBe 2L
      executionContext.pending shouldBe 1
      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(1, 0)

      executionContext.runNext()
      runner.completeNext(storedResult("active-value", 1L))
      executionContext.runNext()
      readValue(active) shouldBe "active-value"
    }
  }

  it should "admit metadata within the same total ClusterBatch bound" in {
    val runner = new ControlledBatchRunner
    val executionContext = new ManualExecutionContext
    withClusterBatchStore(
      runner,
      executionContext,
      Map(
        PropRedisReadMaxInflightBatches -> "2",
        PropRedisReadMaxQueuedBatches -> "0"
      )
    ) { store =>
      val feature = store.multiGet(Seq(GetRequest("feature".getBytes(StandardCharsets.UTF_8), "FEATURE_BATCH")))
      val metadataRequest = GetRequest("metadata".getBytes(StandardCharsets.UTF_8), MetadataDataset)
      val metadata = store.controlMultiGet(Seq(metadataRequest))

      store.readAdmissionController.get.snapshot shouldBe RedisReadAdmissionSnapshot(2, 0)
      executionContext.pending shouldBe 2

      executionContext.runNext()
      executionContext.runNext()
      runner.submittedCount shouldBe 2
      runner.completeNext(storedResult("feature-value", 1L))
      runner.completeNext(storedResult("metadata-value", 2L))
      executionContext.runNext()
      executionContext.runNext()

      readValue(feature) shouldBe "feature-value"
      val metadataResponse = Await.result(metadata, 2.seconds).head
      metadataResponse.request shouldBe metadataRequest
      new String(metadataResponse.values.get.head.bytes, StandardCharsets.UTF_8) shouldBe "metadata-value"
    }
  }

  it should "preserve partial control results when one GLIDE command times out" in {
    val runner = new ControlledBatchRunner
    val executionContext = new ManualExecutionContext
    withClusterBatchStore(runner, executionContext) { store =>
      val first = GetRequest("first".getBytes(StandardCharsets.UTF_8), "FIRST_BATCH")
      val second = GetRequest("second".getBytes(StandardCharsets.UTF_8), "SECOND_BATCH")
      val result = store.controlMultiGet(Seq(first, second))

      executionContext.runNext()
      runner.submittedCount shouldBe 1
      runner.completeNext(
        Vector(
          storedResult("first-value", 1L).head,
          Failure(new GlideTimeoutException("one command timed out"))
        ))
      executionContext.runNext()

      val responses = Await.result(result, 2.seconds)
      new String(responses.head.values.get.head.bytes, StandardCharsets.UTF_8) shouldBe "first-value"
      responses(1).values.failed.get shouldBe a[GlideTimeoutException]
    }
  }

  it should "materialize responses before validating results for a lazy request sequence" in {
    val runner = new ControlledBatchRunner
    val executionContext = new ManualExecutionContext
    withClusterBatchStore(runner, executionContext) { store =>
      val request = GetRequest("metadata".getBytes(StandardCharsets.UTF_8), MetadataDataset)
      val result = store.controlMultiGet(Stream(request))

      executionContext.runNext()
      runner.completeNext(storedResult("metadata-value", 1L))
      executionContext.runNext()

      readValue(result) shouldBe "metadata-value"
    }
  }

  it should "leave direct-mode GLIDE reads outside the ClusterBatch bulkhead" in {
    val runner = new ControlledRunner
    withExecutionContext(runner, new ManualExecutionContext) { store =>
      store.readAdmissionController shouldBe None
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
      store.shutdownReadAdmissionController()
      store.shutdownGlideReadClient()
    }
  }

  private def withClusterBatchStore(testRunner: ControlledBatchRunner,
                                    executionContext: ManualExecutionContext,
                                    additionalConf: Map[String, String] = Map.empty)(
      test: TaskGraphStore => Unit): Unit = {
    val glideClient = new GlideRedisReadClient(testRunner.run, () => ())
    val conf = Map(
      PropRedisReadMaxInflightBatches -> "1",
      PropRedisReadMaxQueuedBatches -> "1",
      PropRedisReadQueueTimeoutMs -> "10000"
    ) ++ additionalConf
    val store = new TaskGraphStore(null, glideClient, executionContext, null, conf)
    try test(store)
    finally {
      store.shutdownReadAdmissionController()
      store.shutdownGlideReadClient()
    }
  }

  private def storedResult(value: String, millis: Long): IndexedSeq[Try[GlideReadResult]] = {
    val payload = value.getBytes(StandardCharsets.UTF_8)
    Vector(Success(Value(Some(RedisBatchUpload.encodeValue(RedisBatchUpload.Upsert, millis, payload)))))
  }

  private def readValue(result: scala.concurrent.Future[Seq[ai.chronon.online.KVStore.GetResponse]]): String = {
    val response = Await.result(result, 2.seconds).head
    new String(response.values.get.head.bytes, StandardCharsets.UTF_8)
  }

  private def missingValue(result: scala.concurrent.Future[Seq[ai.chronon.online.KVStore.GetResponse]],
                           request: GetRequest): Unit = {
    val response = Await.result(result, 2.seconds).head
    response.request shouldBe request
    response.values shouldBe Success(Seq.empty)
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

  private final class ControlledBatchRunner {
    private val results = new ConcurrentLinkedQueue[CompletableFuture[IndexedSeq[Try[GlideReadResult]]]]()
    private val submissions = new AtomicInteger()

    def run(_commands: IndexedSeq[GlideReadCommand]): CompletableFuture[IndexedSeq[Try[GlideReadResult]]] = {
      val result = new CompletableFuture[IndexedSeq[Try[GlideReadResult]]]()
      results.add(result)
      submissions.incrementAndGet()
      result
    }

    def submittedCount: Int = submissions.get()

    def completeNext(value: IndexedSeq[Try[GlideReadResult]]): Unit = {
      val next = results.poll()
      next should not be null
      next.complete(value) shouldBe true
    }
  }

  private final class ManualExecutionContext extends ExecutionContext {
    private val tasks = new ConcurrentLinkedQueue[Runnable]()

    override def execute(runnable: Runnable): Unit = tasks.add(runnable)
    override def reportFailure(cause: Throwable): Unit = throw cause

    def pending: Int = tasks.size()

    def runNext(): Unit = {
      val next = tasks.poll()
      next should not be null
      next.run()
    }

    def runAll(): Unit =
      while (pending > 0) runNext()
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
                                     testLogger: Logger,
                                     additionalConf: Map[String, String] = Map.empty)
      extends RedisKVStoreImpl(
        client,
        Map("redis.read.metrics.enabled" -> "false", ConfigPipelineSyncThreads -> "0") ++ additionalConf,
        glideReadClient = Some(glideClient)
      ) {
    @transient override lazy val logger: Logger =
      if (testLogger == null) LoggerFactory.getLogger(getClass) else testLogger
  }
}
