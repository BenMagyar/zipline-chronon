package ai.chronon.online.test

import ai.chronon.api.{Accuracy, Builders, Join}
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest}
import ai.chronon.online.fetcher.Fetcher.{Request, Response}
import ai.chronon.online.fetcher.{FetchContext, JoinPartFetcher, MetadataStore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Success

/** Opt-in microbenchmark for JoinPartFetcher request planning, deduplication, and response fanout.
  *
  * GroupByFetcher, Redis, the batch cache, value decoding, and aggregation merging are deliberately excluded:
  * fetchGroupBys immediately echoes one lightweight feature value for every deduplicated request.
  */
class JoinPartFetcherPlanningPerfTest extends AnyFlatSpec with Matchers {

  private val CandidateCount = 50
  private val ContextGroupByCount = 25
  private val CandidateGroupByCount = 25
  private val GroupByCount = ContextGroupByCount + CandidateGroupByCount
  private val UniqueGroupByRequestCount =
    ContextGroupByCount + (CandidateCount * CandidateGroupByCount)
  private val BenchmarkAtMillis = Some(1721606400000L)
  private val SharedContextId: AnyRef = "context-0"
  private val FeatureValue: AnyRef = java.lang.Long.valueOf(1L)

  private val contextGroupBys = (0 until ContextGroupByCount).map { index =>
    Builders.GroupBy(
      metaData = Builders.MetaData(name = f"benchmark.context_$index%02d"),
      keyColumns = Seq("context_id"),
      accuracy = Accuracy.SNAPSHOT
    )
  }

  private val candidateGroupBys = (0 until CandidateGroupByCount).map { index =>
    Builders.GroupBy(
      metaData = Builders.MetaData(name = f"benchmark.candidate_$index%02d"),
      keyColumns = Seq("candidate_id"),
      accuracy = Accuracy.SNAPSHOT
    )
  }

  private val benchmarkJoin: Join = Builders.Join(
    metaData = Builders.MetaData(name = "benchmark.join_part_planning"),
    joinParts = contextGroupBys.map { groupBy =>
      Builders.JoinPart(groupBy = groupBy, keyMapping = Map("context_id" -> "context_id"))
    } ++ candidateGroupBys.map { groupBy =>
      Builders.JoinPart(groupBy = groupBy, keyMapping = Map("candidate_id" -> "candidate_id"))
    }
  )

  private val joinRequests: Seq[Request] = (0 until CandidateCount).map { candidateIndex =>
    Request(
      benchmarkJoin.metaData.name,
      Map(
        "context_id" -> SharedContextId,
        "candidate_id" -> s"candidate-$candidateIndex"
      ),
      BenchmarkAtMillis
    )
  }

  behavior of "JoinPartFetcher planning and fanout"

  it should "model 50 candidates, 25 shared GroupBys, and 25 candidate GroupBys" in {
    val executor = newExecutor(2)
    try {
      val fetcher = newFetcher(executor)
      val responses = Await.result(fetcher.fetchJoins(joinRequests, Some(benchmarkJoin)), 30.seconds)

      validateShape(fetcher.capturedRequests.get(), responses)
    } finally {
      closeExecutor(executor)
    }
  }

  it should "run the opt-in planning and fanout concurrency sweep" in {
    if (!envBoolean("CHRONON_RUN_JOIN_PART_PLANNING_BENCHMARK", default = false)) {
      cancel(
        "Set CHRONON_RUN_JOIN_PART_PLANNING_BENCHMARK=true to run the JoinPartFetcher planning/fanout benchmark")
    }

    val warmupBatches = envPositiveInt("CHRONON_JOIN_PART_PLANNING_WARMUP_BATCHES", 50)
    val measuredBatches = envPositiveInt("CHRONON_JOIN_PART_PLANNING_MEASURED_BATCHES", 500)
    val batchConcurrencies = envPositiveInts("CHRONON_JOIN_PART_PLANNING_BATCH_CONCURRENCIES", Seq(1, 4, 16, 32))
    val executorThreadCounts =
      envPositiveInts("CHRONON_JOIN_PART_PLANNING_EXECUTOR_THREADS", Seq(1, 4, 16, 32))

    println(
      "JOIN_PART_PLANNING_FANOUT_SCOPE " +
        "included=JoinPartFetcher.fetchJoins_planning_dedup_response_fanout " +
        "fetchGroupBys=immediate_in_memory_echo " +
        "excluded=GroupByFetcher,Redis,batch_cache,value_decode,aggregation_merge")

    executorThreadCounts.foreach { executorThreads =>
      batchConcurrencies.foreach { batchConcurrency =>
        runScenario(executorThreads, batchConcurrency, warmupBatches, measuredBatches)
      }
    }
  }

  private final class ImmediateGroupByJoinPartFetcher(fetchContext: FetchContext, metadataStore: MetadataStore)
      extends JoinPartFetcher(fetchContext, metadataStore) {
    val capturedRequests = new AtomicReference[Seq[Request]]()

    override def fetchGroupBys(requests: Seq[Request]): Future[Seq[Response]] = {
      require(
        requests.size == UniqueGroupByRequestCount,
        s"Expected $UniqueGroupByRequestCount deduplicated GroupBy requests, got ${requests.size}"
      )
      capturedRequests.compareAndSet(null, requests)
      Future.successful(requests.map(request => Response(request, Success(Map("value" -> FeatureValue)))))
    }
  }

  private case class Measurement(latenciesNanos: Array[Long], wallNanos: Long)

  private final class UnusedKVStore extends KVStore {
    override implicit val executionContext: ExecutionContext = ExecutionContext.global
    override def create(dataset: String): Unit = ()
    override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] =
      Future.failed(new UnsupportedOperationException("JoinPartFetcher planning benchmark excludes KV reads"))
    override def multiPut(keyValueDatasets: Seq[PutRequest]): Future[Seq[Boolean]] =
      Future.failed(new UnsupportedOperationException("JoinPartFetcher planning benchmark excludes KV writes"))
    override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = ()
  }

  private def newFetcher(executor: ExecutionContextExecutorService): ImmediateGroupByJoinPartFetcher = {
    val fetchContext = FetchContext(new UnusedKVStore, executionContextOverride = executor)
    new ImmediateGroupByJoinPartFetcher(fetchContext, new MetadataStore(fetchContext))
  }

  private def runScenario(executorThreads: Int,
                          batchConcurrency: Int,
                          warmupBatches: Int,
                          measuredBatches: Int): Unit = {
    val fanoutExecutor = newExecutor(executorThreads)
    val driverExecutor = Executors.newFixedThreadPool(batchConcurrency)
    try {
      val fetcher = newFetcher(fanoutExecutor)
      val validationResponses = Await.result(fetcher.fetchJoins(joinRequests, Some(benchmarkJoin)), 30.seconds)
      validateShape(fetcher.capturedRequests.get(), validationResponses)

      runPhase(fetcher, driverExecutor, batchConcurrency, warmupBatches)
      val measurement = runPhase(fetcher, driverExecutor, batchConcurrency, measuredBatches)
      printResult(executorThreads, batchConcurrency, warmupBatches, measurement)
    } finally {
      driverExecutor.shutdownNow()
      driverExecutor.awaitTermination(30, TimeUnit.SECONDS)
      closeExecutor(fanoutExecutor)
    }
  }

  private def runPhase(fetcher: ImmediateGroupByJoinPartFetcher,
                       driverExecutor: java.util.concurrent.ExecutorService,
                       batchConcurrency: Int,
                       batches: Int): Measurement = {
    val latenciesNanos = new Array[Long](batches)
    val nextBatch = new AtomicInteger(0)
    val startGate = new CountDownLatch(1)
    val workerCount = math.min(batchConcurrency, batches)
    val workers = (0 until workerCount).map { _ =>
      driverExecutor.submit(new Callable[Unit] {
        override def call(): Unit = {
          startGate.await()
          var batchIndex = nextBatch.getAndIncrement()
          while (batchIndex < batches) {
            val startNanos = System.nanoTime()
            Await.result(fetcher.fetchJoins(joinRequests, Some(benchmarkJoin)), 30.seconds)
            latenciesNanos(batchIndex) = System.nanoTime() - startNanos
            batchIndex = nextBatch.getAndIncrement()
          }
        }
      })
    }

    val wallStartNanos = System.nanoTime()
    startGate.countDown()
    workers.foreach(_.get(10, TimeUnit.MINUTES))
    Measurement(latenciesNanos, System.nanoTime() - wallStartNanos)
  }

  private def validateShape(groupByRequests: Seq[Request], responses: Seq[Response]): Unit = {
    groupByRequests should have size UniqueGroupByRequestCount
    groupByRequests.map(request => (request.name, request.keys, request.atMillis)).distinct should have size
      UniqueGroupByRequestCount

    val contextNames = contextGroupBys.map(_.metaData.name).toSet
    val candidateNames = candidateGroupBys.map(_.metaData.name).toSet
    val contextRequests = groupByRequests.filter(request => contextNames.contains(request.name))
    val candidateRequests = groupByRequests.filter(request => candidateNames.contains(request.name))

    contextRequests should have size ContextGroupByCount
    contextRequests.foreach(_.keys shouldBe Map("context_id" -> SharedContextId))
    candidateRequests should have size (CandidateCount * CandidateGroupByCount)
    candidateNames.foreach { name =>
      candidateRequests.count(_.name == name) shouldBe CandidateCount
    }

    responses should have size CandidateCount
    responses.foreach { response =>
      response.values.isSuccess shouldBe true
      response.values.get should have size GroupByCount
    }
  }

  private def printResult(executorThreads: Int,
                          batchConcurrency: Int,
                          warmupBatches: Int,
                          measurement: Measurement): Unit = {
    val sorted = measurement.latenciesNanos.sorted
    val wallSeconds = measurement.wallNanos.toDouble / TimeUnit.SECONDS.toNanos(1)
    val batchesPerSecond = sorted.length / wallSeconds
    val averageMillis = sorted.iterator.map(_.toDouble).sum / sorted.length / TimeUnit.MILLISECONDS.toNanos(1)

    println(
      f"JOIN_PART_PLANNING_FANOUT_RESULT planning_threads=$batchConcurrency%d " +
        f"fanout_executor_threads=$executorThreads%d batch_concurrency=$batchConcurrency%d " +
        f"warmup_batches=$warmupBatches%d measured_batches=${sorted.length}%d " +
        f"join_part_planning_fanout_batches_per_sec=$batchesPerSecond%.2f " +
        f"avg_ms=$averageMillis%.3f p95_ms=${percentileMillis(sorted, 0.95)}%.3f " +
        f"p99_ms=${percentileMillis(sorted, 0.99)}%.3f " +
        f"unique_groupby_requests_per_batch=$UniqueGroupByRequestCount%d " +
        f"join_responses_per_batch=$CandidateCount%d features_per_response=$GroupByCount%d")
  }

  private def percentileMillis(sortedNanos: Array[Long], percentile: Double): Double = {
    val index = math.max(0, math.min(sortedNanos.length - 1, math.ceil(sortedNanos.length * percentile).toInt - 1))
    sortedNanos(index).toDouble / TimeUnit.MILLISECONDS.toNanos(1)
  }

  private def newExecutor(threads: Int): ExecutionContextExecutorService =
    scala.concurrent.ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(threads))

  private def closeExecutor(executor: ExecutionContextExecutorService): Unit = {
    executor.shutdown()
    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
      executor.shutdownNow()
    }
  }

  private def envBoolean(name: String, default: Boolean): Boolean =
    sys.env.get(name).map(_.trim.toBoolean).getOrElse(default)

  private def envPositiveInt(name: String, default: Int): Int = {
    val value = sys.env.get(name).map(_.trim.toInt).getOrElse(default)
    require(value > 0, s"$name must be positive, got $value")
    value
  }

  private def envPositiveInts(name: String, defaults: Seq[Int]): Seq[Int] = {
    val values = sys.env
      .get(name)
      .map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).map(_.toInt).toSeq)
      .getOrElse(defaults)
      .distinct
    require(values.nonEmpty && values.forall(_ > 0), s"$name must contain positive integers")
    values
  }
}
