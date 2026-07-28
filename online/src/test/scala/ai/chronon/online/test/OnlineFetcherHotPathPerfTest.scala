package ai.chronon.online.test

import ai.chronon.api.{Accuracy, Builders, GroupBy, GroupByServingInfo, StringType, StructField, StructType}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest, TimedValue}
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.fetcher.FetcherCache.{BatchIrCache, BatchResponses, CachedBatchResponse}
import ai.chronon.online.fetcher.{FetchContext, FetcherCache, GroupByFetcher, LambdaKvRequest, MetadataStore}
import ai.chronon.online.metrics.{Metrics, TTLCache}
import ai.chronon.online.serde.AvroConversions
import ai.chronon.online.{GroupByServingInfoParsed, KVStore}
import org.mockito.Mockito.{mock, mockingDetails}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success, Try}

/** Opt-in microbenchmarks for the CPU-side fetcher work surrounding a KV multi-get.
  *
  * Each scenario isolates one production change so sequential commits can be attributed without Redis noise:
  *   - cache_lookup calls FetcherCache.getCachedRequests directly;
  *   - groupby_planning stops immediately after GroupByFetcher constructs the KV requests;
  *   - groupby_response_metrics echoes empty KV responses and replaces value decoding with a no-op.
  *
  * The workload is the same deduplicated 50-candidate shape used by the Redis benchmark: 25 shared context
  * GroupBys, 25 candidate GroupBys, and 10 temporal GroupBys split 5/5 across context and candidate ownership.
  */
class OnlineFetcherHotPathPerfTest extends AnyFlatSpec with Matchers {

  private val CandidateCount = 50
  private val ContextGroupByCount = 25
  private val CandidateGroupByCount = 25
  private val ContextLastGroupByCount = 3
  private val ContextTiledGroupByCount = 2
  private val CandidateLastGroupByCount = 2
  private val CandidateTiledGroupByCount = 3
  private val GroupByCount = ContextGroupByCount + CandidateGroupByCount
  private val ContextTemporalGroupByCount = ContextLastGroupByCount + ContextTiledGroupByCount
  private val CandidateTemporalGroupByCount = CandidateLastGroupByCount + CandidateTiledGroupByCount
  private val UniqueGroupByRequestCount = ContextGroupByCount + CandidateCount * CandidateGroupByCount
  private val UniqueStreamingRequestCount =
    ContextTemporalGroupByCount + CandidateCount * CandidateTemporalGroupByCount
  private val KvRequestCount = UniqueGroupByRequestCount + UniqueStreamingRequestCount
  private val DistinctMetricsContextCount = GroupByCount
  private val LegacyMultiGetMetricWrites = UniqueGroupByRequestCount * 4
  private val DeduplicatedMultiGetMetricWrites = DistinctMetricsContextCount * 4
  private val MultiGetMetricNames = Set(
    "multi_get.batch.size",
    "multi_get.bytes",
    "multi_get.response.length",
    "multi_get.latency.millis"
  )
  private val BenchmarkAtMillis = 1721606400000L
  private val SharedContextId = "context-0"
  private val KeyColumn = "entity_id"
  private val BatchIrCacheSizeProperty = "ai.chronon.fetcher.batch_ir_cache_size_elements"
  private val KeySchema = AvroConversions
    .fromChrononSchema(StructType("BenchmarkKey", Array(StructField(KeyColumn, StringType))))
    .toString

  private sealed trait Ownership
  private case object ContextOwnership extends Ownership
  private case object CandidateOwnership extends Ownership

  private final case class GroupByDefinition(name: String, ownership: Ownership, temporal: Boolean)
  private final case class PreparedGroupBy(definition: GroupByDefinition,
                                           groupBy: GroupBy,
                                           servingInfo: GroupByServingInfoParsed,
                                           metricsContext: Metrics.Context)
  private final case class PreparedRequest(groupBy: PreparedGroupBy,
                                           request: Request,
                                           candidateIndex: Option[Int])

  private val groupByDefinitions: Vector[GroupByDefinition] = {
    def definitions(prefix: String, ownership: Ownership, temporal: Boolean, count: Int): Vector[GroupByDefinition] =
      (0 until count).map(index => GroupByDefinition(f"benchmark.$prefix.$index%02d", ownership, temporal)).toVector

    definitions("context_last", ContextOwnership, temporal = true, ContextLastGroupByCount) ++
      definitions("context_tiled", ContextOwnership, temporal = true, ContextTiledGroupByCount) ++
      definitions("context_snapshot",
                  ContextOwnership,
                  temporal = false,
                  ContextGroupByCount - ContextTemporalGroupByCount) ++
      definitions("candidate_last", CandidateOwnership, temporal = true, CandidateLastGroupByCount) ++
      definitions("candidate_tiled", CandidateOwnership, temporal = true, CandidateTiledGroupByCount) ++
      definitions("candidate_snapshot",
                  CandidateOwnership,
                  temporal = false,
                  CandidateGroupByCount - CandidateTemporalGroupByCount)
  }

  private val preparedGroupBys: Vector[PreparedGroupBy] = groupByDefinitions.map { definition =>
    val groupBy = Builders.GroupBy(
      sources = if (definition.temporal) {
        Seq(
          Builders.Source.events(
            query = Builders.Query(),
            table = s"${definition.name}.events",
            topic = s"${definition.name}.topic"
          ))
      } else Seq.empty,
      metaData = Builders.MetaData(name = definition.name),
      keyColumns = Seq(KeyColumn),
      accuracy = if (definition.temporal) Accuracy.TEMPORAL else Accuracy.SNAPSHOT
    )
    val rawServingInfo = new GroupByServingInfo()
      .setGroupBy(groupBy)
      .setKeyAvroSchema(KeySchema)
      .setBatchEndTs(BenchmarkAtMillis)
    PreparedGroupBy(
      definition,
      groupBy,
      new GroupByServingInfoParsed(rawServingInfo),
      Metrics.Context(
        environment = Metrics.Environment.GroupByFetching,
        groupBy = definition.name,
        accuracy = if (definition.temporal) Accuracy.TEMPORAL else Accuracy.SNAPSHOT
      )
    )
  }

  private val preparedRequests: Vector[PreparedRequest] = {
    val contextRequests = preparedGroupBys
      .filter(_.definition.ownership == ContextOwnership)
      .map { preparedGroupBy =>
        PreparedRequest(
          preparedGroupBy,
          Request(
            preparedGroupBy.definition.name,
            Map(KeyColumn -> SharedContextId.asInstanceOf[AnyRef]),
            Some(BenchmarkAtMillis),
            Some(preparedGroupBy.metricsContext)
          ),
          None
        )
      }

    val candidateGroupBys = preparedGroupBys.filter(_.definition.ownership == CandidateOwnership)
    val candidateRequests = (0 until CandidateCount).flatMap { candidateIndex =>
      candidateGroupBys.map { preparedGroupBy =>
        PreparedRequest(
          preparedGroupBy,
          Request(
            preparedGroupBy.definition.name,
            Map(KeyColumn -> f"candidate-$candidateIndex%02d".asInstanceOf[AnyRef]),
            Some(BenchmarkAtMillis),
            Some(preparedGroupBy.metricsContext)
          ),
          Some(candidateIndex)
        )
      }
    }
    contextRequests ++ candidateRequests
  }

  private val groupByRequests: Vector[Request] = preparedRequests.map(_.request)
  private val servingInfoByName: Map[String, GroupByServingInfoParsed] =
    preparedGroupBys.iterator.map(prepared => prepared.definition.name -> prepared.servingInfo).toMap

  behavior of "online fetcher hot-path workload"

  it should "model the exact deduplicated GroupBy and KV request shape" in {
    preparedGroupBys should have size GroupByCount
    preparedRequests should have size UniqueGroupByRequestCount
    preparedRequests.count(_.candidateIndex.isEmpty) shouldBe ContextGroupByCount
    preparedRequests.count(_.candidateIndex.isDefined) shouldBe CandidateCount * CandidateGroupByCount
    preparedRequests.count(_.groupBy.definition.temporal) shouldBe UniqueStreamingRequestCount
    preparedRequests.map(_.request.context.get).distinct should have size DistinctMetricsContextCount

    val validation = validateGroupByPlanningShape()
    validation.keyEncodes shouldBe KvRequestCount.toLong
    validation.kvRequests shouldBe KvRequestCount
    val metricsValidation = validateGroupByResponseMetricsShape()
    Set(LegacyMultiGetMetricWrites, DeduplicatedMultiGetMetricWrites) should contain(metricsValidation.metricWrites)

    Seq(0, 50, 80, 100).foreach { hitPercent =>
      val fixture = newCacheFixture(hitPercent)
      fixture.fetcherCache.getCachedRequests(fixture.requests).size shouldBe expectedCacheHits(hitPercent)
    }

    println(
      s"ONLINE_FETCHER_HOT_PATH_SHAPE candidates=$CandidateCount groupbys=$GroupByCount " +
        s"unique_groupby_requests=$UniqueGroupByRequestCount unique_streaming_requests=$UniqueStreamingRequestCount " +
        s"kv_requests=$KvRequestCount key_encodes_observed=${validation.keyEncodes} " +
        s"multi_get_metric_writes_observed=${metricsValidation.metricWrites} " +
        s"legacy_metric_writes=$LegacyMultiGetMetricWrites deduplicated_metric_writes=$DeduplicatedMultiGetMetricWrites")
  }

  it should "skip streaming reads for temporal GroupBys without a topic" in {
    val name = "benchmark.batch_only_temporal"
    val groupBy = Builders.GroupBy(
      sources = Seq(Builders.Source.events(query = Builders.Query(), table = s"$name.events")),
      metaData = Builders.MetaData(name = name),
      keyColumns = Seq(KeyColumn),
      accuracy = Accuracy.TEMPORAL
    )
    val servingInfo = new GroupByServingInfoParsed(
      new GroupByServingInfo()
        .setGroupBy(groupBy)
        .setKeyAvroSchema(KeySchema)
        .setBatchEndTs(BenchmarkAtMillis))
    val fixture = newGroupByFixture(
      executorThreads = 1,
      failAfterPlanning = true,
      countKeyEncodes = true,
      servingInfos = Map(name -> servingInfo)
    )
    try {
      awaitPlanningComplete(
        fixture.fetcher.fetchGroupBys(
          Seq(Request(name, Map(KeyColumn -> "batch-only".asInstanceOf[AnyRef]), Some(BenchmarkAtMillis)))))
      fixture.store.keyEncodeCount.get() shouldBe 1L
      fixture.store.lastMultiGetSize.get() shouldBe 1
    } finally {
      closeExecutor(fixture.executor)
    }
  }

  it should "run the opt-in cache lookup benchmark" in {
    requireBenchmarkScenario("cache_lookup")
    val warmupBatches = envPositiveInt("CHRONON_ONLINE_FETCHER_WARMUP_BATCHES", 100)
    val measuredBatches = envPositiveInt("CHRONON_ONLINE_FETCHER_MEASURED_BATCHES", 1000)
    val batchConcurrencies = envPositiveInts("CHRONON_ONLINE_FETCHER_BATCH_CONCURRENCIES", Seq(1, 4))
    val hitPercents = envNonNegativeInts("CHRONON_ONLINE_FETCHER_CACHE_HIT_PERCENTS", Seq(0, 50, 80, 100))
    require(hitPercents.forall(_ <= 100), "CHRONON_ONLINE_FETCHER_CACHE_HIT_PERCENTS values must be <= 100")

    println(
      "ONLINE_FETCHER_HOT_PATH_SCOPE scenario=cache_lookup " +
        "included=FetcherCache.getCachedRequests,Caffeine_lookup,cache_hit_miss_metrics " +
        "excluded=GroupByFetcher,Redis,key_encode,value_decode,aggregation_merge")

    hitPercents.foreach { hitPercent =>
      val fixture = newCacheFixture(hitPercent)
      val expectedHits = expectedCacheHits(hitPercent)
      fixture.fetcherCache.getCachedRequests(fixture.requests).size shouldBe expectedHits
      batchConcurrencies.foreach { batchConcurrency =>
        runScenario(
          scenario = "cache_lookup",
          batchConcurrency = batchConcurrency,
          warmupBatches = warmupBatches,
          measuredBatches = measuredBatches,
          fields = s"cache_hit_percent=$hitPercent expected_cache_hits=$expectedHits",
          operation = () => fixture.fetcherCache.getCachedRequests(fixture.requests).size.toLong
        )
      }
    }
  }

  it should "run the opt-in GroupBy request planning benchmark" in {
    requireBenchmarkScenario("groupby_planning")
    val warmupBatches = envPositiveInt("CHRONON_ONLINE_FETCHER_WARMUP_BATCHES", 100)
    val measuredBatches = envPositiveInt("CHRONON_ONLINE_FETCHER_MEASURED_BATCHES", 1000)
    val batchConcurrencies = envPositiveInts("CHRONON_ONLINE_FETCHER_BATCH_CONCURRENCIES", Seq(1, 4))
    val executorThreadCounts = envPositiveInts("CHRONON_ONLINE_FETCHER_EXECUTOR_THREADS", Seq(1, 4))
    val validation = validateGroupByPlanningShape()

    println(
      "ONLINE_FETCHER_HOT_PATH_SCOPE scenario=groupby_planning " +
        "included=GroupByFetcher.metadata_cache,key_cast,key_encode,GetRequest_planning " +
        "multiGet=immediate_failed_future " +
        "excluded=Redis,Caffeine,response_metrics,value_decode,aggregation_merge")

    executorThreadCounts.foreach { executorThreads =>
      val fixture = newGroupByFixture(executorThreads, failAfterPlanning = true, countKeyEncodes = false)
      try {
        batchConcurrencies.foreach { batchConcurrency =>
          runScenario(
            scenario = "groupby_planning",
            batchConcurrency = batchConcurrency,
            warmupBatches = warmupBatches,
            measuredBatches = measuredBatches,
            fields =
              s"fetcher_executor_threads=$executorThreads key_encodes_per_batch=${validation.keyEncodes} " +
                s"kv_requests_per_batch=$KvRequestCount",
            operation = () => {
              awaitPlanningComplete(fixture.fetcher.fetchGroupBys(groupByRequests))
              UniqueGroupByRequestCount.toLong
            }
          )
        }
      } finally {
        closeExecutor(fixture.executor)
      }
    }
  }

  it should "run the opt-in multi-get response metrics benchmark" in {
    requireBenchmarkScenario("groupby_response_metrics")
    val warmupBatches = envPositiveInt("CHRONON_ONLINE_FETCHER_WARMUP_BATCHES", 100)
    val measuredBatches = envPositiveInt("CHRONON_ONLINE_FETCHER_MEASURED_BATCHES", 1000)
    val batchConcurrencies = envPositiveInts("CHRONON_ONLINE_FETCHER_BATCH_CONCURRENCIES", Seq(1, 4))
    val executorThreadCounts = envPositiveInts("CHRONON_ONLINE_FETCHER_EXECUTOR_THREADS", Seq(1, 4))
    val metricsValidation = validateGroupByResponseMetricsShape()
    Set(LegacyMultiGetMetricWrites, DeduplicatedMultiGetMetricWrites) should contain(metricsValidation.metricWrites)

    println(
      "ONLINE_FETCHER_HOT_PATH_SCOPE scenario=groupby_response_metrics " +
        "included=GroupByFetcher.request_planning,response_index,multi_get_metrics,response_fanout " +
        "multiGet=immediate_empty_echo decodeAndMerge=no_op " +
        "excluded=Redis,Caffeine,value_decode,aggregation_merge")

    executorThreadCounts.foreach { executorThreads =>
      val fixture = newGroupByFixture(executorThreads, failAfterPlanning = false, countKeyEncodes = false)
      try {
        val validationResponses = Await.result(fixture.fetcher.fetchGroupBys(groupByRequests), 30.seconds)
        validationResponses should have size UniqueGroupByRequestCount
        validationResponses.forall(_.values.isSuccess) shouldBe true

        batchConcurrencies.foreach { batchConcurrency =>
          runScenario(
            scenario = "groupby_response_metrics",
            batchConcurrency = batchConcurrency,
            warmupBatches = warmupBatches,
            measuredBatches = measuredBatches,
            fields =
              s"fetcher_executor_threads=$executorThreads legacy_metric_writes_per_batch=$LegacyMultiGetMetricWrites " +
                s"deduplicated_metric_writes_per_batch=$DeduplicatedMultiGetMetricWrites " +
                s"multi_get_metric_writes_observed=${metricsValidation.metricWrites}",
            operation = () =>
              Await.result(fixture.fetcher.fetchGroupBys(groupByRequests), 30.seconds).size.toLong
          )
        }
      } finally {
        closeExecutor(fixture.executor)
      }
    }
  }

  private final case class CacheFixture(fetcherCache: BenchmarkFetcherCache,
                                        requests: Seq[(Request, Try[LambdaKvRequest])])

  private final class BenchmarkFetcherCache(cache: BatchIrCache) extends FetcherCache {
    override val maybeBatchIrCache: Option[BatchIrCache] = Some(cache)
    override def isCachingEnabled(groupBy: GroupBy): Boolean = true
  }

  private def newCacheFixture(hitPercent: Int): CacheFixture = {
    val cache = new BatchIrCache(s"fetcher-hot-path-$hitPercent", UniqueGroupByRequestCount * 2)
    val fetcherCache = new BenchmarkFetcherCache(cache)
    val cachedValue: CachedBatchResponse = BatchResponses(Map("value" -> java.lang.Long.valueOf(1L)))
    val requests = preparedRequests.map { prepared =>
      val batchRequest = GetRequest(
        s"${prepared.request.name}:${prepared.request.keys(KeyColumn)}".getBytes(StandardCharsets.UTF_8),
        prepared.groupBy.servingInfo.groupByOps.batchDataset
      )
      val lambdaRequest = LambdaKvRequest(
        prepared.groupBy.servingInfo,
        prepared.request,
        batchRequest,
        None,
        prepared.request.atMillis,
        prepared.groupBy.metricsContext
      )
      if (isCacheHit(prepared, hitPercent)) {
        val cacheKey = BatchIrCache.Key(batchRequest.dataset,
                                        prepared.request.keys,
                                        prepared.groupBy.servingInfo.batchEndTsMillis)
        cache.cache.put(cacheKey, cachedValue)
      }
      prepared.request -> Success(lambdaRequest)
    }
    CacheFixture(fetcherCache, requests)
  }

  private def isCacheHit(request: PreparedRequest, hitPercent: Int): Boolean = {
    if (hitPercent == 0) false
    else {
      request.candidateIndex match {
        case None => true
        case Some(candidateIndex) =>
          val candidateMisses = math.ceil(CandidateCount * (100 - hitPercent) / 100.0).toInt
          candidateIndex >= candidateMisses
      }
    }
  }

  private def expectedCacheHits(hitPercent: Int): Int = {
    if (hitPercent == 0) 0
    else {
      val candidateMisses = math.ceil(CandidateCount * (100 - hitPercent) / 100.0).toInt
      ContextGroupByCount + (CandidateCount - candidateMisses) * CandidateGroupByCount
    }
  }

  private object PlanningComplete extends RuntimeException("planning complete", null, false, false)

  private final class BenchmarkKVStore(val failAfterPlanning: Boolean, val countKeyEncodes: Boolean) extends KVStore {
    override implicit val executionContext: ExecutionContext = ExecutionContext.global
    val keyEncodeCount = new AtomicLong(0L)
    val lastMultiGetSize = new AtomicInteger(0)

    override def create(dataset: String): Unit = ()

    override def createKeyBytes(keys: Map[String, AnyRef],
                                groupByServingInfo: GroupByServingInfoParsed,
                                dataset: String): Array[Byte] = {
      if (countKeyEncodes) keyEncodeCount.incrementAndGet()
      super.createKeyBytes(keys, groupByServingInfo, dataset)
    }

    override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
      lastMultiGetSize.set(requests.size)
      if (failAfterPlanning) Future.failed(PlanningComplete)
      else Future.successful(requests.map(request => GetResponse(request, Success(Seq.empty))))
    }

    override def multiPut(keyValueDatasets: Seq[PutRequest]): Future[Seq[Boolean]] =
      Future.successful(keyValueDatasets.map(_ => true))

    override def bulkPut(sourceOfflineTable: String,
                         destinationOnlineDataSet: String,
                         partition: String): Unit = ()
  }

  private final class StaticMetadataStore(fetchContext: FetchContext,
                                          servingInfos: Map[String, GroupByServingInfoParsed])
      extends MetadataStore(fetchContext) {
    override lazy val getGroupByServingInfo: TTLCache[String, Try[GroupByServingInfoParsed]] =
      new TTLCache[String, Try[GroupByServingInfoParsed]](
        name => Success(servingInfos(name)),
        name => Metrics.Context(Metrics.Environment.MetaDataFetching, groupBy = name),
        ttlMillis = 24.hours.toMillis
      )
  }

  private final class BenchmarkGroupByFetcher(fetchContext: FetchContext, metadataStore: MetadataStore)
      extends GroupByFetcher(fetchContext, metadataStore) {
    override def decodeAndMerge(batchResponses: BatchResponses,
                                streamingResponsesOpt: Option[Seq[TimedValue]],
                                requestContext: RequestContext): Map[String, AnyRef] = Map.empty
  }

  private final case class GroupByFixture(fetcher: BenchmarkGroupByFetcher,
                                          store: BenchmarkKVStore,
                                          executor: ExecutionContextExecutorService)

  private def newGroupByFixture(executorThreads: Int,
                                failAfterPlanning: Boolean,
                                countKeyEncodes: Boolean,
                                servingInfos: Map[String, GroupByServingInfoParsed] = servingInfoByName): GroupByFixture = {
    val executor = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(executorThreads))
    val store = new BenchmarkKVStore(failAfterPlanning, countKeyEncodes)
    val fetchContext = FetchContext(store, executionContextOverride = executor, kvTimeoutMillis = 0L)
    val metadataStore = new StaticMetadataStore(fetchContext, servingInfos)
    servingInfos.keys.foreach(metadataStore.getGroupByServingInfo(_))
    val fetcher = this.synchronized {
      val previousCacheSize = Option(System.getProperty(BatchIrCacheSizeProperty))
      System.setProperty(BatchIrCacheSizeProperty, "0")
      try new BenchmarkGroupByFetcher(fetchContext, metadataStore)
      finally {
        previousCacheSize match {
          case Some(value) => System.setProperty(BatchIrCacheSizeProperty, value)
          case None        => System.clearProperty(BatchIrCacheSizeProperty)
        }
      }
    }
    GroupByFixture(fetcher, store, executor)
  }

  private final case class PlanningValidation(keyEncodes: Long, kvRequests: Int)
  private final case class MetricsValidation(metricWrites: Int)

  private def validateGroupByPlanningShape(): PlanningValidation = {
    val fixture = newGroupByFixture(executorThreads = 1, failAfterPlanning = true, countKeyEncodes = true)
    try {
      awaitPlanningComplete(fixture.fetcher.fetchGroupBys(groupByRequests))
      PlanningValidation(fixture.store.keyEncodeCount.get(), fixture.store.lastMultiGetSize.get())
    } finally {
      closeExecutor(fixture.executor)
    }
  }

  private def validateGroupByResponseMetricsShape(): MetricsValidation = {
    val contextsByName = preparedGroupBys.iterator.map { prepared =>
      prepared.definition.name -> mock(classOf[Metrics.Context])
    }.toMap
    val requests = groupByRequests.map(request => request.copy(context = Some(contextsByName(request.name))))
    val fixture = newGroupByFixture(executorThreads = 1, failAfterPlanning = false, countKeyEncodes = false)
    try {
      val responses = Await.result(fixture.fetcher.fetchGroupBys(requests), 30.seconds)
      responses should have size UniqueGroupByRequestCount
      responses.forall(_.values.isSuccess) shouldBe true
      val metricWrites = contextsByName.valuesIterator.map { context =>
        mockingDetails(context).getInvocations.asScala.count { invocation =>
          invocation.getArguments.headOption.exists {
            case metric: String => MultiGetMetricNames.contains(metric)
            case _              => false
          }
        }
      }.sum
      MetricsValidation(metricWrites)
    } finally {
      closeExecutor(fixture.executor)
    }
  }

  private def awaitPlanningComplete(future: Future[_]): Unit = {
    Await.ready(future, 30.seconds).value match {
      case Some(Failure(error)) if error eq PlanningComplete => ()
      case Some(Failure(error)) =>
        throw new IllegalStateException("GroupBy planning failed before reaching multiGet", error)
      case Some(Success(_)) =>
        throw new IllegalStateException("GroupBy planning unexpectedly completed multiGet")
      case None =>
        throw new IllegalStateException("GroupBy planning future was not completed")
    }
  }

  private final case class Measurement(latenciesNanos: Array[Long], wallNanos: Long, checksum: Long)

  private def runScenario(scenario: String,
                          batchConcurrency: Int,
                          warmupBatches: Int,
                          measuredBatches: Int,
                          fields: String,
                          operation: () => Long): Unit = {
    val driverExecutor = Executors.newFixedThreadPool(batchConcurrency)
    try {
      runPhase(operation, driverExecutor, batchConcurrency, warmupBatches)
      val measurement = runPhase(operation, driverExecutor, batchConcurrency, measuredBatches)
      printResult(scenario, batchConcurrency, warmupBatches, fields, measurement)
    } finally {
      driverExecutor.shutdownNow()
      driverExecutor.awaitTermination(30, TimeUnit.SECONDS)
    }
  }

  private def runPhase(operation: () => Long,
                       driverExecutor: java.util.concurrent.ExecutorService,
                       batchConcurrency: Int,
                       batches: Int): Measurement = {
    val latenciesNanos = new Array[Long](batches)
    val nextBatch = new AtomicInteger(0)
    val checksum = new AtomicLong(0L)
    val startGate = new CountDownLatch(1)
    val workerCount = math.min(batchConcurrency, batches)
    val workers = (0 until workerCount).map { _ =>
      driverExecutor.submit(new Callable[Unit] {
        override def call(): Unit = {
          startGate.await()
          var localChecksum = 0L
          var batchIndex = nextBatch.getAndIncrement()
          while (batchIndex < batches) {
            val startedNanos = System.nanoTime()
            localChecksum += operation()
            latenciesNanos(batchIndex) = System.nanoTime() - startedNanos
            batchIndex = nextBatch.getAndIncrement()
          }
          checksum.addAndGet(localChecksum)
        }
      })
    }

    val wallStartNanos = System.nanoTime()
    startGate.countDown()
    workers.foreach(_.get(10, TimeUnit.MINUTES))
    Measurement(latenciesNanos, System.nanoTime() - wallStartNanos, checksum.get())
  }

  private def printResult(scenario: String,
                          batchConcurrency: Int,
                          warmupBatches: Int,
                          fields: String,
                          measurement: Measurement): Unit = {
    val sorted = measurement.latenciesNanos.sorted
    val wallSeconds = measurement.wallNanos.toDouble / TimeUnit.SECONDS.toNanos(1)
    val batchesPerSecond = sorted.length / wallSeconds
    val averageMillis = sorted.iterator.map(_.toDouble).sum / sorted.length / TimeUnit.MILLISECONDS.toNanos(1)

    println(
      f"ONLINE_FETCHER_HOT_PATH_RESULT scenario=$scenario batch_concurrency=$batchConcurrency%d " +
        f"warmup_batches=$warmupBatches%d measured_batches=${sorted.length}%d " +
        f"batches_per_sec=$batchesPerSecond%.2f avg_ms=$averageMillis%.3f " +
        f"p95_ms=${percentileMillis(sorted, 0.95)}%.3f p99_ms=${percentileMillis(sorted, 0.99)}%.3f " +
        s"checksum=${measurement.checksum} $fields")
  }

  private def percentileMillis(sortedNanos: Array[Long], percentile: Double): Double = {
    val index = math.max(0, math.min(sortedNanos.length - 1, math.ceil(sortedNanos.length * percentile).toInt - 1))
    sortedNanos(index).toDouble / TimeUnit.MILLISECONDS.toNanos(1)
  }

  private def closeExecutor(executor: ExecutionContextExecutorService): Unit = {
    executor.shutdown()
    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) executor.shutdownNow()
  }

  private def requireBenchmarkScenario(scenario: String): Unit = {
    val runBenchmarks = envBoolean("CHRONON_RUN_ONLINE_FETCHER_HOT_PATH_BENCHMARK", default = false)
    val scenarios = sys.env
      .get("CHRONON_ONLINE_FETCHER_BENCHMARK_SCENARIOS")
      .map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).toSet)
      .getOrElse(Set("cache_lookup", "groupby_planning", "groupby_response_metrics"))
    if (!runBenchmarks || !scenarios.contains(scenario)) {
      cancel(
        s"Set CHRONON_RUN_ONLINE_FETCHER_HOT_PATH_BENCHMARK=true and include $scenario in " +
          "CHRONON_ONLINE_FETCHER_BENCHMARK_SCENARIOS to run this benchmark")
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
    val values = envInts(name, defaults)
    require(values.nonEmpty && values.forall(_ > 0), s"$name must contain positive integers")
    values
  }

  private def envNonNegativeInts(name: String, defaults: Seq[Int]): Seq[Int] = {
    val values = envInts(name, defaults)
    require(values.nonEmpty && values.forall(_ >= 0), s"$name must contain non-negative integers")
    values
  }

  private def envInts(name: String, defaults: Seq[Int]): Seq[Int] =
    sys.env
      .get(name)
      .map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).map(_.toInt).toSeq)
      .getOrElse(defaults)
      .distinct
}
