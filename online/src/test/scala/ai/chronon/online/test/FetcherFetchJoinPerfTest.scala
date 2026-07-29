package ai.chronon.online.test

import ai.chronon.api.Extensions.{GroupByOps, JoinOps, MetadataOps}
import ai.chronon.api.{
  Accuracy,
  Builders,
  Constants,
  GroupBy,
  GroupByServingInfo,
  Join,
  LongType,
  StringType,
  StructField,
  StructType,
  ThriftJsonCodec
}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest, TimedValue}
import ai.chronon.online.fetcher.Fetcher.{Request, Response}
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import ai.chronon.online.{GroupByServingInfoParsed, KVStore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Arrays
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{Callable, CountDownLatch, Executors, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Try

/** Opt-in benchmark for the top-level Fetcher.fetchJoin CPU path with an immediate KV backend.
  *
  * The 80 request batches keep one context key fixed while rotating 4,000 candidate keys. Each batch contains 50
  * candidates and a Join with 25 context GroupBys plus 25 candidate GroupBys. Ten GroupBys are temporal, split evenly
  * across context and candidate ownership, so GroupByFetcher plans 255 streaming reads in addition to 1,275 batch reads.
  *
  * Batch reads return a valid one-field Avro record. Streaming reads return no values and the GroupBys have no
  * aggregations. The benchmark therefore includes batch Avro decoding but deliberately excludes streaming value/tile
  * decoding and temporal aggregation. Redis, Jedis, network I/O, external feature calls, explicit GroupBy derivations,
  * and sampled response encoding/publishing are also excluded by default. Set CHRONON_FETCH_JOIN_CONTEXTUAL_EXTERNAL
  * to add one contextual `query` field shared by all 50 candidates and exercise contextual projection and response
  * fanout. The no-external-parts branch, including its per-row cached join-metadata checks,
  * remains the default measured top-level path.
  */
class FetcherFetchJoinPerfTest extends AnyFlatSpec with Matchers {
  import FetcherFetchJoinPerfTest._

  behavior of "Fetcher.fetchJoin production-shaped immediate-KV workload"

  it should "model 80 rotating 50-candidate batches and the exact deduplicated request shape" in {
    preparedGroupBys should have size GroupByCount
    preparedGroupBys.count(_.ownership == ContextOwnership) shouldBe ContextGroupByCount
    preparedGroupBys.count(_.ownership == CandidateOwnership) shouldBe CandidateGroupByCount
    preparedGroupBys.count(_.temporal) shouldBe TemporalGroupByCount

    requestBatches should have size RequestBatchCount
    all(requestBatches.map(_.size)) shouldBe CandidateCount
    requestBatches.iterator
      .flatMap(_.iterator.map(_.keys(ContextKey)))
      .toSet shouldBe Set(SharedContextId)
    requestBatches.iterator
      .flatMap(_.iterator.map(_.keys(CandidateKey)))
      .toSet should have size (RequestBatchCount * CandidateCount)

    Seq(false, true).foreach { contextualExternal =>
      val fixture = newFixture(executorThreads = 4, cacheSize = 0, contextualExternal)
      try {
        val responses = Await.result(fixture.fetcher.fetchJoin(requestBatches(contextualExternal).head), 30.seconds)
        validateResponses(responses, contextualExternal)

        fixture.store.featureMultiGetCalls.get() shouldBe 1L
        fixture.store.lastFeatureMultiGetSize.get() shouldBe KvRequestCount
        fixture.store.lastBatchRequestCount.get() shouldBe UniqueGroupByRequestCount
        fixture.store.lastStreamingRequestCount.get() shouldBe UniqueStreamingRequestCount
        fixture.store.keyEncodeCount.get() shouldBe ExpectedKeyEncodeCount.toLong
      } finally {
        closeExecutor(fixture.executor)
      }
    }
  }

  it should "run the opt-in Fetcher.fetchJoin thread and concurrency sweep" in {
    if (!envBoolean("CHRONON_RUN_FETCH_JOIN_BENCHMARK", default = false)) {
      cancel("Set CHRONON_RUN_FETCH_JOIN_BENCHMARK=true to run the Fetcher.fetchJoin benchmark")
    }

    val warmupBatches = envPositiveInt("CHRONON_FETCH_JOIN_WARMUP_BATCHES", 100)
    val measuredBatches = envPositiveInt("CHRONON_FETCH_JOIN_MEASURED_BATCHES", 1000)
    val batchConcurrencies = envPositiveInts("CHRONON_FETCH_JOIN_BATCH_CONCURRENCIES", Seq(1, 4, 16))
    val executorThreadCounts = envPositiveInts("CHRONON_FETCH_JOIN_EXECUTOR_THREADS", Seq(4, 16, 32))
    val cacheSizes = envNonNegativeInts("CHRONON_FETCH_JOIN_CACHE_SIZES", Seq(0, 10000))
    val contextualExternal = envBoolean("CHRONON_FETCH_JOIN_CONTEXTUAL_EXTERNAL", default = false)
    val externalScope =
      if (contextualExternal) "contextual_request_projection_response_fanout"
      else "empty_external_path_metadata_checks"
    val excludedExternal = if (contextualExternal) "non_contextual_external_feature_calls" else "external_feature_calls"

    println(
      s"FETCH_JOIN_END_TO_END_SCOPE backend=immediate_in_memory entrypoint=Fetcher.fetchJoin " +
        s"contextual_external=$contextualExternal included=join_metadata_TTL_cache,join_codec_cache,$externalScope," +
        s"JoinPartFetcher_planning_dedup_fanout,GroupByFetcher_metadata_key_encode_request_planning_response_index," +
        s"Caffeine_lookup_fill_when_enabled,batch_Avro_decode,identity_join_derivation,response_finalization " +
        s"excluded=Redis,Jedis,network_IO,streaming_value_decode,tile_decode,temporal_aggregation," +
        s"$excludedExternal,explicit_groupby_derivations,sampled_response_encode_publish " +
        s"candidates=$CandidateCount request_batch_ring=$RequestBatchCount group_bys=$GroupByCount " +
        s"context_group_bys=$ContextGroupByCount candidate_group_bys=$CandidateGroupByCount " +
        s"temporal_group_bys=$TemporalGroupByCount logical_groupby_requests=$LogicalGroupByRequestCount " +
        s"unique_groupby_requests=$UniqueGroupByRequestCount batch_kv_requests=$UniqueGroupByRequestCount " +
        s"streaming_kv_requests=$UniqueStreamingRequestCount kv_requests=$KvRequestCount " +
        s"expected_key_encodes=$ExpectedKeyEncodeCount " +
        s"batch_concurrencies=${batchConcurrencies.mkString(",")} " +
        s"executor_threads=${executorThreadCounts.mkString(",")} cache_sizes=${cacheSizes.mkString(",")}")

    executorThreadCounts.foreach { executorThreads =>
      cacheSizes.foreach { cacheSize =>
        batchConcurrencies.foreach { batchConcurrency =>
          runScenario(
            executorThreads,
            cacheSize,
            batchConcurrency,
            warmupBatches,
            measuredBatches,
            contextualExternal)
        }
      }
    }
  }

  private def runScenario(executorThreads: Int,
                          cacheSize: Int,
                          batchConcurrency: Int,
                          warmupBatches: Int,
                          measuredBatches: Int,
                          contextualExternal: Boolean): Unit = {
    val fixture = newFixture(executorThreads, cacheSize, contextualExternal)
    val driverExecutor = Executors.newFixedThreadPool(batchConcurrency)
    val nextRequestBatch = new AtomicInteger(0)
    try {
      validateResponses(
        Await.result(fixture.fetcher.fetchJoin(requestBatches(contextualExternal).head), 30.seconds),
        contextualExternal)
      runPhase(fixture, driverExecutor, batchConcurrency, warmupBatches, nextRequestBatch)
      val measurement = runPhase(fixture, driverExecutor, batchConcurrency, measuredBatches, nextRequestBatch)
      printResult(executorThreads, cacheSize, batchConcurrency, warmupBatches, contextualExternal, measurement)
    } finally {
      driverExecutor.shutdownNow()
      driverExecutor.awaitTermination(30, TimeUnit.SECONDS)
      closeExecutor(fixture.executor)
    }
  }

  private def runPhase(fixture: Fixture,
                       driverExecutor: java.util.concurrent.ExecutorService,
                       batchConcurrency: Int,
                       batches: Int,
                       nextRequestBatch: AtomicInteger): Measurement = {
    val latenciesNanos = new Array[Long](batches)
    val nextMeasurement = new AtomicInteger(0)
    val checksum = new AtomicLong(0L)
    val peakQueue = new AtomicInteger(0)
    val peakActive = new AtomicInteger(0)
    val sampling = new AtomicBoolean(true)
    val startGate = new CountDownLatch(1)
    val tasksBefore = fixture.executor.getTaskCount
    val completedBefore = fixture.executor.getCompletedTaskCount
    val featureCallsBefore = fixture.store.featureMultiGetCalls.get()
    val featureRequestsBefore = fixture.store.featureRequestCount.get()
    val keyEncodesBefore = fixture.store.keyEncodeCount.get()

    val sampler = new Thread(
      () => {
        while (sampling.get()) {
          peakQueue.accumulateAndGet(fixture.executor.getQueue.size(), Math.max)
          peakActive.accumulateAndGet(fixture.executor.getActiveCount, Math.max)
          Thread.sleep(1L)
        }
      },
      "fetch-join-benchmark-pool-sampler"
    )
    sampler.setDaemon(true)
    sampler.start()

    val workerCount = math.min(batchConcurrency, batches)
    val workers = (0 until workerCount).map { _ =>
      driverExecutor.submit(new Callable[Unit] {
        override def call(): Unit = {
          startGate.await()
          var localChecksum = 0L
          var measurementIndex = nextMeasurement.getAndIncrement()
          while (measurementIndex < batches) {
            val requestBatchIndex = Math.floorMod(nextRequestBatch.getAndIncrement(), RequestBatchCount)
            val startedNanos = System.nanoTime()
            val responses = Await.result(
              fixture.fetcher.fetchJoin(requestBatches(fixture.contextualExternal)(requestBatchIndex)),
              30.seconds)
            localChecksum += responseChecksum(responses)
            latenciesNanos(measurementIndex) = System.nanoTime() - startedNanos
            measurementIndex = nextMeasurement.getAndIncrement()
          }
          checksum.addAndGet(localChecksum)
        }
      })
    }

    val wallStartNanos = System.nanoTime()
    startGate.countDown()
    val wallNanos =
      try {
        workers.foreach(_.get(10, TimeUnit.MINUTES))
        System.nanoTime() - wallStartNanos
      } finally {
        sampling.set(false)
        sampler.join(TimeUnit.SECONDS.toMillis(1))
      }

    Measurement(
      latenciesNanos = latenciesNanos,
      wallNanos = wallNanos,
      checksum = checksum.get(),
      featureMultiGetCalls = fixture.store.featureMultiGetCalls.get() - featureCallsBefore,
      featureRequests = fixture.store.featureRequestCount.get() - featureRequestsBefore,
      keyEncodes = fixture.store.keyEncodeCount.get() - keyEncodesBefore,
      executorTasks = fixture.executor.getTaskCount - tasksBefore,
      executorCompleted = fixture.executor.getCompletedTaskCount - completedBefore,
      peakExecutorQueue = peakQueue.get(),
      peakExecutorActive = peakActive.get()
    )
  }

  private def printResult(executorThreads: Int,
                          cacheSize: Int,
                          batchConcurrency: Int,
                          warmupBatches: Int,
                          contextualExternal: Boolean,
                          measurement: Measurement): Unit = {
    val sorted = measurement.latenciesNanos.sorted
    val wallSeconds = measurement.wallNanos.toDouble / TimeUnit.SECONDS.toNanos(1)
    val batchesPerSecond = sorted.length / wallSeconds
    val candidatesPerSecond = batchesPerSecond * CandidateCount
    val logicalGroupBysPerSecond = batchesPerSecond * LogicalGroupByRequestCount
    val averageMillis = sorted.iterator.map(_.toDouble).sum / sorted.length / TimeUnit.MILLISECONDS.toNanos(1)
    val featureRequestsPerBatch = measurement.featureRequests.toDouble / sorted.length
    val keyEncodesPerBatch = measurement.keyEncodes.toDouble / sorted.length

    println(
      f"FETCH_JOIN_END_TO_END_RESULT backend=immediate_in_memory executor_threads=$executorThreads%d " +
        f"contextual_external=$contextualExternal%s cache_size=$cacheSize%d " +
        f"batch_concurrency=$batchConcurrency%d warmup_batches=$warmupBatches%d " +
        f"measured_batches=${sorted.length}%d batches_per_sec=$batchesPerSecond%.2f " +
        f"candidate_rows_per_sec=$candidatesPerSecond%.2f logical_group_bys_per_sec=$logicalGroupBysPerSecond%.2f " +
        f"avg_ms=$averageMillis%.3f p50_ms=${percentileMillis(sorted, 0.50)}%.3f " +
        f"p95_ms=${percentileMillis(sorted, 0.95)}%.3f p99_ms=${percentileMillis(sorted, 0.99)}%.3f " +
        f"feature_multi_get_calls=${measurement.featureMultiGetCalls}%d " +
        f"feature_kv_requests_per_batch=$featureRequestsPerBatch%.2f key_encodes_per_batch=$keyEncodesPerBatch%.2f " +
        f"executor_tasks=${measurement.executorTasks}%d executor_completed=${measurement.executorCompleted}%d " +
        f"executor_peak_active=${measurement.peakExecutorActive}%d " +
        f"executor_peak_queue=${measurement.peakExecutorQueue}%d checksum=${measurement.checksum}%d")
  }

  private def validateResponses(responses: Seq[Response], contextualExternal: Boolean): Unit = {
    responses should have size CandidateCount
    responses.foreach { response =>
      response.values.isSuccess shouldBe true
      response.values.get should have size (GroupByCount + (if (contextualExternal) 1 else 0))
      if (contextualExternal) {
        response.values.get(ContextualOutputField) shouldBe SharedQuery
      } else {
        response.values.get should not contain key(ContextualOutputField)
      }
    }
  }

  private def responseChecksum(responses: Seq[Response]): Long = {
    if (responses.size != CandidateCount || responses.exists(_.values.isFailure)) {
      throw new IllegalStateException("Fetcher.fetchJoin benchmark returned an invalid response batch")
    }
    responses.iterator.map(_.values.get.size.toLong).sum
  }

  private def percentileMillis(sortedNanos: Array[Long], percentile: Double): Double = {
    val index = math.max(0, math.min(sortedNanos.length - 1, math.ceil(sortedNanos.length * percentile).toInt - 1))
    sortedNanos(index).toDouble / TimeUnit.MILLISECONDS.toNanos(1)
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

private object FetcherFetchJoinPerfTest {
  private val CandidateCount = 50
  private val RequestBatchCount = 80
  private val ContextGroupByCount = 25
  private val CandidateGroupByCount = 25
  private val ContextTemporalGroupByCount = 5
  private val CandidateTemporalGroupByCount = 5
  private val GroupByCount = ContextGroupByCount + CandidateGroupByCount
  private val TemporalGroupByCount = ContextTemporalGroupByCount + CandidateTemporalGroupByCount
  private val LogicalGroupByRequestCount = CandidateCount * GroupByCount
  private val UniqueGroupByRequestCount = ContextGroupByCount + CandidateCount * CandidateGroupByCount
  private val UniqueStreamingRequestCount =
    ContextTemporalGroupByCount + CandidateCount * CandidateTemporalGroupByCount
  private val KvRequestCount = UniqueGroupByRequestCount + UniqueStreamingRequestCount
  private val ExpectedKeyEncodeCount = KvRequestCount
  private val BenchmarkAtMillis = 1721606400000L
  private val SharedContextId: AnyRef = "context-shared"
  private val SharedQuery: AnyRef = "red trainers"
  private val ContextKey = "context_id"
  private val CandidateKey = "candidate_id"
  private val ContextualField = "query"
  private val ContextualOutputField = s"${Constants.ContextualPrefix}_$ContextualField"
  private val ValueField = "value"
  private val BatchIrCacheSizeProperty = "ai.chronon.fetcher.batch_ir_cache_size_elements"

  private sealed trait Ownership {
    def keyColumn: String
  }
  private case object ContextOwnership extends Ownership {
    override val keyColumn: String = ContextKey
  }
  private case object CandidateOwnership extends Ownership {
    override val keyColumn: String = CandidateKey
  }

  private final case class PreparedGroupBy(ownership: Ownership,
                                           temporal: Boolean,
                                           groupBy: GroupBy,
                                           servingInfo: GroupByServingInfoParsed)

  private val valueSchema = StructType("FetchJoinBenchmarkValue", Array(StructField(ValueField, LongType)))
  private val valueAvroSchema = AvroConversions.fromChrononSchema(valueSchema).toString
  private val featureValueBytes = AvroCodec.of(valueAvroSchema).encode(Map(ValueField -> java.lang.Long.valueOf(1L)))

  private def buildPreparedGroupBys(ownership: Ownership, count: Int, temporalCount: Int): Vector[PreparedGroupBy] = {
    (0 until count).map { index =>
      val temporal = index < temporalCount
      val ownershipName = ownership match {
        case ContextOwnership   => "context"
        case CandidateOwnership => "candidate"
      }
      val name = f"benchmark_fetch_join_${ownershipName}_$index%02d"
      val query = Builders.Query(selects = Map(ownership.keyColumn -> ownership.keyColumn, ValueField -> ValueField))
      val sources =
        if (temporal) {
          Seq(Builders.Source.events(query = query, table = s"$name.events", topic = s"$name.topic"))
        } else {
          Seq(Builders.Source.events(query = query, table = s"$name.events"))
        }
      val groupBy = Builders.GroupBy(
        sources = sources,
        metaData = Builders.MetaData(name = name, online = true, samplePercent = 0),
        keyColumns = Seq(ownership.keyColumn),
        accuracy = if (temporal) Accuracy.TEMPORAL else Accuracy.SNAPSHOT
      )
      val keySchema = StructType(s"${ownershipName}_key", Array(StructField(ownership.keyColumn, StringType)))
      val inputSchema = StructType(
        s"${ownershipName}_input",
        Array(StructField(ownership.keyColumn, StringType), StructField(ValueField, LongType))
      )
      val servingInfo = new GroupByServingInfo()
        .setGroupBy(groupBy)
        .setKeyAvroSchema(AvroConversions.fromChrononSchema(keySchema).toString)
        .setInputAvroSchema(AvroConversions.fromChrononSchema(inputSchema).toString)
        .setSelectedAvroSchema(valueAvroSchema)
        .setBatchEndTs(BenchmarkAtMillis)
      PreparedGroupBy(ownership, temporal, groupBy, new GroupByServingInfoParsed(servingInfo))
    }.toVector
  }

  private val preparedGroupBys: Vector[PreparedGroupBy] =
    buildPreparedGroupBys(ContextOwnership, ContextGroupByCount, ContextTemporalGroupByCount) ++
      buildPreparedGroupBys(CandidateOwnership, CandidateGroupByCount, CandidateTemporalGroupByCount)

  private def benchmarkJoin(contextualExternal: Boolean): Join = {
    val externalParts =
      if (contextualExternal) {
        Seq(Builders.ExternalPart(Builders.ContextualSource(Array(StructField(ContextualField, StringType)))))
      } else {
        null
      }
    Builders.Join(
      metaData = Builders.MetaData(name = "benchmark_fetch_join", samplePercent = 0),
      left = Builders.Source.events(
        query = Builders.Query(selects = Map(ContextKey -> ContextKey, CandidateKey -> CandidateKey)),
        table = "benchmark_fetch_join.requests"
      ),
      joinParts = preparedGroupBys.map { prepared =>
        Builders.JoinPart(
          groupBy = prepared.groupBy,
          keyMapping = Map(prepared.ownership.keyColumn -> prepared.ownership.keyColumn)
        )
      },
      externalParts = externalParts
    )
  }

  private val requestBatchesByContextualExternal: Map[Boolean, Vector[Vector[Request]]] =
    Seq(false, true).map { contextualExternal =>
      val batches = Vector.tabulate(RequestBatchCount) { batchIndex =>
        Vector.tabulate(CandidateCount) { candidateIndex =>
          val globalCandidateIndex = batchIndex * CandidateCount + candidateIndex
          val baseKeys: Map[String, AnyRef] = Map(
            ContextKey -> SharedContextId,
            CandidateKey -> f"candidate-$globalCandidateIndex%04d"
          )
          val keys = if (contextualExternal) baseKeys.updated(ContextualField, SharedQuery) else baseKeys
          Request(
            "benchmark_fetch_join",
            keys,
            Some(BenchmarkAtMillis)
          )
        }
      }
      contextualExternal -> batches
    }.toMap

  private def requestBatches(contextualExternal: Boolean): Vector[Vector[Request]] =
    requestBatchesByContextualExternal(contextualExternal)

  private val requestBatches: Vector[Vector[Request]] = requestBatches(contextualExternal = false)

  private final class ImmediateBenchmarkKVStore(
      join: Join,
      servingInfos: Seq[GroupByServingInfoParsed],
      override implicit val executionContext: ExecutionContext
  ) extends KVStore {
    private val metadataKeyBytes = join.keyNameForKvStore.getBytes(Constants.UTF8)
    private val metadataValueBytes = ThriftJsonCodec.toJsonStr(join).getBytes(Constants.UTF8)
    private val servingInfoKeyBytes = Constants.GroupByServingInfoKey.getBytes(Constants.UTF8)
    private val servingInfoByBatchDataset: Map[String, Array[Byte]] = servingInfos.iterator.map { servingInfo =>
      servingInfo.groupByOps.batchDataset -> ThriftJsonCodec
        .toJsonStr(servingInfo.groupByServingInfo)
        .getBytes(Constants.UTF8)
    }.toMap
    private val batchDatasets = servingInfos.iterator.map(_.groupByOps.batchDataset).toSet
    private val streamingDatasets = servingInfos.iterator
      .filter(_.groupByOps.streamingSource.isDefined)
      .map(_.groupByOps.streamingDataset)
      .toSet

    val featureMultiGetCalls = new AtomicLong(0L)
    val featureRequestCount = new AtomicLong(0L)
    val keyEncodeCount = new AtomicLong(0L)
    val lastFeatureMultiGetSize = new AtomicInteger(0)
    val lastBatchRequestCount = new AtomicInteger(0)
    val lastStreamingRequestCount = new AtomicInteger(0)

    override def create(dataset: String): Unit = ()

    override def createKeyBytes(keys: Map[String, AnyRef],
                                groupByServingInfo: GroupByServingInfoParsed,
                                dataset: String): Array[Byte] = {
      keyEncodeCount.incrementAndGet()
      super.createKeyBytes(keys, groupByServingInfo, dataset)
    }

    override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
      val featureRequests = requests.filter(isFeatureRequest)
      if (featureRequests.nonEmpty) {
        featureMultiGetCalls.incrementAndGet()
        featureRequestCount.addAndGet(featureRequests.size.toLong)
        lastFeatureMultiGetSize.set(featureRequests.size)
        lastBatchRequestCount.set(featureRequests.count(request => batchDatasets.contains(request.dataset)))
        lastStreamingRequestCount.set(featureRequests.count(request => streamingDatasets.contains(request.dataset)))
      }
      Future.successful(requests.map(request => GetResponse(request, responseValues(request))))
    }

    private def isFeatureRequest(request: GetRequest): Boolean = {
      val servingInfoRead =
        batchDatasets.contains(request.dataset) && Arrays.equals(request.keyBytes, servingInfoKeyBytes)
      !servingInfoRead && (batchDatasets.contains(request.dataset) || streamingDatasets.contains(request.dataset))
    }

    private def responseValues(request: GetRequest): Try[Seq[TimedValue]] = Try {
      if (request.dataset == Constants.MetadataDataset && Arrays.equals(request.keyBytes, metadataKeyBytes)) {
        Seq(TimedValue(metadataValueBytes, BenchmarkAtMillis))
      } else if (
        servingInfoByBatchDataset.contains(request.dataset) && Arrays.equals(request.keyBytes, servingInfoKeyBytes)
      ) {
        Seq(TimedValue(servingInfoByBatchDataset(request.dataset), BenchmarkAtMillis))
      } else if (streamingDatasets.contains(request.dataset)) {
        Seq.empty
      } else if (batchDatasets.contains(request.dataset)) {
        Seq(TimedValue(featureValueBytes, BenchmarkAtMillis))
      } else {
        throw new NoSuchElementException(
          s"Unexpected benchmark KV request dataset=${request.dataset} key=${new String(request.keyBytes, StandardCharsets.UTF_8)}")
      }
    }

    override def multiPut(keyValueDatasets: Seq[PutRequest]): Future[Seq[Boolean]] =
      Future.successful(keyValueDatasets.map(_ => true))

    override def bulkPut(sourceOfflineTable: String,
                         destinationOnlineDataSet: String,
                         partition: String): Unit = ()
  }

  private final case class Fixture(fetcher: Fetcher,
                                   store: ImmediateBenchmarkKVStore,
                                   executor: ThreadPoolExecutor,
                                   contextualExternal: Boolean)

  private def newFixture(executorThreads: Int, cacheSize: Int, contextualExternal: Boolean): Fixture = {
    val executor = Executors.newFixedThreadPool(executorThreads).asInstanceOf[ThreadPoolExecutor]
    executor.prestartAllCoreThreads()
    val executionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(executor)
    val join = benchmarkJoin(contextualExternal)
    val store = new ImmediateBenchmarkKVStore(join, preparedGroupBys.map(_.servingInfo), executionContext)
    val fetcher = this.synchronized {
      val previousCacheSize = Option(System.getProperty(BatchIrCacheSizeProperty))
      System.setProperty(BatchIrCacheSizeProperty, cacheSize.toString)
      try {
        new Fetcher(
          store,
          Constants.MetadataDataset,
          externalSourceRegistry = new ai.chronon.online.ExternalSourceRegistry,
          executionContextOverride = executionContext
        )
      } finally {
        previousCacheSize match {
          case Some(value) => System.setProperty(BatchIrCacheSizeProperty, value)
          case None        => System.clearProperty(BatchIrCacheSizeProperty)
        }
      }
    }
    Fixture(fetcher, store, executor, contextualExternal)
  }

  private final case class Measurement(latenciesNanos: Array[Long],
                                       wallNanos: Long,
                                       checksum: Long,
                                       featureMultiGetCalls: Long,
                                       featureRequests: Long,
                                       keyEncodes: Long,
                                       executorTasks: Long,
                                       executorCompleted: Long,
                                       peakExecutorQueue: Int,
                                       peakExecutorActive: Int)

  private def closeExecutor(executor: ThreadPoolExecutor): Unit = {
    executor.shutdown()
    if (!executor.awaitTermination(30, TimeUnit.SECONDS)) executor.shutdownNow()
  }
}
