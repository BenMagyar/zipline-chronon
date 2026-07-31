package ai.chronon.integrations.redis

import ai.chronon.api.Extensions.{GroupByOps, JoinOps, MetadataOps}
import ai.chronon.api.{Accuracy, Builders, Constants, GroupBy, GroupByServingInfo, Join, LongType, StringType, StructField, StructType, ThriftJsonCodec}
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.fetcher.Fetcher.{Request, Response}
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import ai.chronon.online.{FlagStore, FlagStoreConstants, GroupByServingInfoParsed, KVStore}

import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{Callable, CountDownLatch, ExecutorService, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Production-shaped fixture for comparing Redis clients through the real Fetcher.fetchJoin entry point.
  *
  * The configuration mirrors FetcherFetchJoinPerfTest: 50 candidates, 25 context GroupBys, 25 candidate GroupBys,
  * and ten temporal GroupBys. One hot request therefore plans 1,275 batch reads plus 255 streaming reads. Metadata,
  * serving information, and valid Avro feature rows are loaded into Redis; only the feature names and values are
  * synthetic. Fetcher performs the real planning, key encoding, RedisKVStoreImpl multiGet, Avro decoding, fanout,
  * JoinCodec identity derivation, and response finalization.
  */
private[redis] object RedisFetcherOnlineShape {
  val CandidateCount = 50
  val RequestBatchCount = 80
  val ContextGroupByCount = 25
  val CandidateGroupByCount = 25
  val ContextTemporalGroupByCount = 5
  val CandidateTemporalGroupByCount = 5
  val GroupByCount: Int = ContextGroupByCount + CandidateGroupByCount
  val LogicalGroupByRequests: Int = CandidateCount * GroupByCount
  val UniqueBatchRequests: Int = ContextGroupByCount + CandidateCount * CandidateGroupByCount
  val UniqueStreamingRequests: Int =
    ContextTemporalGroupByCount + CandidateCount * CandidateTemporalGroupByCount
  val UniqueKvRequests: Int = UniqueBatchRequests + UniqueStreamingRequests

  // Keep streaming reads within the current UTC day. A historical fixed timestamp would make Redis expand every
  // temporal request into one ZRANGE per elapsed day and benchmark fixture age instead of the online request path.
  private val BenchmarkAtMillis = System.currentTimeMillis()
  private val SharedContextId: AnyRef = "context-shared"
  private val ContextKey = "context_id"
  private val CandidateKey = "candidate_id"
  private val ValueField = "value"
  private val BatchIrCacheSizeProperty = "ai.chronon.fetcher.batch_ir_cache_size_elements"
  private val TilingEnabledFlagStore = new FlagStore {
    override def isSet(flagName: String, attributes: util.Map[String, String]): java.lang.Boolean =
      flagName == FlagStoreConstants.TILING_ENABLED
  }

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

  final case class Fixture(fetcher: Fetcher, executionContext: ExecutionContext)

  final case class Measurement(endToEndNanos: Array[Long],
                               fetchJoinNanos: Array[Long],
                               finalCallbackQueueNanos: Array[Long],
                               finalCallbackCpuNanos: Array[Long],
                               failures: Int,
                               firstFailure: Throwable,
                               wallNanos: Long,
                               checksum: Long,
                               executorTasks: Long,
                               executorCompleted: Long,
                               peakExecutorQueue: Int,
                               peakExecutorActive: Int)

  private val valueSchema = StructType("RedisFetcherBenchmarkValue", Array(StructField(ValueField, LongType)))
  private val valueAvroSchema = AvroConversions.fromChrononSchema(valueSchema).toString
  private val featureValueBytes = AvroCodec.of(valueAvroSchema).encode(Map(ValueField -> java.lang.Long.valueOf(1L)))

  private def buildPreparedGroupBys(ownership: Ownership, count: Int, temporalCount: Int): Vector[PreparedGroupBy] = {
    (0 until count).map { index =>
      val temporal = index < temporalCount
      val ownershipName = ownership match {
        case ContextOwnership   => "context"
        case CandidateOwnership => "candidate"
      }
      val name = f"benchmark_redis_fetch_join_${ownershipName}_$index%02d"
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

  private val join: Join = Builders.Join(
    metaData = Builders.MetaData(name = "benchmark_redis_fetch_join", samplePercent = 0),
    left = Builders.Source.events(
      query = Builders.Query(selects = Map(ContextKey -> ContextKey, CandidateKey -> CandidateKey)),
      table = "benchmark_redis_fetch_join.requests"
    ),
    joinParts = preparedGroupBys.map { prepared =>
      Builders.JoinPart(
        groupBy = prepared.groupBy,
        keyMapping = Map(prepared.ownership.keyColumn -> prepared.ownership.keyColumn)
      )
    }
  )

  private val requestBatches: Vector[Vector[Request]] = Vector.tabulate(RequestBatchCount) { batchIndex =>
    Vector.tabulate(CandidateCount) { candidateIndex =>
      val globalCandidateIndex = batchIndex * CandidateCount + candidateIndex
      Request(
        join.metaData.name,
        Map(
          ContextKey -> SharedContextId,
          CandidateKey -> f"candidate-$globalCandidateIndex%04d"
        ),
        Some(BenchmarkAtMillis)
      )
    }
  }

  def load(kvStore: RedisKVStoreImpl, loadBatchSize: Int, timeout: FiniteDuration): Int = {
    val datasets = Constants.MetadataDataset +: preparedGroupBys.flatMap { prepared =>
      prepared.servingInfo.groupByOps.batchDataset +: prepared.servingInfo.groupByOps.streamingSource
        .map(_ => prepared.servingInfo.groupByOps.streamingDataset)
        .toVector
    }
    datasets.distinct.foreach(kvStore.create)

    val metadataPuts = Vector(
      PutRequest(
        join.keyNameForKvStore.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(join).getBytes(Constants.UTF8),
        Constants.MetadataDataset,
        Some(BenchmarkAtMillis)
      )) ++ preparedGroupBys.map { prepared =>
      PutRequest(
        Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(prepared.servingInfo.groupByServingInfo).getBytes(Constants.UTF8),
        prepared.servingInfo.groupByOps.batchDataset,
        Some(BenchmarkAtMillis)
      )
    }

    val candidateIds = Vector.tabulate(RequestBatchCount * CandidateCount)(index => f"candidate-$index%04d": AnyRef)
    val featurePuts = preparedGroupBys.iterator.flatMap { prepared =>
      val ownerKeys = prepared.ownership match {
        case ContextOwnership   => Iterator.single(SharedContextId)
        case CandidateOwnership => candidateIds.iterator
      }
      ownerKeys.map { ownerKey =>
        val keys = Map(prepared.ownership.keyColumn -> ownerKey)
        val dataset = prepared.servingInfo.groupByOps.batchDataset
        PutRequest(
          kvStore.createKeyBytes(keys, prepared.servingInfo, dataset),
          featureValueBytes,
          dataset,
          Some(BenchmarkAtMillis)
        )
      }
    }

    var loaded = 0
    (metadataPuts.iterator ++ featurePuts).grouped(loadBatchSize).foreach { batch =>
      val results = Await.result(kvStore.multiPut(batch.toSeq), timeout)
      val failures = results.count(result => !result)
      require(failures == 0, s"Online-shaped Redis fixture load had $failures failure(s) in ${batch.size} puts")
      loaded += batch.size
    }
    loaded
  }

  def newFixture(kvStore: KVStore, executionContext: ExecutionContext): Fixture = {
    val fetcher = this.synchronized {
      val previousCacheSize = Option(System.getProperty(BatchIrCacheSizeProperty))
      System.setProperty(BatchIrCacheSizeProperty, "0")
      try {
        new Fetcher(
          kvStore,
          Constants.MetadataDataset,
          externalSourceRegistry = new ai.chronon.online.ExternalSourceRegistry,
          flagStore = TilingEnabledFlagStore,
          executionContextOverride = executionContext
        )
      } finally {
        previousCacheSize match {
          case Some(value) => System.setProperty(BatchIrCacheSizeProperty, value)
          case None        => System.clearProperty(BatchIrCacheSizeProperty)
        }
      }
    }
    Fixture(fetcher, executionContext)
  }

  def validate(fixture: Fixture, timeout: FiniteDuration): Unit = {
    val responses = Await.result(fixture.fetcher.fetchJoin(requestBatches.head), timeout)
    validateResponses(responses)
  }

  def execute(fixture: Fixture,
              sharedExecutor: ThreadPoolExecutor,
              ingressExecutor: ExecutorService,
              iterations: Int,
              batchConcurrency: Int,
              timeout: FiniteDuration,
              nextRequestBatch: AtomicInteger): Measurement = {
    val endToEndNanos = Array.fill(iterations)(0L)
    val fetchJoinNanos = Array.fill(iterations)(0L)
    val finalCallbackQueueNanos = Array.fill(iterations)(0L)
    val finalCallbackCpuNanos = Array.fill(iterations)(0L)
    val nextIteration = new AtomicInteger()
    val failures = new AtomicInteger()
    val firstFailure = new AtomicReference[Throwable]()
    val checksum = new AtomicLong()
    val peakQueue = new AtomicInteger()
    val peakActive = new AtomicInteger()
    val sampling = new AtomicBoolean(true)
    val startGate = new CountDownLatch(1)
    val tasksBefore = sharedExecutor.getTaskCount
    val completedBefore = sharedExecutor.getCompletedTaskCount

    val sampler = new Thread(
      () => {
        while (sampling.get()) {
          peakQueue.accumulateAndGet(sharedExecutor.getQueue.size(), Math.max)
          peakActive.accumulateAndGet(sharedExecutor.getActiveCount, Math.max)
          Thread.sleep(1L)
        }
      },
      "redis-fetch-join-pool-sampler"
    )
    sampler.setDaemon(true)
    sampler.start()

    val workerCount = math.min(batchConcurrency, iterations)
    val workers = (0 until workerCount).map { _ =>
      ingressExecutor.submit(new Callable[Unit] {
        override def call(): Unit = {
          startGate.await()
          var localChecksum = 0L
          var iteration = nextIteration.getAndIncrement()
          while (iteration < iterations) {
            val batchIndex = Math.floorMod(nextRequestBatch.getAndIncrement(), RequestBatchCount)
            val startedNanos = System.nanoTime()
            try {
              val raw = fixture.fetcher.fetchJoin(requestBatches(batchIndex))
              val fetchCompletedNanos = new AtomicLong()
              val handoff = Promise[Seq[Response]]()
              raw.onComplete { result =>
                fetchCompletedNanos.set(System.nanoTime())
                handoff.complete(result)
              }(DirectExecutionContext)
              val checked: Future[Long] = handoff.future.map { responses =>
                val callbackStartedNanos = System.nanoTime()
                finalCallbackQueueNanos(iteration) = callbackStartedNanos - fetchCompletedNanos.get()
                fetchJoinNanos(iteration) = fetchCompletedNanos.get() - startedNanos
                val result = responseChecksum(responses)
                finalCallbackCpuNanos(iteration) = System.nanoTime() - callbackStartedNanos
                result
              }(fixture.executionContext)
              localChecksum += Await.result(checked, timeout)
            } catch {
              case throwable: Throwable =>
                failures.incrementAndGet()
                firstFailure.compareAndSet(null, throwable)
            } finally {
              endToEndNanos(iteration) = System.nanoTime() - startedNanos
            }
            iteration = nextIteration.getAndIncrement()
          }
          checksum.addAndGet(localChecksum)
        }
      })
    }

    val wallStartedNanos = System.nanoTime()
    startGate.countDown()
    val wallNanos =
      try {
        workers.foreach(_.get(10, TimeUnit.MINUTES))
        System.nanoTime() - wallStartedNanos
      } finally {
        sampling.set(false)
        sampler.join(TimeUnit.SECONDS.toMillis(1))
      }

    Measurement(
      endToEndNanos,
      fetchJoinNanos,
      finalCallbackQueueNanos,
      finalCallbackCpuNanos,
      failures.get(),
      firstFailure.get(),
      wallNanos,
      checksum.get(),
      sharedExecutor.getTaskCount - tasksBefore,
      sharedExecutor.getCompletedTaskCount - completedBefore,
      peakQueue.get(),
      peakActive.get()
    )
  }

  private def validateResponses(responses: Seq[Response]): Unit = {
    require(responses.size == CandidateCount, s"Expected $CandidateCount Fetcher responses, got ${responses.size}")
    responses.foreach { response =>
      response.values match {
        case Success(values) =>
          require(values.size == GroupByCount,
                  s"Expected $GroupByCount features for ${response.request.keys}, got ${values.size}")
        case Failure(error) => throw error
      }
    }
  }

  private def responseChecksum(responses: Seq[Response]): Long = {
    validateResponses(responses)
    responses.iterator.map(_.values.get.size.toLong).sum
  }

  private object DirectExecutionContext extends ExecutionContext {
    override def execute(runnable: Runnable): Unit = runnable.run()
    override def reportFailure(cause: Throwable): Unit = throw cause
  }
}
