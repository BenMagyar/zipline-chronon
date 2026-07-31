package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants._
import ai.chronon.online.KVStore.GetResponse
import ai.chronon.online.metrics.{FlexibleExecutionContext, InstrumentedThreadPoolExecutor}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.util.JedisClusterCRC16

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{Executors, ThreadPoolExecutor, TimeUnit}
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Opt-in integration benchmark for production-shaped Redis KVStore reads.
  *
  * This measures the real client-selected RedisKVStoreImpl path, so JMH would add iteration machinery without removing network
  * variance. It starts at KVStore.multiGet, after join planning and context deduplication, and excludes downstream
  * feature decoding. By default it starts a native local three-primary/three-replica Redis cluster and sweeps both
  * worker-pool sizes and concurrent batches. The native local fixture defaults to Valkey. Synthetic cache-demand
  * profiles assume all context batch IRs are hot and
  * apply the named hit percentage to candidate keys; they do not instantiate Caffeine or measure fetcher cache work.
  * Set PERF_REDIS_PRIMARY_COUNT and PERF_REDIS_REPLICAS_PER_PRIMARY to measure shard-topology tradeoffs. Set
  * PERF_REDIS_CPU_LIMIT to constrain the local Redis container for fixed-resource comparisons. Set
  * PERF_REDIS_PIPELINE_SYNC_THREAD_COUNTS to compare Jedis' per-pipeline executors (0) with shared executor sizes. Set
  * PERF_REDIS_USE_NATIVE_LOCAL=false for the legacy amd64 fixture, or PERF_REDIS_USE_EXTERNAL=true with
  * REDIS_CLUSTER_NODES for one isolated external-cluster configuration.
  *
  * Example:
  *   CHRONON_PERF_TEST_ENABLED=true \
  *     PERF_REDIS_PROFILES=deduplicated,synthetic-context-hot-candidate-hit-80 \
  *     ./mill --no-server redis.test.testOnly ai.chronon.integrations.redis.RedisFetcherReadPerfTestHarness
  */
class RedisFetcherReadPerfTestHarness extends AnyFlatSpec with BeforeAndAfterAll {
  import RedisFetcherReadPerfTestHarness._

  private final class BenchmarkRedisKVStore(
      client: JedisCluster,
      glideClient: Option[GlideRedisReadClient],
      conf: Map[String, String],
      override implicit val executionContext: ExecutionContext
  ) extends RedisKVStoreImpl(client, conf, glideReadClient = glideClient)

  private val enabled = sys.env.get("CHRONON_PERF_TEST_ENABLED").exists(_.equalsIgnoreCase("true"))
  private val kvOnlyEnabled =
    !sys.env.get("PERF_REDIS_KV_ONLY_ENABLED").exists(_.equalsIgnoreCase("false"))
  private val warmupIterations = envInt("PERF_WARMUP_ITERATIONS", 500)
  private val measurementIterations = envInt("PERF_MEASUREMENT_ITERATIONS", 5000)
  private val batchConcurrencies = envIntsWithLegacy(
    "PERF_BATCH_CONCURRENCIES",
    "PERF_BATCH_CONCURRENCY",
    envInt("PERF_NUM_THREADS", 0),
    Seq(32, 48, 64, 96)
  )
  private val workerThreadCounts = envInts("PERF_REDIS_THREAD_COUNTS", Seq(16, 32, 48, 64))
  private val pipelineSyncThreadCounts =
    envInts("PERF_REDIS_PIPELINE_SYNC_THREAD_COUNTS", Seq(DefaultPipelineSyncThreads))
  private val queueCapacities = envInts("PERF_REDIS_QUEUE_CAPACITIES", Seq(10000))
  private val onlineShapedWarmupIterations = envInt("PERF_ONLINE_SHAPED_WARMUP_ITERATIONS", 0)
  private val onlineShapedMeasurementIterations = envInt("PERF_ONLINE_SHAPED_ITERATIONS", 0)
  private val onlineShapedBatchConcurrencies = envInts("PERF_ONLINE_SHAPED_BATCH_CONCURRENCIES", Seq(16))
  private val onlineShapedQueueCapacity = envInt("PERF_ONLINE_SHAPED_QUEUE_CAPACITY", 10000)
  private val onlineShapedPipelineSyncThreads =
    envInt("PERF_ONLINE_SHAPED_PIPELINE_SYNC_THREADS", DefaultPipelineSyncThreads)
  private val candidates = envInt("PERF_CANDIDATES", RedisFetcherReadWorkload.Config().candidates)
  private val loadBatchSize = envInt("PERF_LOAD_BATCH_SIZE", 100)
  private val maxConnections = envInt("PERF_REDIS_MAX_CONNECTIONS", 64)
  private val maxIdleConnections =
    envInt("PERF_REDIS_MAX_IDLE_CONNECTIONS", math.min(RedisKVStoreConstants.DefaultMaxIdleConnections, maxConnections))
  private val minIdleConnections =
    envInt("PERF_REDIS_MIN_IDLE_CONNECTIONS", math.min(DefaultMinIdleConnections, maxIdleConnections))
  private val readTimeout = envInt("PERF_READ_TIMEOUT_SECONDS", 30).seconds
  private val keyPrefix = sys.env.getOrElse("PERF_REDIS_KEY_PREFIX", "chronon-perf")
  private val keyHashTagMode =
    sys.env.getOrElse("PERF_REDIS_HASH_TAG_MODE", RedisKVStore.DatasetAndEntityHashTagMode)
  private val readMetricsEnabled =
    !sys.env.get("PERF_REDIS_READ_METRICS_ENABLED").exists(_.equalsIgnoreCase("false"))
  private val useExternalCluster = sys.env.get("PERF_REDIS_USE_EXTERNAL").exists(_.equalsIgnoreCase("true"))
  private val useNativeLocalCluster =
    sys.env.getOrElse("PERF_REDIS_USE_NATIVE_LOCAL", "true").equalsIgnoreCase("true")
  private val localServer = sys.env.getOrElse("PERF_REDIS_SERVER", "valkey").trim.toLowerCase
  private val effectiveServerLabel =
    if (useExternalCluster) "external" else if (useNativeLocalCluster) localServer else "redis"
  private val redisClient = sys.env.getOrElse("PERF_REDIS_CLIENT", DefaultRedisClient).trim.toLowerCase
  private val glideExecutionMode =
    sys.env.getOrElse("PERF_REDIS_GLIDE_EXECUTION_MODE", DefaultGlideExecutionMode).trim.toLowerCase
  private val readFrom = sys.env.getOrElse("PERF_REDIS_READ_FROM", DefaultRedisReadFrom).trim.toLowerCase
  private val clientAz = sys.env
    .get("PERF_REDIS_CLIENT_AZ")
    .map(_.trim)
    .filter(_.nonEmpty)
    .orElse(
      if (!useExternalCluster && readFrom == RedisReadFromAzAffinityReplicasAndPrimary) Some("zone-a") else None)
  private val redisSoTimeoutMs = envInt("PERF_REDIS_SO_TIMEOUT_MS", DefaultSoTimeoutMs)
  private val localPrimaryCount = envInt("PERF_REDIS_PRIMARY_COUNT", 3)
  private val localReplicasPerPrimary = envInt("PERF_REDIS_REPLICAS_PER_PRIMARY", 1)
  private val localCpuLimit = envOptionalDouble("PERF_REDIS_CPU_LIMIT")
  private val selectedProfileNames = sys.env
    .getOrElse(
      "PERF_REDIS_PROFILES",
      "deduplicated,deduplicated-cross-day-24h"
    )
    .split(",")
    .iterator
    .map(_.trim)
    .filter(_.nonEmpty)
    .toVector

  private val poolConfigs = for {
    threads <- workerThreadCounts
    queueCapacity <- queueCapacities
  } yield PoolConfig(threads, queueCapacity)

  require(warmupIterations >= 0)
  require(measurementIterations > 0)
  require(batchConcurrencies.nonEmpty && batchConcurrencies.forall(_ > 0))
  require(workerThreadCounts.nonEmpty && workerThreadCounts.forall(_ > 0))
  require(pipelineSyncThreadCounts.nonEmpty && pipelineSyncThreadCounts.forall(_ >= 0))
  require(queueCapacities.nonEmpty && queueCapacities.forall(_ > 0))
  require(onlineShapedWarmupIterations >= 0)
  require(onlineShapedMeasurementIterations >= 0)
  require(onlineShapedBatchConcurrencies.nonEmpty && onlineShapedBatchConcurrencies.forall(_ > 0))
  require(onlineShapedQueueCapacity > 0)
  require(onlineShapedPipelineSyncThreads >= 0)
  require(candidates > 0)
  require(loadBatchSize > 0)
  require(maxConnections > 0)
  require(localPrimaryCount >= 3)
  require(localReplicasPerPrimary >= 0)
  require(Set("redis", "valkey").contains(localServer),
          s"PERF_REDIS_SERVER must be redis or valkey, got '$localServer'")
  require(Set(RedisClientJedis, RedisClientGlide).contains(redisClient),
          s"PERF_REDIS_CLIENT must be jedis or glide, got '$redisClient'")
  require(Set(GlideExecutionDirect, GlideExecutionClusterBatch).contains(glideExecutionMode),
          s"PERF_REDIS_GLIDE_EXECUTION_MODE must be direct or cluster-batch, got '$glideExecutionMode'")
  require(Set(RedisReadFromPrimary, RedisReadFromAllNodes, RedisReadFromAzAffinityReplicasAndPrimary).contains(readFrom),
          s"Unsupported PERF_REDIS_READ_FROM '$readFrom'")
  require(redisClient == RedisClientGlide || readFrom == RedisReadFromPrimary,
          "Replica/AZ read policies require PERF_REDIS_CLIENT=glide")
  require(useExternalCluster || readFrom == RedisReadFromPrimary || localReplicasPerPrimary > 0,
          "Local non-primary read policies require PERF_REDIS_REPLICAS_PER_PRIMARY > 0")
  require(readFrom != RedisReadFromAzAffinityReplicasAndPrimary || clientAz.isDefined,
          "PERF_REDIS_CLIENT_AZ is required for external AZ-affinity benchmarks")
  require(redisClient != RedisClientGlide || redisSoTimeoutMs > 0,
          "PERF_REDIS_SO_TIMEOUT_MS must be positive for GLIDE")
  require(localCpuLimit.forall(_ > 0))
  require(minIdleConnections >= 0 && minIdleConnections <= maxIdleConnections)
  require(maxIdleConnections >= 0 && maxIdleConnections <= maxConnections)
  if (useExternalCluster) {
    require(localCpuLimit.isEmpty, "PERF_REDIS_CPU_LIMIT only applies to the local Redis fixture")
    require(poolConfigs.size == 1,
            "External Redis runs require one worker/queue configuration per JVM for an isolated executor measurement")
    require(pipelineSyncThreadCounts.size == 1,
            "External Redis runs require one pipeline-sync thread configuration per JVM")
  }

  @volatile private var resultSink: AnyRef = _
  private var localCluster: RedisClusterFixture = _
  private var externalClientSettings: RedisKVStoreFactory.ClientSettings = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (enabled) {
      if (useExternalCluster) {
        val poolConfig = poolConfigs.head
        System.setProperty(FlexibleExecutionContext.ThreadPoolSizeProperty, poolConfig.threads.toString)
        System.setProperty(FlexibleExecutionContext.QueueCapacityProperty, poolConfig.queueCapacity.toString)
        Seq(
          EnvRedisMaxConnections -> maxConnections,
          EnvRedisMinIdleConnections -> minIdleConnections,
          EnvRedisMaxIdleConnections -> maxIdleConnections
        ).foreach { case (name, expected) =>
          sys.env.get(name).foreach { configured =>
            require(configured.toInt == expected, s"$name=$configured conflicts with benchmark setting $expected")
          }
        }
        val nodes = sys.env.getOrElse(
          EnvRedisClusterNodes,
          throw new IllegalArgumentException("REDIS_CLUSTER_NODES is required when PERF_REDIS_USE_EXTERNAL=true")
        )
        val benchmarkClientEnvKeys = Set(
          EnvRedisClient,
          EnvRedisReadFrom,
          EnvRedisSoTimeoutMs
        )
        externalClientSettings = RedisKVStoreFactory.settings(
          Map(
            EnvRedisClusterNodes -> nodes,
            EnvRedisMaxConnections -> maxConnections.toString,
            EnvRedisMinIdleConnections -> minIdleConnections.toString,
            EnvRedisMaxIdleConnections -> maxIdleConnections.toString,
            "redis.key.prefix" -> keyPrefix,
            "redis.key.hash.tag.mode" -> keyHashTagMode,
            "redis.read.metrics.enabled" -> readMetricsEnabled.toString,
            ConfigPipelineSyncThreads -> pipelineSyncThreadCounts.head.toString,
            PropRedisClient -> redisClient,
            PropRedisReadFrom -> readFrom,
            PropRedisSoTimeoutMs -> redisSoTimeoutMs.toString
          ) ++ clientAz.map(PropRedisClientAz -> _),
          env = sys.env -- benchmarkClientEnvKeys)
      } else {
        localCluster =
          if (useNativeLocalCluster)
            RedisClusterFixture.startNative(
              maxConnections,
              minIdleConnections,
              maxIdleConnections,
              localPrimaryCount,
              localReplicasPerPrimary,
              localCpuLimit,
              localServer
            )
          else
            RedisClusterFixture.start(maxConnections, minIdleConnections, maxIdleConnections)
      }
    }
  }

  override def afterAll(): Unit = {
    try {
      if (localCluster != null) localCluster.close()
    } finally {
      super.afterAll()
    }
  }

  "Redis fetcher read benchmark" should "measure the mixed GroupBy workload" in {
    assume(enabled, "Set CHRONON_PERF_TEST_ENABLED=true to run Redis performance tests")
    assume(kvOnlyEnabled, "PERF_REDIS_KV_ONLY_ENABLED=false skips the existing KV-only benchmark")

    val workload = RedisFetcherReadWorkload.build(RedisFetcherReadWorkload.Config(candidates = candidates))
    val profilesByName =
      (Seq(workload.deduplicated, workload.deduplicatedCrossDay, workload.logical, workload.batchOnly) ++
        workload.batchCacheDemand ++ workload.batchCacheDemandCrossDay)
        .map(profile => profile.name -> profile)
        .toMap
    val selectedProfiles = selectedProfileNames.map { name =>
      profilesByName.getOrElse(name,
                               throw new IllegalArgumentException(
                                 s"Unknown PERF_REDIS_PROFILES entry '$name'; expected one of " +
                                   profilesByName.keys.toSeq.sorted.mkString(", ")))
    }

    println(s"REDIS_FETCHER_READ_SHAPE scope=kvstore_multi_get candidates=${workload.config.candidates} " +
      s"group_bys=${workload.config.totalGroupBys} streaming_group_bys=${workload.config.totalStreamingGroupBys} " +
      s"context_group_bys=${workload.config.contextGroupBys} candidate_group_bys=${workload.config.candidateGroupBys} " +
      s"context_streaming_group_bys=" +
      s"${workload.config.contextLastGroupBys + workload.config.contextTiledGroupBys} " +
      s"candidate_streaming_group_bys=" +
      s"${workload.config.candidateLastGroupBys + workload.config.candidateTiledGroupBys} " +
      s"last_group_bys=${workload.config.contextLastGroupBys + workload.config.candidateLastGroupBys} " +
      s"tiled_group_bys=${workload.config.contextTiledGroupBys + workload.config.candidateTiledGroupBys} " +
      s"logical_group_by_requests=${workload.logicalGroupByRequests} " +
      s"unique_group_by_requests=${workload.uniqueGroupByRequests} " +
      s"unique_kv_requests=${workload.deduplicated.requests.size} " +
      s"logical_redis_read_groups=${workload.config.totalGroupBys + workload.config.totalStreamingGroupBys} " +
      s"kvstore_multi_get_calls_per_batch=1 " +
      s"redis_server=$effectiveServerLabel redis_client=$redisClient " +
      s"glide_execution_mode=$glideExecutionMode read_from=$readFrom client_az=${clientAz.getOrElse("unset")} " +
      s"redis_so_timeout_ms=$redisSoTimeoutMs " +
      s"cluster_pipelines_per_batch=" +
      s"${if (redisClient == RedisClientJedis || glideExecutionMode == GlideExecutionClusterBatch) 1 else 0} " +
      s"range_hours=${workload.config.tiledPoints} hourly_tiles=${workload.config.tiledPoints} " +
      s"payload_bytes=${workload.deduplicated.expectedPayloadBytes} redis_hash_tag_mode=$keyHashTagMode " +
      s"redis_read_metrics_enabled=$readMetricsEnabled " +
      s"batch_concurrencies=${batchConcurrencies.mkString(",")} max_connections_per_node=$maxConnections " +
      s"min_idle_connections_per_node=$minIdleConnections max_idle_connections_per_node=$maxIdleConnections " +
      s"test_on_borrow=false test_on_return=false test_while_idle=true " +
      s"redis_cluster_image=${Option(localCluster).map(_.image).getOrElse("external")} " +
      s"redis_cluster_primaries=${Option(localCluster).map(_.primaryCount.toString).getOrElse("external")} " +
      s"redis_replicas_per_primary=" +
      s"${Option(localCluster).map(_.replicasPerPrimary.toString).getOrElse("external")} " +
      s"redis_container_cpu_limit=" +
      s"${Option(localCluster).flatMap(_.cpuLimit).map(_.toString).getOrElse("unlimited")} " +
      s"redis_io_threads=${Option(localCluster).map(_.ioThreads.toString).getOrElse("external")} " +
      s"redis_io_threads_do_reads=${Option(localCluster).map(_.ioThreadsDoReads.toString).getOrElse("external")} " +
      s"pool_sample_interval_ms=$PoolSampleIntervalMillis " +
      s"jedis_dedicated_pipeline_sync_workers_per_pipeline=" +
      s"${redis.clients.jedis.MultiNodePipelineBase.MULTI_NODE_PIPELINE_SYNC_WORKERS} " +
      s"pipeline_sync_thread_counts=${pipelineSyncThreadCounts.mkString(",")} " +
      s"worker_threads=${workerThreadCounts.mkString(",")} queue_capacities=${queueCapacities.mkString(",")}")

    withRedisStore(poolConfigs.maxBy(_.threads), pipelineSyncThreads = 0, loader = true) { (loaderStore, _, _) =>
      load(loaderStore, workload)
    }

    poolConfigs.foreach { poolConfig =>
      pipelineSyncThreadCounts.foreach { pipelineSyncThreads =>
        withRedisStore(poolConfig, pipelineSyncThreads, loader = false) { (kvStore, redisExecutor, jedisCluster) =>
          selectedProfiles.foreach(profile => awaitValidateProfile(kvStore, profile))
          if (!useExternalCluster) selectedProfiles.foreach(profile => printReadRouting(kvStore, profile))
          batchConcurrencies.foreach { batchConcurrency =>
            selectedProfiles.foreach { profile =>
              if (warmupIterations > 0) {
                val warmup = execute(kvStore, redisExecutor, jedisCluster, profile, warmupIterations, batchConcurrency)
                requireSuccessful(profile.name, "warmup", warmup)
              }

              val measurement =
                execute(kvStore, redisExecutor, jedisCluster, profile, measurementIterations, batchConcurrency)
              printResult(workload, profile, poolConfig, measurement, batchConcurrency)
              requireSuccessful(profile.name, "measurement", measurement)
            }
          }
        }
      }
    }
  }

  "Redis fetcher online-shaped benchmark" should "measure Fetcher.fetchJoin on the shared production executor" in {
    assume(enabled, "Set CHRONON_PERF_TEST_ENABLED=true to run Redis performance tests")
    assume(
      onlineShapedMeasurementIterations > 0,
      "Set PERF_ONLINE_SHAPED_ITERATIONS to a positive value to run the full Fetcher.fetchJoin benchmark")

    val sharedPool = PoolConfig(threads = 16, queueCapacity = onlineShapedQueueCapacity)
    println(
      s"REDIS_FETCHER_ONLINE_SHAPED_SCOPE entrypoint=Fetcher.fetchJoin kv_backend=RedisKVStoreImpl " +
        s"redis_server=$effectiveServerLabel redis_client=$redisClient glide_execution_mode=$glideExecutionMode " +
        s"read_from=$readFrom shared_executor_threads=${sharedPool.threads} " +
        s"shared_executor_queue_capacity=${sharedPool.queueCapacity} ingress_executor=separate " +
        s"candidates=${RedisFetcherOnlineShape.CandidateCount} " +
        s"request_batch_ring=${RedisFetcherOnlineShape.RequestBatchCount} " +
        s"group_bys=${RedisFetcherOnlineShape.GroupByCount} " +
        s"logical_group_by_requests=${RedisFetcherOnlineShape.LogicalGroupByRequests} " +
        s"unique_batch_reads=${RedisFetcherOnlineShape.UniqueBatchRequests} " +
        s"unique_streaming_reads=${RedisFetcherOnlineShape.UniqueStreamingRequests} " +
        s"unique_kv_reads=${RedisFetcherOnlineShape.UniqueKvRequests} " +
        s"included=Fetcher_planning,key_encoding,Redis_IO,Avro_decode,fanout,JoinCodec_identity_derivation,response_finalization " +
        s"excluded=external_feature_calls,non_identity_UDF_derivations,response_Avro_encode,Caffeine_batch_IR_cache " +
        s"batch_concurrencies=${onlineShapedBatchConcurrencies.mkString(",")} " +
        s"warmup_iterations=$onlineShapedWarmupIterations " +
        s"measurement_iterations=$onlineShapedMeasurementIterations")

    withRedisStore(sharedPool, pipelineSyncThreads = 0, loader = true) { (loaderStore, _, _) =>
      val startedNanos = System.nanoTime()
      val puts = RedisFetcherOnlineShape.load(loaderStore, loadBatchSize, 5.minutes)
      val seconds = (System.nanoTime() - startedNanos).toDouble / TimeUnit.SECONDS.toNanos(1)
      println(f"REDIS_FETCHER_ONLINE_SHAPED_LOAD puts=$puts%d seconds=$seconds%.3f")
    }

    withRedisStore(sharedPool, onlineShapedPipelineSyncThreads, loader = false) {
      (kvStore, sharedExecutor, _) =>
        val fixture = RedisFetcherOnlineShape.newFixture(kvStore, kvStore.executionContext)
        RedisFetcherOnlineShape.validate(fixture, readTimeout)
        val ingressExecutor = Executors
          .newFixedThreadPool(onlineShapedBatchConcurrencies.max)
          .asInstanceOf[ThreadPoolExecutor]
        ingressExecutor.prestartAllCoreThreads()
        val nextRequestBatch = new AtomicInteger()
        try {
          onlineShapedBatchConcurrencies.foreach { batchConcurrency =>
            if (onlineShapedWarmupIterations > 0) {
              val warmup = RedisFetcherOnlineShape.execute(
                fixture,
                sharedExecutor,
                ingressExecutor,
                onlineShapedWarmupIterations,
                batchConcurrency,
                readTimeout,
                nextRequestBatch
              )
              requireOnlineShapedSuccessful("warmup", warmup)
            }
            val measurement = RedisFetcherOnlineShape.execute(
              fixture,
              sharedExecutor,
              ingressExecutor,
              onlineShapedMeasurementIterations,
              batchConcurrency,
              readTimeout,
              nextRequestBatch
            )
            requireOnlineShapedSuccessful("measurement", measurement)
            printOnlineShapedResult(sharedPool, batchConcurrency, measurement)
            resultSink = java.lang.Long.valueOf(measurement.checksum)
          }
        } finally {
          ingressExecutor.shutdownNow()
          ingressExecutor.awaitTermination(5, TimeUnit.SECONDS)
        }
    }
  }

  private def requireOnlineShapedSuccessful(phase: String,
                                            measurement: RedisFetcherOnlineShape.Measurement): Unit = {
    val failureMessage = Option(measurement.firstFailure).map(failureChain).getOrElse("unknown failure")
    require(measurement.failures == 0,
            s"Online-shaped Redis $phase had ${measurement.failures} failure(s): $failureMessage")
  }

  private def printOnlineShapedResult(poolConfig: PoolConfig,
                                      batchConcurrency: Int,
                                      measurement: RedisFetcherOnlineShape.Measurement): Unit = {
    val endToEnd = measurement.endToEndNanos.sorted
    val fetchJoin = measurement.fetchJoinNanos.sorted
    val callbackQueue = measurement.finalCallbackQueueNanos.sorted
    val callbackCpu = measurement.finalCallbackCpuNanos.sorted
    val wallSeconds = measurement.wallNanos.toDouble / TimeUnit.SECONDS.toNanos(1)
    val batchesPerSecond = endToEnd.length / wallSeconds
    val candidateRowsPerSecond = batchesPerSecond * RedisFetcherOnlineShape.CandidateCount
    val averageMillis = endToEnd.iterator.map(_.toDouble).sum / endToEnd.length / TimeUnit.MILLISECONDS.toNanos(1)

    println(
      f"REDIS_FETCHER_ONLINE_SHAPED_RESULT entrypoint=Fetcher.fetchJoin kv_backend=RedisKVStoreImpl " +
        f"redis_server=$effectiveServerLabel%s redis_client=$redisClient%s " +
        f"glide_execution_mode=$glideExecutionMode%s read_from=$readFrom%s " +
        f"shared_executor_threads=${poolConfig.threads}%d " +
        f"shared_executor_queue_capacity=${poolConfig.queueCapacity}%d " +
        f"batch_concurrency=$batchConcurrency%d iterations=${endToEnd.length}%d failures=${measurement.failures}%d " +
        f"batches_per_sec=$batchesPerSecond%.2f candidate_rows_per_sec=$candidateRowsPerSecond%.2f " +
        f"avg_ms=$averageMillis%.3f p50_ms=${percentileMillis(endToEnd, 0.50)}%.3f " +
        f"p95_ms=${percentileMillis(endToEnd, 0.95)}%.3f p99_ms=${percentileMillis(endToEnd, 0.99)}%.3f " +
        f"fetch_join_p95_ms=${percentileMillis(fetchJoin, 0.95)}%.3f " +
        f"fetch_join_p99_ms=${percentileMillis(fetchJoin, 0.99)}%.3f " +
        f"final_callback_queue_p50_ms=${percentileMillis(callbackQueue, 0.50)}%.3f " +
        f"final_callback_queue_p95_ms=${percentileMillis(callbackQueue, 0.95)}%.3f " +
        f"final_callback_queue_p99_ms=${percentileMillis(callbackQueue, 0.99)}%.3f " +
        f"final_callback_cpu_p95_ms=${percentileMillis(callbackCpu, 0.95)}%.3f " +
        f"shared_executor_peak_active=${measurement.peakExecutorActive}%d " +
        f"shared_executor_peak_queue=${measurement.peakExecutorQueue}%d " +
        f"shared_executor_tasks=${measurement.executorTasks}%d " +
        f"shared_executor_completed=${measurement.executorCompleted}%d checksum=${measurement.checksum}%d")
  }

  private def withRedisStore(poolConfig: PoolConfig, pipelineSyncThreads: Int, loader: Boolean)(
      run: (RedisKVStoreImpl, ThreadPoolExecutor, Option[JedisCluster]) => Unit): Unit = {
    if (useExternalCluster) {
      val executor = newRedisExecutor(poolConfig)
      var jedisClient: JedisCluster = null
      var kvStore: BenchmarkRedisKVStore = null
      try {
        requireExecutorConfig(executor, poolConfig)
        val settings = externalClientSettings.copy(pipelineSyncThreads = pipelineSyncThreads)
        jedisClient = RedisKVStoreFactory.createClient(settings)
        val glideClient =
          if (!loader && redisClient == RedisClientGlide)
            Some(RedisKVStoreFactory.createGlideReadClient(settings, glideExecutionMode))
          else None
        kvStore = new BenchmarkRedisKVStore(
          jedisClient,
          glideClient,
          Map(
            "redis.key.prefix" -> keyPrefix,
            "redis.key.hash.tag.mode" -> keyHashTagMode,
            "redis.read.metrics.enabled" -> readMetricsEnabled.toString,
            ConfigPipelineSyncThreads -> pipelineSyncThreads.toString
          ),
          ExecutionContext.fromExecutor(executor)
        )
        run(kvStore,
            executor,
            if (!loader && redisClient == RedisClientJedis) Some(jedisClient) else None)
      }
      finally {
        try {
          if (kvStore != null) kvStore.close()
          else if (jedisClient != null) jedisClient.close()
        } finally shutdownExecutor(executor)
      }
    } else {
      val effectivePoolConfig =
        if (loader) PoolConfig(math.max(16, poolConfig.threads), math.max(10000, poolConfig.queueCapacity))
        else poolConfig
      val executor = newRedisExecutor(effectivePoolConfig)
      var kvStore: BenchmarkRedisKVStore = null
      try {
        val glideClient =
          if (!loader && redisClient == RedisClientGlide) {
            Some(
              RedisKVStoreFactory.createGlideReadClient(
                localCluster.clientSettings.copy(
                  client = RedisClientGlide,
                  readFrom = readFrom,
                  clientAz = clientAz,
                  soTimeoutMs = redisSoTimeoutMs
                ),
                glideExecutionMode
              ))
          } else None
        kvStore = new BenchmarkRedisKVStore(
          localCluster.client,
          glideClient,
          Map(
            "redis.key.prefix" -> keyPrefix,
            "redis.key.hash.tag.mode" -> keyHashTagMode,
            "redis.read.metrics.enabled" -> readMetricsEnabled.toString,
            ConfigPipelineSyncThreads -> pipelineSyncThreads.toString
          ),
          ExecutionContext.fromExecutor(executor)
        )
        run(kvStore,
            executor,
            if (!loader && redisClient == RedisClientJedis) Some(localCluster.client) else None)
      }
      finally {
        try shutdownExecutor(executor)
        finally {
          if (kvStore != null) {
            kvStore.shutdownPipelineSyncExecutor()
            kvStore.shutdownGlideReadClient()
          }
        }
      }
    }
  }

  private def shutdownExecutor(executor: ThreadPoolExecutor): Unit = {
    executor.shutdown()
    if (!executor.awaitTermination(readTimeout.toMillis, TimeUnit.MILLISECONDS)) {
      executor.shutdownNow()
      require(executor.awaitTermination(5, TimeUnit.SECONDS), "Redis benchmark executor did not terminate")
    }
  }

  private def newRedisExecutor(poolConfig: PoolConfig): ThreadPoolExecutor = {
    System.setProperty(FlexibleExecutionContext.ThreadPoolSizeProperty, poolConfig.threads.toString)
    System.setProperty(FlexibleExecutionContext.QueueCapacityProperty, poolConfig.queueCapacity.toString)
    val executor = FlexibleExecutionContext.buildExecutor(InstrumentedThreadPoolExecutor.DefaultMetricsContext)
    requireExecutorConfig(executor, poolConfig)
    executor.prestartAllCoreThreads()
    executor
  }

  private def requireExecutorConfig(executor: ThreadPoolExecutor, poolConfig: PoolConfig): Unit = {
    val actualQueueCapacity = executor.getQueue.size() + executor.getQueue.remainingCapacity()
    require(
      executor.getCorePoolSize == poolConfig.threads && executor.getMaximumPoolSize == poolConfig.threads,
      s"Expected ${poolConfig.threads} Redis workers, got core=${executor.getCorePoolSize} " +
        s"max=${executor.getMaximumPoolSize}; run external configurations in a fresh JVM"
    )
    require(
      actualQueueCapacity == poolConfig.queueCapacity,
      s"Expected Redis queue capacity ${poolConfig.queueCapacity}, got $actualQueueCapacity; " +
        "run external configurations in a fresh JVM"
    )
  }

  private def load(kvStore: RedisKVStoreImpl, workload: RedisFetcherReadWorkload.Workload): Unit = {
    workload.datasets.foreach(kvStore.create)
    val startedNanos = System.nanoTime()
    var loaded = 0
    workload.puts.grouped(loadBatchSize).foreach { batch =>
      val results = Await.result(kvStore.multiPut(batch), 5.minutes)
      val failures = results.count(result => !result)
      require(failures == 0, s"Redis fixture load had $failures failure(s) in a batch of ${batch.size}")
      loaded += batch.size
    }
    val elapsedSeconds = (System.nanoTime() - startedNanos).toDouble / TimeUnit.SECONDS.toNanos(1)
    println(f"REDIS_FETCHER_READ_LOAD puts=$loaded%d seconds=$elapsedSeconds%.3f")
  }

  private def validateProfile(kvStore: RedisKVStoreImpl, profile: RedisFetcherReadWorkload.ReadProfile): Unit = {
    val responses = Await.result(kvStore.multiGet(profile.requests), readTimeout)
    val failures = responses.count(_.values.isFailure)
    val timedValues = responses.iterator.flatMap(_.values.getOrElse(Seq.empty)).toVector
    val payloadBytes = timedValues.iterator.map(value => value.bytes.length.toLong).sum

    require(responses.size == profile.requests.size,
            s"${profile.name}: expected ${profile.requests.size} responses, got ${responses.size}")
    require(failures == 0, s"${profile.name}: validation had $failures failed Redis response(s)")
    require(timedValues.size == profile.expectedTimedValues,
            s"${profile.name}: expected ${profile.expectedTimedValues} values, got ${timedValues.size}")
    require(payloadBytes == profile.expectedPayloadBytes,
            s"${profile.name}: expected ${profile.expectedPayloadBytes} payload bytes, got $payloadBytes")
  }

  private def awaitValidateProfile(kvStore: RedisKVStoreImpl,
                                   profile: RedisFetcherReadWorkload.ReadProfile): Unit = {
    val deadlineNanos = System.nanoTime() + readTimeout.toNanos
    var lastFailure: Throwable = null
    do {
      try {
        validateProfile(kvStore, profile)
        return
      } catch {
        case throwable: Exception =>
          lastFailure = throwable
          Thread.sleep(25)
      }
    } while (System.nanoTime() < deadlineNanos)
    throw new IllegalStateException(s"${profile.name}: replicas did not expose the loaded fixture before timeout",
                                    lastFailure)
  }

  private def printReadRouting(kvStore: RedisKVStoreImpl,
                               profile: RedisFetcherReadWorkload.ReadProfile): Unit = {
    localCluster.resetCommandStats()
    validateProfile(kvStore, profile)
    val stats = localCluster.readCommandStats()
    val primaryCalls = stats.filter(_.role == "master").map(_.totalReadCalls).sum
    val primaryStats = stats.filter(_.role == "master")
    val replicaStats = stats.filter(node => node.role == "slave" || node.role == "replica")
    val replicaCalls = replicaStats.map(_.totalReadCalls).sum
    val totalCalls = primaryCalls + replicaCalls
    val expectedPrimaryFraction = primaryStats.size.toDouble / (primaryStats.size + replicaStats.size)
    val observedPrimaryFraction = if (totalCalls == 0L) 0.0 else primaryCalls.toDouble / totalCalls
    val allNodesUsedAndBalanced =
      stats.forall(_.totalReadCalls > 0L) && math.abs(observedPrimaryFraction - expectedPrimaryFraction) <= 0.10
    val routeMatchesPolicy = readFrom match {
      case RedisReadFromPrimary => primaryCalls > 0 && replicaCalls == 0
      case RedisReadFromAllNodes => primaryCalls > 0 && replicaCalls > 0 && allNodesUsedAndBalanced
      case RedisReadFromAzAffinityReplicasAndPrimary =>
        val localStats = stats.filter(_.availabilityZone == clientAz)
        val localPrimaryCalls = localStats.filter(_.role == "master").map(_.totalReadCalls).sum
        val localReplicaCalls = localStats
          .filter(node => node.role == "slave" || node.role == "replica")
          .map(_.totalReadCalls)
          .sum
        val localCalls = localPrimaryCalls + localReplicaCalls
        localPrimaryCalls > 0L && localReplicaCalls > 0L && localCalls.toDouble / totalCalls > 0.5
    }
    val nodes = stats
      .map { node =>
        s"${node.address}|${node.role}|${node.availabilityZone.getOrElse("unset")}|" +
          s"get=${node.getCalls}|mget=${node.mgetCalls}|zrange=${node.zrangeCalls}"
      }
      .mkString(";")
    require(
      routeMatchesPolicy,
      s"Local read route did not satisfy read_from=$readFrom: $nodes"
    )
    println(
      s"REDIS_FETCHER_READ_ROUTE profile=${profile.name} redis_client=$redisClient " +
        s"redis_server=$effectiveServerLabel redis_cluster_image=${localCluster.image} " +
        s"glide_execution_mode=$glideExecutionMode read_from=$readFrom client_az=${clientAz.getOrElse("unset")} " +
        s"primary_read_calls=$primaryCalls replica_read_calls=$replicaCalls nodes=$nodes")
  }

  private def execute(kvStore: RedisKVStoreImpl,
                      redisExecutor: ThreadPoolExecutor,
                      jedisCluster: Option[JedisCluster],
                      profile: RedisFetcherReadWorkload.ReadProfile,
                      iterations: Int,
                      batchConcurrency: Int): Measurement = {
    val nextIteration = new AtomicInteger()
    val failures = new AtomicInteger()
    val firstFailure = new AtomicReference[Throwable]()
    val peakQueue = new AtomicInteger()
    val peakActive = new AtomicInteger()
    val peakJedisActive = new AtomicInteger()
    val peakJedisWaiters = new AtomicInteger()
    val peakPipelineActive = new AtomicInteger()
    val peakPipelineQueue = new AtomicInteger()
    val sampling = new AtomicBoolean(true)
    val latenciesNanos = Array.fill(iterations)(0L)
    val submittedBefore = redisExecutor.getTaskCount
    val completedBefore = redisExecutor.getCompletedTaskCount
    val pipelineExecutor = kvStore.pipelineSyncExecutor
    val pipelineSubmittedBefore = pipelineExecutor.map(_.getTaskCount).getOrElse(0L)
    val pipelineCompletedBefore = pipelineExecutor.map(_.getCompletedTaskCount).getOrElse(0L)
    val sampler = new Thread(
      () => {
        while (sampling.get()) {
          peakQueue.accumulateAndGet(redisExecutor.getQueue.size(), Math.max)
          peakActive.accumulateAndGet(redisExecutor.getActiveCount, Math.max)
          jedisCluster.foreach { client =>
            val pools = client.getClusterNodes.values().asScala
            peakJedisActive.accumulateAndGet(pools.iterator.map(_.getNumActive).sum, Math.max)
            peakJedisWaiters.accumulateAndGet(pools.iterator.map(_.getNumWaiters).sum, Math.max)
          }
          pipelineExecutor.foreach { executor =>
            peakPipelineActive.accumulateAndGet(executor.getActiveCount, Math.max)
            peakPipelineQueue.accumulateAndGet(executor.getQueue.size(), Math.max)
          }
          Thread.sleep(PoolSampleIntervalMillis)
        }
      },
      "redis-benchmark-pool-sampler"
    )
    sampler.setDaemon(true)
    sampler.start()

    val ingressExecutor =
      Executors.newFixedThreadPool(math.min(batchConcurrency, iterations)).asInstanceOf[ThreadPoolExecutor]
    ingressExecutor.prestartAllCoreThreads()
    val startedNanos = System.nanoTime()

    try {
      val workers = (0 until math.min(batchConcurrency, iterations)).map { _ =>
        ingressExecutor.submit(new Runnable {
          override def run(): Unit = {
            var iteration = nextIteration.getAndIncrement()
            while (iteration < iterations) {
              val requestStartedNanos = System.nanoTime()
              try {
                val responses: Seq[GetResponse] = Await.result(kvStore.multiGet(profile.requests), readTimeout)
                requireSuccessfulResponses(profile, responses)
                resultSink = responses.asInstanceOf[AnyRef]
              } catch {
                case throwable: Throwable =>
                  failures.incrementAndGet()
                  firstFailure.compareAndSet(null, throwable)
              } finally {
                latenciesNanos(iteration) = System.nanoTime() - requestStartedNanos
              }
              iteration = nextIteration.getAndIncrement()
            }
          }
        })
      }
      workers.foreach(_.get())
      val wallNanos = System.nanoTime() - startedNanos
      sampling.set(false)
      sampler.join(TimeUnit.SECONDS.toMillis(1))
      Measurement(
        latenciesNanos,
        failures.get(),
        firstFailure.get(),
        wallNanos,
        PoolSnapshot(
          peakQueue.get(),
          peakActive.get(),
          redisExecutor.getTaskCount - submittedBefore,
          redisExecutor.getCompletedTaskCount - completedBefore,
          jedisCluster.isDefined,
          peakJedisActive.get(),
          peakJedisWaiters.get(),
          kvStore.pipelineSyncThreads,
          pipelineExecutor.isDefined,
          pipelineExecutor.map(_ => peakPipelineActive.get()).getOrElse(-1),
          pipelineExecutor.map(_ => peakPipelineQueue.get()).getOrElse(-1),
          pipelineExecutor.map(_.getTaskCount - pipelineSubmittedBefore).getOrElse(-1L),
          pipelineExecutor.map(_.getCompletedTaskCount - pipelineCompletedBefore).getOrElse(-1L),
          pipelineExecutor.map(_.getLargestPoolSize).getOrElse(-1)
        )
      )
    } finally {
      sampling.set(false)
      ingressExecutor.shutdownNow()
      ingressExecutor.awaitTermination(5, TimeUnit.SECONDS)
    }
  }

  private def requireSuccessfulResponses(profile: RedisFetcherReadWorkload.ReadProfile,
                                         responses: Seq[GetResponse]): Unit = {
    if (responses.size != profile.requests.size) {
      throw new IllegalStateException(
        s"${profile.name}: expected ${profile.requests.size} timed responses, got ${responses.size}")
    }
    responses.iterator.find(_.values.isFailure).foreach { failedResponse =>
      throw new IllegalStateException(s"${profile.name}: timed Redis response failed", failedResponse.values.failed.get)
    }
  }

  private def printResult(workload: RedisFetcherReadWorkload.Workload,
                          profile: RedisFetcherReadWorkload.ReadProfile,
                          poolConfig: PoolConfig,
                          measurement: Measurement,
                          batchConcurrency: Int): Unit = {
    val sorted = measurement.latenciesNanos.sorted
    val wallSeconds = measurement.wallNanos.toDouble / TimeUnit.SECONDS.toNanos(1)
    val demandBatchesPerSecond = measurement.latenciesNanos.length / wallSeconds
    val effectiveBatchReadCommands = effectiveBatchCommands(profile)
    val effectiveStreamingCommands = effectiveStreamingRangeCommands(profile)
    val effectiveRedisCommands = effectiveBatchReadCommands + effectiveStreamingCommands
    val candidatesPerSecond = demandBatchesPerSecond * workload.config.candidates
    val logicalGroupBysPerSecond = demandBatchesPerSecond * workload.logicalGroupByRequests
    val batchGetCommandsPerSecond = demandBatchesPerSecond * effectiveBatchReadCommands
    val zrangeCommandsPerSecond = demandBatchesPerSecond * effectiveStreamingCommands
    val redisCommandsPerSecond = demandBatchesPerSecond * effectiveRedisCommands
    val kvResponsesPerSecond = demandBatchesPerSecond * profile.requests.size
    val timedValuesPerSecond = demandBatchesPerSecond * profile.expectedTimedValues
    val payloadMiBPerSecond = demandBatchesPerSecond * profile.expectedPayloadBytes / (1024.0 * 1024.0)
    val averageMillis = sorted.iterator.map(_.toDouble).sum / sorted.length / TimeUnit.MILLISECONDS.toNanos(1)
    val imageLabel = if (useExternalCluster) "external" else localCluster.image
    val perCpuMetrics = localCpuLimit
      .map { cpus =>
        f"redis_container_cpus=$cpus%.3f " +
          f"batches_per_sec_per_redis_cpu=${demandBatchesPerSecond / cpus}%.2f " +
          f"candidate_rows_per_sec_per_redis_cpu=${candidatesPerSecond / cpus}%.2f " +
          f"redis_commands_per_sec_per_redis_cpu=${redisCommandsPerSecond / cpus}%.2f " +
          f"timed_values_per_sec_per_redis_cpu=${timedValuesPerSecond / cpus}%.2f " +
          f"payload_mib_per_sec_per_redis_cpu=${payloadMiBPerSecond / cpus}%.2f "
      }
      .getOrElse("redis_container_cpus=unlimited ")

    println(
      f"REDIS_FETCHER_READ_RESULT profile=${profile.name} batch_concurrency=$batchConcurrency%d " +
        f"redis_server=$effectiveServerLabel redis_cluster_image=$imageLabel redis_client=$redisClient " +
        f"glide_execution_mode=$glideExecutionMode read_from=$readFrom " +
        f"redis_so_timeout_ms=$redisSoTimeoutMs%d " +
        f"redis_threads=${poolConfig.threads}%d redis_queue_capacity=${poolConfig.queueCapacity}%d " +
        f"redis_max_connections_per_node=$maxConnections%d redis_min_idle_per_node=$minIdleConnections%d " +
        f"redis_max_idle_per_node=$maxIdleConnections%d iterations=${sorted.length}%d failures=${measurement.failures}%d " +
        f"kvstore_demand_batches_per_sec=$demandBatchesPerSecond%.2f candidates_per_sec=$candidatesPerSecond%.2f " +
        f"logical_group_bys_per_sec=$logicalGroupBysPerSecond%.2f " +
        f"batch_get_commands_per_sec=$batchGetCommandsPerSecond%.2f " +
        f"zrange_commands_per_sec=$zrangeCommandsPerSecond%.2f " +
        f"redis_commands_per_sec=$redisCommandsPerSecond%.2f kv_responses_per_sec=$kvResponsesPerSecond%.2f " +
        f"timed_values_per_sec=$timedValuesPerSecond%.2f payload_mib_per_sec=$payloadMiBPerSecond%.2f " +
        perCpuMetrics +
        f"avg_ms=$averageMillis%.3f " +
        f"p50_ms=${percentileMillis(sorted, 0.50)}%.3f p95_ms=${percentileMillis(sorted, 0.95)}%.3f " +
        f"p99_ms=${percentileMillis(sorted, 0.99)}%.3f max_ms=${sorted.last.toDouble / TimeUnit.MILLISECONDS.toNanos(1)}%.3f " +
        f"redis_sampled_peak_active=${measurement.pool.peakActive}%d " +
        f"redis_sampled_peak_queue=${measurement.pool.peakQueue}%d " +
        f"redis_tasks=${measurement.pool.submittedTasks}%d redis_completed=${measurement.pool.completedTasks}%d " +
        f"jedis_pool_metrics=${measurement.pool.jedisMetricsAvailable}%s " +
        f"jedis_sampled_peak_active_sum=${measurement.pool.peakJedisActive}%d " +
        f"jedis_sampled_peak_waiters_sum=${measurement.pool.peakJedisWaiters}%d " +
        f"jedis_pipeline_executor_mode=" +
        (if (redisClient != RedisClientJedis) "not_applicable "
         else if (measurement.pool.pipelineExecutorAvailable) "shared "
         else "dedicated_per_pipeline ") +
        f"jedis_pipeline_shared_threads=${measurement.pool.pipelineSyncThreads}%d " +
        f"jedis_pipeline_dedicated_workers_per_pipeline=" +
        f"${redis.clients.jedis.MultiNodePipelineBase.MULTI_NODE_PIPELINE_SYNC_WORKERS}%d " +
        f"jedis_pipeline_sampled_peak_active=${measurement.pool.peakPipelineActive}%d " +
        f"jedis_pipeline_sampled_peak_queue=${measurement.pool.peakPipelineQueue}%d " +
        f"jedis_pipeline_tasks=${measurement.pool.pipelineSubmittedTasks}%d " +
        f"jedis_pipeline_completed=${measurement.pool.pipelineCompletedTasks}%d " +
        f"jedis_pipeline_prestarted_threads=${measurement.pool.pipelineLargestPoolSize}%d " +
        f"batch_get_commands_per_batch=$effectiveBatchReadCommands%d " +
        f"zrange_commands_per_batch=$effectiveStreamingCommands%d " +
        f"redis_commands_per_batch=$effectiveRedisCommands%d kv_responses_per_batch=${profile.requests.size}%d " +
        f"timed_values_per_batch=${profile.expectedTimedValues}%d " +
        f"payload_bytes_per_batch=${profile.expectedPayloadBytes}%d"
    )
  }

  private def effectiveBatchCommands(profile: RedisFetcherReadWorkload.ReadProfile): Int = {
    val slots = profile.requests.iterator.collect {
      case request if request.startTsMillis.isEmpty =>
        val redisKey = RedisKVStore
          .buildRedisKey(request.keyBytes, request.dataset, keyPrefix = keyPrefix, hashTagMode = keyHashTagMode)
          .getBytes(java.nio.charset.StandardCharsets.UTF_8)
        JedisClusterCRC16.getSlot(redisKey)
    }.toSet
    slots.size
  }

  private def effectiveStreamingRangeCommands(profile: RedisFetcherReadWorkload.ReadProfile): Int = {
    profile.zrangeCommands
  }

  private def requireSuccessful(profile: String, phase: String, measurement: Measurement): Unit = {
    val failureMessage = Option(measurement.firstFailure).map(failureChain).getOrElse("unknown failure")
    require(measurement.failures == 0,
            s"Redis $phase for profile $profile had ${measurement.failures} failure(s): $failureMessage")
  }

  private def failureChain(error: Throwable): String = {
    val causes = mutable.ArrayBuffer.empty[String]
    var current = error
    var remaining = 8
    while (current != null && remaining > 0) {
      causes += s"${current.getClass.getSimpleName}: ${Option(current.getMessage).getOrElse("no message")}"
      current = current.getCause
      remaining -= 1
    }
    causes.mkString(" caused by ")
  }

  private def percentileMillis(sortedNanos: Array[Long], percentile: Double): Double = {
    val index = math.max(0, math.min(sortedNanos.length - 1, math.ceil(sortedNanos.length * percentile).toInt - 1))
    sortedNanos(index).toDouble / TimeUnit.MILLISECONDS.toNanos(1)
  }

  private def envInt(name: String, default: Int): Int =
    sys.env.get(name).map(_.toInt).getOrElse(default)

  private def envOptionalDouble(name: String): Option[Double] =
    sys.env.get(name).map(_.trim.toDouble)

  private def envInts(name: String, defaults: Seq[Int]): Vector[Int] =
    sys.env
      .get(name)
      .map(_.split(",").iterator.map(_.trim).filter(_.nonEmpty).map(_.toInt).toVector)
      .getOrElse(defaults.toVector)
      .distinct

  private def envIntsWithLegacy(name: String,
                                legacyName: String,
                                secondLegacyDefault: Int,
                                defaults: Seq[Int]): Vector[Int] =
    sys.env.get(name) match {
      case Some(_) => envInts(name, defaults)
      case None =>
        sys.env
          .get(legacyName)
          .map(value => Vector(value.toInt))
          .orElse(if (secondLegacyDefault > 0) Some(Vector(secondLegacyDefault)) else None)
          .getOrElse(defaults.toVector)
    }
}

private object RedisFetcherReadPerfTestHarness {
  val PoolSampleIntervalMillis = 1L

  final case class PoolConfig(threads: Int, queueCapacity: Int)

  final case class PoolSnapshot(
      peakQueue: Int,
      peakActive: Int,
      submittedTasks: Long,
      completedTasks: Long,
      jedisMetricsAvailable: Boolean,
      peakJedisActive: Int,
      peakJedisWaiters: Int,
      pipelineSyncThreads: Int,
      pipelineExecutorAvailable: Boolean,
      peakPipelineActive: Int,
      peakPipelineQueue: Int,
      pipelineSubmittedTasks: Long,
      pipelineCompletedTasks: Long,
      pipelineLargestPoolSize: Int
  )

  final case class Measurement(
      latenciesNanos: Array[Long],
      failures: Int,
      firstFailure: Throwable,
      wallNanos: Long,
      pool: PoolSnapshot
  )
}
