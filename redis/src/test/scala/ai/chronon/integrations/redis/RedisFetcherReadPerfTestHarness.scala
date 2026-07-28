package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisKVStoreConstants.{
  DefaultMinIdleConnections,
  EnvRedisClusterNodes,
  EnvRedisMaxConnections,
  EnvRedisMaxIdleConnections,
  EnvRedisMinIdleConnections
}
import ai.chronon.online.KVStore.GetResponse
import ai.chronon.online.metrics.{FlexibleExecutionContext, InstrumentedThreadPoolExecutor}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import redis.clients.jedis.JedisCluster

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import java.util.concurrent.{Executors, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Opt-in integration benchmark for production-shaped Redis KVStore reads.
  *
  * This measures the real Jedis/RedisKVStoreImpl path, so JMH would add iteration machinery without removing network
  * variance. It starts at KVStore.multiGet, after join planning and context deduplication, and excludes downstream
  * feature decoding. By default it starts a native local three-primary/three-replica Redis cluster and sweeps both
  * worker-pool sizes and concurrent batches. Synthetic cache-demand profiles assume all context batch IRs are hot and
  * apply the named hit percentage to candidate keys; they do not instantiate Caffeine or measure fetcher cache work.
  * Set PERF_REDIS_PRIMARY_COUNT and PERF_REDIS_REPLICAS_PER_PRIMARY to measure shard-topology tradeoffs. Set
  * PERF_REDIS_CPU_LIMIT to constrain the local Redis container for fixed-resource comparisons. Set
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
      conf: Map[String, String],
      override implicit val executionContext: ExecutionContext
  ) extends RedisKVStoreImpl(client, conf)

  private val enabled = sys.env.get("CHRONON_PERF_TEST_ENABLED").exists(_.equalsIgnoreCase("true"))
  private val warmupIterations = envInt("PERF_WARMUP_ITERATIONS", 500)
  private val measurementIterations = envInt("PERF_MEASUREMENT_ITERATIONS", 5000)
  private val batchConcurrencies = envIntsWithLegacy(
    "PERF_BATCH_CONCURRENCIES",
    "PERF_BATCH_CONCURRENCY",
    envInt("PERF_NUM_THREADS", 0),
    Seq(32, 48, 64, 96)
  )
  private val workerThreadCounts = envInts("PERF_REDIS_THREAD_COUNTS", Seq(16, 32, 48, 64))
  private val queueCapacities = envInts("PERF_REDIS_QUEUE_CAPACITIES", Seq(10000))
  private val loadBatchSize = envInt("PERF_LOAD_BATCH_SIZE", 100)
  private val maxConnections = envInt("PERF_REDIS_MAX_CONNECTIONS", 64)
  private val maxIdleConnections =
    envInt("PERF_REDIS_MAX_IDLE_CONNECTIONS", math.min(RedisKVStoreConstants.DefaultMaxIdleConnections, maxConnections))
  private val minIdleConnections =
    envInt("PERF_REDIS_MIN_IDLE_CONNECTIONS", math.min(DefaultMinIdleConnections, maxIdleConnections))
  private val readTimeout = envInt("PERF_READ_TIMEOUT_SECONDS", 30).seconds
  private val keyPrefix = sys.env.getOrElse("PERF_REDIS_KEY_PREFIX", "chronon-perf")
  private val useExternalCluster = sys.env.get("PERF_REDIS_USE_EXTERNAL").exists(_.equalsIgnoreCase("true"))
  private val useNativeLocalCluster =
    sys.env.getOrElse("PERF_REDIS_USE_NATIVE_LOCAL", "true").equalsIgnoreCase("true")
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
  require(queueCapacities.nonEmpty && queueCapacities.forall(_ > 0))
  require(loadBatchSize > 0)
  require(maxConnections > 0)
  require(localPrimaryCount >= 3)
  require(localReplicasPerPrimary >= 0)
  require(localCpuLimit.forall(_ > 0))
  require(minIdleConnections >= 0 && minIdleConnections <= maxIdleConnections)
  require(maxIdleConnections >= 0 && maxIdleConnections <= maxConnections)
  if (useExternalCluster) {
    require(localCpuLimit.isEmpty, "PERF_REDIS_CPU_LIMIT only applies to the local Redis fixture")
    require(poolConfigs.size == 1,
            "External Redis runs require one worker/queue configuration per JVM for an isolated executor measurement")
  }

  @volatile private var resultSink: AnyRef = _
  private var localCluster: RedisClusterFixture = _
  private var externalKvStore: RedisKVStoreImpl = _

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
        externalKvStore = RedisKVStoreFactory.create(
          Map(
            EnvRedisClusterNodes -> nodes,
            EnvRedisMaxConnections -> maxConnections.toString,
            EnvRedisMinIdleConnections -> minIdleConnections.toString,
            EnvRedisMaxIdleConnections -> maxIdleConnections.toString,
            "redis.key.prefix" -> keyPrefix
          ))
      } else {
        localCluster =
          if (useNativeLocalCluster)
            RedisClusterFixture.startNative(
              maxConnections,
              minIdleConnections,
              maxIdleConnections,
              localPrimaryCount,
              localReplicasPerPrimary,
              localCpuLimit
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

  "Redis fetcher read benchmark" should "measure the 50-candidate mixed GroupBy workload" in {
    assume(enabled, "Set CHRONON_PERF_TEST_ENABLED=true to run Redis performance tests")

    val workload = RedisFetcherReadWorkload.build()
    val profilesByName =
      (Seq(workload.deduplicated, workload.deduplicatedCrossDay, workload.logical, workload.batchOnly) ++
        workload.batchCacheDemand)
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
      s"cluster_pipelines_per_batch=1 " +
      s"range_hours=${workload.config.tiledPoints} hourly_tiles=${workload.config.tiledPoints} " +
      s"payload_bytes=${workload.deduplicated.expectedPayloadBytes} " +
      s"batch_concurrencies=${batchConcurrencies.mkString(",")} max_connections_per_node=$maxConnections " +
      s"min_idle_connections_per_node=$minIdleConnections max_idle_connections_per_node=$maxIdleConnections " +
      s"test_on_borrow=true test_on_return=true test_while_idle=true " +
      s"redis_cluster_image=${Option(localCluster).map(_.image).getOrElse("external")} " +
      s"redis_cluster_primaries=${Option(localCluster).map(_.primaryCount.toString).getOrElse("external")} " +
      s"redis_replicas_per_primary=" +
      s"${Option(localCluster).map(_.replicasPerPrimary.toString).getOrElse("external")} " +
      s"redis_container_cpu_limit=" +
      s"${Option(localCluster).flatMap(_.cpuLimit).map(_.toString).getOrElse("unlimited")} " +
      s"pool_sample_interval_ms=$PoolSampleIntervalMillis " +
      s"jedis_pipeline_sync_workers_per_pipeline=" +
      s"${redis.clients.jedis.MultiNodePipelineBase.MULTI_NODE_PIPELINE_SYNC_WORKERS} " +
      s"worker_threads=${workerThreadCounts.mkString(",")} queue_capacities=${queueCapacities.mkString(",")}")

    withRedisStore(poolConfigs.maxBy(_.threads), loader = true) { (loaderStore, _, _) =>
      load(loaderStore, workload)
    }

    poolConfigs.foreach { poolConfig =>
      withRedisStore(poolConfig, loader = false) { (kvStore, redisExecutor, jedisCluster) =>
        selectedProfiles.foreach(profile => validateProfile(kvStore, profile))
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

  private def withRedisStore(poolConfig: PoolConfig, loader: Boolean)(
      run: (RedisKVStoreImpl, ThreadPoolExecutor, Option[JedisCluster]) => Unit): Unit = {
    if (useExternalCluster) {
      val executor = FlexibleExecutionContext.buildExecutor
      requireExecutorConfig(executor, poolConfig)
      run(externalKvStore, executor, None)
    } else {
      val effectivePoolConfig =
        if (loader) PoolConfig(math.max(16, poolConfig.threads), math.max(10000, poolConfig.queueCapacity))
        else poolConfig
      val executor = newRedisExecutor(effectivePoolConfig)
      val kvStore = new BenchmarkRedisKVStore(
        localCluster.client,
        Map("redis.key.prefix" -> keyPrefix),
        ExecutionContext.fromExecutor(executor)
      )
      try run(kvStore, executor, Some(localCluster.client))
      finally {
        executor.shutdownNow()
        executor.awaitTermination(5, TimeUnit.SECONDS)
      }
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
    val sampling = new AtomicBoolean(true)
    val latenciesNanos = Array.fill(iterations)(0L)
    val submittedBefore = redisExecutor.getTaskCount
    val completedBefore = redisExecutor.getCompletedTaskCount
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
          peakJedisWaiters.get()
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
    val candidatesPerSecond = demandBatchesPerSecond * workload.config.candidates
    val logicalGroupBysPerSecond = demandBatchesPerSecond * workload.logicalGroupByRequests
    val batchGetCommandsPerSecond = demandBatchesPerSecond * profile.batchGetCommands
    val zrangeCommandsPerSecond = demandBatchesPerSecond * profile.zrangeCommands
    val redisCommandsPerSecond = demandBatchesPerSecond * profile.redisCommands
    val kvResponsesPerSecond = demandBatchesPerSecond * profile.requests.size
    val timedValuesPerSecond = demandBatchesPerSecond * profile.expectedTimedValues
    val payloadMiBPerSecond = demandBatchesPerSecond * profile.expectedPayloadBytes / (1024.0 * 1024.0)
    val averageMillis = sorted.iterator.map(_.toDouble).sum / sorted.length / TimeUnit.MILLISECONDS.toNanos(1)
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
        f"jedis_pipeline_sync_workers_per_pipeline=" +
        f"${redis.clients.jedis.MultiNodePipelineBase.MULTI_NODE_PIPELINE_SYNC_WORKERS}%d " +
        f"batch_get_commands_per_batch=${profile.batchGetCommands}%d " +
        f"zrange_commands_per_batch=${profile.zrangeCommands}%d " +
        f"redis_commands_per_batch=${profile.redisCommands}%d kv_responses_per_batch=${profile.requests.size}%d " +
        f"timed_values_per_batch=${profile.expectedTimedValues}%d " +
        f"payload_bytes_per_batch=${profile.expectedPayloadBytes}%d"
    )
  }

  private def requireSuccessful(profile: String, phase: String, measurement: Measurement): Unit = {
    val failureMessage = Option(measurement.firstFailure).map(_.toString).getOrElse("unknown failure")
    require(measurement.failures == 0,
            s"Redis $phase for profile $profile had ${measurement.failures} failure(s): $failureMessage")
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
      peakJedisWaiters: Int
  )

  final case class Measurement(
      latenciesNanos: Array[Long],
      failures: Int,
      firstFailure: Throwable,
      wallNanos: Long,
      pool: PoolSnapshot
  )
}
