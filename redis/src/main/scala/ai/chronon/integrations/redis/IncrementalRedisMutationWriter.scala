package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.RedisBatchUpload.{BatchStatus, Delete, Operation}
import org.apache.spark.TaskContext
import org.apache.spark.scheduler.{
  AccumulableInfo,
  SparkListener,
  SparkListenerExecutorMetricsUpdate,
  SparkListenerTaskEnd,
  SparkListenerTaskStart
}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{ClusterPipeline, JedisCluster, Response}
import redis.clients.jedis.exceptions.{
  JedisClusterOperationException,
  JedisConnectionException,
  JedisDataException,
  JedisMovedDataException,
  JedisNoScriptException,
  JedisRedirectionException
}

import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, ThreadFactory, ThreadLocalRandom, TimeUnit}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.control.NonFatal

/** Executor-side Redis writes, pipelining, rate limiting, and bounded retry behavior for incremental publication. */
private[redis] object IncrementalRedisMutationWriter {
  import IncrementalRedisBatchModel._

  private val logger = LoggerFactory.getLogger(getClass)
  private val UnsetPipelineResponseMessage = "Please close pipeline or multi block before calling this method."
  private val UploadProgressLogIntervalSeconds = 30L

  private[redis] case class WriteSummary(attempted: Long,
                                         written: Long,
                                         replayed: Long,
                                         rejected: Long,
                                         retries: Long) {
    def +(other: WriteSummary): WriteSummary =
      WriteSummary(attempted + other.attempted,
                   written + other.written,
                   replayed + other.replayed,
                   rejected + other.rejected,
                   retries + other.retries)
  }

  private[redis] case class WriteProgress(generation: String, phase: String, totalKeys: Option[Long]) {
    require(generation.nonEmpty, "Redis upload progress generation must be non-empty")
    require(phase.nonEmpty, "Redis upload progress phase must be non-empty")
    require(totalKeys.forall(_ >= 0), s"Redis upload progress total must be non-negative: $totalKeys")
  }

  private[redis] final class UploadProgressTracker {
    private val taskPartitions = mutable.Map.empty[Long, Int]
    private val partitionHighWater = mutable.Map.empty[Int, Long]

    def taskStarted(taskId: Long, partition: Int): Unit = synchronized {
      require(taskId >= 0, s"Spark task ID must be non-negative: $taskId")
      require(partition >= 0, s"Spark partition must be non-negative: $partition")
      taskPartitions.update(taskId, partition)
    }

    def taskProgress(taskId: Long, completed: Long): Unit = synchronized {
      require(completed >= 0, s"Redis task progress must be non-negative: $completed")
      taskPartitions.get(taskId).foreach { partition =>
        partitionHighWater.update(partition, math.max(partitionHighWater.getOrElse(partition, 0L), completed))
      }
    }

    def completed: Long = synchronized {
      partitionHighWater.valuesIterator.foldLeft(0L)(_ + _)
    }
  }

  private final class UploadProgressReporter(dataset: String,
                                             operation: String,
                                             progress: WriteProgress,
                                             accumulatorId: Long,
                                             writerPartitions: Int,
                                             batchSize: Int)
      extends SparkListener
      with AutoCloseable {
    private val tracker = new UploadProgressTracker
    private val startedAtMillis = System.currentTimeMillis()
    private val closed = new AtomicBoolean(false)
    private val scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(runnable, "redis-upload-progress")
        thread.setDaemon(true)
        thread
      }
    })

    def start(): Unit = {
      logger.info(s"Redis upload started dataset=$dataset generation=${progress.generation} phase=${progress.phase} " +
        s"operation=$operation ${totalText(progress.totalKeys)} writerPartitions=$writerPartitions batchSize=$batchSize")
      scheduler.scheduleAtFixedRate(
        new Runnable {
          override def run(): Unit =
            try logProgress()
            catch {
              case NonFatal(error) => logger.warn("Could not report Redis upload progress", error)
            }
        },
        UploadProgressLogIntervalSeconds,
        UploadProgressLogIntervalSeconds,
        TimeUnit.SECONDS
      )
    }

    override def onTaskStart(event: SparkListenerTaskStart): Unit =
      tracker.taskStarted(event.taskInfo.taskId, event.taskInfo.index)

    override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit =
      event.accumUpdates.foreach { case (taskId, _, _, updates) =>
        accumulatorValue(updates).foreach(value => tracker.taskProgress(taskId, value))
      }

    override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
      accumulatorValue(event.taskInfo.accumulables).foreach(value => tracker.taskProgress(event.taskInfo.taskId, value))

    def complete(completed: Long, retries: Long): Unit = {
      if (closed.compareAndSet(false, true)) {
        scheduler.shutdownNow()
        val elapsedMillis = math.max(1L, System.currentTimeMillis() - startedAtMillis)
        logger.info(
          s"Redis upload completed dataset=$dataset generation=${progress.generation} phase=${progress.phase} " +
            s"operation=$operation completed=$completed ${completedTotalText(progress.totalKeys)}" +
            s"elapsedMs=$elapsedMillis rate=${keysPerSecond(completed, elapsedMillis)} keys/s retries=$retries")
      }
    }

    override def close(): Unit = {
      if (closed.compareAndSet(false, true)) scheduler.shutdownNow()
    }

    private def logProgress(): Unit = {
      if (!closed.get()) {
        val elapsedMillis = math.max(1L, System.currentTimeMillis() - startedAtMillis)
        val completed = progress.totalKeys.fold(tracker.completed)(total => math.min(total, tracker.completed))
        logger.info(
          s"Redis upload progress dataset=$dataset generation=${progress.generation} phase=${progress.phase} " +
            s"operation=$operation completed=$completed ${progressTotalText(completed, progress.totalKeys)}" +
            s"elapsedMs=$elapsedMillis rate=${keysPerSecond(completed, elapsedMillis)} keys/s")
      }
    }

    private def accumulatorValue(updates: scala.collection.Seq[AccumulableInfo]): Option[Long] =
      updates.iterator
        .find(_.id == accumulatorId)
        .flatMap(info => info.update.orElse(info.value))
        .collect { case number: java.lang.Number => number.longValue() }

    private def totalText(totalKeys: Option[Long]): String =
      totalKeys.fold("total=unknown")(total => s"total=$total")

    private def progressTotalText(completed: Long, totalKeys: Option[Long]): String =
      totalKeys.fold("") { total =>
        val percent = if (total == 0) 99 else math.min(99, (completed.toDouble * 100.0 / total).toInt)
        s"total=$total percent=$percent "
      }

    private def completedTotalText(totalKeys: Option[Long]): String =
      totalKeys.fold("")(total => s"total=$total percent=100 ")

    private def keysPerSecond(completed: Long, elapsedMillis: Long): Long =
      math.round(completed.toDouble * 1000.0 / elapsedMillis)
  }

  private[redis] case class OomRetryEvent(attempt: Int, waitMillis: Long, elapsedMillis: Long, remainingMillis: Long)

  private final class TaskClient(settings: RedisKVStoreFactory.ClientSettings) {
    private var current = RedisKVStoreFactory.createClient(settings)

    def client: JedisCluster = current

    def refresh(): Unit = {
      val replacement = RedisKVStoreFactory.createClient(settings)
      val previous = current
      current = replacement
      Try(previous.close())
    }

    def close(): Unit = Try(current.close())
  }

  private[redis] sealed trait WriteMode extends Product with Serializable
  private[redis] case object MutationWrite extends WriteMode
  private[redis] case object PhysicalDelete extends WriteMode
  private[redis] case class LeaseRefresh(expectedMinimumDeadlineMillis: Option[Long]) extends WriteMode
  private case class MutationColumnIndices(tombstoneExpiresAt: Option[Int], allowExpired: Option[Int])

  private[redis] def writeStatusWithOomRetry(client: JedisCluster,
                                             statusKey: String,
                                             status: BatchStatus,
                                             expireAtMillis: Long,
                                             settings: IncrementalRedisBatchSettings,
                                             leaseDeadlineMillis: Long): Long = {
    val retryStartMillis = System.currentTimeMillis()
    val retryDeadlineMillis =
      redisOomRetryDeadline(retryStartMillis, RedisBatchUploadDefaults.OomRetryTimeoutMs, leaseDeadlineMillis)
    withRedisOomRetry(
      operation = () => {
        remainingLeaseSeconds(leaseDeadlineMillis)
        RedisBatchUpload.writeStatusUntil(client, statusKey, status, expireAtMillis)
      },
      retryStartMillis = retryStartMillis,
      retryDeadlineMillis = retryDeadlineMillis,
      retryWaitMillis = attempt => jitteredOomRetryBackoffMillis(attempt, settings.retryBackoffMs),
      nowMillis = () => System.currentTimeMillis(),
      sleepMillis = millis => interruptibleSleep(millis),
      onRetry = event => logRedisOomRetry(settings.batchDataset, "status", batchSize = 1, event)
    )
  }

  private[redis] def writeToRedis(df: DataFrame,
                                  mode: WriteMode,
                                  settings: IncrementalRedisBatchSettings,
                                  limiterKey: String,
                                  leaseDeadlineMillis: Long,
                                  tombstoneDeadlineMillis: Long = 0L,
                                  progress: Option[WriteProgress] = None): WriteSummary = {
    val clientSettings = settings.clientSettings.bulkWriter
    val batchDataset = settings.batchDataset
    val keyPrefix = settings.keyPrefix
    val batchSize = settings.batchSize
    val maxKeysPerSecond = settings.maxKeysPerSecond
    val maxRetries = settings.maxRetries
    val retryBackoffMs = settings.retryBackoffMs
    // This absolute driver-created deadline is captured by every task attempt in the Spark stage. A retried task cannot
    // start a fresh OOM window and multiply the one-hour timeout by Spark's task retry count.
    val oomRetryStartMillis = System.currentTimeMillis()
    val oomRetryDeadlineMillis =
      redisOomRetryDeadline(oomRetryStartMillis, RedisBatchUploadDefaults.OomRetryTimeoutMs, leaseDeadlineMillis)
    val effectiveTombstoneDeadline =
      if (tombstoneDeadlineMillis > 0) tombstoneDeadlineMillis else leaseDeadlineMillis

    val sparkContext = df.sparkSession.sparkContext
    val activeProgress = progress.filter(_.totalKeys.forall(_ > 0))
    val progressAccumulator = activeProgress.map { spec =>
      sparkContext.longAccumulator(s"redis-upload-${spec.generation}-${spec.phase}")
    }
    val progressReporter = activeProgress.zip(progressAccumulator).map { case (spec, accumulator) =>
      new UploadProgressReporter(batchDataset,
                                 writeModeName(mode),
                                 spec,
                                 accumulator.id,
                                 settings.writerPartitions,
                                 batchSize)
    }
    progressReporter.foreach(sparkContext.addSparkListener)

    try {
      progressReporter.foreach(_.start())
      val summaries = df
        .repartition(settings.writerPartitions, col(BucketColumn))
        .rdd
        .mapPartitions { rows =>
          if (!rows.hasNext) Iterator.empty
          else {
            val taskClient = new TaskClient(clientSettings)
            var total = WriteSummary(0, 0, 0, 0, 0)
            try {
              rows.grouped(batchSize).foreach { batch =>
                val batchSummary = writeBatch(
                  taskClient,
                  batch,
                  mode,
                  batchDataset,
                  keyPrefix,
                  limiterKey,
                  batchSize,
                  maxKeysPerSecond,
                  maxRetries,
                  retryBackoffMs,
                  oomRetryStartMillis,
                  oomRetryDeadlineMillis,
                  leaseDeadlineMillis,
                  effectiveTombstoneDeadline
                )
                total = total + batchSummary
                progressAccumulator.foreach(_.add(batchSummary.attempted))
              }
              Iterator.single(total)
            } finally {
              taskClient.close()
            }
          }
        }
        .collect()

      val summary = summaries.foldLeft(WriteSummary(0, 0, 0, 0, 0))(_ + _)
      activeProgress.foreach { spec =>
        spec.totalKeys.foreach { expected =>
          require(summary.attempted == expected,
                  s"Redis upload phase ${spec.phase} attempted ${summary.attempted} keys; expected $expected")
        }
      }
      progressReporter.foreach(_.complete(summary.attempted, summary.retries))
      summary
    } finally {
      progressReporter.foreach { reporter =>
        sparkContext.removeSparkListener(reporter)
        reporter.close()
      }
    }
  }

  private def writeBatch(taskClient: TaskClient,
                         batch: Seq[Row],
                         mode: WriteMode,
                         dataset: String,
                         keyPrefix: String,
                         limiterKey: String,
                         maxBurst: Int,
                         maxKeysPerSecond: Int,
                         maxRetries: Int,
                         retryBackoffMs: Long,
                         oomRetryStartMillis: Long,
                         oomRetryDeadlineMillis: Long,
                         leaseDeadlineMillis: Long,
                         tombstoneDeadlineMillis: Long): WriteSummary = {
    require(batch.nonEmpty, "Redis command batch must be non-empty")
    val batchSchema = batch.head.schema
    val mutationColumnIndices = MutationColumnIndices(
      Option(batchSchema.fields.indexWhere(_.name == TombstoneExpiresAtColumn)).filter(_ >= 0),
      Option(batchSchema.fields.indexWhere(_.name == AllowExpiredMutationColumn)).filter(_ >= 0)
    )
    var attempt = 0
    var lastFailure: Throwable = null
    var directReplay = false
    var refreshClient = false
    var pipelineMovedFailures = 0
    var oomRetries = 0L
    while (attempt <= maxRetries) {
      if (attempt > 0 && retryBackoffMs > 0) {
        val multiplier = 1L << math.min(attempt - 1, 20)
        val cap = java.lang.Math.multiplyExact(retryBackoffMs, multiplier)
        val waitMillis = ThreadLocalRandom.current().nextLong(cap + 1L)
        requireWaitWithinLease(waitMillis, leaseDeadlineMillis, System.currentTimeMillis())
        if (waitMillis > 0) Thread.sleep(waitMillis)
      }

      try {
        val counted = withRedisOomRetry(
          operation = () => {
            remainingLeaseSeconds(leaseDeadlineMillis)
            if (refreshClient) {
              taskClient.refresh()
              refreshClient = false
            }
            acquireRate(taskClient.client, limiterKey, batch.size, maxKeysPerSecond, maxBurst, leaseDeadlineMillis)
            remainingLeaseSeconds(leaseDeadlineMillis)
            val results =
              if (directReplay)
                executeDirect(taskClient.client,
                              batch,
                              mode,
                              dataset,
                              keyPrefix,
                              leaseDeadlineMillis,
                              tombstoneDeadlineMillis,
                              mutationColumnIndices)
              else
                executePipeline(taskClient.client,
                                batch,
                                mode,
                                dataset,
                                keyPrefix,
                                leaseDeadlineMillis,
                                tombstoneDeadlineMillis,
                                mutationColumnIndices)
            countResults(results, mode)
          },
          retryStartMillis = oomRetryStartMillis,
          retryDeadlineMillis = oomRetryDeadlineMillis,
          retryWaitMillis = oomAttempt => jitteredOomRetryBackoffMillis(oomAttempt, retryBackoffMs),
          nowMillis = () => System.currentTimeMillis(),
          sleepMillis = millis => interruptibleSleep(millis),
          onRetry = event => {
            oomRetries += 1
            logRedisOomRetry(dataset, writeModeName(mode), batch.size, event)
          }
        )
        return counted.copy(retries = attempt + oomRetries)
      } catch {
        case error if retryableRedisFailure(error) =>
          lastFailure = error
          val failedMode = if (directReplay) "direct" else "pipeline"
          if (!directReplay && error.isInstanceOf[JedisMovedDataException]) pipelineMovedFailures += 1
          attempt += 1
          val attemptsRemaining = maxRetries + 1 - attempt
          directReplay = directReplay || directReplayRequired(error, pipelineMovedFailures, attemptsRemaining)
          refreshClient = refreshClient || refreshRequired(error)
          val nextMode = if (attemptsRemaining == 0) "none" else if (directReplay) "direct" else "pipeline"
          logger.warn(
            s"Redis command batch failed dataset=$dataset operation=${writeModeName(mode)} batchSize=${batch.size} " +
              s"failedMode=$failedMode; attempt $attempt of ${maxRetries + 1}, attemptsRemaining=$attemptsRemaining " +
              s"nextMode=$nextMode refreshClient=$refreshClient",
            error
          )
      }
    }
    throw new RuntimeException(s"Redis command batch failed after ${maxRetries + 1} attempts", lastFailure)
  }

  private def executePipeline(client: JedisCluster,
                              batch: Seq[Row],
                              mode: WriteMode,
                              dataset: String,
                              keyPrefix: String,
                              leaseDeadlineMillis: Long,
                              tombstoneDeadlineMillis: Long,
                              mutationColumnIndices: MutationColumnIndices): Seq[Any] = {
    val pipeline = client.pipelined()
    val responses = completePipeline(pipeline) {
      batch.map(row =>
        enqueueCommand(pipeline,
                       row,
                       mode,
                       dataset,
                       keyPrefix,
                       leaseDeadlineMillis,
                       tombstoneDeadlineMillis,
                       mutationColumnIndices))
    }
    val (results, replayedFailures) = resolvePipelineResponses(batch, responses) { row =>
      executeCommand(client,
                     row,
                     mode,
                     dataset,
                     keyPrefix,
                     leaseDeadlineMillis,
                     tombstoneDeadlineMillis,
                     mutationColumnIndices)
    }
    if (replayedFailures.nonEmpty) {
      val causes = replayedFailures
        .groupBy(_.getClass.getSimpleName)
        .toSeq
        .sortBy(_._1)
        .map { case (name, failures) => s"$name:${failures.size}" }
        .mkString(",")
      logger.warn(
        s"Redis pipeline replayed affected commands directly dataset=$dataset operation=${writeModeName(mode)} " +
          s"batchSize=${batch.size} replayCount=${replayedFailures.size} causes=$causes")
    }
    results
  }

  private[redis] def completePipeline(pipeline: ClusterPipeline)(enqueue: => Seq[Response[_]]): Seq[Response[_]] =
    try enqueue
    finally {
      // close is both the sync and the connection-release step. A second sync after a failed one can make Jedis
      // revisit response queues whose node connections it already removed and fail on its executor thread.
      pipeline.close()
    }

  private[redis] def resolvePipelineResponses[T](items: Seq[T], responses: Seq[Response[_]])(
      directReplay: T => Any): (Seq[Any], Seq[Throwable]) = {
    require(items.size == responses.size,
            s"Redis pipeline returned ${responses.size} responses for ${items.size} commands")
    val results = Array.fill[Any](items.size)(null)
    val replayBuilder = Vector.newBuilder[(Int, T, Throwable)]

    items.iterator.zip(responses.iterator).zipWithIndex.foreach { case ((item, response), index) =>
      try results(index) = response.get()
      catch {
        case error if pipelineResponseDirectReplayRequired(error) =>
          replayBuilder += ((index, item, error))
      }
    }

    val replays = replayBuilder.result()
    replays.foreach { case (index, item, _) => results(index) = directReplay(item) }
    (results.toIndexedSeq, replays.map(_._3))
  }

  private def executeDirect(client: JedisCluster,
                            batch: Seq[Row],
                            mode: WriteMode,
                            dataset: String,
                            keyPrefix: String,
                            leaseDeadlineMillis: Long,
                            tombstoneDeadlineMillis: Long,
                            mutationColumnIndices: MutationColumnIndices): Seq[Any] =
    batch.map(row =>
      executeCommand(client,
                     row,
                     mode,
                     dataset,
                     keyPrefix,
                     leaseDeadlineMillis,
                     tombstoneDeadlineMillis,
                     mutationColumnIndices))

  private def enqueueCommand(pipeline: ClusterPipeline,
                             row: Row,
                             mode: WriteMode,
                             dataset: String,
                             keyPrefix: String,
                             leaseDeadlineMillis: Long,
                             tombstoneDeadlineMillis: Long,
                             mutationColumnIndices: MutationColumnIndices): Response[_] = {
    val key = redisKey(row, dataset, keyPrefix)
    mode match {
      case MutationWrite =>
        val (timestamp, writeEpoch, encoded, expireAt, allowExpired) =
          encodedMutation(row, leaseDeadlineMillis, tombstoneDeadlineMillis, mutationColumnIndices)
        pipeline.evalsha(RedisBatchUpload.putBaseIfNewerSha,
                         List(key).asJava,
                         List(timestamp, writeEpoch, encoded, expireAt, allowExpired).asJava)
      case PhysicalDelete => pipeline.unlink(key)
      case LeaseRefresh(Some(expectedDeadline)) =>
        pipeline.evalsha(
          RedisBatchUpload.validateExpiryAndAdvanceSha,
          List(key).asJava,
          expiryValidationArgs(row, expectedDeadline, leaseDeadlineMillis).asJava
        )
      case LeaseRefresh(None) =>
        pipeline.evalsha(RedisBatchUpload.validateAndExpireSha,
                         List(key).asJava,
                         leaseValidationArgs(row, leaseDeadlineMillis).asJava)
    }
  }

  private def executeCommand(client: JedisCluster,
                             row: Row,
                             mode: WriteMode,
                             dataset: String,
                             keyPrefix: String,
                             leaseDeadlineMillis: Long,
                             tombstoneDeadlineMillis: Long,
                             mutationColumnIndices: MutationColumnIndices): Any = {
    val key = redisKey(row, dataset, keyPrefix)
    mode match {
      case MutationWrite =>
        val (timestamp, writeEpoch, encoded, expireAt, allowExpired) =
          encodedMutation(row, leaseDeadlineMillis, tombstoneDeadlineMillis, mutationColumnIndices)
        RedisBatchUpload.putBaseIfNewer(client, key, List(timestamp, writeEpoch, encoded, expireAt, allowExpired))
      case PhysicalDelete => client.unlink(key)
      case LeaseRefresh(Some(expectedDeadline)) =>
        RedisBatchUpload.validateExpiryAndAdvance(client,
                                                  key,
                                                  expiryValidationArgs(row, expectedDeadline, leaseDeadlineMillis))
      case LeaseRefresh(None) =>
        RedisBatchUpload.validateAndExpire(client, key, leaseValidationArgs(row, leaseDeadlineMillis))
    }
  }

  private def redisKey(row: Row, dataset: String, keyPrefix: String): Array[Byte] =
    RedisKVStore
      .buildRedisKey(row.getAs[Array[Byte]](KeyColumn).toSeq, dataset, keyPrefix = keyPrefix)
      .getBytes(StandardCharsets.UTF_8)

  private def encodedMutation(row: Row,
                              leaseDeadlineMillis: Long,
                              tombstoneDeadlineMillis: Long,
                              mutationColumnIndices: MutationColumnIndices)
      : (Array[Byte], Array[Byte], Array[Byte], Array[Byte], Array[Byte]) = {
    val operation = Operation.fromName(row.getAs[String](OperationColumn))
    val timestamp = row.getAs[Long](BatchTimestampColumn)
    val writeEpoch = row.getAs[Long](WriteEpochColumn)
    val payload = row.getAs[Array[Byte]](ValueColumn)
    val encoded = RedisBatchUpload.encodeValue(operation, timestamp, payload, writeEpoch)
    val explicitTombstoneDeadline = mutationColumnIndices.tombstoneExpiresAt.flatMap { index =>
      if (row.isNullAt(index)) None else Some(row.getLong(index))
    }
    val expireAtMillis =
      if (operation == Delete) {
        explicitTombstoneDeadline match {
          case Some(PendingTombstoneExpiry) => tombstoneDeadlineMillis
          case Some(deadline)               => deadline
          case None                         => tombstoneDeadlineMillis
        }
      } else leaseDeadlineMillis
    require(expireAtMillis > 0, s"Redis mutation deadline must be positive: $expireAtMillis")
    val allowExpired = mutationColumnIndices.allowExpired.exists { index =>
      !row.isNullAt(index) && row.getBoolean(index)
    }
    (timestamp.toString.getBytes(StandardCharsets.UTF_8),
     writeEpoch.toString.getBytes(StandardCharsets.UTF_8),
     encoded,
     expireAtMillis.toString.getBytes(StandardCharsets.UTF_8),
     (if (allowExpired) "1" else "0").getBytes(StandardCharsets.UTF_8))
  }

  private def leaseValidationArgs(row: Row, leaseDeadlineMillis: Long): Seq[Array[Byte]] =
    Seq(
      row.getAs[Long](ValueTimestampColumn).toString.getBytes(StandardCharsets.UTF_8),
      row.getAs[Long](ValueWriteEpochColumn).toString.getBytes(StandardCharsets.UTF_8),
      leaseDeadlineMillis.toString.getBytes(StandardCharsets.UTF_8)
    )

  private def expiryValidationArgs(row: Row,
                                   expectedDeadlineMillis: Long,
                                   desiredDeadlineMillis: Long): Seq[Array[Byte]] =
    Seq(expectedDeadlineMillis,
        desiredDeadlineMillis,
        row.getAs[Long](ValueTimestampColumn),
        row.getAs[Long](ValueWriteEpochColumn))
      .map(_.toString.getBytes(StandardCharsets.UTF_8))

  private def countResults(results: Seq[Any], mode: WriteMode): WriteSummary = {
    mode match {
      case MutationWrite =>
        val codes = results.map(redisLong)
        WriteSummary(codes.size, codes.count(_ == 1L), codes.count(_ == 2L), codes.count(_ == 0L), retries = 0)
      case PhysicalDelete =>
        val codes = results.map(redisLong)
        require(codes.forall(code => code == 0L || code == 1L), s"Redis UNLINK returned unexpected counts: $codes")
        WriteSummary(codes.size, codes.count(_ == 1L), codes.count(_ == 0L), rejected = 0L, retries = 0L)
      case _: LeaseRefresh =>
        val codes = results.map(redisLong)
        WriteSummary(codes.size, codes.count(_ == 1L), codes.count(_ == 2L), codes.count(_ == 0L), retries = 0)
    }
  }

  private def redisLong(value: Any): Long = value match {
    case number: java.lang.Long => number.longValue()
    case other => throw new IllegalStateException(s"Redis command returned unexpected response: $other")
  }

  private[redis] def redisOutOfMemoryFailure(error: Throwable): Boolean = {
    val canonicalMessage = "OOM COMMAND NOT ALLOWED WHEN USED MEMORY > 'MAXMEMORY'"
    var current = error
    var depth = 0
    var redisOom = false
    while (current != null && depth < 32) {
      if (current.isInstanceOf[OutOfMemoryError]) return false
      current match {
        case dataError: JedisDataException =>
          val message = Option(dataError.getMessage).getOrElse("").trim.toUpperCase(Locale.ROOT)
          redisOom = redisOom || message == "OOM" || message.startsWith("OOM ") ||
            message == "ERR OOM" || message.startsWith("ERR OOM ") || message.contains(canonicalMessage)
        case _ =>
      }
      val cause = current.getCause
      current = if (cause eq current) null else cause
      depth += 1
    }
    redisOom
  }

  private[redis] def withRedisOomRetry[T](operation: () => T,
                                          retryStartMillis: Long,
                                          retryDeadlineMillis: Long,
                                          retryWaitMillis: Int => Long,
                                          nowMillis: () => Long,
                                          sleepMillis: Long => Unit,
                                          onRetry: OomRetryEvent => Unit): T = {
    require(retryDeadlineMillis >= retryStartMillis,
            s"Redis OOM retry deadline $retryDeadlineMillis precedes its start $retryStartMillis")
    var oomAttempt = 0
    var lastOom: Throwable = null
    while (true) {
      val beforeAttemptMillis = nowMillis()
      if (lastOom != null && beforeAttemptMillis >= retryDeadlineMillis) {
        throw redisOomRetryTimeout(retryStartMillis, beforeAttemptMillis, lastOom)
      }
      try {
        return operation()
      } catch {
        case error if redisOutOfMemoryFailure(error) =>
          lastOom = error
          oomAttempt += 1
          val retryAtMillis = nowMillis()
          val remainingMillis = retryDeadlineMillis - retryAtMillis
          if (remainingMillis <= 0) throw redisOomRetryTimeout(retryStartMillis, retryAtMillis, error)
          val requestedWaitMillis = retryWaitMillis(oomAttempt)
          require(requestedWaitMillis >= 0, s"Redis OOM retry wait must be non-negative: $requestedWaitMillis")
          val waitMillis = math.min(requestedWaitMillis, remainingMillis)
          onRetry(
            OomRetryEvent(oomAttempt, waitMillis, math.max(0L, retryAtMillis - retryStartMillis), remainingMillis))
          if (waitMillis > 0) sleepMillis(waitMillis)
      }
    }
    throw new IllegalStateException("Redis OOM retry loop terminated unexpectedly")
  }

  private[redis] def redisOomRetryDeadline(startMillis: Long, timeoutMillis: Long, leaseDeadlineMillis: Long): Long = {
    require(timeoutMillis >= 0, s"Redis OOM retry timeout must be non-negative: $timeoutMillis")
    val timeoutDeadline =
      if (startMillis > Long.MaxValue - timeoutMillis) Long.MaxValue else startMillis + timeoutMillis
    math.min(timeoutDeadline, leaseDeadlineMillis)
  }

  private[redis] def cappedOomRetryBackoffMillis(attempt: Int, initialBackoffMillis: Long): Long = {
    require(attempt > 0, s"Redis OOM retry attempt must be positive: $attempt")
    require(initialBackoffMillis >= 0, s"Redis OOM retry backoff must be non-negative: $initialBackoffMillis")
    var cap = math.min(math.max(1L, initialBackoffMillis), RedisBatchUploadDefaults.OomRetryMaxBackoffMs)
    var currentAttempt = 1
    while (currentAttempt < attempt && cap < RedisBatchUploadDefaults.OomRetryMaxBackoffMs) {
      cap = math.min(RedisBatchUploadDefaults.OomRetryMaxBackoffMs, cap * 2L)
      currentAttempt += 1
    }
    cap
  }

  private def jitteredOomRetryBackoffMillis(attempt: Int, initialBackoffMillis: Long): Long = {
    val cap = cappedOomRetryBackoffMillis(attempt, initialBackoffMillis)
    val lowerBound = (cap + 1L) / 2L
    if (lowerBound == cap) cap
    else ThreadLocalRandom.current().nextLong(lowerBound, cap + 1L)
  }

  private def redisOomRetryTimeout(startMillis: Long, nowMillis: Long, cause: Throwable): RuntimeException =
    new RuntimeException(
      s"Redis remained above maxmemory for ${math.max(0L, nowMillis - startMillis)} ms; " +
        "external scaling or memory release did not complete before the OOM retry or publication lease deadline",
      cause
    )

  private def interruptibleSleep(millis: Long): Unit =
    try Thread.sleep(millis)
    catch {
      case interrupted: InterruptedException =>
        Thread.currentThread().interrupt()
        throw interrupted
    }

  private def logRedisOomRetry(dataset: String, operation: String, batchSize: Int, event: OomRetryEvent): Unit = {
    val task = Option(TaskContext.get())
      .map(context =>
        s"stage=${context.stageId()} partition=${context.partitionId()} " +
          s"taskAttempt=${context.attemptNumber()} taskAttemptId=${context.taskAttemptId()}")
      .getOrElse("driver")
    logger.warn(
      s"Redis maxmemory OOM dataset=$dataset operation=$operation batchSize=$batchSize $task; " +
        s"OOM retry ${event.attempt} in ${event.waitMillis} ms " +
        s"(elapsed=${event.elapsedMillis} ms remaining=${event.remainingMillis} ms) while waiting for external " +
        "scaling or memory release")
  }

  private def writeModeName(mode: WriteMode): String = mode match {
    case MutationWrite   => "mutation"
    case PhysicalDelete  => "physical-delete"
    case _: LeaseRefresh => "lease-refresh"
  }

  private[redis] def retryableRedisFailure(error: Throwable): Boolean = error match {
    case _: JedisConnectionException       => true
    case _: JedisClusterOperationException => true
    case _: JedisRedirectionException      => true
    case _: JedisNoScriptException         => true
    case dataError: JedisDataException     => retryableDataError(dataError)
    case stateError: IllegalStateException => unsetPipelineResponse(stateError)
    case _                                 => false
  }

  private[redis] def directReplayRequired(error: Throwable,
                                          pipelineMovedFailures: Int,
                                          attemptsRemaining: Int): Boolean = {
    require(pipelineMovedFailures >= 0, s"Pipeline MOVED failures must be non-negative: $pipelineMovedFailures")
    require(attemptsRemaining >= 0, s"Redis attempts remaining must be non-negative: $attemptsRemaining")
    error match {
      // Response-level MOVED failures are normally replayed selectively. If one escapes that recovery, refresh
      // topology and re-pipeline once while preserving a final redirect-aware direct attempt.
      case _: JedisMovedDataException => pipelineMovedFailures > 1 || attemptsRemaining <= 1
      case _                          => pipelineResponseDirectReplayRequired(error)
    }
  }

  private[redis] def pipelineResponseDirectReplayRequired(error: Throwable): Boolean = error match {
    case _: JedisRedirectionException      => true
    case _: JedisNoScriptException         => true
    case stateError: IllegalStateException => unsetPipelineResponse(stateError)
    case _                                 => false
  }

  private def refreshRequired(error: Throwable): Boolean = error match {
    case _: JedisConnectionException       => true
    case _: JedisClusterOperationException => true
    case _: JedisRedirectionException      => true
    case dataError: JedisDataException =>
      val message = Option(dataError.getMessage).getOrElse("").toUpperCase(Locale.ROOT)
      Seq("CLUSTERDOWN", "MASTERDOWN", "READONLY").exists(message.startsWith)
    case stateError: IllegalStateException => unsetPipelineResponse(stateError)
    case _                                 => false
  }

  private def unsetPipelineResponse(error: IllegalStateException): Boolean =
    Option(error.getMessage).contains(UnsetPipelineResponseMessage)

  private def retryableDataError(error: JedisDataException): Boolean = {
    val message = Option(error.getMessage).getOrElse("").toUpperCase(Locale.ROOT)
    Seq("CLUSTERDOWN", "LOADING", "MASTERDOWN", "READONLY", "TRYAGAIN").exists(message.startsWith)
  }

  private def acquireRate(client: JedisCluster,
                          limiterKey: String,
                          keyCount: Int,
                          maxKeysPerSecond: Int,
                          maxBurst: Int,
                          leaseDeadlineMillis: Long): Unit = {
    val waitMillis = RedisBatchUpload.reserveRate(client, limiterKey, keyCount, maxKeysPerSecond, maxBurst)
    requireWaitWithinLease(waitMillis, leaseDeadlineMillis, System.currentTimeMillis())
    if (waitMillis > 0) Thread.sleep(waitMillis)
  }

  private[redis] def requireWaitWithinLease(waitMillis: Long, leaseDeadlineMillis: Long, nowMillis: Long): Unit = {
    require(waitMillis >= 0, s"Redis upload wait must be non-negative: $waitMillis")
    val remainingMillis = leaseDeadlineMillis - nowMillis
    require(
      remainingMillis > waitMillis,
      s"Redis upload cannot wait $waitMillis ms with only $remainingMillis ms remaining in its configured TTL"
    )
  }

  private[redis] def remainingLeaseSeconds(deadlineMillis: Long): Int = {
    val remainingMillis = deadlineMillis - System.currentTimeMillis()
    require(remainingMillis > 0, "Redis upload exceeded its configured TTL")
    math.max(1L, (remainingMillis + 999L) / 1000L).toInt
  }

}
