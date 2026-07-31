package ai.chronon.integrations.redis

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import redis.clients.jedis.{ClusterPipeline, HostAndPort, Response}
import redis.clients.jedis.exceptions.{
  JedisAskDataException,
  JedisClusterException,
  JedisConnectionException,
  JedisDataException,
  JedisMovedDataException,
  JedisNoScriptException
}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.collection.mutable.ArrayBuffer

class Spark2RedisLoaderTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers with MockitoSugar {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = RedisSparkSessionTestHelper.acquire("Spark2RedisLoaderTest")
  }

  override def afterAll(): Unit = {
    RedisSparkSessionTestHelper.release()
    super.afterAll()
  }

  "Redis batch diffing" should "emit only added, changed, and deleted keys" in {
    val previousRows = frame(
      "unchanged" -> "value-1",
      "changed" -> "old-value",
      "deleted" -> "deleted-value"
    )
    val previousState = Spark2RedisLoader
      .buildDiff(previousRows, previousState = None, batchTimestamp = 1000L, stateBuckets = 8)
      .currentState

    val currentRows = frame(
      "unchanged" -> "value-1",
      "changed" -> "new-value",
      "added" -> "added-value"
    )
    val frames = Spark2RedisLoader.buildDiff(
      currentRows,
      previousState = Some(previousState),
      batchTimestamp = 2000L,
      stateBuckets = 8
    )

    val delta = frames.dataDelta.collect().map { row =>
      val key = new String(row.getAs[Array[Byte]](Spark2RedisLoader.KeyColumn), StandardCharsets.UTF_8)
      val value = Option(row.getAs[Array[Byte]](Spark2RedisLoader.ValueColumn))
        .map(bytes => new String(bytes, StandardCharsets.UTF_8))
      val operation = row.getAs[String](Spark2RedisLoader.OperationColumn)
      val timestamp = row.getAs[Long](Spark2RedisLoader.BatchTimestampColumn)
      key -> (operation, value, timestamp)
    }.toMap

    delta.keySet shouldBe Set("changed", "added", "deleted")
    delta("changed") shouldBe ((RedisBatchUpload.UpsertOperation, Some("new-value"), 2000L))
    delta("added") shouldBe ((RedisBatchUpload.UpsertOperation, Some("added-value"), 2000L))
    delta("deleted") shouldBe ((RedisBatchUpload.DeleteOperation, None, 2000L))

    frames.currentState.count() shouldBe 3L
    val stateVersions = frames.currentState.collect().map { row =>
      val key = new String(row.getAs[Array[Byte]](Spark2RedisLoader.KeyColumn), StandardCharsets.UTF_8)
      key -> (row.getAs[Long](Spark2RedisLoader.ValueTimestampColumn),
              row.getAs[Long](Spark2RedisLoader.ValueWriteEpochColumn))
    }.toMap
    stateVersions("unchanged") shouldBe (1000L -> 0L)
    stateVersions("changed") shouldBe (2000L -> 0L)
    stateVersions("added") shouldBe (2000L -> 0L)
    val buckets = frames.currentState
      .select(Spark2RedisLoader.BucketColumn)
      .collect()
      .map(_.getInt(0))
    all(buckets) should (be >= 0 and be < 8)
  }

  it should "write every current key for an initial generation and no keys for an identical generation" in {
    val currentRows = frame("key-1" -> "value-1", "key-2" -> "value-2")
    val initial = Spark2RedisLoader.buildDiff(
      currentRows,
      previousState = None,
      batchTimestamp = 1000L,
      stateBuckets = 4
    )

    initial.dataDelta.count() shouldBe 2L
    initial.dataDelta
      .select(Spark2RedisLoader.OperationColumn)
      .distinct()
      .collect()
      .map(_.getString(0)) shouldBe Array(RedisBatchUpload.UpsertOperation)
    all(initial.currentState.select(Spark2RedisLoader.DigestColumn).collect().map(_.getAs[Array[Byte]](0).length)) shouldBe 32

    val identical = Spark2RedisLoader.buildDiff(
      currentRows,
      previousState = Some(initial.currentState),
      batchTimestamp = 2000L,
      stateBuckets = 4
    )
    identical.currentState.count() shouldBe 2L
    identical.dataDelta.count() shouldBe 0L

    val emptied = Spark2RedisLoader.buildDiff(
      frame(),
      previousState = Some(initial.currentState),
      batchTimestamp = 3000L,
      stateBuckets = 4,
      writeEpoch = 2L)
    emptied.currentState.count() shouldBe 0L
    emptied.dataDelta.count() shouldBe 2L
    all(emptied.dataDelta.collect().map(_.getAs[String](Spark2RedisLoader.OperationColumn))) shouldBe
      RedisBatchUpload.DeleteOperation
  }

  "Redis mutation retries" should "retry only transient cluster failures" in {
    Spark2RedisLoader.retryableRedisFailure(new JedisConnectionException("connection reset")) shouldBe true
    Spark2RedisLoader.retryableRedisFailure(new JedisClusterException("CLUSTERDOWN unavailable")) shouldBe true
    Spark2RedisLoader.retryableRedisFailure(new JedisDataException("TRYAGAIN migrating")) shouldBe true
    Spark2RedisLoader.retryableRedisFailure(new JedisNoScriptException("NOSCRIPT missing")) shouldBe true
    Spark2RedisLoader.retryableRedisFailure(
      new IllegalStateException("Please close pipeline or multi block before calling this method.")) shouldBe true
    Spark2RedisLoader.retryableRedisFailure(new IllegalStateException("application invariant failed")) shouldBe false
    Spark2RedisLoader.retryableRedisFailure(new JedisDataException("WRONGTYPE bad input")) shouldBe false
    Spark2RedisLoader.retryableRedisFailure(new IllegalArgumentException("invalid mutation")) shouldBe false
  }

  it should "close a write pipeline once before resolving its responses" in {
    val pipeline = mock[ClusterPipeline]
    val response = mock[Response[Any]]
    val unsetResponse =
      new IllegalStateException("Please close pipeline or multi block before calling this method.")
    when(response.get()).thenThrow(unsetResponse)

    val responses = Spark2RedisLoader.completePipeline(pipeline)(Seq(response))

    responses shouldBe Seq(response)
    verify(pipeline, times(1)).close()
    verify(pipeline, never()).sync()
    val failure = the[IllegalStateException] thrownBy {
      responses.head.get()
    }
    failure shouldBe unsetResponse
  }

  it should "directly replay only commands whose pipeline responses require it" in {
    val successful = mock[Response[java.lang.Long]]
    val redirected = mock[Response[java.lang.Long]]
    val unchanged = mock[Response[java.lang.Long]]
    val target = new HostAndPort("127.0.0.1", 6380)
    val moved = new JedisMovedDataException("MOVED 42 127.0.0.1:6380", target, 42)
    when(successful.get()).thenReturn(java.lang.Long.valueOf(1L))
    when(redirected.get()).thenThrow(moved)
    when(unchanged.get()).thenReturn(java.lang.Long.valueOf(0L))
    val replayed = ArrayBuffer.empty[String]

    val (results, failures) = Spark2RedisLoader.resolvePipelineResponses(
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

    Spark2RedisLoader.directReplayRequired(moved, pipelineMovedFailures = 1, attemptsRemaining = 3) shouldBe false
    Spark2RedisLoader.directReplayRequired(moved, pipelineMovedFailures = 2, attemptsRemaining = 2) shouldBe true
    Spark2RedisLoader.directReplayRequired(moved, pipelineMovedFailures = 1, attemptsRemaining = 1) shouldBe true
  }

  it should "selectively replay only redirect, script, and unset response failures" in {
    val unsetResponse =
      new IllegalStateException("Please close pipeline or multi block before calling this method.")
    val ask = new JedisAskDataException("ASK 42 127.0.0.1:6380", new HostAndPort("127.0.0.1", 6380), 42)

    Spark2RedisLoader.pipelineResponseDirectReplayRequired(ask) shouldBe true
    Spark2RedisLoader.pipelineResponseDirectReplayRequired(new JedisNoScriptException("NOSCRIPT missing")) shouldBe true
    Spark2RedisLoader.pipelineResponseDirectReplayRequired(unsetResponse) shouldBe true
    Spark2RedisLoader.pipelineResponseDirectReplayRequired(new JedisConnectionException("connection reset")) shouldBe false
    Spark2RedisLoader.pipelineResponseDirectReplayRequired(new JedisDataException("WRONGTYPE bad input")) shouldBe false
  }

  "Redis upload progress" should "deduplicate retries and speculative attempts by partition" in {
    val tracker = new Spark2RedisLoader.UploadProgressTracker
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

  "Redis OOM retries" should "recognize only Redis maxmemory failures" in {
    val reported = new JedisDataException(
      "OOM command not allowed when used memory > 'maxmemory'. script: abc, on @user_script:22.")
    val wrapped = new RuntimeException(
      "pipeline failed",
      new JedisDataException(
        "ERR Error running script (call to f_abc): @user_script:22: -OOM command not allowed when used memory > 'maxmemory'."))

    Spark2RedisLoader.redisOutOfMemoryFailure(reported) shouldBe true
    Spark2RedisLoader.redisOutOfMemoryFailure(wrapped) shouldBe true
    Spark2RedisLoader.redisOutOfMemoryFailure(new JedisDataException("ERR OOM maxmemory reached")) shouldBe true
    Spark2RedisLoader.redisOutOfMemoryFailure(new JedisDataException("WRONGTYPE bad input")) shouldBe false
    Spark2RedisLoader.redisOutOfMemoryFailure(new OutOfMemoryError("Java heap space")) shouldBe false
    Spark2RedisLoader.retryableRedisFailure(reported) shouldBe false
  }

  it should "back off until Redis has capacity" in {
    var nowMillis = 1000L
    var attempts = 0
    val sleeps = ArrayBuffer.empty[Long]
    val events = ArrayBuffer.empty[Spark2RedisLoader.OomRetryEvent]

    val result = Spark2RedisLoader.withRedisOomRetry(
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

  it should "stop at the shared thirty-minute deadline" in {
    val oom = new JedisDataException("OOM command not allowed when used memory > 'maxmemory'.")
    val sevenMinutes = 7.minutes.toMillis
    val thirtyMinutes = 30.minutes.toMillis
    var nowMillis = 0L
    var attempts = 0
    val sleeps = ArrayBuffer.empty[Long]

    val failure = the[RuntimeException] thrownBy {
      Spark2RedisLoader.withRedisOomRetry(
        operation = () => {
          attempts += 1
          throw oom
        },
        retryStartMillis = 0L,
        retryDeadlineMillis = thirtyMinutes,
        retryWaitMillis = _ => sevenMinutes,
        nowMillis = () => nowMillis,
        sleepMillis = millis => {
          sleeps += millis
          nowMillis += millis
        },
        onRetry = _ => ()
      )
    }

    attempts shouldBe 5
    sleeps shouldBe Seq(sevenMinutes, sevenMinutes, sevenMinutes, sevenMinutes, 2.minutes.toMillis)
    failure.getMessage should include("external scaling or memory release")
    failure.getCause shouldBe oom
  }

  it should "propagate non-OOM data errors without waiting" in {
    val wrongType = new JedisDataException("WRONGTYPE bad input")
    var attempts = 0
    var sleeps = 0

    val failure = the[JedisDataException] thrownBy {
      Spark2RedisLoader.withRedisOomRetry(
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
    (1 to 8).map(Spark2RedisLoader.cappedOomRetryBackoffMillis(_, 1000L)) shouldBe
      Seq(1000L, 2000L, 4000L, 8000L, 16000L, 30000L, 30000L, 30000L)
    Spark2RedisLoader.redisOomRetryDeadline(startMillis = 1000L,
                                            timeoutMillis = 30.minutes.toMillis,
                                            leaseDeadlineMillis = 5000L) shouldBe 5000L
  }

  "Redis lease renewal" should "renew missing and half-spent leases while leaving fresh leases untouched" in {
    val nowMillis = 1000000L
    val ttlSeconds = 5 * 24 * 60 * 60
    val halfTtlMillis = ttlSeconds.toLong * 1000L / 2L

    Spark2RedisLoader.leaseRefreshRequired(None, ttlSeconds, nowMillis) shouldBe true
    Spark2RedisLoader.leaseRefreshRequired(Some(nowMillis + halfTtlMillis + 1L), ttlSeconds, nowMillis) shouldBe false
    Spark2RedisLoader.leaseRefreshRequired(Some(nowMillis + halfTtlMillis), ttlSeconds, nowMillis) shouldBe true
    Spark2RedisLoader.leaseRefreshRequired(Some(nowMillis), ttlSeconds, nowMillis) shouldBe true
  }

  it should "advance one generation monotonically without inheriting its parent's longer lease" in {
    Spark2RedisLoader.generationLeaseDeadline(2000L, None) shouldBe 2000L
    Spark2RedisLoader.generationLeaseDeadline(2000L, Some(3000L)) shouldBe 3000L
    Spark2RedisLoader.generationLeaseDeadline(4000L, Some(3000L)) shouldBe 4000L
    Spark2RedisLoader.renewedLeaseDeadline(2000L, None) shouldBe 2000L
    Spark2RedisLoader.renewedLeaseDeadline(2000L, Some(3000L)) shouldBe 3001L
  }

  it should "round-trip the generation-bound lease checkpoint" in {
    val status = RedisBatchUpload.BatchStatus("generation-a", "2024-07-27", 1000L, 2L)
    val digest = Array.tabulate[Byte](32)(_.toByte)
    val lease = Spark2RedisLoader.LeaseRecord(status, digest, minimumActiveDeadlineMillis = 2000000L)

    val decoded = Spark2RedisLoader.decodeLease(Spark2RedisLoader.encodeLease(lease))

    decoded.status shouldBe status
    decoded.readyDigest.toSeq shouldBe digest.toSeq
    decoded.minimumActiveDeadlineMillis shouldBe 2000000L
  }

  it should "read legacy READY records and round-trip a separately recorded delete-fence TTL" in {
    val status = RedisBatchUpload.BatchStatus("generation-a", "2024-07-27", 1000L, 2L)
    val manifest = Spark2RedisLoader.ReadyManifest(
      status = status,
      parentGeneration = None,
      stateGeneration = status.generation,
      sourceTable = "source",
      batchDataset = "dataset_BATCH",
      keyPrefix = "chronon",
      keyHashTagMode = RedisKVStore.EntityHashTagMode,
      stateBuckets = 4,
      sourceRows = 2L,
      dataRows = 1L,
      changedKeys = 1L,
      deletedKeys = 0L,
      deltaRows = 2L,
      tombstoneRows = 0L,
      liveTTLSeconds = 1800,
      deleteFenceTTLSeconds = 3600,
      fullRebuild = true,
      retired = false
    )

    val encoded = Spark2RedisLoader.encodeReady(manifest)
    Spark2RedisLoader.decodeReady(encoded) shouldBe manifest

    val version5 = encoded.take(encoded.length - 6) ++ encoded.takeRight(2)
    ByteBuffer.wrap(version5, 4, 4).putInt(5)
    val decodedVersion5 = Spark2RedisLoader.decodeReady(version5)
    decodedVersion5.keyHashTagMode shouldBe RedisKVStore.EntityHashTagMode
    decodedVersion5.deleteFenceTTLSeconds shouldBe 1800

    val hashTagBytes = manifest.keyHashTagMode.getBytes(StandardCharsets.UTF_8)
    val hashTagValueOffset = version5.indexOfSlice(hashTagBytes)
    hashTagValueOffset should be > 2
    val version4 =
      version5.take(hashTagValueOffset - 2) ++ version5.drop(hashTagValueOffset + hashTagBytes.length)
    ByteBuffer.wrap(version4, 4, 4).putInt(4)
    val decodedVersion4 = Spark2RedisLoader.decodeReady(version4)
    decodedVersion4.keyHashTagMode shouldBe RedisKVStore.DatasetAndEntityHashTagMode
    decodedVersion4.deleteFenceTTLSeconds shouldBe 1800
  }

  it should "advance a lease checkpoint monotonically" in {
    val status = RedisBatchUpload.BatchStatus("generation-a", "2024-07-27", 1000L, 2L)
    val digest = Array.tabulate[Byte](32)(_.toByte)
    val manifest = Spark2RedisLoader.ReadyManifest(
      status = status,
      parentGeneration = None,
      stateGeneration = status.generation,
      sourceTable = "source",
      batchDataset = "dataset_BATCH",
      keyPrefix = "chronon",
      keyHashTagMode = RedisKVStore.DatasetAndEntityHashTagMode,
      stateBuckets = 4,
      sourceRows = 2L,
      dataRows = 1L,
      changedKeys = 1L,
      deletedKeys = 0L,
      deltaRows = 2L,
      tombstoneRows = 0L,
      liveTTLSeconds = 3600,
      deleteFenceTTLSeconds = 3600,
      fullRebuild = true,
      retired = false
    )
    val path = new org.apache.hadoop.fs.Path(Files.createTempDirectory("redis-lease").resolve("_LEASE").toUri)

    def advance(deadlineMillis: Long): Spark2RedisLoader.LeaseRecord =
      Spark2RedisLoader.advanceLease(
        path,
        Spark2RedisLoader.LeaseRecord(status, digest, deadlineMillis),
        manifest,
        digest,
        spark,
        HadoopConditionalObjectWriter
      )

    advance(2000L).minimumActiveDeadlineMillis shouldBe 2000L
    advance(1000L).minimumActiveDeadlineMillis shouldBe 2000L
    advance(3000L).minimumActiveDeadlineMillis shouldBe 3000L
    Spark2RedisLoader
      .decodeLease(HadoopConditionalObjectWriter.read(path, spark.sparkContext.hadoopConfiguration).get.bytes)
      .minimumActiveDeadlineMillis shouldBe 3000L
  }

  "Redis recovery diffing" should "force every current value into a rebuild without inventing deletes" in {
    val previousRows = frame("a" -> "a-1", "b" -> "b-1")
    val previousState = Spark2RedisLoader
      .buildDiff(previousRows, None, batchTimestamp = 1000L, stateBuckets = 4)
      .currentState
    val currentRows = frame("a" -> "a-1", "b" -> "b-1")

    val rebuild = Spark2RedisLoader.buildDiff(currentRows,
                                              Some(previousState),
                                              batchTimestamp = 2000L,
                                              stateBuckets = 4,
                                              writeEpoch = 2L,
                                              forceAllUpserts = true)

    rebuild.dataDelta.count() shouldBe 2L
    rebuild.dataDelta.collect().foreach { row =>
      row.getAs[String](Spark2RedisLoader.OperationColumn) shouldBe RedisBatchUpload.UpsertOperation
      row.getAs[Long](Spark2RedisLoader.BatchTimestampColumn) shouldBe 2000L
      row.getAs[Long](Spark2RedisLoader.WriteEpochColumn) shouldBe 2L
    }
  }

  "Redis source validation" should "reject duplicate entity keys before publication" in {
    Spark2RedisLoader.validateSourceRows(frame("a" -> "a-1", "b" -> "b-1"))

    val duplicateError = the[IllegalArgumentException] thrownBy {
      Spark2RedisLoader.validateSourceRows(frame("duplicate" -> "first", "duplicate" -> "second"))
    }
    duplicateError.getMessage should include("duplicate key_bytes")
  }

  "Conditional local control records" should "compare and set by version" in {
    val path = new org.apache.hadoop.fs.Path(Files.createTempDirectory("redis-claim").resolve("_HEAD").toUri)
    val conf = spark.sparkContext.hadoopConfiguration
    val first = "generation-a".getBytes(StandardCharsets.UTF_8)
    val second = "generation-b".getBytes(StandardCharsets.UTF_8)
    val third = "generation-c".getBytes(StandardCharsets.UTF_8)

    val created = HadoopConditionalObjectWriter.putIfAbsent(path, first, conf)
    HadoopConditionalObjectWriter.putIfAbsent(path, second, conf).bytes shouldBe first
    val replaced = HadoopConditionalObjectWriter.compareAndSet(path, created.version, second, conf)
    replaced.bytes shouldBe second
    HadoopConditionalObjectWriter.compareAndSet(path, created.version, third, conf).bytes shouldBe second
  }

  it should "admit exactly one concurrent replacement" in {
    implicit val executionContext: ExecutionContext = ExecutionContext.global
    val path = new org.apache.hadoop.fs.Path(Files.createTempDirectory("redis-race").resolve("_HEAD").toUri)
    val conf = spark.sparkContext.hadoopConfiguration
    val initial = HadoopConditionalObjectWriter.putIfAbsent(path, "initial".getBytes(StandardCharsets.UTF_8), conf)
    val ready = new CountDownLatch(2)
    val start = new CountDownLatch(1)

    def contender(value: String): Future[Array[Byte]] = Future {
      ready.countDown()
      start.await(10, TimeUnit.SECONDS) shouldBe true
      HadoopConditionalObjectWriter
        .compareAndSet(path, initial.version, value.getBytes(StandardCharsets.UTF_8), conf)
        .bytes
    }

    val contenders = Seq(contender("generation-a"), contender("generation-b"))
    ready.await(10, TimeUnit.SECONDS) shouldBe true
    start.countDown()
    val results = Await.result(Future.sequence(contenders), 10.seconds)
    val winner = HadoopConditionalObjectWriter.read(path, conf).get.bytes

    results.foreach(_ shouldBe winner)
    Set("generation-a", "generation-b") should contain(new String(winner, StandardCharsets.UTF_8))
  }

  "Candidate cleanup fencing" should "make either claimed or abandoned state immutable once it wins" in {
    val conf = spark.sparkContext.hadoopConfiguration
    val generation = "generation-a"
    val claimed = Spark2RedisLoader.CandidateClaim(generation, abandoned = false)
    val abandoned = Spark2RedisLoader.CandidateClaim(generation, abandoned = true)

    Seq(claimed -> abandoned, abandoned -> claimed).zipWithIndex.foreach { case ((first, second), index) =>
      val path = new org.apache.hadoop.fs.Path(
        Files.createTempDirectory(s"redis-candidate-claim-$index").resolve(generation).toUri)
      val firstWrite = HadoopConditionalObjectWriter.putIfAbsent(path, Spark2RedisLoader.encodeClaim(first), conf)
      val secondWrite = HadoopConditionalObjectWriter.putIfAbsent(path, Spark2RedisLoader.encodeClaim(second), conf)

      Spark2RedisLoader.decodeClaim(firstWrite.bytes) shouldBe first
      Spark2RedisLoader.decodeClaim(secondWrite.bytes) shouldBe first
    }
  }

  "Redis generation cleanup" should "retain grace from the direct admitted child after the head advances again" in {
    Spark2RedisLoader.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 1000L,
      directChildAppliedLastModifiedMillis = Some(2000L),
      currentHeadSupersededAtMillis = None,
      graceMillis = 500L
    ) shouldBe 2500L

    Spark2RedisLoader.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 1000L,
      directChildAppliedLastModifiedMillis = None,
      currentHeadSupersededAtMillis = Some(2000L),
      graceMillis = 500L
    ) shouldBe 2500L

    Spark2RedisLoader.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 1000L,
      directChildAppliedLastModifiedMillis = Some(2000L),
      currentHeadSupersededAtMillis = Some(2500L),
      graceMillis = 500L
    ) shouldBe 3000L

    Spark2RedisLoader.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 4000L,
      directChildAppliedLastModifiedMillis = Some(2000L),
      currentHeadSupersededAtMillis = Some(2500L),
      graceMillis = 500L
    ) shouldBe 4000L

    Spark2RedisLoader.unreachableGenerationCleanupDeadline(
      cleanupAfterMillis = 1000L,
      directChildAppliedLastModifiedMillis = None,
      currentHeadSupersededAtMillis = None,
      graceMillis = 500L
    ) shouldBe 1000L
  }

  "Redis upload lease waits" should "fail before sleeping beyond the publication deadline" in {
    noException should be thrownBy
      Spark2RedisLoader.requireWaitWithinLease(waitMillis = 99L,
                                               leaseDeadlineMillis = 1000L,
                                               nowMillis = 900L)

    val expired = the[IllegalArgumentException] thrownBy {
      Spark2RedisLoader.requireWaitWithinLease(waitMillis = 100L,
                                               leaseDeadlineMillis = 1000L,
                                               nowMillis = 900L)
    }
    expired.getMessage should include("cannot wait 100 ms")
  }

  "Redis INFO parsing" should "read exact fields from CRLF-delimited responses" in {
    val info = "# Memory\r\nmaxmemory:1000\r\nmaxmemory_policy:noeviction\r\n"
    Spark2RedisLoader.redisInfoField(info, "maxmemory_policy") shouldBe Some("noeviction")
    Spark2RedisLoader.redisInfoField(info, "maxmemory") shouldBe Some("1000")
    Spark2RedisLoader.redisInfoField(info, "missing") shouldBe None
  }

  "Redis upload safety configuration" should "require noeviction by default and allow an explicit opt-out" in {
    val property = RedisKVStoreConstants.PropRedisBulkUploadRequireNoEviction

    Spark2RedisLoader.configuredBoolean(Map.empty, property, default = true) shouldBe true
    Spark2RedisLoader.configuredBoolean(Map(property -> "false"), property, default = true) shouldBe false
    an[IllegalArgumentException] should be thrownBy
      Spark2RedisLoader.configuredBoolean(Map(property -> "not-a-boolean"), property, default = true)
  }

  it should "leave older-version retirement disabled by default and parse an explicit opt-in" in {
    val property = RedisKVStoreConstants.PropRedisBulkUploadDeleteOlderVersions

    RedisKVStoreConstants.DefaultBulkUploadDeleteOlderVersions shouldBe false
    Spark2RedisLoader.configuredBoolean(Map.empty, property, default = false) shouldBe false
    Spark2RedisLoader.configuredBoolean(Map(property -> "true"), property, default = false) shouldBe true
    an[IllegalArgumentException] should be thrownBy
      Spark2RedisLoader.configuredBoolean(Map(property -> "not-a-boolean"), property, default = false)
  }

  "Redis version parsing" should "recognize only trailing numeric GroupBy and batch versions" in {
    Spark2RedisLoader.parseVersionedDataset("ranking.user.views__14") shouldBe
      Some(Spark2RedisLoader.VersionedDataset("ranking.user.views", 14L))
    Spark2RedisLoader.parseVersionedDataset("ranking.user.views") shouldBe None
    Spark2RedisLoader.parseVersionedDataset("ranking.user.views__v14") shouldBe None
    Spark2RedisLoader.parseVersionedDataset("ranking.user.views__latest") shouldBe None

    Spark2RedisLoader.parseVersionedBatchDataset("RANKING_USER_VIEWS__14_BATCH") shouldBe
      Some(Spark2RedisLoader.VersionedDataset("RANKING_USER_VIEWS", 14L))
    Spark2RedisLoader.parseVersionedBatchDataset("RANKING_USER_VIEWS_BATCH") shouldBe None
    Spark2RedisLoader.parseVersionedBatchDataset("RANKING_USER_VIEWS__14_STREAMING") shouldBe None
  }

  it should "require matching numeric GroupBy and batch versions before retirement" in {
    Spark2RedisLoader.requireMatchingVersionedDatasets(
      "ranking.user.views__14",
      "RANKING_USER_VIEWS__14_BATCH") shouldBe
      (Spark2RedisLoader.VersionedDataset("ranking.user.views", 14L) ->
        Spark2RedisLoader.VersionedDataset("RANKING_USER_VIEWS", 14L))

    an[IllegalArgumentException] should be thrownBy
      Spark2RedisLoader.requireMatchingVersionedDatasets("ranking.user.views", "RANKING_USER_VIEWS__14_BATCH")
    an[IllegalArgumentException] should be thrownBy
      Spark2RedisLoader.requireMatchingVersionedDatasets("ranking.user.views__14", "RANKING_USER_VIEWS_BATCH")
    an[IllegalArgumentException] should be thrownBy
      Spark2RedisLoader.requireMatchingVersionedDatasets("ranking.user.views__14",
                                                         "RANKING_USER_VIEWS__13_BATCH")
  }

  "Redis publisher Spark configuration" should "prefer submitted Spark settings over metadata defaults" in {
    val credentialKey = "spark.sql.catalog.depop_iceberg.credential"
    val warehouseKey = "spark.sql.catalog.depop_iceberg.warehouse"
    val submissionOnlyKey = "spark.sql.catalog.depop_iceberg.uri"
    val metadataConfig = Map(
      credentialKey -> "{DATABRICKS_CREDENTIAL}",
      warehouseKey -> "catalog/production",
      "spark.sql.session.timeZone" -> "UTC",
      "redis.host" -> "redis.example"
    )
    val submissionConfig = Map(
      credentialKey -> "resolved-client:resolved-secret",
      warehouseKey -> "catalog/staging",
      submissionOnlyKey -> "https://workspace.cloud.databricks.com/api/2.1/unity-catalog"
    )

    RedisKVStore.sparkSessionConfig(metadataConfig, submissionConfig) shouldBe Map(
      credentialKey -> "resolved-client:resolved-secret",
      warehouseKey -> "catalog/staging",
      submissionOnlyKey -> "https://workspace.cloud.databricks.com/api/2.1/unity-catalog",
      "spark.sql.session.timeZone" -> "UTC",
      "spark.speculation" -> "false"
    )
  }

  private def frame(rows: (String, String)*): DataFrame = {
    val session = spark
    import session.implicits._
    rows
      .map { case (key, value) =>
        (key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))
      }
      .toDF(Spark2RedisLoader.KeyColumn, Spark2RedisLoader.ValueColumn)
  }
}
