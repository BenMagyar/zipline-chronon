package ai.chronon.integrations.redis

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import redis.clients.jedis.{ClusterPipeline, Response}
import redis.clients.jedis.exceptions.{
  JedisClusterException,
  JedisConnectionException,
  JedisDataException,
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

  it should "close a write pipeline once before surfacing an unset response" in {
    val pipeline = mock[ClusterPipeline]
    val response = mock[Response[Any]]
    val unsetResponse =
      new IllegalStateException("Please close pipeline or multi block before calling this method.")
    when(response.get()).thenThrow(unsetResponse)

    val failure = the[IllegalStateException] thrownBy {
      Spark2RedisLoader.completePipeline(pipeline)(Seq(response))
    }

    failure shouldBe unsetResponse
    verify(pipeline, times(1)).close()
    verify(pipeline, never()).sync()
  }

  it should "use direct replay after an unset pipeline response" in {
    val unsetResponse =
      new IllegalStateException("Please close pipeline or multi block before calling this method.")

    Spark2RedisLoader.directReplayRequired(unsetResponse) shouldBe true
    Spark2RedisLoader.directReplayRequired(new JedisNoScriptException("NOSCRIPT missing")) shouldBe true
    Spark2RedisLoader.directReplayRequired(new JedisConnectionException("connection reset")) shouldBe false
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
