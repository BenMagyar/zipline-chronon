package ai.chronon.integrations.redis

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class IncrementalRedisBatchLoaderTest extends AnyFlatSpec with BeforeAndAfterAll with Matchers {
  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = RedisSparkSessionTestHelper.acquire("IncrementalRedisBatchLoaderTest")
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
    val previousState = IncrementalRedisBatchLoader
      .buildDiff(previousRows, previousState = None, batchTimestamp = 1000L, stateBuckets = 8)
      .currentState

    val currentRows = frame(
      "unchanged" -> "value-1",
      "changed" -> "new-value",
      "added" -> "added-value"
    )
    val frames = IncrementalRedisBatchLoader.buildDiff(
      currentRows,
      previousState = Some(previousState),
      batchTimestamp = 2000L,
      stateBuckets = 8
    )

    val delta = frames.dataDelta.collect().map { row =>
      val key = new String(row.getAs[Array[Byte]](IncrementalRedisBatchModel.KeyColumn), StandardCharsets.UTF_8)
      val value = Option(row.getAs[Array[Byte]](IncrementalRedisBatchModel.ValueColumn))
        .map(bytes => new String(bytes, StandardCharsets.UTF_8))
      val operation = row.getAs[String](IncrementalRedisBatchModel.OperationColumn)
      val timestamp = row.getAs[Long](IncrementalRedisBatchModel.BatchTimestampColumn)
      key -> (operation, value, timestamp)
    }.toMap

    delta.keySet shouldBe Set("changed", "added", "deleted")
    delta("changed") shouldBe ((RedisBatchUpload.UpsertOperation, Some("new-value"), 2000L))
    delta("added") shouldBe ((RedisBatchUpload.UpsertOperation, Some("added-value"), 2000L))
    delta("deleted") shouldBe ((RedisBatchUpload.DeleteOperation, None, 2000L))

    frames.currentState.count() shouldBe 3L
    val stateVersions = frames.currentState.collect().map { row =>
      val key = new String(row.getAs[Array[Byte]](IncrementalRedisBatchModel.KeyColumn), StandardCharsets.UTF_8)
      key -> (row.getAs[Long](IncrementalRedisBatchModel.ValueTimestampColumn),
              row.getAs[Long](IncrementalRedisBatchModel.ValueWriteEpochColumn))
    }.toMap
    stateVersions("unchanged") shouldBe (1000L -> 0L)
    stateVersions("changed") shouldBe (2000L -> 0L)
    stateVersions("added") shouldBe (2000L -> 0L)
    val buckets = frames.currentState
      .select(IncrementalRedisBatchModel.BucketColumn)
      .collect()
      .map(_.getInt(0))
    all(buckets) should (be >= 0 and be < 8)
  }

  it should "write every current key for an initial generation and no keys for an identical generation" in {
    val currentRows = frame("key-1" -> "value-1", "key-2" -> "value-2")
    val initial = IncrementalRedisBatchLoader.buildDiff(
      currentRows,
      previousState = None,
      batchTimestamp = 1000L,
      stateBuckets = 4
    )

    initial.dataDelta.count() shouldBe 2L
    initial.dataDelta
      .select(IncrementalRedisBatchModel.OperationColumn)
      .distinct()
      .collect()
      .map(_.getString(0)) shouldBe Array(RedisBatchUpload.UpsertOperation)
    all(initial.currentState.select(IncrementalRedisBatchModel.DigestColumn).collect().map(_.getAs[Array[Byte]](0).length)) shouldBe 32

    val identical = IncrementalRedisBatchLoader.buildDiff(
      currentRows,
      previousState = Some(initial.currentState),
      batchTimestamp = 2000L,
      stateBuckets = 4
    )
    identical.currentState.count() shouldBe 2L
    identical.dataDelta.count() shouldBe 0L

    val emptied = IncrementalRedisBatchLoader.buildDiff(
      frame(),
      previousState = Some(initial.currentState),
      batchTimestamp = 3000L,
      stateBuckets = 4,
      writeEpoch = 2L)
    emptied.currentState.count() shouldBe 0L
    emptied.dataDelta.count() shouldBe 2L
    all(emptied.dataDelta.collect().map(_.getAs[String](IncrementalRedisBatchModel.OperationColumn))) shouldBe
      RedisBatchUpload.DeleteOperation
  }

  "Redis lease renewal" should "renew missing and half-spent leases while leaving fresh leases untouched" in {
    val nowMillis = 1000000L
    val ttlSeconds = 5 * 24 * 60 * 60
    val halfTtlMillis = ttlSeconds.toLong * 1000L / 2L

    IncrementalRedisBatchLoader.leaseRefreshRequired(None, ttlSeconds, nowMillis) shouldBe true
    IncrementalRedisBatchLoader.leaseRefreshRequired(Some(nowMillis + halfTtlMillis + 1L), ttlSeconds, nowMillis) shouldBe false
    IncrementalRedisBatchLoader.leaseRefreshRequired(Some(nowMillis + halfTtlMillis), ttlSeconds, nowMillis) shouldBe true
    IncrementalRedisBatchLoader.leaseRefreshRequired(Some(nowMillis), ttlSeconds, nowMillis) shouldBe true
  }

  it should "advance one generation monotonically without inheriting its parent's longer lease" in {
    IncrementalRedisBatchLoader.generationLeaseDeadline(2000L, None) shouldBe 2000L
    IncrementalRedisBatchLoader.generationLeaseDeadline(2000L, Some(3000L)) shouldBe 3000L
    IncrementalRedisBatchLoader.generationLeaseDeadline(4000L, Some(3000L)) shouldBe 4000L
    IncrementalRedisBatchLoader.renewedLeaseDeadline(2000L, None) shouldBe 2000L
    IncrementalRedisBatchLoader.renewedLeaseDeadline(2000L, Some(3000L)) shouldBe 3001L
  }

  it should "advance a lease checkpoint monotonically" in {
    val status = RedisBatchUpload.BatchStatus("generation-a", "2024-07-27", 1000L, 2L)
    val digest = Array.tabulate[Byte](32)(_.toByte)
    val manifest = IncrementalRedisBatchModel.ReadyManifest(
      status = status,
      parentGeneration = None,
      stateGeneration = status.generation,
      sourceTable = "source",
      batchDataset = "dataset_BATCH",
      keyPrefix = "chronon",
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

    def advance(deadlineMillis: Long): IncrementalRedisBatchModel.LeaseRecord =
      IncrementalRedisStateStore.advanceLease(
        path,
        IncrementalRedisBatchModel.LeaseRecord(status, digest, deadlineMillis),
        manifest,
        digest,
        spark,
        HadoopConditionalObjectWriter
      )

    advance(2000L).minimumActiveDeadlineMillis shouldBe 2000L
    advance(1000L).minimumActiveDeadlineMillis shouldBe 2000L
    advance(3000L).minimumActiveDeadlineMillis shouldBe 3000L
    IncrementalRedisStateStore
      .decodeLease(HadoopConditionalObjectWriter.read(path, spark.sparkContext.hadoopConfiguration).get.bytes)
      .minimumActiveDeadlineMillis shouldBe 3000L
  }

  "Redis recovery diffing" should "force every current value into a rebuild without inventing deletes" in {
    val previousRows = frame("a" -> "a-1", "b" -> "b-1")
    val previousState = IncrementalRedisBatchLoader
      .buildDiff(previousRows, None, batchTimestamp = 1000L, stateBuckets = 4)
      .currentState
    val currentRows = frame("a" -> "a-1", "b" -> "b-1")

    val rebuild = IncrementalRedisBatchLoader.buildDiff(currentRows,
                                              Some(previousState),
                                              batchTimestamp = 2000L,
                                              stateBuckets = 4,
                                              writeEpoch = 2L,
                                              forceAllUpserts = true)

    rebuild.dataDelta.count() shouldBe 2L
    rebuild.dataDelta.collect().foreach { row =>
      row.getAs[String](IncrementalRedisBatchModel.OperationColumn) shouldBe RedisBatchUpload.UpsertOperation
      row.getAs[Long](IncrementalRedisBatchModel.BatchTimestampColumn) shouldBe 2000L
      row.getAs[Long](IncrementalRedisBatchModel.WriteEpochColumn) shouldBe 2L
    }
  }

  "Redis source validation" should "reject duplicate entity keys before publication" in {
    IncrementalRedisBatchLoader.validateSourceRows(frame("a" -> "a-1", "b" -> "b-1"))

    val duplicateError = the[IllegalArgumentException] thrownBy {
      IncrementalRedisBatchLoader.validateSourceRows(frame("duplicate" -> "first", "duplicate" -> "second"))
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

  "Conditional writer validation" should "reject local writers for S3 state" in {
    val settings = IncrementalRedisBatchSettings(
      job = BatchUploadJob("unused_table",
                           "unused.dataset",
                           "2024-10-05",
                           ai.chronon.api.PartitionSpec.daily),
      target = RedisKVStoreFactory.StoreSettings(
        RedisKVStoreFactory.settings(
          Map(RedisKVStoreConstants.PropRedisClusterNodes -> "localhost:6379"),
          env = Map.empty),
        "chronon"
      ),
      options = IncrementalOptions("s3a://bucket/redis-state")
    )

    val error = the[IllegalArgumentException] thrownBy {
      IncrementalRedisBatchLoader.run(settings, spark, null, HadoopConditionalObjectWriter)
    }

    error.getMessage should include("requires a distributed conditional object writer")
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
    val claimed = IncrementalRedisBatchModel.CandidateClaim(generation, abandoned = false)
    val abandoned = IncrementalRedisBatchModel.CandidateClaim(generation, abandoned = true)

    Seq(claimed -> abandoned, abandoned -> claimed).zipWithIndex.foreach { case ((first, second), index) =>
      val path = new org.apache.hadoop.fs.Path(
        Files.createTempDirectory(s"redis-candidate-claim-$index").resolve(generation).toUri)
      val firstWrite = HadoopConditionalObjectWriter.putIfAbsent(path, IncrementalRedisStateStore.encodeClaim(first), conf)
      val secondWrite = HadoopConditionalObjectWriter.putIfAbsent(path, IncrementalRedisStateStore.encodeClaim(second), conf)

      IncrementalRedisStateStore.decodeClaim(firstWrite.bytes) shouldBe first
      IncrementalRedisStateStore.decodeClaim(secondWrite.bytes) shouldBe first
    }
  }

  "Redis INFO parsing" should "read exact fields from CRLF-delimited responses" in {
    val info = "# Memory\r\nmaxmemory:1000\r\nmaxmemory_policy:noeviction\r\n"
    IncrementalRedisBatchLoader.redisInfoField(info, "maxmemory_policy") shouldBe Some("noeviction")
    IncrementalRedisBatchLoader.redisInfoField(info, "maxmemory") shouldBe Some("1000")
    IncrementalRedisBatchLoader.redisInfoField(info, "missing") shouldBe None
  }

  "Redis version parsing" should "recognize only trailing numeric GroupBy and batch versions" in {
    IncrementalRedisBatchLoader.parseVersionedDataset("ranking.user.views__14") shouldBe
      Some(IncrementalRedisBatchLoader.VersionedDataset("ranking.user.views", 14L))
    IncrementalRedisBatchLoader.parseVersionedDataset("ranking.user.views") shouldBe None
    IncrementalRedisBatchLoader.parseVersionedDataset("ranking.user.views__v14") shouldBe None
    IncrementalRedisBatchLoader.parseVersionedDataset("ranking.user.views__latest") shouldBe None

    IncrementalRedisBatchLoader.parseVersionedBatchDataset("RANKING_USER_VIEWS__14_BATCH") shouldBe
      Some(IncrementalRedisBatchLoader.VersionedDataset("RANKING_USER_VIEWS", 14L))
    IncrementalRedisBatchLoader.parseVersionedBatchDataset("RANKING_USER_VIEWS_BATCH") shouldBe None
    IncrementalRedisBatchLoader.parseVersionedBatchDataset("RANKING_USER_VIEWS__14_STREAMING") shouldBe None
  }

  it should "require matching numeric GroupBy and batch versions before retirement" in {
    IncrementalRedisBatchLoader.requireMatchingVersionedDatasets(
      "ranking.user.views__14",
      "RANKING_USER_VIEWS__14_BATCH") shouldBe
      (IncrementalRedisBatchLoader.VersionedDataset("ranking.user.views", 14L) ->
        IncrementalRedisBatchLoader.VersionedDataset("RANKING_USER_VIEWS", 14L))

    an[IllegalArgumentException] should be thrownBy
      IncrementalRedisBatchLoader.requireMatchingVersionedDatasets("ranking.user.views", "RANKING_USER_VIEWS__14_BATCH")
    an[IllegalArgumentException] should be thrownBy
      IncrementalRedisBatchLoader.requireMatchingVersionedDatasets("ranking.user.views__14", "RANKING_USER_VIEWS_BATCH")
    an[IllegalArgumentException] should be thrownBy
      IncrementalRedisBatchLoader.requireMatchingVersionedDatasets("ranking.user.views__14",
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
      .toDF(IncrementalRedisBatchModel.KeyColumn, IncrementalRedisBatchModel.ValueColumn)
  }
}
