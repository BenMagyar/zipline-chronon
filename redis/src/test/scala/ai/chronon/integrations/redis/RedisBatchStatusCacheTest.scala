package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.GlideReadCommand.{Get, MGet, ZRangeByScore}
import ai.chronon.integrations.redis.GlideReadResult.{OptionalValues, Value, Values}
import ai.chronon.integrations.redis.RedisKVStoreConstants.{ConfigPipelineSyncThreads, GlideExecutionDirect}
import ai.chronon.online.KVStore.GetRequest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{CompletableFuture, CountDownLatch, Executors, ThreadPoolExecutor, TimeUnit}
import redis.clients.jedis.exceptions.JedisConnectionException
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

class RedisBatchStatusCacheTest extends AnyFlatSpec with Matchers {

  private val activeStatus =
    RedisBatchUpload.BatchStatus("generation-1", "2026-07-30", batchTimestamp = 1000L, writeEpoch = 1L)

  "Redis managed batch status reads" should "resolve and negative-cache a missing marker once per dataset" in {
    val statusReads = new AtomicInteger()
    val currentStatus = new AtomicReference[Option[RedisBatchUpload.BatchStatus]](None)

    withStore(
      dataset => {
        statusReads.incrementAndGet()
        currentStatus.get()
      },
      storedBytes = null
    ) { store =>
      val requests = batchRequests("MISSING_STATUS_BATCH", 50)

      val first = Await.result(store.multiGet(requests), 2.seconds)
      all(first.map(_.values.isFailure)) shouldBe true
      statusReads.get() shouldBe 1

      val cachedMissing = Await.result(store.multiGet(requests), 2.seconds)
      all(cachedMissing.map(_.values.isFailure)) shouldBe true
      statusReads.get() shouldBe 1

      currentStatus.set(Some(activeStatus))
      Thread.sleep(RedisKVStore.MissingBatchStatusCacheMillis + 50L)

      val recovered = Await.result(store.multiGet(requests), 2.seconds)
      all(recovered.map(_.values.get.isEmpty)) shouldBe true
      statusReads.get() shouldBe 2
    }
  }

  it should "pipeline distinct stale markers in one batch" in {
    val directReads = new AtomicInteger()
    val batchReads = new AtomicInteger()
    val batchedDatasets = new AtomicReference[Vector[String]](Vector.empty)

    withStore(
      _ => {
        directReads.incrementAndGet()
        Some(activeStatus)
      },
      storedBytes = null,
      batchStatusReader = Some { datasets =>
        batchReads.incrementAndGet()
        batchedDatasets.set(datasets.toVector)
        datasets.map(_ => Success(Some(activeStatus)))
      }
    ) { store =>
      val requests = Vector.tabulate(42)(index =>
        GetRequest(s"key-$index".getBytes(StandardCharsets.UTF_8), s"DATASET_${index}_BATCH"))

      val cold = Await.result(store.multiGet(requests), 2.seconds)
      all(cold.map(_.values.get.isEmpty)) shouldBe true
      batchReads.get() shouldBe 1
      batchedDatasets.get() should contain theSameElementsInOrderAs requests.map(_.dataset)
      directReads.get() shouldBe 0

      val warm = Await.result(store.multiGet(requests), 2.seconds)
      all(warm.map(_.values.get.isEmpty)) shouldBe true
      batchReads.get() shouldBe 1
    }
  }

  it should "single-flight concurrent refreshes for one dataset" in {
    val statusReads = new AtomicInteger()
    val readEntered = new CountDownLatch(1)
    val releaseRead = new CountDownLatch(1)

    withStore(
      _ => {
        statusReads.incrementAndGet()
        readEntered.countDown()
        releaseRead.await(2, TimeUnit.SECONDS)
        Some(activeStatus)
      },
      storedBytes = null,
      threads = 4
    ) { store =>
      val requests = batchRequests("SINGLE_FLIGHT_STATUS_BATCH", 1)
      val first = store.multiGet(requests)
      readEntered.await(1, TimeUnit.SECONDS) shouldBe true
      val second = store.multiGet(requests)

      try {
        Thread.sleep(50L)
        statusReads.get() shouldBe 1
      } finally {
        releaseRead.countDown()
      }

      Await.result(first, 2.seconds).head.values.get shouldBe empty
      Await.result(second, 2.seconds).head.values.get shouldBe empty
      statusReads.get() shouldBe 1
    }
  }

  it should "not block one dataset behind another dataset's status read" in {
    val slowReadEntered = new CountDownLatch(1)
    val releaseSlowRead = new CountDownLatch(1)

    withStore(
      dataset => {
        if (dataset == "SLOW_STATUS_BATCH") {
          slowReadEntered.countDown()
          releaseSlowRead.await(2, TimeUnit.SECONDS)
        }
        Some(activeStatus)
      },
      storedBytes = null,
      threads = 2
    ) { store =>
      val slow = store.multiGet(batchRequests("SLOW_STATUS_BATCH", 1))
      slowReadEntered.await(1, TimeUnit.SECONDS) shouldBe true

      try {
        val fast = Await.result(store.multiGet(batchRequests("FAST_STATUS_BATCH", 1)), 1.second)
        fast.head.values.get shouldBe empty
      } finally {
        releaseSlowRead.countDown()
      }

      Await.result(slow, 2.seconds).head.values.get shouldBe empty
    }
  }

  it should "reuse one forced marker refresh for every value planned from the same stale status" in {
    val statusReads = new AtomicInteger()
    val currentStatus = new AtomicReference[RedisBatchUpload.BatchStatus](activeStatus)
    val replacement =
      RedisBatchUpload.BatchStatus("generation-2", "2026-07-31", batchTimestamp = 2000L, writeEpoch = 2L)
    val storedBytes = RedisBatchUpload.encodeValue(
      RedisBatchUpload.Upsert,
      replacement.batchTimestamp,
      "new-value".getBytes(StandardCharsets.UTF_8),
      replacement.writeEpoch
    )

    withStore(
      _ => {
        statusReads.incrementAndGet()
        Some(currentStatus.get())
      },
      storedBytes
    ) { store =>
      val requests = batchRequests("AHEAD_STATUS_BATCH", 50)

      val fenced = Await.result(store.multiGet(requests), 2.seconds)
      all(fenced.map(_.values.isFailure)) shouldBe true
      // One ordinary cache load plus one forced refresh, rather than one forced read per returned value.
      statusReads.get() shouldBe 2

      currentStatus.set(replacement)
      Thread.sleep(RedisKVStore.AheadBatchStatusRefreshBackoffMillis + 50L)

      val published = Await.result(store.multiGet(requests), 2.seconds)
      all(published.map(_.values.get.size)) shouldBe 1
      all(published.map(_.values.get.head.millis)) shouldBe replacement.batchTimestamp
      statusReads.get() shouldBe 3
    }
  }

  it should "retry a failed pipelined marker through the direct cluster path" in {
    val directReads = new AtomicInteger()

    withStore(
      _ => {
        directReads.incrementAndGet()
        Some(activeStatus)
      },
      storedBytes = null
    ) { store =>
      val recovered = store.recoverBatchStatusRead(
        "RESHARDED_STATUS_BATCH",
        Failure(new JedisConnectionException("stale cluster route"))
      )

      recovered shouldBe Success(Some(activeStatus))
      directReads.get() shouldBe 1
    }
  }

  it should "leave failed marker refreshes uncached so the next request retries" in {
    val batchReads = new AtomicInteger()

    withStore(
      _ => Some(activeStatus),
      storedBytes = null,
      batchStatusReader = Some { datasets =>
        val attempt = batchReads.incrementAndGet()
        datasets.map { _ =>
          if (attempt == 1) Failure(new IllegalStateException("temporary marker failure"))
          else Success(Some(activeStatus))
        }
      }
    ) { store =>
      val requests = batchRequests("RETRY_STATUS_BATCH", 10)
      val failed = Await.result(store.multiGet(requests), 2.seconds)
      all(failed.map(_.values.isFailure)) shouldBe true

      val recovered = Await.result(store.multiGet(requests), 2.seconds)
      all(recovered.map(_.values.get.isEmpty)) shouldBe true
      batchReads.get() shouldBe 2
    }
  }

  it should "bound the publication marker cache" in {
    RedisKVStore.BatchStatusCacheMaxEntries shouldBe 4096
  }

  private def batchRequests(dataset: String, count: Int): Vector[GetRequest] =
    Vector.tabulate(count)(index => GetRequest(s"key-$index".getBytes(StandardCharsets.UTF_8), dataset))

  private def withStore(statusReader: String => Option[RedisBatchUpload.BatchStatus],
                        storedBytes: Array[Byte],
                        threads: Int = 4,
                        batchStatusReader: Option[
                          Seq[String] => Seq[Try[Option[RedisBatchUpload.BatchStatus]]]] = None)(test: TestStore => Unit): Unit = {
    val executor = Executors.newFixedThreadPool(threads).asInstanceOf[ThreadPoolExecutor]
    val glideClient = new GlideRedisReadClient(
      GlideExecutionDirect,
      directMaxInflight = 1000,
      directMaxPending = 10000,
      directQueueTimeoutMs = 5000,
      command =>
        CompletableFuture.completedFuture(
          Success(command match {
            case Get(_)                  => Value(Option(storedBytes))
            case MGet(keys)              => OptionalValues(Vector.fill(keys.size)(Option(storedBytes)))
            case ZRangeByScore(_, _, _) => Values(Vector.empty)
          })),
      () => ()
    )
    val store = new TestStore(statusReader, batchStatusReader, glideClient, ExecutionContext.fromExecutor(executor))
    try test(store)
    finally {
      store.shutdownGlideReadClient()
      executor.shutdownNow()
      executor.awaitTermination(2, TimeUnit.SECONDS)
    }
  }

  private final class TestStore(statusReader: String => Option[RedisBatchUpload.BatchStatus],
                                batchStatusReader: Option[
                                  Seq[String] => Seq[Try[Option[RedisBatchUpload.BatchStatus]]]],
                                glideClient: GlideRedisReadClient,
                                override implicit val executionContext: ExecutionContext)
      extends RedisKVStoreImpl(
        null,
        Map("redis.read.metrics.enabled" -> "false", ConfigPipelineSyncThreads -> "0"),
        requireBatchStatus = true,
        glideReadClient = Some(glideClient)
      ) {
    override protected[redis] def readBatchStatus(dataset: String): Option[RedisBatchUpload.BatchStatus] =
      statusReader(dataset)

    override protected[redis] def readBatchStatuses(
        datasets: Seq[String]): Seq[Try[Option[RedisBatchUpload.BatchStatus]]] =
      batchStatusReader.map(_(datasets)).getOrElse(datasets.map(dataset => Try(statusReader(dataset))))
  }
}
