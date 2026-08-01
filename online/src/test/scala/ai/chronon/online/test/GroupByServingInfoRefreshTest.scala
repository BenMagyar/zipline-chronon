package ai.chronon.online.test

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{Constants, ThriftJsonCodec}
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.InMemoryKvStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class GroupByServingInfoRefreshTest extends AnyFlatSpec with Matchers {

  private def putServingInfo(kvStore: InMemoryKvStore, dataset: String, value: String, tsMillis: Long): Unit =
    Await.result(
      kvStore.put(
        PutRequest(Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
                   value.getBytes(Constants.UTF8),
                   dataset,
                   Some(tsMillis))),
      1.second)

  "GroupBy serving-info refresh" should "reuse parsed state until the metadata payload changes" in {
    val kvStore = InMemoryKvStore.build(s"GroupByServingInfoRefreshTest_${System.nanoTime()}")
    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    val groupByName = servingInfo.groupBy.metaData.name
    val batchDataset = new GroupByOps(servingInfo.groupBy).batchDataset
    kvStore.create(batchDataset)

    putServingInfo(kvStore, batchDataset, ThriftJsonCodec.toJsonStr(servingInfo), 1L)
    val metadataStore = new MetadataStore(FetchContext(kvStore))
    val first = metadataStore.getGroupByServingInfoFresh(groupByName).get
    val firstAggregator = first.aggregator
    val unchanged = metadataStore.getGroupByServingInfoFresh(groupByName).get

    (unchanged eq first) shouldBe true
    (unchanged.aggregator eq firstAggregator) shouldBe true

    servingInfo.setBatchEndDate("2026-07-31")
    servingInfo.setBatchEndTs(first.batchEndTsMillis + 86400000L)
    putServingInfo(kvStore, batchDataset, ThriftJsonCodec.toJsonStr(servingInfo), 2L)

    val changed = metadataStore.getGroupByServingInfoFresh(groupByName).get
    (changed eq first) shouldBe false
    changed.batchEndDate shouldBe "2026-07-31"
    changed.batchEndTsMillis shouldBe first.batchEndTsMillis + 86400000L

    // A fresh read reflects the stored payload even when an operator deliberately republishes an older watermark.
    // Identity reuse is only valid for byte-for-byte identical metadata; it must not impose new monotonic semantics.
    servingInfo.setBatchEndDate("2026-07-29")
    servingInfo.setBatchEndTs(first.batchEndTsMillis - 86400000L)
    putServingInfo(kvStore, batchDataset, ThriftJsonCodec.toJsonStr(servingInfo), 3L)

    val rolledBack = metadataStore.getGroupByServingInfoFresh(groupByName).get
    (rolledBack eq changed) shouldBe false
    rolledBack.batchEndDate shouldBe "2026-07-29"
    rolledBack.batchEndTsMillis shouldBe first.batchEndTsMillis - 86400000L
  }

  it should "reuse one parsed identity across concurrent cold refreshes" in {
    val kvStore = InMemoryKvStore.build(s"GroupByServingInfoRefreshConcurrentTest_${System.nanoTime()}")
    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    val groupByName = servingInfo.groupBy.metaData.name
    val batchDataset = new GroupByOps(servingInfo.groupBy).batchDataset
    kvStore.create(batchDataset)
    putServingInfo(kvStore, batchDataset, ThriftJsonCodec.toJsonStr(servingInfo), 1L)

    val metadataStore = new MetadataStore(FetchContext(kvStore))
    val threadCount = 32
    val pool = Executors.newFixedThreadPool(threadCount)
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(threadCount)
    val identities = new java.util.concurrent.ConcurrentLinkedQueue[AnyRef]()

    try {
      (0 until threadCount).foreach { _ =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            try {
              start.await()
              identities.add(metadataStore.getGroupByServingInfoFresh(groupByName).get)
            } finally {
              done.countDown()
            }
          }
        })
      }
      start.countDown()
      done.await(10, TimeUnit.SECONDS) shouldBe true
      identities.size() shouldBe threadCount

      val iterator = identities.iterator()
      val first = iterator.next()
      while (iterator.hasNext) {
        (iterator.next() eq first) shouldBe true
      }
    } finally {
      pool.shutdownNow()
    }
  }
}
