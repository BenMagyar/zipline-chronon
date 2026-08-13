package ai.chronon.online.test

import ai.chronon.api.Extensions.{GroupByOps, JoinOps}
import ai.chronon.api.{Builders, Constants, GroupBy, MetaData, ThriftJsonCodec}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest}
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.InMemoryKvStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

class MetadataWarmupTest extends AnyFlatSpec with Matchers {

  private final class CountingKvStore(reverseResponses: Boolean = false) extends InMemoryKvStore(() => null) {
    val multiGetCalls = new AtomicInteger()
    val batchSizes = mutable.ArrayBuffer.empty[Int]

    override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = {
      multiGetCalls.incrementAndGet()
      batchSizes.synchronized(batchSizes += requests.size)
      // InMemoryKvStore represents an absent key as a failed null dereference; Redis represents it as an empty value
      // sequence. Normalize the test double to the Redis contract so warmup can distinguish a real miss from a read
      // or admission failure.
      super.multiGet(requests).map { responses =>
        val normalized = responses.map { response =>
          response.values match {
            case Failure(_: NullPointerException) => response.copy(values = Success(Seq.empty))
            case _                                => response
          }
        }
        if (reverseResponses) normalized.reverse else normalized
      }
    }
  }

  private def put(kv: CountingKvStore, request: PutRequest): Unit =
    Await.result(kv.put(request), 1.second)

  "Metadata warmup" should "bulk-prime exact join and serving-info keys and back off misses" in {
    // Exercise a backend that returns multi-get responses out of request order. Warmup must prime metadata by key,
    // not by response position.
    val kv = new CountingKvStore(reverseResponses = true)
    kv.create(Constants.MetadataDataset)

    val foundInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    foundInfo.groupBy.metaData.setName("unit.metadata.found")
    val foundGb = foundInfo.groupBy
    val missingGb = new GroupBy(foundGb)
    missingGb.setMetaData(new MetaData(foundGb.metaData).setName("unit.metadata.missing"))

    val join = Builders.Join(
      metaData = Builders.MetaData(name = "unit.metadata.join", online = true),
      left = Builders.Source.events(Builders.Query(), "unit.requests"),
      joinParts = Seq(
        Builders.JoinPart(foundGb),
        Builders.JoinPart(foundGb, prefix = "duplicate"),
        Builders.JoinPart(missingGb)
      )
    )
    val joinKey = new JoinOps(join).keyNameForKvStore
    put(
      kv,
      PutRequest(
        joinKey.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(join).getBytes(Constants.UTF8),
        Constants.MetadataDataset
      )
    )

    val foundDataset = new GroupByOps(foundGb).batchDataset
    val missingDataset = new GroupByOps(missingGb).batchDataset
    kv.create(foundDataset)
    kv.create(missingDataset)
    put(
      kv,
      PutRequest(
        Constants.GroupByServingInfoKey.getBytes(Constants.UTF8),
        ThriftJsonCodec.toJsonStr(foundInfo).getBytes(Constants.UTF8),
        foundDataset
      )
    )

    val metadata = new MetadataStore(FetchContext(kv, timeoutMillis = 1000L))
    val result = metadata.warmJoinMetadata(Seq(join.metaData.name), timeoutMillis = 1000L)

    result.loadedJoinCount shouldBe 1
    result.groupByNames should contain theSameElementsAs Seq("unit.metadata.found", "unit.metadata.missing")
    result.loadedGroupByCount shouldBe 1
    result.missingJoinNames shouldBe empty
    result.missingGroupByNames shouldBe Seq("unit.metadata.missing")
    result.failures shouldBe empty
    kv.batchSizes.toSeq shouldBe Seq(1, 2)

    val callsAfterWarmup = kv.multiGetCalls.get()
    metadata.getJoinConf(join.metaData.name).isSuccess shouldBe true
    metadata.getGroupByServingInfo("unit.metadata.found").isSuccess shouldBe true
    (0 until 100).foreach(_ => metadata.getGroupByServingInfo("unit.metadata.missing").isFailure shouldBe true)
    metadata.buildJoinCodec(join, refreshOnFail = true).hasPartialFailure shouldBe true
    kv.multiGetCalls.get() shouldBe callsAfterWarmup
  }
}
