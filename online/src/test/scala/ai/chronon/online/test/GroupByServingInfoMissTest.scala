package ai.chronon.online.test

import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, GetResponse}
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** ext-depop: a client kept calling a join version whose batch GroupByServingInfo had been retired.
  * On the production stores (DynamoDB/BigTable) a missing key comes back as Success(Seq.empty), which
  * getString turns into a raw NoSuchElementException. This asserts getGroupByServingInfo captures that
  * as a Failure instead of letting it escape the TTLCache loader and collapse the whole fetch into an
  * opaque 500. The KV layer's throw-on-empty behavior is intentional and left unchanged.
  */
class GroupByServingInfoMissTest extends AnyFlatSpec with Matchers {

  // Mirrors production miss semantics: the batch dataset is reachable but the key has no rows.
  private class EmptyResultKvStore extends KVStore {
    override implicit val executionContext: ExecutionContext = ExecutionContext.global
    override def create(dataset: String): Unit = {}
    override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] =
      Future.successful(requests.map(r => GetResponse(r, Success(Seq.empty))))
    override def multiPut(keyValueDatasets: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] =
      Future.successful(keyValueDatasets.map(_ => true))
    override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = {}
  }

  "getGroupByServingInfo for a retired/absent batch upload" should "return a Failure, not throw" in {
    val metadataStore = new MetadataStore(FetchContext(new EmptyResultKvStore))

    // Must not throw - the loader returns Try, so an absent upload should be a Failure value.
    val result = metadataStore.getGroupByServingInfo("pricing_foo")

    result shouldBe a[Failure[_]]
    // the missing batch dataset should be recoverable from the error, not opaque
    result.failed.get.getMessage should include("PRICING_FOO_BATCH")
  }
}
