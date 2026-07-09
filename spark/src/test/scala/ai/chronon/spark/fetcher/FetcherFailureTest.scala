package ai.chronon.spark.fetcher

import ai.chronon.api.Constants.MetadataDataset
import ai.chronon.api.Extensions.JoinOps
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.{GetRequest, GetResponse}
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.{FetcherUtil, fetcher}
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.{MockApi, OnlineUtils, SparkTestBase}
import org.apache.spark.sql.functions.col
import org.junit.Assert.{assertEquals, assertTrue}
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Decorator that makes an InMemoryKvStore behave like the production stores Depop actually uses
  * (DynamoDBKVStoreImpl / BigTableKVStoreImpl): a lookup for an absent key returns Success(Seq.empty)
  * rather than InMemoryKvStore's Failure. This difference is exactly what hides the serving-info-miss
  * bug in the in-memory tests - see DynamoDBKVStoreImpl.extractTimedValues (empty item -> Success(empty))
  * and BigTableKVStoreImpl.multiGet (absent row -> Success(empty)).
  */
class ProdMissSemanticsKvStore(inner: KVStore) extends KVStore {
  override implicit val executionContext: ExecutionContext = inner.executionContext
  override def create(dataset: String): Unit = inner.create(dataset)
  override def create(dataset: String, props: Map[String, Any]): Unit = inner.create(dataset, props)
  override def multiPut(keyValueDatasets: Seq[KVStore.PutRequest]): Future[Seq[Boolean]] =
    inner.multiPut(keyValueDatasets)
  override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit =
    inner.bulkPut(sourceOfflineTable, destinationOnlineDataSet, partition)
  override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] =
    inner.multiGet(requests).map { responses =>
      responses.map { r =>
        r.values match {
          case Failure(_) => GetResponse(r.request, Success(Seq.empty))
          case _          => r
        }
      }
    }
}

class FetcherFailureTest extends SparkTestBase {

  private val tableUtils = TableUtils(spark)

  private val topic = "test_topic"
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())
  private val yesterday = tableUtils.partitionSpec.before(today)

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  // test soft-fail on missing keys
  it should "test empty request" in {
    val namespace = "empty_request"
    val joinConf = FetcherTestUtil.generateRandomData(namespace, tableUtils, spark, topic, today, yesterday, 5, 5)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetcherFailureTest#empty_request")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val metadataStore = new fetcher.MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)

    val request = Request(joinConf.metaData.name, Map.empty)
    val (responses, _) = FetcherTestUtil.joinResponses(spark, Array(request), mockApi)
    val responseMap = responses.head.values.get

    logger.info("====== Empty request response map ======")
    logger.info(responseMap.toString)
    // In this case because of empty keys, both attempts to compute derivation will fail
    val derivationExceptionTypes = Seq("derivation_fetch_exception", "derivation_rename_exception")
    assertEquals(joinConf.joinParts.size() + derivationExceptionTypes.size, responseMap.size)
    assertTrue(responseMap.keys.forall(_.endsWith(FetcherUtil.FeatureExceptionSuffix)))
  }

  it should "test KVStore partial failure" in {
    val namespace = "test_kv_store_partial_failure"
    val joinConf = FetcherTestUtil.generateRandomData(namespace, tableUtils, spark, topic, today, yesterday, 5, 5)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val kvStoreFunc = () =>
      OnlineUtils.buildInMemoryKVStore("FetcherFailureTest#test_kv_store_partial_failure",
                                       hardFailureOnInvalidDataset = true)
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    val metadataStore = new MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)

    val keys = joinConf.leftKeyCols
    val keyData = spark.table(s"$namespace.queries_table").select(keys.map(col): _*).head
    val keyMap = keys.indices.map { idx =>
      keys(idx) -> keyData.get(idx).asInstanceOf[AnyRef]
    }.toMap

    val request = Request(joinConf.metaData.name, keyMap)
    val (responses, _) = FetcherTestUtil.joinResponses(spark, Array(request), mockApi)
    val responseMap = responses.head.values.get
    val exceptionKeys = joinConf.joinPartOps.map(jp => jp.columnPrefix + "exception")

    println(responseMap)
    if(!responseMap.contains(s"derivation_fetch${FetcherUtil.FeatureExceptionSuffix}")){
      exceptionKeys.foreach(k => assertTrue(responseMap.contains(k)))
    }
  }

  // Reproduces the ext-depop report: after a version bump, a client keeps calling a join whose
  // batch GroupByServingInfo has been retired. On the production stores Depop uses, the missing
  // serving-info key comes back as an empty (Success(Seq.empty)) result, which getString turns into
  // a raw NoSuchElementException that escapes every downstream .recover and collapses the whole join
  // fetch into an opaque failure. This is a full fetchJoin, not just a getString probe.
  //
  // Currently RED: the fetch throws instead of returning a typed per-feature failure. Once the KV
  // layer returns a Failure for an empty result, this should behave like "test empty request" above -
  // the request succeeds and the missing groupBy surfaces as feature-level exception keys.
  it should "return a typed failure (not an opaque 500) when serving info is missing" in {
    val namespace = "serving_info_miss"
    val joinConf = FetcherTestUtil.generateRandomData(namespace, tableUtils, spark, topic, today, yesterday, 5, 5)
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

    // Share one backing InMemory store, wrapped so misses look like production (Success(empty)).
    val inner = OnlineUtils.buildInMemoryKVStore("FetcherFailureTest#serving_info_miss")
    val prodStore = new ProdMissSemanticsKvStore(inner)
    val kvStoreFunc = () => prodStore
    val mockApi = new MockApi(kvStoreFunc, namespace)

    // Join conf is present, but we deliberately never upload (retire) any groupBy serving info.
    val metadataStore = new MetadataStore(FetchContext(prodStore))
    prodStore.create(MetadataDataset)
    metadataStore.putJoinConf(joinConf)

    val keys = joinConf.leftKeyCols
    val keyData = spark.table(s"$namespace.queries_table").select(keys.map(col): _*).head
    val keyMap = keys.indices.map { idx => keys(idx) -> keyData.get(idx).asInstanceOf[AnyRef] }.toMap

    val request = Request(joinConf.metaData.name, keyMap)
    val (responses, _) = FetcherTestUtil.joinResponses(spark, Array(request), mockApi)

    // Before the fix, the missing serving info throws out of fetchJoin (Await re-throws) and this
    // call never returns. The fix's guarantee: the whole request comes back Success (no opaque 500),
    // with the retired groupBy surfaced as feature-level exception markers. The generated join has
    // derivations, so those markers land as derivation_*_exception keys rather than per-part
    // <prefix>_exception keys - either way every populated key is an exception marker.
    assertTrue("join request must not fail as a whole (no opaque 500)", responses.head.values.isSuccess)
    val responseMap = responses.head.values.get
    assertTrue(s"expected feature-level exception markers, got: $responseMap",
               responseMap.nonEmpty && responseMap.keys.forall(_.endsWith(FetcherUtil.FeatureExceptionSuffix)))
  }

}
