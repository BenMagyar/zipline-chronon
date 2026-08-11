package ai.chronon.online.test

import ai.chronon.api.Constants.{
  ContinuationKey,
  JoinFolder,
  ListEntityOnline,
  ListEntityType,
  ListKeysOnly,
  ListValuesAreKeysOnly,
  MetadataDataset
}
import org.mockito.ArgumentMatchers.any
import ai.chronon.online.KVStore.{ListRequest, ListResponse, ListValue}
import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
import ai.chronon.online.{Api, KVStore}
import org.mockito.Answers
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source
import scala.util.{Success, Try}

class ListJoinsTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfter with Matchers {

  var api: Api = _
  var kvStore: KVStore = _
  var joinKVMap: Map[Array[Byte], Array[Byte]] = _

  implicit val ec: ExecutionContext = ExecutionContext.global

  before {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    api = mock[Api]
    // The KVStore execution context is implicitly used for
    // Future compositions in the Fetcher so provision it in
    // the mock to prevent hanging.
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
    when(api.genKvStore).thenReturn(kvStore)
    joinKVMap = loadJoinKVMap()
  }

  it should "return only online joins" in {
    val metadataStore = new MetadataStore(FetchContext(kvStore))
    when(kvStore.list(any())).thenReturn(generateListResponse())
    val resultFuture = metadataStore.listJoins()
    val result = Await.result(resultFuture, 10.seconds)
    assert(result.size == 1)
    result.toSet shouldEqual Set("risk.user_transactions.txn_join_d")
  }

  it should "return indexed join names without parsing full values" in {
    val metadataStore = new MetadataStore(FetchContext(kvStore))
    val indexedNames = Seq("ranking.search.attempt__50", "ranking.search.attempt__51")
    val indexedValues = Seq(
      ListValue(indexedNames.head.getBytes(StandardCharsets.UTF_8), Array.emptyByteArray),
      ListValue(indexedNames.last.getBytes(StandardCharsets.UTF_8), "not-json".getBytes(StandardCharsets.UTF_8))
    )
    when(kvStore.list(any())).thenReturn(
      Future.successful(
        ListResponse(
          ListRequest(MetadataDataset, Map.empty),
          Success(indexedValues),
          Map(ListValuesAreKeysOnly -> true)
        )))

    Await.result(metadataStore.listJoins(), 10.seconds).toSet shouldEqual indexedNames.toSet

    val requestCaptor = ArgumentCaptor.forClass(classOf[ListRequest])
    verify(kvStore).list(requestCaptor.capture())
    val props = requestCaptor.getValue.props
    props.get(ListEntityType) shouldBe Some(JoinFolder)
    props.get(ListEntityOnline) shouldBe Some(true)
    props.get(ListKeysOnly) shouldBe Some(true)
  }

  it should "fail the call on internal issues" in {
    val metadataStore = new MetadataStore(FetchContext(kvStore))
    when(kvStore.list(any())).thenReturn(generateBrokenListResponse())
    an[Exception] should be thrownBy Await.result(metadataStore.listJoins(), 10.seconds)
  }

  it should "paginate list calls" in {
    val metadataStore = new MetadataStore(FetchContext(kvStore))

    val responses: Seq[ListValue] = joinKVMap.map(kv => ListValue(kv._1, kv._2)).toSeq
    // we want each of these ListValue responses to be returned in a separate ListResponse so we
    // can test pagination. So we'll wrap each of these elements in a Try[Seq[..]]
    val listResponseValues: Seq[Try[Seq[ListValue]]] = responses.map(v => Success(Seq(v)))

    // first response will have a continuation key
    val first = Future(
      ListResponse(ListRequest(MetadataDataset, Map.empty), listResponseValues.head, Map(ContinuationKey -> "1")))
    // second response will not have a continuation key
    val second = Future(ListResponse(ListRequest(MetadataDataset, Map.empty), listResponseValues.last, Map.empty))

    when(kvStore.list(any())).thenReturn(first, second)
    val resultFuture = metadataStore.listJoins()
    val result = Await.result(resultFuture, 10.seconds)
    assert(result.size == 1)
    result.toSet shouldEqual Set("risk.user_transactions.txn_join_d")
  }

  it should "paginate indexed join names" in {
    val metadataStore = new MetadataStore(FetchContext(kvStore))
    val firstName = "ranking.search.attempt__50"
    val secondName = "ranking.search.attempt__51"
    val first = Future.successful(
      ListResponse(
        ListRequest(MetadataDataset, Map.empty),
        Success(Seq(ListValue(firstName.getBytes(StandardCharsets.UTF_8), Array.emptyByteArray))),
        Map(ContinuationKey -> "next", ListValuesAreKeysOnly -> true)
      ))
    val second = Future.successful(
      ListResponse(
        ListRequest(MetadataDataset, Map.empty),
        Success(Seq(ListValue(secondName.getBytes(StandardCharsets.UTF_8), Array.emptyByteArray))),
        Map(ListValuesAreKeysOnly -> true)
      ))

    when(kvStore.list(any())).thenReturn(first, second)

    Await.result(metadataStore.listJoins(), 10.seconds).toSet shouldEqual Set(firstName, secondName)

    val requestCaptor = ArgumentCaptor.forClass(classOf[ListRequest])
    verify(kvStore, times(2)).list(requestCaptor.capture())
    requestCaptor.getAllValues.get(0).props.contains(ContinuationKey) shouldBe false
    requestCaptor.getAllValues.get(1).props.get(ContinuationKey) shouldBe Some("next")
  }

  it should "use legacy full-value listing for offline joins" in {
    val metadataStore = new MetadataStore(FetchContext(kvStore))
    when(kvStore.list(any())).thenReturn(generateListResponse())

    Await.result(metadataStore.listJoins(isOnline = false), 10.seconds).toSet shouldEqual
      Set("risk.user_transactions.txn_join_a")

    val requestCaptor = ArgumentCaptor.forClass(classOf[ListRequest])
    verify(kvStore).list(requestCaptor.capture())
    val props = requestCaptor.getValue.props
    props.get(ListEntityType) shouldBe Some(JoinFolder)
    props.contains(ListEntityOnline) shouldBe false
    props.contains(ListKeysOnly) shouldBe false
  }

  private def loadJoinKVMap(): Map[Array[Byte], Array[Byte]] = {
    val paths = Seq(
      // first is online = false
      "joins/user_transactions.txn_join_a",
      // this one is online = true
      "joins/user_transactions.txn_join_d"
    )

    paths.map { path =>
      val inputStream = getClass.getClassLoader.getResourceAsStream(path)
      if (inputStream == null) {
        throw new IllegalArgumentException(s"Resource not found: $path")
      }
      val src = Source.fromInputStream(inputStream)
      (path.getBytes(StandardCharsets.UTF_8), src.mkString.getBytes(StandardCharsets.UTF_8))
    }.toMap
  }

  private def generateListResponse(): Future[ListResponse] = {
    val listResponseValues: Try[Seq[ListValue]] = Success(joinKVMap.map(kv => ListValue(kv._1, kv._2)).toSeq)
    Future(ListResponse(ListRequest(MetadataDataset, Map.empty), listResponseValues, Map.empty))
  }

  private def generateBrokenListResponse(): Future[ListResponse] = {
    // we expect things to fail as 'broken_value' is not a valid join
    val listResponseValues: Try[Seq[ListValue]] = Success(Seq(ListValue("some_key".getBytes, "broken_value".getBytes)))
    Future(ListResponse(ListRequest(MetadataDataset, Map.empty), listResponseValues, Map.empty))
  }

}
