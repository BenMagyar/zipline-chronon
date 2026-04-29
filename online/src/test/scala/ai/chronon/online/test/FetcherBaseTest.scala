/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online.test

import ai.chronon.aggregator.windowing.FinalBatchIr
import ai.chronon.api.{
  Accuracy,
  Builders,
  DoubleType,
  GroupByServingInfo,
  IntType,
  LongType,
  MetaData,
  Operation,
  StringType,
  StructField,
  StructType,
  TimeUnit,
  Window
}
import ai.chronon.api.Extensions.{GroupByOps, WindowOps}
import ai.chronon.online.fetcher.Fetcher.ColumnSpec
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.fetcher.Fetcher.Response
import ai.chronon.online.fetcher.FetcherCache.BatchResponses
import ai.chronon.online.KVStore.TimedValue
import ai.chronon.online.fetcher.{FetchContext, GroupByFetcher, MetadataStore}
import ai.chronon.online.{fetcher, _}
import org.junit.Assert.assertEquals
import org.mockito.Answers
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar
import ai.chronon.online.metrics.TTLCache
import ai.chronon.online.serde.AvroConversions

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class FetcherBaseTest extends AnyFlatSpec with MockitoSugar with Matchers with MockitoHelper with BeforeAndAfter {
  val GroupBy = "relevance.short_term_user_features"
  val Column = "pdp_view_count_14d"
  val GuestKey = "guest"
  val HostKey = "host"
  val GuestId: AnyRef = 123.asInstanceOf[AnyRef]
  val HostId = "456"
  var joinPartFetcher: fetcher.JoinPartFetcher = _
  var groupByFetcher: fetcher.GroupByFetcher = _
  var kvStore: KVStore = _
  var fetchContext: FetchContext = _
  var metadataStore: MetadataStore = _

  before {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    // The KVStore execution context is implicitly used for
    // Future compositions in the Fetcher so provision it in
    // the mock to prevent hanging.
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
    fetchContext = FetchContext(kvStore)
    metadataStore = spy[fetcher.MetadataStore](new MetadataStore(fetchContext))
    joinPartFetcher = spy[fetcher.JoinPartFetcher](new fetcher.JoinPartFetcher(fetchContext, metadataStore))
    groupByFetcher = spy[fetcher.GroupByFetcher](new GroupByFetcher(fetchContext, metadataStore))
  }

  private def makeKeyTransformServingInfo(): GroupByServingInfoParsed = {
    val groupBy = Builders.GroupBy(
      sources = Seq(
        Builders.Source.events(
          table = "events.my_stream_raw",
          topic = "events.my_stream",
          query = Builders.Query(
            selects = Map(
              "id" -> "id",
              "int_val" -> "int_val",
              "double_val" -> "double_val"
            ),
            wheres = Seq.empty,
            timeColumn = "ts",
            startPartition = "20231106"
          )
        )
      ),
      keyColumns = Seq("id"),
      keyTransforms = Map("id" -> "lower(id)"),
      aggregations = Seq(
        Builders.Aggregation(
          operation = Operation.SUM,
          inputColumn = "double_val",
          windows = Seq(new Window(1, TimeUnit.DAYS))
        )
      ),
      metaData = Builders.MetaData(name = "key_transform_test_group_by"),
      accuracy = Accuracy.SNAPSHOT
    )
    val groupByServingInfo = new GroupByServingInfo()
    groupByServingInfo.setGroupBy(groupBy)

    val inputSchema = StructType(
      "Input",
      Array(
        StructField("id", StringType),
        StructField("int_val", IntType),
        StructField("double_val", DoubleType),
        StructField("ts", LongType)
      )
    )
    groupByServingInfo.setInputAvroSchema(AvroConversions.fromChrononSchema(inputSchema).toString(true))

    val keySchema = StructType("Key", Array(StructField("id", StringType)))
    groupByServingInfo.setKeyAvroSchema(AvroConversions.fromChrononSchema(keySchema).toString(true))

    val selectedSchema = StructType(
      "Selected",
      Array(
        StructField("id", StringType),
        StructField("int_val", IntType),
        StructField("double_val", DoubleType)
      )
    )
    groupByServingInfo.setSelectedAvroSchema(AvroConversions.fromChrononSchema(selectedSchema).toString(true))

    groupByServingInfo.setBatchEndDate("2023-11-06")
    groupByServingInfo.setDateFormat("yyyy-MM-dd")
    new GroupByServingInfoParsed(groupByServingInfo)
  }

  it should "fetch columns single query" in {
    // Fetch a single query
    val keyMap = Map(GuestKey -> GuestId)
    val query = ColumnSpec(GroupBy, Column, None, Some(keyMap))
    doAnswer(new Answer[Future[Seq[fetcher.Fetcher.Response]]] {
      def answer(invocation: InvocationOnMock): Future[Seq[Response]] = {
        val requests = invocation.getArgument(0).asInstanceOf[Seq[Request]]
        val request = requests.head
        val response = Response(request, Success(Map(request.name -> "100")))
        Future.successful(Seq(response))
      }
    }).when(groupByFetcher).fetchGroupBys(any())

    // Map should contain query with valid response
    val queryResults = Await.result(groupByFetcher.fetchColumns(Seq(query)), 1.second)
    queryResults.contains(query) shouldBe true
    queryResults.get(query).map(_.values) shouldBe Some(Success(Map(s"$GroupBy.$Column" -> "100")))

    // GroupBy request sent to KV store for the query
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(groupByFetcher, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequest = requestsCaptor.getValue.asInstanceOf[Seq[Request]].headOption
    actualRequest shouldNot be(None)
    actualRequest.get.name shouldBe s"${query.groupByName}.${query.columnName}"
    actualRequest.get.keys shouldBe query.keyMapping.get
  }

  it should "fetch columns batch" in {
    // Fetch a batch of queries
    val guestKeyMap = Map(GuestKey -> GuestId)
    val guestQuery = ColumnSpec(GroupBy, Column, Some(GuestKey), Some(guestKeyMap))
    val hostKeyMap = Map(HostKey -> HostId)
    val hostQuery = ColumnSpec(GroupBy, Column, Some(HostKey), Some(hostKeyMap))

    doAnswer(new Answer[Future[Seq[fetcher.Fetcher.Response]]] {
      def answer(invocation: InvocationOnMock): Future[Seq[Response]] = {
        val requests = invocation.getArgument(0).asInstanceOf[Seq[Request]]
        val responses = requests.map(r => Response(r, Success(Map(r.name -> "100"))))
        Future.successful(responses)
      }
    }).when(groupByFetcher).fetchGroupBys(any())

    // Map should contain query with valid response
    val queryResults = Await.result(groupByFetcher.fetchColumns(Seq(guestQuery, hostQuery)), 1.second)
    queryResults.contains(guestQuery) shouldBe true
    queryResults.get(guestQuery).map(_.values) shouldBe Some(Success(Map(s"${GuestKey}_$GroupBy.$Column" -> "100")))
    queryResults.contains(hostQuery) shouldBe true
    queryResults.get(hostQuery).map(_.values) shouldBe Some(Success(Map(s"${HostKey}_$GroupBy.$Column" -> "100")))

    // GroupBy request sent to KV store for the query
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(groupByFetcher, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequests = requestsCaptor.getValue.asInstanceOf[Seq[Request]]
    actualRequests.length shouldBe 2
    actualRequests.head.name shouldBe s"${guestQuery.groupByName}.${guestQuery.columnName}"
    actualRequests.head.keys shouldBe guestQuery.keyMapping.get
    actualRequests(1).name shouldBe s"${hostQuery.groupByName}.${hostQuery.columnName}"
    actualRequests(1).keys shouldBe hostQuery.keyMapping.get
  }

  it should "fetch columns missing response" in {
    // Fetch a single query
    val keyMap = Map(GuestKey -> GuestId)
    val query = ColumnSpec(GroupBy, Column, None, Some(keyMap))

    doAnswer(new Answer[Future[Seq[fetcher.Fetcher.Response]]] {
      def answer(invocation: InvocationOnMock): Future[Seq[Response]] = {
        Future.successful(Seq())
      }
    }).when(groupByFetcher).fetchGroupBys(any())

    // Map should contain query with Failure response
    val queryResults = Await.result(groupByFetcher.fetchColumns(Seq(query)), 1.second)
    queryResults.contains(query) shouldBe true
    queryResults.get(query).map(_.values) match {
      case Some(Failure(_: IllegalStateException)) => succeed
      case _                                       => fail()
    }

    // GroupBy request sent to KV store for the query
    val requestsCaptor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(groupByFetcher, times(1)).fetchGroupBys(requestsCaptor.capture().asInstanceOf[Seq[Request]])
    val actualRequest = requestsCaptor.getValue.asInstanceOf[Seq[Request]].headOption
    actualRequest shouldNot be(None)
    actualRequest.get.name shouldBe query.groupByName + "." + query.columnName
    actualRequest.get.keys shouldBe query.keyMapping.get
  }

  it should "transform groupBy keys before encoding kv requests" in {
    val servingInfo = makeKeyTransformServingInfo()
    val ttlCache = mock[TTLCache[String, Try[GroupByServingInfoParsed]]]
    val encodedKeys = mutable.ArrayBuffer.empty[Map[String, AnyRef]]

    doReturn(ttlCache).when(metadataStore).getGroupByServingInfo
    doReturn(Success(servingInfo)).when(ttlCache).apply("key_transform_test_group_by")

    doAnswer(new Answer[Array[Byte]] {
      def answer(invocation: InvocationOnMock): Array[Byte] = {
        val keys = invocation.getArgument(0).asInstanceOf[Map[String, AnyRef]]
        val dataset = invocation.getArgument(2).asInstanceOf[String]
        encodedKeys.append(keys)
        s"$dataset:${keys("id")}".getBytes
      }
    }).when(kvStore).createKeyBytes(any(), any(), any())

    doAnswer(new Answer[Future[Seq[KVStore.GetResponse]]] {
      def answer(invocation: InvocationOnMock): Future[Seq[KVStore.GetResponse]] = {
        val requests = invocation.getArgument(0).asInstanceOf[Seq[KVStore.GetRequest]]
        Future.successful(requests.map(request => KVStore.GetResponse(request, Success(Seq.empty))))
      }
    }).when(kvStore).multiGet(any())

    val request = Request(
      name = "key_transform_test_group_by",
      keys = Map("id" -> "TEST_USER_123"),
      atMillis = None,
      context = None
    )

    Await.result(groupByFetcher.fetchGroupBys(Seq(request)), 1.second)

    encodedKeys should not be empty
    encodedKeys.foreach(_ shouldEqual Map("id" -> "test_user_123"))
  }

  // updateServingInfo() is called when the batch response is from the KV store.
  it should "get serving info should call update serving info if batch response is from kv store" in {
    val oldServingInfo = mock[GroupByServingInfoParsed]
    val updatedServingInfo = mock[GroupByServingInfoParsed]
    doReturn(updatedServingInfo).when(joinPartFetcher).getServingInfo(any(), any())

    val batchTimedValuesSuccess = Success(Seq(TimedValue(Array(1.toByte), 2000L)))
    val kvStoreBatchResponses = BatchResponses(batchTimedValuesSuccess)

    val result = joinPartFetcher.getServingInfo(oldServingInfo, kvStoreBatchResponses)

    // updateServingInfo is called
    result shouldEqual updatedServingInfo
    verify(joinPartFetcher).getServingInfo(any(), any())
  }

  // If a batch response is cached, the serving info should be refreshed. This is needed to prevent
  // the serving info from becoming stale if all the requests are cached.
  it should "get serving info should refresh serving info if batch response is cached" in {
    val ttlCache = mock[TTLCache[String, Try[GroupByServingInfoParsed]]]
    doReturn(ttlCache).when(metadataStore).getGroupByServingInfo

    val oldServingInfo = mock[GroupByServingInfoParsed]
    doReturn(Success(oldServingInfo)).when(ttlCache).refresh(any[String])

    val metaDataMock = mock[MetaData]
    val groupByOpsMock = mock[GroupByOps]
    metaDataMock.name = "test"
    groupByOpsMock.metaData = metaDataMock
    doReturn(groupByOpsMock).when(oldServingInfo).groupByOps

    val cachedBatchResponses = BatchResponses(mock[FinalBatchIr])
    val result = groupByFetcher.getServingInfo(oldServingInfo, cachedBatchResponses)

    // FetcherBase.updateServingInfo is not called, but getGroupByServingInfo.refresh() is.
    result shouldEqual oldServingInfo
    verify(ttlCache).refresh(any())
    verify(ttlCache, never()).apply(any())
  }

  it should "fetch in the happy case" in {
    val fetchContext = mock[FetchContext]
    val baseFetcher = new fetcher.JoinPartFetcher(fetchContext, mock[MetadataStore])
    val request = Request(name = "name", keys = Map("email" -> "email"), atMillis = None, context = None)
    val response: Map[Request, Try[Map[String, AnyRef]]] = Map(
      request -> Success(
        Map(
          "key" -> "value"
        ))
    )

    val result = baseFetcher.parseGroupByResponse("prefix_", request, response)
    assertEquals(result, Map("prefix_key" -> "value"))
  }

  it should "Not fetch with null keys" in {
    val baseFetcher = new fetcher.JoinPartFetcher(mock[FetchContext], mock[MetadataStore])
    val request = Request(name = "name", keys = Map("email" -> null), atMillis = None, context = None)
    val request2 = Request(name = "name2", keys = Map("email" -> null), atMillis = None, context = None)

    val response: Map[Request, Try[Map[String, AnyRef]]] = Map(
      request2 -> Success(
        Map(
          "key" -> "value"
        ))
    )

    val result = baseFetcher.parseGroupByResponse("prefix_", request, response)
    result shouldBe Map()
  }

  it should "parse with missing keys" in {
    val baseFetcher = new fetcher.JoinPartFetcher(mock[FetchContext], mock[MetadataStore])
    val request = Request(name = "name", keys = Map("email" -> "email"), atMillis = None, context = None)
    val request2 = Request(name = "name2", keys = Map("email" -> "email"), atMillis = None, context = None)

    val response: Map[Request, Try[Map[String, AnyRef]]] = Map(
      request2 -> Success(
        Map(
          "key" -> "value"
        ))
    )

    val result = baseFetcher.parseGroupByResponse("prefix_", request, response)
    result.keySet shouldBe Set(s"prefix${FetcherUtil.FeatureExceptionSuffix}")
  }

  it should "check late batch data is handled correctly" in {
    // lookup request - 03/20/2024 01:00 UTC
    // batch landing time 03/17/2024 00:00 UTC
    val longWindows = Seq(new Window(7, TimeUnit.DAYS), new Window(10, TimeUnit.DAYS))
    val tailHops2d = new Window(2, TimeUnit.DAYS).millis
    val result = groupByFetcher.checkLateBatchData(1710896400000L, "myGroupBy", 1710633600000L, tailHops2d, longWindows)
    result shouldBe 1L

    // try the same with a shorter lookback window
    val shortWindows = Seq(new Window(1, TimeUnit.DAYS), new Window(10, TimeUnit.HOURS))
    val result2 =
      groupByFetcher.checkLateBatchData(1710896400000L, "myGroupBy", 1710633600000L, tailHops2d, shortWindows)
    result2 shouldBe 0L
  }

  it should "check late batch data handles case when batch data isn't late" in {
    // lookup request - 03/20/2024 01:00 UTC
    // batch landing time 03/19/2024 00:00 UTC
    val longWindows = Seq(new Window(7, TimeUnit.DAYS), new Window(10, TimeUnit.DAYS))
    val tailHops2d = new Window(2, TimeUnit.DAYS).millis
    val result = groupByFetcher.checkLateBatchData(1710896400000L, "myGroupBy", 1710806400000L, tailHops2d, longWindows)
    result shouldBe 0L

    // try the same with a shorter lookback window
    val shortWindows = Seq(new Window(1, TimeUnit.DAYS), new Window(10, TimeUnit.HOURS))
    val result2 =
      groupByFetcher.checkLateBatchData(1710896400000L, "myGroupBy", 1710633600000L, tailHops2d, shortWindows)
    result2 shouldBe 0L
  }
}
