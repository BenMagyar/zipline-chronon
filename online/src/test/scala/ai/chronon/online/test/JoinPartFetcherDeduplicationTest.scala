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

import ai.chronon.api.Extensions.JoinOps
import ai.chronon.api.{Accuracy, Builders}
import ai.chronon.online.fetcher.Fetcher.{Request, Response}
import ai.chronon.online.fetcher.{FetchContext, JoinPartFetcher, MetadataStore}
import ai.chronon.online.KVStore
import ai.chronon.online.metrics.TTLCache
import org.mockito.{Answers, Mockito}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.{Answer, Stubber}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.collection.JavaConverters._
import scala.util.{Success, Try}

/** Tests that fetchJoins deduplicates GroupBy requests with identical (name, keys) across a bulk batch,
  * avoiding redundant KV lookups for contextual (e.g. user-level) GroupBys shared across all product rows.
  */
// Inline doReturn fix for Java/Scala interop (avoids ambiguous overload errors)
trait LocalMockitoHelper extends MockitoSugar {
  def doReturn(toBeReturned: Any): Stubber = Mockito.doReturn(toBeReturned, Nil: _*)
}

class JoinPartFetcherDeduplicationTest
    extends AnyFlatSpec
    with MockitoSugar
    with Matchers
    with LocalMockitoHelper
    with BeforeAndAfter {

  var joinPartFetcher: JoinPartFetcher = _
  var kvStore: KVStore = _
  var fetchContext: FetchContext = _
  var metadataStore: MetadataStore = _

  // A join with two GroupBys: one contextual (keyed on user_id) and one product-level (keyed on product_id).
  val contextualGroupBy = Builders.GroupBy(
    metaData = Builders.MetaData(name = "unit_test.contextual_user_features"),
    keyColumns = Seq("user_id"),
    accuracy = Accuracy.SNAPSHOT
  )

  val productGroupBy = Builders.GroupBy(
    metaData = Builders.MetaData(name = "unit_test.product_features"),
    keyColumns = Seq("product_id"),
    accuracy = Accuracy.SNAPSHOT
  )

  val testJoin = Builders.Join(
    metaData = Builders.MetaData(name = "unit_test.recsys_join"),
    joinParts = Seq(
      Builders.JoinPart(groupBy = contextualGroupBy, keyMapping = Map("user_id" -> "user_id")),
      Builders.JoinPart(groupBy = productGroupBy, keyMapping = Map("product_id" -> "product_id"))
    )
  )

  before {
    kvStore = mock[KVStore](Answers.RETURNS_DEEP_STUBS)
    when(kvStore.executionContext).thenReturn(ExecutionContext.global)
    fetchContext = FetchContext(kvStore)
    metadataStore = spy[MetadataStore](new MetadataStore(fetchContext))
    joinPartFetcher = spy[JoinPartFetcher](new JoinPartFetcher(fetchContext, metadataStore))
  }

  // Stub fetchGroupBys on the joinPartFetcher spy to echo back requests as successful empty responses.
  private def stubFetchGroupBys(featureValue: AnyRef = "feature_val"): Unit = {
    doAnswer(new Answer[Future[Seq[Response]]] {
      def answer(invocation: InvocationOnMock): Future[Seq[Response]] = {
        val requests = invocation.getArgument(0).asInstanceOf[Seq[Request]]
        Future.successful(requests.map(r => Response(r, Success(Map("feature" -> featureValue)))))
      }
    }).when(joinPartFetcher).fetchGroupBys(any())
  }

  it should "not dedup when all join requests have distinct keys (baseline)" in {
    stubFetchGroupBys()

    // 3 requests, each with a distinct user_id AND product_id — no duplication expected
    val joinRequests = Seq(
      Request(testJoin.metaData.name, Map("user_id" -> "u1".asInstanceOf[AnyRef], "product_id" -> "p1".asInstanceOf[AnyRef])),
      Request(testJoin.metaData.name, Map("user_id" -> "u2".asInstanceOf[AnyRef], "product_id" -> "p2".asInstanceOf[AnyRef])),
      Request(testJoin.metaData.name, Map("user_id" -> "u3".asInstanceOf[AnyRef], "product_id" -> "p3".asInstanceOf[AnyRef]))
    )

    val responses = Await.result(joinPartFetcher.fetchJoins(joinRequests, Some(testJoin)), 1.second)

    responses.length shouldBe 3
    responses.foreach(r => r.values.isSuccess shouldBe true)

    // 3 unique contextual + 3 unique product = 6 requests total
    val captor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(joinPartFetcher, times(1)).fetchGroupBys(captor.capture().asInstanceOf[Seq[Request]])
    captor.getValue.asInstanceOf[Seq[Request]].length shouldBe 6
  }

  it should "resolve one join configuration per name for a request batch" in {
    stubFetchGroupBys()
    val joinConfCache = mock[TTLCache[String, Try[JoinOps]]]
    when(joinConfCache.apply(testJoin.metaData.name)).thenReturn(Success(new JoinOps(testJoin)))
    doReturn(joinConfCache).when(metadataStore).getJoinConf
    val joinRequests = (1 to 5).map { i =>
      Request(
        testJoin.metaData.name,
        Map("user_id" -> "u42".asInstanceOf[AnyRef], "product_id" -> s"p$i".asInstanceOf[AnyRef])
      )
    }

    val responses = Await.result(joinPartFetcher.fetchJoins(joinRequests), 1.second)

    responses should have size 5
    verify(joinConfCache, times(1)).apply(testJoin.metaData.name)
  }

  it should "reuse the correct join and part metric contexts in a mixed-join batch" in {
    stubFetchGroupBys()
    val secondGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "unit_test.second_product_features"),
      keyColumns = Seq("product_id"),
      accuracy = Accuracy.SNAPSHOT
    )
    val secondJoin = Builders.Join(
      metaData = Builders.MetaData(name = "unit_test.second_recsys_join"),
      joinParts = Seq(Builders.JoinPart(groupBy = secondGroupBy, keyMapping = Map("product_id" -> "product_id")))
    )
    val joinConfCache = mock[TTLCache[String, Try[JoinOps]]]
    when(joinConfCache.apply(testJoin.metaData.name)).thenReturn(Success(new JoinOps(testJoin)))
    when(joinConfCache.apply(secondJoin.metaData.name)).thenReturn(Success(new JoinOps(secondJoin)))
    doReturn(joinConfCache).when(metadataStore).getJoinConf

    val requests = Seq(
      Request(testJoin.metaData.name,
              Map("user_id" -> "u1".asInstanceOf[AnyRef], "product_id" -> "p1".asInstanceOf[AnyRef])),
      Request(testJoin.metaData.name,
              Map("user_id" -> "u1".asInstanceOf[AnyRef], "product_id" -> "p2".asInstanceOf[AnyRef])),
      Request(secondJoin.metaData.name, Map("product_id" -> "p3".asInstanceOf[AnyRef])),
      Request(secondJoin.metaData.name, Map("product_id" -> "p4".asInstanceOf[AnyRef]))
    )

    val responses = Await.result(joinPartFetcher.fetchJoins(requests), 1.second)
    val firstJoinContexts = responses.take(2).map(_.request.context.get)
    val secondJoinContexts = responses.drop(2).map(_.request.context.get)
    (firstJoinContexts.head eq firstJoinContexts.last) shouldBe true
    (secondJoinContexts.head eq secondJoinContexts.last) shouldBe true
    (firstJoinContexts.head eq secondJoinContexts.head) shouldBe false
    firstJoinContexts.head.join shouldBe "unit_test_recsys_join"
    secondJoinContexts.head.join shouldBe "unit_test_second_recsys_join"

    val captor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(joinPartFetcher).fetchGroupBys(captor.capture().asInstanceOf[Seq[Request]])
    val sentRequests = captor.getValue.asInstanceOf[Seq[Request]]
    val firstProductContexts = sentRequests.filter(_.name == productGroupBy.metaData.name).map(_.context.get)
    val secondProductContexts = sentRequests.filter(_.name == secondGroupBy.metaData.name).map(_.context.get)
    firstProductContexts should have size 2
    secondProductContexts should have size 2
    firstProductContexts.foreach(context => (context eq firstProductContexts.head) shouldBe true)
    secondProductContexts.foreach(context => (context eq secondProductContexts.head) shouldBe true)
    firstProductContexts.head.join shouldBe "unit_test_recsys_join"
    firstProductContexts.head.groupBy shouldBe "unit_test_product_features"
    firstProductContexts.head.accuracy shouldBe Accuracy.SNAPSHOT
    secondProductContexts.head.join shouldBe "unit_test_second_recsys_join"
    secondProductContexts.head.groupBy shouldBe "unit_test_second_product_features"
    secondProductContexts.head.accuracy shouldBe Accuracy.SNAPSHOT
  }

  it should "reuse prepared metric contexts until the underlying join configuration is refreshed" in {
    stubFetchGroupBys()
    val refreshedProductGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "unit_test.refreshed_product_features"),
      keyColumns = Seq("product_id"),
      accuracy = Accuracy.TEMPORAL
    )
    val addedProductGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "unit_test.added_product_features"),
      keyColumns = Seq("product_id"),
      accuracy = Accuracy.SNAPSHOT
    )
    val refreshedJoin = Builders.Join(
      metaData = Builders.MetaData(name = testJoin.metaData.name, production = true, team = "refreshed_team"),
      joinParts = Seq(
        Builders.JoinPart(groupBy = contextualGroupBy, keyMapping = Map("user_id" -> "user_id")),
        Builders.JoinPart(
          groupBy = refreshedProductGroupBy,
          keyMapping = Map("product_id" -> "product_id"),
          prefix = "refreshed"
        ),
        Builders.JoinPart(groupBy = addedProductGroupBy, keyMapping = Map("product_id" -> "product_id"))
      )
    )
    val joinConfCache = mock[TTLCache[String, Try[JoinOps]]]
    when(joinConfCache.apply(testJoin.metaData.name))
      .thenReturn(
        Success(new JoinOps(testJoin)),
        Success(new JoinOps(testJoin)),
        Success(new JoinOps(refreshedJoin))
      )
    doReturn(joinConfCache).when(metadataStore).getJoinConf
    val request = Request(
      testJoin.metaData.name,
      Map("user_id" -> "u1".asInstanceOf[AnyRef], "product_id" -> "p1".asInstanceOf[AnyRef])
    )

    val responseContexts = (0 until 3).map { _ =>
      Await.result(joinPartFetcher.fetchJoins(Seq(request)), 1.second).head.request.context.get
    }
    (responseContexts(0) eq responseContexts(1)) shouldBe true
    (responseContexts(1) eq responseContexts(2)) shouldBe false
    responseContexts(2).join shouldBe "unit_test_recsys_join"
    responseContexts(2).production shouldBe true
    responseContexts(2).team shouldBe "refreshed_team"

    val captor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(joinPartFetcher, times(3)).fetchGroupBys(captor.capture().asInstanceOf[Seq[Request]])
    val capturedRequests = captor.getAllValues.asScala.map(_.asInstanceOf[Seq[Request]])
    val originalPartContexts = capturedRequests.take(2).map { captured =>
      captured
        .find(_.name == productGroupBy.metaData.name)
        .flatMap(_.context)
        .get
    }
    (originalPartContexts(0) eq originalPartContexts(1)) shouldBe true

    capturedRequests(2).map(_.name).toSet shouldBe Set(
      contextualGroupBy.metaData.name,
      refreshedProductGroupBy.metaData.name,
      addedProductGroupBy.metaData.name
    )
    val refreshedPartContext = capturedRequests(2)
      .find(_.name == refreshedProductGroupBy.metaData.name)
      .flatMap(_.context)
      .get
    (originalPartContexts(1) eq refreshedPartContext) shouldBe false
    refreshedPartContext.groupBy shouldBe "unit_test_refreshed_product_features"
    refreshedPartContext.accuracy shouldBe Accuracy.TEMPORAL
    refreshedPartContext.joinPartPrefix shouldBe "refreshed"
    refreshedPartContext.production shouldBe true
    refreshedPartContext.team shouldBe "refreshed_team"
  }

  it should "reuse prepared metric contexts across repeated join overrides" in {
    stubFetchGroupBys()
    val request = Request(
      testJoin.metaData.name,
      Map("user_id" -> "u1".asInstanceOf[AnyRef], "product_id" -> "p1".asInstanceOf[AnyRef])
    )

    val responseContexts = (0 until 2).map { _ =>
      Await.result(joinPartFetcher.fetchJoins(Seq(request), Some(testJoin)), 1.second).head.request.context.get
    }
    (responseContexts(0) eq responseContexts(1)) shouldBe true

    val captor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(joinPartFetcher, times(2)).fetchGroupBys(captor.capture().asInstanceOf[Seq[Request]])
    val productPartContexts = captor.getAllValues.asScala.map { captured =>
      captured
        .asInstanceOf[Seq[Request]]
        .find(_.name == productGroupBy.metaData.name)
        .flatMap(_.context)
        .get
    }
    (productPartContexts(0) eq productPartContexts(1)) shouldBe true
  }

  it should "dedup contextual GroupBy requests when user_id is repeated across bulk product rows" in {
    stubFetchGroupBys()

    // 5 join requests, all sharing the same user_id (contextual) but distinct product_ids
    val sharedUserId = "u42".asInstanceOf[AnyRef]
    val joinRequests = (1 to 5).map { i =>
      Request(
        testJoin.metaData.name,
        Map("user_id" -> sharedUserId, "product_id" -> s"p$i".asInstanceOf[AnyRef])
      )
    }

    val responses = Await.result(joinPartFetcher.fetchJoins(joinRequests, Some(testJoin)), 1.second)

    responses.length shouldBe 5
    // All responses should succeed and contain contextual features
    responses.foreach { r =>
      r.values.isSuccess shouldBe true
      r.values.get should contain key "unit_test_contextual_user_features_feature"
    }

    // 1 deduplicated contextual request + 5 distinct product requests = 6 total
    val captor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(joinPartFetcher, times(1)).fetchGroupBys(captor.capture().asInstanceOf[Seq[Request]])
    val sentRequests = captor.getValue.asInstanceOf[Seq[Request]]
    sentRequests.length shouldBe 6

    val contextualRequests = sentRequests.filter(_.name == contextualGroupBy.metaData.name)
    contextualRequests.length shouldBe 1
    contextualRequests.head.keys shouldBe Map("user_id" -> sharedUserId)
  }

  it should "dedup contextual requests with mixed-type keys regardless of insertion order" in {
    stubFetchGroupBys()

    // Contextual GB has a composite key with mixed types: Long, String, Double
    val multiKeyContextualGroupBy = Builders.GroupBy(
      metaData = Builders.MetaData(name = "unit_test.multi_key_contextual_features"),
      keyColumns = Seq("user_id", "country", "score"),
      accuracy = Accuracy.SNAPSHOT
    )
    val multiKeyJoin = Builders.Join(
      metaData = Builders.MetaData(name = "unit_test.multi_key_recsys_join"),
      joinParts = Seq(
        Builders.JoinPart(
          groupBy = multiKeyContextualGroupBy,
          keyMapping = Map("user_id" -> "user_id", "country" -> "country", "score" -> "score")
        ),
        Builders.JoinPart(groupBy = productGroupBy, keyMapping = Map("product_id" -> "product_id"))
      )
    )

    // Two sets of requests: same logical contextual keys but constructed in different insertion orders
    val userId: AnyRef = 99L.asInstanceOf[AnyRef]
    val country: AnyRef = "US".asInstanceOf[AnyRef]
    val score: AnyRef = 3.14.asInstanceOf[AnyRef]

    val joinRequests = Seq(
      // insertion order: user_id, country, score
      Request(
        multiKeyJoin.metaData.name,
        Map("user_id" -> userId, "country" -> country, "score" -> score, "product_id" -> "p1".asInstanceOf[AnyRef])
      ),
      // insertion order: score, user_id, country
      Request(
        multiKeyJoin.metaData.name,
        Map("score" -> score, "user_id" -> userId, "country" -> country, "product_id" -> "p2".asInstanceOf[AnyRef])
      ),
      // insertion order: country, score, user_id
      Request(
        multiKeyJoin.metaData.name,
        Map("country" -> country, "score" -> score, "user_id" -> userId, "product_id" -> "p3".asInstanceOf[AnyRef])
      )
    )

    val responses = Await.result(joinPartFetcher.fetchJoins(joinRequests, Some(multiKeyJoin)), 1.second)

    responses.length shouldBe 3
    responses.foreach { r =>
      r.values.isSuccess shouldBe true
      r.values.get should contain key "unit_test_multi_key_contextual_features_feature"
    }

    // 1 deduplicated contextual request (Map equality is order-independent) + 3 product requests = 4
    val captor = ArgumentCaptor.forClass(classOf[Seq[_]])
    verify(joinPartFetcher, times(1)).fetchGroupBys(captor.capture().asInstanceOf[Seq[Request]])
    val sentRequests = captor.getValue.asInstanceOf[Seq[Request]]
    sentRequests.length shouldBe 4

    val contextualRequests = sentRequests.filter(_.name == multiKeyContextualGroupBy.metaData.name)
    contextualRequests.length shouldBe 1
    contextualRequests.head.keys shouldBe Map("user_id" -> userId, "country" -> country, "score" -> score)
  }
}
