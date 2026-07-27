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

import ai.chronon.api.{Accuracy, Builders}
import ai.chronon.online.fetcher.Fetcher.{Request, Response}
import ai.chronon.online.fetcher.{FetchContext, JoinPartFetcher, MetadataStore}
import ai.chronon.online.KVStore
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
import scala.util.Success

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
