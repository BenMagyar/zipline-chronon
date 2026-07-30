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

import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.serde.SerDe
import ai.chronon.online.{
  Api,
  ExternalSourceRegistry,
  GroupByServingInfoParsed,
  InMemoryKvStore,
  KVStore,
  LoggableResponse
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.function.Consumer

class ResponseLoggingTest extends AnyFlatSpec with Matchers {

  private class TestApi(enabled: Boolean) extends Api(Map.empty) {
    private val store = InMemoryKvStore.build(s"ResponseLoggingTest_${System.nanoTime()}")
    var loggedResponses: Int = 0

    override protected def responseLoggingEnabled: Boolean = enabled
    override def streamDecoder(groupByServingInfoParsed: GroupByServingInfoParsed): SerDe = null
    override def genKvStore: KVStore = store
    override def genMetricsKvStore(tableBaseName: String): KVStore = store
    override def genEnhancedStatsKvStore(tableBaseName: String): KVStore = store
    override def externalRegistry: ExternalSourceRegistry = new ExternalSourceRegistry()
    override def logResponse(resp: LoggableResponse): Unit = loggedResponses += 1
  }

  private val noOpConsumer = new Consumer[LoggableResponse] {
    override def accept(response: LoggableResponse): Unit = ()
  }

  "Api response logging" should "install a consumer by default" in {
    val api = new TestApi(enabled = true)
    val response = LoggableResponse(Array.emptyByteArray, Array.emptyByteArray, "test.join", 0L, "hash")

    api.responseConsumerOrNull should not be null
    api.responseConsumerOrNull.accept(response)
    api.loggedResponses shouldBe 1
  }

  it should "omit the consumer when the API has no response-log sink" in {
    new TestApi(enabled = false).responseConsumerOrNull shouldBe null
  }

  "Fetcher response logging" should "skip encoding when neither a sink nor debug logging is enabled" in {
    Fetcher.shouldEncodeResponseLog(null, debug = false, samplePercent = 100.0) shouldBe false
  }

  it should "retain production sink and debug logging behavior" in {
    Fetcher.shouldEncodeResponseLog(noOpConsumer, debug = false, samplePercent = 100.0) shouldBe true
    Fetcher.shouldEncodeResponseLog(null, debug = true, samplePercent = 100.0) shouldBe true
    Fetcher.shouldEncodeResponseLog(noOpConsumer, debug = true, samplePercent = 0.0) shouldBe false
  }
}
