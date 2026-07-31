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

package ai.chronon.online.metrics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

class MetricsContextMemoizationTest extends AnyFlatSpec with Matchers {

  behavior of "Metrics.Context hot-path memoization"

  it should "reuse suffix contexts and metric names under concurrent access" in {
    val context = Metrics.Context(Metrics.Environment.JoinFetching, join = "team.join")

    val suffixContexts = concurrently(128)(context.withSuffix("batch"))
    val suffixContext = suffixContexts.head
    suffixContexts.foreach(candidate => (candidate eq suffixContext) shouldBe true)
    suffixContext.suffix shouldBe "batch"
    context.memoizedSuffixCount shouldBe 1

    val metricNames = concurrently(128)(suffixContext.prefixedMetricName(Metrics.Name.RowCount))
    val metricName = metricNames.head
    metricNames.foreach(candidate => (candidate eq metricName) shouldBe true)
    metricName should endWith("join.fetch.batch.row.count")
    suffixContext.memoizedMetricNameCount shouldBe 1

    val responseContext = suffixContext.withSuffix("response")
    responseContext.suffix shouldBe "batch.response"
    responseContext.prefixedMetricName(Metrics.Name.Bytes) should endWith("join.fetch.batch.response.bytes")
  }

  it should "keep each per-context memo strictly bounded" in {
    val context = Metrics.Context(Metrics.Environment.GroupByFetching, groupBy = "team.group_by")

    (0 until Metrics.ContextSuffixMemoMaxEntries + 20).foreach(index => context.withSuffix(s"suffix-$index"))
    context.memoizedSuffixCount shouldBe Metrics.ContextSuffixMemoMaxEntries

    (0 until Metrics.ContextMetricNameMemoMaxEntries + 20).foreach(index =>
      context.prefixedMetricName(s"metric-$index"))
    context.memoizedMetricNameCount shouldBe Metrics.ContextMetricNameMemoMaxEntries
    context.prefixedMetricName("uncached-overflow") should endWith("group_by.fetch.uncached-overflow")
  }

  private def concurrently[A](count: Int)(body: => A): Seq[A] = {
    val executor = Executors.newFixedThreadPool(16)
    val start = new CountDownLatch(1)
    try {
      val futures = (0 until count).map { _ =>
        executor.submit(new Callable[A] {
          override def call(): A = {
            start.await()
            body
          }
        })
      }
      start.countDown()
      futures.map(_.get(10, TimeUnit.SECONDS))
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    }
  }
}
