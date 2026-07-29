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

import ai.chronon.online.FetcherUtil
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.metrics.Metrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

class FeatureMetricsAccumulatorTest extends AnyFlatSpec with Matchers {

  it should "aggregate additive feature counters by context and feature" in {
    type Emission = (Metrics.Context, String, Long, Map[String, String])
    val emissions = ArrayBuffer.empty[Emission]
    val accumulator = new Fetcher.FeatureMetricsAccumulator(
      (context, metric, value, tags) => emissions.append((context, metric, value, tags)))

    val firstContext = Metrics.Context(Metrics.Environment.JoinFetching, join = "team.join")
    val equalContext = firstContext.copy()
    val otherContext = firstContext.copy(join = "team.other_join")

    accumulator.add(
      Map(
        "feature_a" -> null,
        "feature_b" -> java.lang.Long.valueOf(1L),
        s"failed${FetcherUtil.FeatureExceptionSuffix}" -> "ignored"
      ),
      firstContext
    )
    accumulator.add(
      Map("feature_a" -> java.lang.Long.valueOf(2L), "feature_b" -> null),
      equalContext
    )
    accumulator.add(Map("feature_a" -> java.lang.Long.valueOf(3L)), otherContext)

    accumulator.flush()

    val featureTag: String => Map[String, String] = name => Map(Metrics.Tag.Feature -> name)
    emissions.toSet shouldBe Set[Emission](
      (firstContext, Metrics.Name.FeatureNulls, 1L, featureTag("feature_a")),
      (firstContext, Metrics.Name.FeatureCount, 2L, featureTag("feature_a")),
      (firstContext, Metrics.Name.FeatureNulls, 1L, featureTag("feature_b")),
      (firstContext, Metrics.Name.FeatureCount, 2L, featureTag("feature_b")),
      (otherContext, Metrics.Name.FeatureCount, 1L, featureTag("feature_a"))
    )

    accumulator.flush()
    emissions should have size 5
  }
}
