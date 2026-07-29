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

import ai.chronon.api.{Builders, IntType, StringType, StructField, StructType}
import ai.chronon.online.FetcherUtil
import ai.chronon.online.JoinCodec
import ai.chronon.online.OnlineDerivationUtil.{applyDeriveFunc, buildDerivationFunction, reintroduceExceptions}
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.serde.AvroCodec
import ai.chronon.online.serde.AvroConversions
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import ai.chronon.api.Extensions._

class JoinCodecTest extends AnyFlatSpec {
  it should "reintroduce exception" in {

    val preDerived = Map(s"group_by_2${FetcherUtil.FeatureExceptionSuffix}" -> "ex", s"group_by_1${FetcherUtil.FeatureExceptionSuffix}" -> "ex", s"group_by_4${FetcherUtil.FeatureExceptionSuffix}" -> "ex")
    val derived = Map(
      "group_by_1_feature1" -> "val1",
      "group_by_2_feature1" -> "val1",
      "group_by_2_feature2" -> "val2",
      "group_by_3_feature1" -> "val1",
      "derived1" -> "val1",
      "derived2" -> "val2"
    )

    val result = reintroduceExceptions(derived, preDerived)

    val expected = Map(
      "group_by_3_feature1" -> "val1",
      "derived1" -> "val1",
      "derived2" -> "val2",
      s"group_by_2${FetcherUtil.FeatureExceptionSuffix}" -> "ex",
      s"group_by_1${FetcherUtil.FeatureExceptionSuffix}" -> "ex",
      s"group_by_4${FetcherUtil.FeatureExceptionSuffix}" -> "ex"
    )
    assertEquals(expected, result)
  }

  // Verifies that setups on join.left.query are threaded through to the derivation
  // PooledCatalystUtil, so Hive UDFs registered in setups are available in Derivation expressions.
  // This mirrors JoinBase's offline behavior where joinConf.setups are run once before all SQL.
  it should "evaluate a Hive UDF registered via join setups in a Derivation" in {
    val keySchema = StructType("key", Array(StructField("user_id", StringType)))
    val baseValueSchema = StructType("value", Array(StructField("int_feature", IntType)))

    val setups = Seq("CREATE FUNCTION MINUS_TWO AS 'ai.chronon.online.test.Minus_Two'")

    val derivationsScala = List(
      Builders.Derivation(name = "int_feature_minus_2", expression = "MINUS_TWO(int_feature)"),
      Builders.Derivation(name = "*", expression = "*")
    )

    val deriveFunc = buildDerivationFunction(derivationsScala, keySchema, baseValueSchema, setups)

    val request = Request("test_join", Map("user_id" -> "u1"), atMillis = Some(System.currentTimeMillis()))
    val baseMap: Map[String, AnyRef] = Map("int_feature" -> java.lang.Integer.valueOf(10))

    val result = applyDeriveFunc(deriveFunc, request, baseMap)

    assertEquals(java.lang.Integer.valueOf(8), result("int_feature_minus_2"))
    assertEquals(java.lang.Integer.valueOf(10), result("int_feature"))
  }
}
