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

package ai.chronon.api.test

import ai.chronon.api.Extensions._
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Accuracy, Builders, ConfigProperties, Constants, ExecutionInfo, GroupBy}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.mockito.Mockito.{spy, when}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.Arrays

class ExtensionsTest extends AnyFlatSpec {

  it should "sub partition filters" in {
    val source = Builders.Source.events(query = null, table = "db.table/system=mobile/currency=USD")
    assertEquals(
      Map("system" -> "mobile", "currency" -> "USD"),
      source.subPartitionFilters
    )
  }

  it should "part skew filter should return none when no skew key" in {
    val joinPart = Builders.JoinPart()
    val join = Builders.Join(joinParts = Seq(joinPart))
    assertTrue(join.partSkewFilter(joinPart).isEmpty)
  }

  it should "part skew filter should return correctly with skew keys" in {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("a", "c"), metaData = groupByMetadata)
    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))
    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("a NOT IN (b) OR c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  it should "part skew filter should return correctly with partial skew keys" in {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("c"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  it should "part skew filter should return correctly with skew keys with mapping" in {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("x", "c"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy, keyMapping = Map("a" -> "x"))
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).nonEmpty)
    assertEquals("x NOT IN (b) OR c NOT IN (d)", join.partSkewFilter(joinPart).get)
  }

  it should "part skew filter should return none if join part has no related keys" in {
    val groupByMetadata = Builders.MetaData(name = "test")
    val groupBy = Builders.GroupBy(keyColumns = Seq("non_existent"), metaData = groupByMetadata)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val join = Builders.Join(joinParts = Seq(joinPart), skewKeys = Map("a" -> Seq("b"), "c" -> Seq("d")))

    assertTrue(join.partSkewFilter(joinPart).isEmpty)
  }

  // A GroupBy with no aggregations (aggregations left null - a supported "pass raw columns through" config,
  // same as what valueColumns already handles a few lines below aggregationInputs in Extensions.scala) should
  // not throw. Reproduces a NullPointerException seen in production warmup logs for exactly this shape of
  // GroupBy: "Cannot invoke java.util.List.iterator() because this.groupBy.aggregations is null".
  it should "compute aggregationInputs for a no-agg GroupBy instead of throwing on null aggregations" in {
    val source = Builders.Source.events(
      query = Builders.Query(selects = Map("user_id" -> "user_id", "amount" -> "amount", "currency" -> "currency")),
      table = "db.table"
    )
    val groupBy = Builders.GroupBy(sources = Seq(source), keyColumns = Seq("user_id"), metaData = Builders.MetaData(name = "test"))

    assertEquals(Set("amount", "currency"), groupBy.aggregationInputs.toSet)
  }

  it should "group by keys should contain partition column" in {
    val groupBy = spy[GroupBy](new GroupBy())
    val baseKeys = List("a", "b")
    val partitionColumn = "ds"
    groupBy.accuracy = Accuracy.SNAPSHOT
    groupBy.keyColumns = baseKeys.toJava
    when(groupBy.isSetKeyColumns).thenReturn(true)

    val keys = groupBy.keys(partitionColumn)
    assertTrue(baseKeys.forall(keys.contains(_)))
    assertTrue(keys.contains(partitionColumn))
    assertEquals(3, keys.size)
  }

  it should "group by keys should contain time column for temporal accuracy" in {
    val groupBy = spy[GroupBy](new GroupBy())
    val baseKeys = List("a", "b")
    val partitionColumn = "ds"
    groupBy.accuracy = Accuracy.TEMPORAL
    groupBy.keyColumns = baseKeys.toJava
    when(groupBy.isSetKeyColumns).thenReturn(true)

    val keys = groupBy.keys(partitionColumn)
    assertTrue(baseKeys.forall(keys.contains(_)))
    assertTrue(keys.contains(partitionColumn))
    assertTrue(keys.contains(Constants.TimeColumn))
    assertEquals(4, keys.size)
  }

}
