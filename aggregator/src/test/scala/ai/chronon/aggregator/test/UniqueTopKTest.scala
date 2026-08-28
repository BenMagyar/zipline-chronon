package ai.chronon.aggregator.test

import ai.chronon.aggregator.base.UniqueTopKAggregator
import ai.chronon.api._
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import scala.collection.JavaConverters._
import scala.util.Random

class UniqueTopKAggregatorTest extends AnyFlatSpec {

  private val uniqueTopKStructType = StructType("TestStruct",
                                                Array(
                                                  StructField("sort_key", StringType),
                                                  StructField("unique_id", LongType),
                                                  StructField("value", IntType)
                                                ))

  private def structResult(aggregator: UniqueTopKAggregator[Array[Any]], ir: Any): List[(String, Long, Int)] =
    aggregator
      .finalize(ir)
      .asScala
      .map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[Long], row(2).asInstanceOf[Int]))
      .toList

  private def aggregateStructs(inputs: Seq[Array[Any]], k: Int): List[(String, Long, Int)] = {
    val aggregator = new UniqueTopKAggregator[Array[Any]](uniqueTopKStructType, k)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))
    structResult(aggregator, ir)
  }

  "UniqueTopKAggregator with IntType" should "return top k unique integers" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val inputs = List(5, 3, 8, 3, 1, 9, 5, 2)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals(List(9, 8, 5), resultList)
  }

  "UniqueTopKAggregator with IntType" should "handle duplicates correctly" in {
    val k = 5
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val inputs = List(1, 1, 1, 2, 2, 3, 3, 3, 3)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(3, result.size()) // Only 3 unique values
    assertEquals(List(3, 2, 1), resultList)
  }

  "UniqueTopKAggregator with LongType" should "return top k unique longs" in {
    val k = 2
    val aggregator = new UniqueTopKAggregator[Long](LongType, k)

    val inputs = List(100L, 200L, 50L, 300L, 100L)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals(List(300L, 200L), resultList)
  }

  "UniqueTopKAggregator with StringType" should "return top k unique strings" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[String](StringType, k)

    val inputs = List("apple", "banana", "cherry", "apple", "date", "banana")
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals(List("date", "cherry", "banana"), resultList)
  }

  "UniqueTopKAggregator with StructType" should "return top k unique structs" in {
    val k = 2
    val aggregator = new UniqueTopKAggregator[Array[Any]](uniqueTopKStructType, k)

    val inputs = List(
      Array("z", 1L, 10),
      Array("y", 2L, 20),
      Array("x", 3L, 30),
      Array("x", 1L, 40), // Lower-ranked duplicate unique_id
      Array("w", 4L, 50)
    )

    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals("z", resultList(0)(0)) // Top by sort_key
    assertEquals("y", resultList(1)(0)) // Second by sort_key
  }

  it should "retain the maximum sort key for each unique id regardless of input order" in {
    val lowerRankedDuplicate = Array[Any]("a", 1L, 10)
    val higherRankedDuplicate = Array[Any]("c", 1L, 30)
    val other = Array[Any]("b", 2L, 20)

    val lowerFirst = aggregateStructs(Seq(lowerRankedDuplicate, other, higherRankedDuplicate), k = 2)
    val higherFirst = aggregateStructs(Seq(higherRankedDuplicate, other, lowerRankedDuplicate), k = 2)

    val expected = List(("c", 1L, 30), ("b", 2L, 20))
    assertEquals(expected, lowerFirst)
    assertEquals(expected, higherFirst)
  }

  it should "retain the maximum sort key for each unique id regardless of merge order" in {
    def states() = {
      val aggregator = new UniqueTopKAggregator[Array[Any]](uniqueTopKStructType, k = 3)

      val left = aggregator.prepare(Array[Any]("a", 1L, 10))
      aggregator.update(left, Array[Any]("b", 2L, 20))

      val right = aggregator.prepare(Array[Any]("d", 1L, 40))
      aggregator.update(right, Array[Any]("c", 3L, 30))

      (aggregator, left, right)
    }

    def merge(leftFirst: Boolean): List[(String, Long, Int)] = {
      val (aggregator, left, right) = states()
      val merged = if (leftFirst) aggregator.merge(left, right) else aggregator.merge(right, left)
      structResult(aggregator, merged)
    }

    val expected = List(("d", 1L, 40), ("c", 3L, 30), ("b", 2L, 20))
    assertEquals(expected, merge(leftFirst = true))
    assertEquals(expected, merge(leftFirst = false))
  }

  it should "use unique id to deterministically break equal sort key ties" in {
    val inputs = Seq(
      Array[Any]("a", 3L, 30),
      Array[Any]("a", 2L, 20),
      Array[Any]("a", 1L, 10)
    )

    val expected = List(("a", 1L, 10), ("a", 2L, 20))
    assertEquals(expected, aggregateStructs(inputs, k = 2))
    assertEquals(expected, aggregateStructs(inputs.reverse, k = 2))
  }

  it should "reject conflicting payloads for the same unique id and sort key" in {
    val aggregator = new UniqueTopKAggregator[Array[Any]](uniqueTopKStructType, k = 2)
    val ir = aggregator.prepare(Array[Any]("a", 1L, 10))

    val exception = intercept[IllegalArgumentException] {
      aggregator.update(ir, Array[Any]("a", 1L, 20))
    }

    assertTrue(exception.getMessage.contains("unique_id=1"))
    assertTrue(exception.getMessage.contains("sort_key=a"))
  }

  it should "match an exact top k oracle across pruning and merge orders" in {
    val k = 5
    val inputs = (0 until 120).map { index =>
      val uniqueId = (index % 17).toLong
      val score = (index * 37) % 20
      Array[Any](f"$score%02d", uniqueId, index)
    }
    val expected = inputs
      .groupBy(row => row(1).asInstanceOf[Long])
      .values
      .map(_.maxBy(row => row(0).asInstanceOf[String]))
      .toSeq
      .sortWith { (left, right) =>
        val leftSortKey = left(0).asInstanceOf[String]
        val rightSortKey = right(0).asInstanceOf[String]
        leftSortKey > rightSortKey ||
        (leftSortKey == rightSortKey && left(1).asInstanceOf[Long] < right(1).asInstanceOf[Long])
      }
      .take(k)
      .map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[Long], row(2).asInstanceOf[Int]))
      .toList

    (0 until 5).foreach { seed =>
      val aggregator = new UniqueTopKAggregator[Array[Any]](uniqueTopKStructType, k)
      val partialStates = new Random(seed)
        .shuffle(inputs)
        .grouped(7)
        .map { group =>
          val state = aggregator.prepare(group.head)
          group.tail.foreach(row => aggregator.update(state, row))
          state
        }
        .toSeq
      val merged = new Random(seed + 100).shuffle(partialStates).reduce(aggregator.merge)

      assertEquals(s"seed=$seed", expected, structResult(aggregator, merged))
    }
  }

  "UniqueTopKAggregator with StructType" should "validate struct field requirements" in {
    val invalidStructType = StructType("InvalidStruct",
                                       Array(
                                         StructField("wrong_field", StringType),
                                         StructField("unique_id", LongType)
                                       ))

    assertThrows[IllegalArgumentException] {
      new UniqueTopKAggregator[Array[Any]](invalidStructType, 3)
    }
  }

  "UniqueTopKAggregator" should "handle merge operations" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    // First batch
    val ir1 = aggregator.prepare(5)
    aggregator.update(ir1, 3)
    aggregator.update(ir1, 8)

    // Second batch
    val ir2 = aggregator.prepare(7)
    aggregator.update(ir2, 1)
    aggregator.update(ir2, 5) // Duplicate

    val merged = aggregator.merge(ir1, ir2)
    val result = aggregator.finalize(merged)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())
    assertEquals(List(8, 7, 5), resultList)
  }

  "UniqueTopKAggregator" should "handle normalization round-trip" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[String](StringType, k)

    val inputs = List("zebra", "apple", "banana", "cherry")
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val originalResult = aggregator.finalize(aggregator.clone(ir))

    val normalized = aggregator.normalize(ir)
    val denormalized = aggregator.denormalize(normalized)
    val roundTripResult = aggregator.finalize(denormalized)

    assertEquals(originalResult, roundTripResult)
  }

  "UniqueTopKAggregator" should "handle empty input gracefully" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val ir = aggregator.prepare(42)
    val result = aggregator.finalize(ir)

    assertEquals(1, result.size())
    assertEquals(42, result.get(0))
  }

  "UniqueTopKAggregator" should "respect k limit" in {
    val k = 2
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val inputs = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val result = aggregator.finalize(ir)

    assertEquals(k, result.size())
    assertEquals(List(10, 9), result.asScala.toList)
  }

  "UniqueTopKAggregator" should "handle clone operation correctly" in {
    val k = 3
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    val inputs = List(5, 3, 8, 1)
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    val cloned = aggregator.clone(ir)
    val originalResult = aggregator.finalize(ir)
    val clonedResult = aggregator.finalize(cloned)

    assertEquals(originalResult, clonedResult)

    // Verify they are independent
    aggregator.update(ir, 10)
    val newOriginalResult = aggregator.finalize(ir)
    val unchangedClonedResult = aggregator.finalize(cloned)

    assertNotEquals(newOriginalResult, unchangedClonedResult)
  }

  "UniqueTopKAggregator" should "handle maxSize limit and merge groups correctly" in {
    val k = 5
    val aggregator = new UniqueTopKAggregator[Int](IntType, k) // maxSize = 2*k = 10

    // Create 10 groups of 10 random numbers between 1-100
    val random = new Random(42) // Fixed seed for reproducible test
    val groups = (1 to 10).map { _ =>
      (1 to 10).map(_ => random.nextInt(100) + 1).toList
    }

    val allNums = groups.flatten
    val top5 = allNums.distinct.sorted(Ordering[Int].reverse).take(k).toList

    // Create intermediate results for each group
    val intermediateResults = groups.map { group =>
      val ir = aggregator.prepare(group.head)
      group.tail.foreach(num => aggregator.update(ir, num))
      ir
    }

    // Merge all groups into one state
    val finalState = intermediateResults.reduce((ir1, ir2) => aggregator.merge(ir1, ir2))

    val result = aggregator.finalize(finalState)
    val resultList = result.asScala.toList

    assertEquals(k, result.size())

    // The result should contain the top 5 values (96-100)
    val expected = top5
    assertEquals(expected, resultList)

    // Verify all results are >= 96
    resultList.foreach { value =>
      assertTrue(s"Value $value should be >= 96", value >= 96)
    }
  }

  "UniqueTopKAggregator" should "respect maxSize constraint during operations" in {
    val k = 3
    val maxSize = 2 * k // Default maxSize
    val aggregator = new UniqueTopKAggregator[Int](IntType, k)

    // Add more than maxSize unique elements
    val inputs = (1 to 20).toList
    val ir = aggregator.prepare(inputs.head)
    inputs.tail.foreach(input => aggregator.update(ir, input))

    // The internal state should be limited by maxSize during processing
    // but final result should still be k elements
    val result = aggregator.finalize(ir)

    assertEquals(k, result.size())
    assertEquals(List(20, 19, 18), result.asScala.toList)
  }
}
