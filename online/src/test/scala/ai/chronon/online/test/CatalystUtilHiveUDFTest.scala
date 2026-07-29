package ai.chronon.online.test

import ai.chronon.online.{CatalystUtil, PooledCatalystUtil}
import org.junit.Assert.assertEquals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}

// Hive UDFs (subclass of org.apache.hadoop.hive.ql.exec.UDF) defined in ExampleUDFs.scala.
// These are on the test classpath so no ADD JAR is needed.
class CatalystUtilHiveUDFTest extends AnyFlatSpec with CatalystUtilTestSparkSQLStructs with Matchers {

  private val udfSetups = Seq(
    "CREATE FUNCTION MINUS_TWO AS 'ai.chronon.online.test.Minus_Two'",
    "CREATE FUNCTION CAT_STR AS 'ai.chronon.online.test.Cat_Str'"
  )

  private val udfSelects = Seq(
    "a" -> "MINUS_TWO(int32_x)",
    "b" -> "CAT_STR(string_x)"
  )

  "catalyst util" should "register Hive UDFs via CREATE FUNCTION and evaluate them" in {
    val cu = new CatalystUtil(CommonScalarsStruct, selects = udfSelects, setups = udfSetups)
    val resList = cu.performSql(CommonScalarsRow)
    assertEquals(resList.size, 1)
    val resMap = resList.head
    assertEquals(resMap.size, 2)
    assertEquals(resMap("a"), Int.MaxValue - 2)
    assertEquals(resMap("b"), "hello123")
  }

  it should "share one blueprint across multiple CatalystUtil instances with the same setups" in {
    val first = new CatalystUtil(CommonScalarsStruct, selects = udfSelects, setups = udfSetups)
    val second = new CatalystUtil(CommonScalarsStruct, selects = udfSelects, setups = udfSetups)

    // same blueprint object — setups ran exactly once across both instances
    first.outputChrononSchema should be theSameInstanceAs second.outputChrononSchema
    first.getOutputSparkSchema should be theSameInstanceAs second.getOutputSparkSchema
  }

  it should "produce distinct blueprints for different setup lists" in {
    val withUdf = CatalystUtil.blueprintOf(CommonScalarsStruct, udfSelects, Seq.empty, udfSetups, Set.empty)
    val noSetup = CatalystUtil.blueprintOf(CommonScalarsStruct, Seq("v" -> "int32_x"), Seq.empty, Seq.empty, Set.empty)
    withUdf should not be theSameInstanceAs(noSetup)
  }

  it should "evaluate UDF expressions correctly under concurrent load via PooledCatalystUtil" in {
    val pooled = new PooledCatalystUtil(udfSelects, CommonScalarsStruct, setups = udfSetups)

    val expected = pooled.performSql(CommonScalarsRow).head
    expected("a") shouldBe (Int.MaxValue - 2)
    expected("b") shouldBe "hello123"

    val threads = 16
    val failures = new ConcurrentLinkedQueue[String]()
    val executor = Executors.newFixedThreadPool(threads)
    val ready = new CountDownLatch(threads)
    val go = new CountDownLatch(1)
    val done = new CountDownLatch(threads)

    (0 until threads).foreach { t =>
      executor.submit(new Runnable {
        override def run(): Unit = {
          ready.countDown()
          go.await()
          try {
            (0 until 200).foreach { _ =>
              val res = pooled.performSql(CommonScalarsRow).head
              if (res("a") != expected("a"))
                failures.add(s"thread $t: a=${res("a")} want ${expected("a")}")
              if (res("b") != expected("b"))
                failures.add(s"thread $t: b=${res("b")} want ${expected("b")}")
            }
          } catch {
            case e: Throwable => failures.add(s"thread $t threw: $e")
          } finally {
            done.countDown()
          }
        }
      })
    }

    ready.await()
    go.countDown()
    val finished = done.await(2, TimeUnit.MINUTES)
    executor.shutdownNow()

    finished shouldBe true
    withClue(s"${failures.size} failures, first few: ${failures.toArray.take(5).mkString("; ")} ") {
      failures shouldBe empty
    }
  }
}
