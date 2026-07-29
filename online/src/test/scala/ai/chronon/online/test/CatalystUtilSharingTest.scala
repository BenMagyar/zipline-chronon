package ai.chronon.online.test

import ai.chronon.api._
import ai.chronon.online.{CatalystUtil, PooledCatalystUtil}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}

/** CatalystUtil instances for one query share a blueprint: the physical plan, the compiled whole stage class
  * and the row conversion functions. These cover what that sharing must guarantee - that the plan is built
  * once, and that instances built from it cannot corrupt each other under concurrent load.
  */
class CatalystUtilSharingTest extends AnyFlatSpec with Matchers {

  private val schema: StructType = StructType(
    "SharingStruct",
    Array(
      StructField("user_id", StringType),
      StructField("event_ts", LongType),
      StructField("amount", DoubleType),
      StructField("quantity", IntType),
      StructField("currency", StringType),
      StructField("is_member", BooleanType),
      StructField("tags", ListType(StringType)),
      StructField("counters", MapType(StringType, LongType)),
      StructField("payload", StringType),
      StructField("nested", StructType("N", Array(StructField("score", DoubleType), StructField("bucket", StringType))))
    )
  )

  /** Deliberately spans the shapes that put shared state behind a CatalystUtil:
    *   - get_json_object is a CodegenFallback, so the expression object itself lands in the references array
    *     and the top level Project stays outside whole stage codegen
    *   - from_unixtime under the legacy time parser policy puts a LegacySimpleTimestampFormatter in
    *     references, and that wraps a SimpleDateFormat, which is not thread safe
    *   - collections and nested access exercise the row conversion functions
    */
  private val selects: Seq[(String, String)] = Seq(
    "user_id" -> "user_id",
    "event_ts" -> "event_ts",
    "amount_cents" -> "CAST(amount * 100 AS LONG)",
    "amount_bucket" -> "CASE WHEN amount > 1000 THEN 'high' WHEN amount > 100 THEN 'mid' ELSE 'low' END",
    "unit_price" -> "IF(quantity > 0, amount / quantity, 0.0)",
    "currency_upper" -> "UPPER(currency)",
    "member_flag" -> "IF(is_member, 1, 0)",
    "tag_count" -> "SIZE(tags)",
    "first_tag" -> "ELEMENT_AT(tags, 1)",
    "click_count" -> "COALESCE(counters['clicks'], 0)",
    "json_score" -> "CAST(GET_JSON_OBJECT(payload, '$.score') AS DOUBLE)",
    "json_label" -> "GET_JSON_OBJECT(payload, '$.label')",
    "nested_score" -> "nested.score",
    "day" -> "FROM_UNIXTIME(event_ts / 1000, 'yyyy-MM-dd')",
    "stamp" -> "FROM_UNIXTIME(event_ts / 1000, 'yyyy-MM-dd HH:mm:ss')"
  )

  private val wheres: Seq[String] = Seq("amount > 0", "quantity > 0")

  private def rowFor(i: Int): Map[String, Any] = {
    val tags = new java.util.ArrayList[String]()
    tags.add(s"cohort_$i")
    val counters = new java.util.HashMap[String, Long]()
    counters.put("clicks", (i % 13).toLong)
    Map(
      "user_id" -> s"user_$i",
      // spread across days so a raced date formatter produces a visibly wrong day, not the same one
      "event_ts" -> (1600000000000L + i.toLong * 86400000L),
      "amount" -> (10.0 + i),
      "quantity" -> (1 + (i % 5)),
      "currency" -> "usd",
      "is_member" -> (i % 2 == 0),
      "tags" -> tags,
      "counters" -> counters,
      "payload" -> s"""{"score": ${i % 100}.5, "label": "l_$i"}""",
      "nested" -> Map[Any, Any]("score" -> 0.5, "bucket" -> "b1")
    )
  }

  it should "plan a query once and reuse it for later instances" in {
    // only this case uses these selects, so the first construction below is genuinely cold
    val onceSelects = selects :+ ("planned_once" -> "quantity * 7")

    def elapsedMs(f: => Any): Double = {
      val start = System.nanoTime()
      f
      (System.nanoTime() - start) / 1e6
    }

    val cold = elapsedMs(new CatalystUtil(schema, onceSelects, wheres))
    val warmTotal = elapsedMs((0 until 20).foreach(_ => new CatalystUtil(schema, onceSelects, wheres)))

    // Proof that no re-planning happened, independent of timing: everything a CatalystUtil derives from the
    // plan comes off a shared blueprint, so two instances hand back the very same object.
    val first = new CatalystUtil(schema, onceSelects, wheres)
    val second = new CatalystUtil(schema, onceSelects, wheres)
    first.outputChrononSchema should be theSameInstanceAs second.outputChrononSchema
    first.getOutputSparkSchema should be theSameInstanceAs second.getOutputSparkSchema

    CatalystUtil.blueprintOf(schema, onceSelects, wheres, Seq.empty, Set.empty) should be theSameInstanceAs
      CatalystUtil.blueprintOf(schema, onceSelects, wheres, Seq.empty, Set.empty)

    // a query that differs anywhere in the key gets its own plan
    CatalystUtil.blueprintOf(schema, onceSelects, wheres, Seq.empty, Set.empty) should not be theSameInstanceAs(
      CatalystUtil.blueprintOf(schema, onceSelects, Seq("amount > 1"), Seq.empty, Set.empty))

    // relative rather than an absolute millisecond budget, so this does not go flaky on a loaded runner.
    // Without the blueprint each of the 20 would re-plan, putting this at roughly 20x cold.
    withClue(s"20 warm constructions took $warmTotal ms against a single cold one at $cold ms: ") {
      warmTotal should be < cold
    }
  }

  it should "produce identical results whether instances are used serially or concurrently" in {
    val threads = 16
    val rowsPerThread = 150
    val rowCount = threads * rowsPerThread
    val pooled = new PooledCatalystUtil(selects, schema)

    // single threaded, so nothing shared can have corrupted it
    val expected: Map[Int, Map[String, Any]] =
      (0 until rowCount).map(i => i -> pooled.performSql(rowFor(i)).head).toMap
    expected(0)("day") shouldBe "2020-09-13"
    expected(0)("json_label") shouldBe "l_0"

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
            // every thread walks every row, so instances race on the same shared state repeatedly
            (0 until rowCount).foreach { i =>
              val got = pooled.performSql(rowFor(i)).head
              expected(i).foreach { case (field, want) =>
                if (got(field) != want) failures.add(s"thread $t row $i field $field: got ${got(field)} want $want")
              }
            }
          } catch {
            case e: Throwable => failures.add(s"thread $t threw $e")
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
    withClue(s"${failures.size} mismatches, first few: ${failures.toArray.take(5).mkString("; ")} ") {
      failures shouldBe empty
    }
  }
}
