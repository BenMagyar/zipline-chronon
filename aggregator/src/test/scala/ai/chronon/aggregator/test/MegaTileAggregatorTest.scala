package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing._
import ai.chronon.api._
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import scala.collection.mutable

class MegaTileAggregatorTest extends AnyFlatSpec {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val gson = new Gson

  val AllWindows: Seq[Window] = Seq(
    new Window(6, TimeUnit.HOURS),  // 6h - NO BATCH, 5min tier
    new Window(1, TimeUnit.DAYS),   // 1d - NO BATCH, 1hr tier
    new Window(47, TimeUnit.HOURS), // 47h - NO BATCH, 1hr tier, just under tailBuffer
    new Window(2, TimeUnit.DAYS),   // 2d - NO BATCH, 1hr tier, exactly tailBuffer
    new Window(49, TimeUnit.HOURS), // 49h - BATCH, 1hr tier, just over tailBuffer
    new Window(3, TimeUnit.DAYS),   // 3d - BATCH, 1hr tier, 1d collapsed
    new Window(7, TimeUnit.DAYS)    // 7d - BATCH, 1hr tier, 5d collapsed
  )

  val TailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis

  // Simulate the full mega tile pipeline and compare against naive aggregation
  def megaTileAggregate(allEvents: Array[TestRow],
                        queryTimes: Array[Long],
                        aggregations: Seq[Aggregation],
                        schema: Seq[(String, DataType)],
                        batchEnd: Long): Array[Array[Any]] = {

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)

    // 1. Build batch IR from pre-batchEnd events
    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg =
      new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    // 2. Simulate Flink: bucket ALL events into small tiles per tier
    //    (In production Flink sees all events; eviction handles retention)
    val tiles: Map[Long, mutable.Map[Long, Array[Any]]] =
      megaTileAgg.activeTiers.map(hop => hop -> mutable.Map.empty[Long, Array[Any]]).toMap

    // Simulate Flink: incrementally add events, query at correct times
    // Events only enter tiles when ts <= queryTs (real-time arrival)
    val sortedEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted
    var eventIdx = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    for (queryTs <- sortedQueries) {
      // Add events that have arrived by queryTs
      while (eventIdx < sortedEvents.length && sortedEvents(eventIdx).ts <= queryTs) {
        val event = sortedEvents(eventIdx)
        val tileStarts = megaTileAgg.tileStartsForEvent(event.ts)
        for ((hopSize, tileStart) <- tileStarts) {
          val tierTiles = tiles(hopSize)
          val ir = tierTiles.getOrElseUpdate(tileStart, megaTileAgg.baseAggregator.init)
          megaTileAgg.baseAggregator.update(ir, event)
        }
        eventIdx += 1
      }

      // Evict stale tiles
      for ((hopSize, tierTiles) <- tiles) {
        val floor = megaTileAgg.retentionFloor(hopSize, queryTs, batchEnd)
        tierTiles.keys.filter(_ < floor).foreach(tierTiles.remove)
      }

      val megaTileIr = megaTileAgg.buildMegaTileIr(tiles, queryTs, batchEnd)
      resultsByQueryTs(queryTs) = megaTileAgg.serveMegaTile(finalBatchIr, megaTileIr, queryTs, batchEnd)
    }

    // Return results in original query order
    queryTimes.map(resultsByQueryTs)
  }

  def naiveAggregate(allEvents: Array[TestRow],
                     queryTimes: Array[Long],
                     aggregations: Seq[Aggregation],
                     schema: Seq[(String, DataType)]): Array[Array[Any]] = {
    import scala.collection.JavaConverters._
    val unpackedParts = aggregations.flatMap(_.unpack)
    val unpacked = unpackedParts.map(_.window).toArray
    val tailHops = unpacked.map(w => FiveMinuteResolution.calculateTailHop(w))
    val rowAgg = new RowAggregator(schema, unpackedParts)
    val naiveAgg = new NaiveAggregator(rowAgg, unpacked, tailHops)
    val rawResults = naiveAgg.aggregate(allEvents, queryTimes)
    // Finalize to match serveMegaTile output
    rawResults.map(ir => rowAgg.finalize(ir))
  }

  val Epsilon = 1e-6

  def approxEqual(a: Any, b: Any): Boolean = (a, b) match {
    case (null, null)                         => true
    case (null, _) | (_, null)                => false
    case (x: Double, y: Double)               => Math.abs(x - y) <= Epsilon * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Float, y: Float)                 => Math.abs(x - y) <= Epsilon.toFloat * Math.max(1.0f, Math.max(Math.abs(x), Math.abs(y)))
    case (x: java.util.List[_], y: java.util.List[_]) =>
      x.size() == y.size() && (0 until x.size()).forall(i => approxEqual(x.get(i), y.get(i)))
    case (x: Array[_], y: Array[_])           =>
      x.length == y.length && x.zip(y).forall { case (a, b) => approxEqual(a, b) }
    case _                                    => a == b
  }

  def compareResults(megaTileResults: Array[Array[Any]],
                     naiveResults: Array[Array[Any]],
                     queryTimes: Array[Long],
                     label: String): Unit = {
    assertEquals(s"$label: result count mismatch", naiveResults.length, megaTileResults.length)
    for (i <- queryTimes.indices) {
      if (!approxEqual(megaTileResults(i), naiveResults(i))) {
        val naiveStr = gson.toJson(naiveResults(i))
        val megaStr = gson.toJson(megaTileResults(i))
        fail(s"$label: mismatch at query ${queryTimes(i)} (index $i)\n  expected: $naiveStr\n  got:      $megaStr")
      }
    }
  }

  // Generate events spanning a time range
  def generateEvents(windowDays: Int, count: Int): (Array[TestRow], Seq[(String, DataType)]) = {
    val columns = Seq(
      Column("ts", LongType, windowDays),
      Column("num", LongType, 1000),
      Column("amount", DoubleType, 500)
    )
    val data = CStream.gen(columns, count)
    (data.rows, columns.map(_.schema))
  }

  it should "match naive aggregation with batch fresh" in {
    val (events, schema) = generateEvents(10, 10000)
    val maxTs = events.map(_.ts).max
    val minTs = events.map(_.ts).min

    // batchEnd = roughly 1 day before maxTs, rounded to day boundary
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val batchEnd = TsUtils.round(maxTs - dayMillis, dayMillis)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    // Query at multiple points after batchEnd
    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,  // 6h after batch
      batchEnd + 14 * 3600 * 1000L, // 14h after batch (mid-day)
      batchEnd + 23 * 3600 * 1000L  // 23h after batch (near midnight)
    ).filter(_ <= maxTs)

    val megaResults = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(megaResults, naiveResults, queryTimes, "batch_fresh")
  }

  it should "match naive aggregation with batch delayed" in {
    val (events, schema) = generateEvents(10, 10000)
    val maxTs = events.map(_.ts).max
    val dayMillis = new Window(1, TimeUnit.DAYS).millis

    // Simulate batch delay: batchEnd is 2 days before maxTs (stale)
    val batchEnd = TsUtils.round(maxTs - 2 * dayMillis, dayMillis)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    // Query at early morning (simulates batch delay scenario)
    val queryTimes = Array(
      batchEnd + dayMillis + 2 * 3600 * 1000L, // 26h after batchEnd (02:00 next day)
      batchEnd + dayMillis + 5 * 3600 * 1000L  // 29h after batchEnd (05:00 next day)
    ).filter(_ <= maxTs)

    val megaResults = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(megaResults, naiveResults, queryTimes, "batch_delayed")
  }

  it should "handle idle entities with no streaming events" in {
    val (events, schema) = generateEvents(10, 5000)
    val maxTs = events.map(_.ts).max
    val dayMillis = new Window(1, TimeUnit.DAYS).millis

    // batchEnd AFTER all events — no streaming data
    val batchEnd = maxTs + dayMillis

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L)

    val megaResults = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(megaResults, naiveResults, queryTimes, "idle_entity")
  }

  it should "handle eviction correctly at window boundaries" in {
    val (events, schema) = generateEvents(10, 10000)
    val maxTs = events.map(_.ts).max
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val batchEnd = TsUtils.round(maxTs - dayMillis, dayMillis)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    // Query at exact hop boundaries to stress eviction
    val fiveMin = 5 * 60 * 1000L
    val oneHour = 3600 * 1000L
    val queryTimes = Array(
      TsUtils.round(batchEnd + 14 * oneHour, fiveMin),        // aligned to 5min
      TsUtils.round(batchEnd + 14 * oneHour, oneHour),        // aligned to 1hr
      TsUtils.round(batchEnd + 14 * oneHour + fiveMin, fiveMin) // one hop after
    ).filter(_ <= maxTs).distinct

    val megaResults = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(megaResults, naiveResults, queryTimes, "eviction_boundary")
  }

  it should "match naive with events at exact batchEnd boundary" in {
    val baseTs = 1700000000000L
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val batchEnd = TsUtils.round(baseTs, dayMillis)

    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)

    // Events at and around batchEnd
    val events = Array(
      new TestRow(batchEnd - 1L, 10L)(0),    // just before batchEnd → batch
      new TestRow(batchEnd, 20L)(0),          // exactly at batchEnd → streaming
      new TestRow(batchEnd + 1L, 30L)(0),     // just after batchEnd → streaming
      new TestRow(batchEnd + dayMillis / 2, 40L)(0) // mid-day → streaming
    )

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + dayMillis / 2 + 1L)

    val megaResults = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(megaResults, naiveResults, queryTimes, "batchEnd_boundary")
  }

  it should "not mutate batch tail hops across repeated serves" in {
    val batchEnd = TsUtils.round(1700000000000L, new Window(1, TimeUnit.DAYS).millis)
    val queryTs = batchEnd + 3600 * 1000L
    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "amount" -> DoubleType)
    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.AVERAGE, "amount", Seq(new Window(3, TimeUnit.DAYS)))
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val onlineAgg =
      new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchIr = onlineAgg.update(batchIr, new TestRow(batchEnd - 26 * 3600 * 1000L, 2.0)(0))
    batchIr = onlineAgg.update(batchIr, new TestRow(batchEnd - 25 * 3600 * 1000L, 4.0)(0))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    val megaTileIr = megaTileAgg.buildMegaTileIr(Map.empty[Long, collection.Map[Long, Array[Any]]],
                                                now = queryTs,
                                                batchEnd = batchEnd)
    val firstServe = megaTileAgg.serveMegaTile(finalBatchIr, megaTileIr, queryTs, batchEnd)
    val secondServe = megaTileAgg.serveMegaTile(finalBatchIr, megaTileIr, queryTs, batchEnd)

    assertEquals(3.0, firstServe.head.asInstanceOf[Double], Epsilon)
    assertTrue(approxEqual(firstServe, secondServe))
  }

  it should "match naive with multiple aggregation types" in {
    val (events, schema) = generateEvents(10, 10000)
    val maxTs = events.map(_.ts).max
    val dayMillis = new Window(1, TimeUnit.DAYS).millis
    val batchEnd = TsUtils.round(maxTs - dayMillis, dayMillis)

    val aggregations: Seq[Aggregation] = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows),
      Builders.Aggregation(Operation.MIN, "num", AllWindows),
      Builders.Aggregation(Operation.MAX, "num", AllWindows),
      Builders.Aggregation(Operation.LAST, "num", AllWindows),
      Builders.Aggregation(Operation.FIRST, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)

    val megaResults = megaTileAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naiveResults = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(megaResults, naiveResults, queryTimes, "multi_agg_types")
  }
}
