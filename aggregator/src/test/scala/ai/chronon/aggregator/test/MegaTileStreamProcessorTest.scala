package ai.chronon.aggregator.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing._
import ai.chronon.api._
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.collection.mutable

/**
  * Tests the full MegaTile streaming pipeline:
  * MegaTileStreamProcessor (Flink state management) + MegaTileMerger (fetcher merge).
  *
  * Replays events through the processor, simulates watermark advancement and eviction,
  * then merges streaming entries with batch IR and compares against NaiveAggregator.
  */
class MegaTileStreamProcessorTest extends AnyFlatSpec {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val gson = new Gson

  val AllWindows: Seq[Window] = Seq(
    new Window(6, TimeUnit.HOURS),
    new Window(1, TimeUnit.DAYS),
    new Window(47, TimeUnit.HOURS),
    new Window(2, TimeUnit.DAYS),
    new Window(49, TimeUnit.HOURS),
    new Window(3, TimeUnit.DAYS),
    new Window(7, TimeUnit.DAYS)
  )

  val TailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis
  val DayMillis: Long = new Window(1, TimeUnit.DAYS).millis
  val Epsilon = 1e-6

  // sketchTolerance: relative tolerance for Long values (sketch-based aggs like APPROX_UNIQUE_COUNT
  // produce slightly different results when merging partial sketches vs building one sketch).
  def approxEqual(a: Any, b: Any, sketchTolerance: Double = 0.0): Boolean = (a, b) match {
    case (null, null)                         => true
    case (null, _) | (_, null)                => false
    case (x: Double, y: Double)               => Math.abs(x - y) <= Epsilon * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Float, y: Float)                 => Math.abs(x - y) <= Epsilon.toFloat * Math.max(1.0f, Math.max(Math.abs(x), Math.abs(y)))
    case (x: Long, y: Long) if sketchTolerance > 0 =>
      x == y || Math.abs(x - y).toDouble <= sketchTolerance * Math.max(1.0, Math.max(Math.abs(x), Math.abs(y)).toDouble)
    case (x: java.util.List[_], y: java.util.List[_]) =>
      x.size() == y.size() && (0 until x.size()).forall(i => approxEqual(x.get(i), y.get(i), sketchTolerance))
    case (x: java.util.Map[_, _], y: java.util.Map[_, _]) =>
      x.size() == y.size() && x.keySet().toArray.forall(k => approxEqual(x.get(k), y.get(k), sketchTolerance))
    case (x: Array[_], y: Array[_]) =>
      x.length == y.length && x.zip(y).forall { case (a, b) => approxEqual(a, b, sketchTolerance) }
    case _ => a == b
  }

  def compareResults(actual: Array[Array[Any]], expected: Array[Array[Any]], queryTimes: Array[Long],
                     label: String, sketchTolerance: Double = 0.0): Unit = {
    assertEquals(s"$label: result count mismatch", expected.length, actual.length)
    for (i <- queryTimes.indices) {
      if (!approxEqual(actual(i), expected(i), sketchTolerance)) {
        val expStr = gson.toJson(expected(i))
        val actStr = gson.toJson(actual(i))
        fail(s"$label: mismatch at query ${queryTimes(i)} (index $i)\n  expected: $expStr\n  got:      $actStr")
      }
    }
  }

  def generateEvents(windowDays: Int, count: Int): (Array[TestRow], Seq[(String, DataType)]) = {
    val columns = Seq(Column("ts", LongType, windowDays), Column("num", LongType, 1000), Column("amount", DoubleType, 500))
    val data = CStream.gen(columns, count)
    (data.rows, columns.map(_.schema))
  }

  def generateEventsWithCategory(windowDays: Int, count: Int): (Array[TestRow], Seq[(String, DataType)]) = {
    val columns = Seq(
      Column("ts", LongType, windowDays),
      Column("num", LongType, 1000),
      Column("amount", DoubleType, 500),
      Column("category", StringType, 5)
    )
    val data = CStream.gen(columns, count)
    (data.rows, columns.map(_.schema))
  }

  def toMillis(instant: String): Long = Instant.parse(instant).toEpochMilli

  def finalizeEntry(megaTileAgg: MegaTileAggregator, entry: Array[Any]): Array[Any] =
    megaTileAgg.windowedAggregator.finalize(entry.clone())

  def naiveAggregate(allEvents: Array[TestRow], queryTimes: Array[Long], aggregations: Seq[Aggregation], schema: Seq[(String, DataType)]): Array[Array[Any]] = {
    val unpackedParts = aggregations.flatMap(_.unpack)
    val unpacked = unpackedParts.map(_.window).toArray
    val tailHops = unpacked.map(w => FiveMinuteResolution.calculateTailHop(w))
    val rowAgg = new RowAggregator(schema, unpackedParts)
    val naiveAgg = new NaiveAggregator(rowAgg, unpacked, tailHops)
    naiveAgg.aggregate(allEvents, queryTimes).map(ir => rowAgg.finalize(ir))
  }

  /**
    * Full pipeline simulation:
    * 1. Build batch IR from pre-batchEnd events
    * 2. Create MegaTileStreamProcessor, replay streaming events through onEvent()
    * 3. Simulate watermark advancement + eviction at regular intervals
    * 4. At each query: pack today/yesterday from processor, merge with batch via MegaTileMerger
    * 5. Compare with naive
    */
  def streamProcessorAggregate(allEvents: Array[TestRow],
                                queryTimes: Array[Long],
                                aggregations: Seq[Aggregation],
                                schema: Seq[(String, DataType)],
                                batchEnd: Long): Array[Array[Any]] = {

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val store = new InMemoryTileStore(megaTileAgg.windowedAggregator)
    val processor = new MegaTileStreamProcessor(megaTileAgg, store)
    val merger = new MegaTileMerger(megaTileAgg)

    // Build batch IR from pre-batchEnd events
    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    // Flink processes ALL events (it doesn't know about batchEnd).
    // batchEnd is a fetcher-side concept — the merge logic handles the split.
    val streamingEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted

    val evictionInterval = processor.minSmallWindowTileSize
    // Next eviction fires at the next tile boundary after the first event
    var nextEvictionTs = Long.MaxValue
    val kvStore = mutable.Map[Long, Array[Any]]() // dayStart -> mega tile entry

    var eventIdx = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    // Helper: fire all pending evictions up to a given timestamp
    def firePendingEvictions(upToTs: Long): Unit = {
      if (!processor.hasSmallWindows) return
      while (nextEvictionTs <= upToTs) {
        processor.advanceWatermark(nextEvictionTs)
        val evictResult = processor.onEviction(nextEvictionTs)
        if (evictResult.todayEntry != null) kvStore(evictResult.todayStart) = evictResult.todayEntry
        nextEvictionTs += evictionInterval
      }
    }

    for (queryTs <- sortedQueries) {
      // Process all streaming events up to queryTs
      while (eventIdx < streamingEvents.length && streamingEvents(eventIdx).ts <= queryTs) {
        val event = streamingEvents(eventIdx)

        // Fire eviction timers that should have fired before this event
        firePendingEvictions(event.ts)

        // Advance watermark to event time (simulating in-order processing)
        processor.advanceWatermark(event.ts)

        // Process event
        val result = processor.onEvent(event, event.ts, queryTs)
        if (result.todayEntry != null) kvStore(result.todayStart) = result.todayEntry
        if (result.yesterdayEntry != null) kvStore(result.yesterdayStart) = result.yesterdayEntry

        // Initialize eviction schedule from first event
        if (nextEvictionTs == Long.MaxValue && processor.hasSmallWindows) {
          nextEvictionTs = TsUtils.round(event.ts, evictionInterval) + evictionInterval
        }

        eventIdx += 1
      }

      // Fire pending evictions up to query time
      firePendingEvictions(queryTs)

      // Advance watermark to query time (may trigger day transition for idle periods)
      processor.advanceWatermark(queryTs)

      // Read today's entry from processor cached state (sawtooth-corrected by eviction)
      val (todayStart, yesterdayStart) = merger.streamingDayKeys(queryTs)
      val todayIr = processor.packTodayEntry()
      val yesterdayIr = kvStore.getOrElse(yesterdayStart, null)

      resultsByQueryTs(queryTs) = merger.merge(finalBatchIr, todayIr, yesterdayIr, todayStart, queryTs, batchEnd)
    }

    queryTimes.map(resultsByQueryTs)
  }

  it should "match naive with batch fresh (14 day sim)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 14 * 3600 * 1000L,
      batchEnd + 23 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = streamProcessorAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "stream_batch_fresh")
  }

  it should "match naive with batch delayed (14 day sim)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + DayMillis + 2 * 3600 * 1000L,
      batchEnd + DayMillis + 18 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = streamProcessorAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "stream_batch_delayed")
  }

  it should "handle idle entities" in {
    val (events, schema) = generateEvents(14, 10000)
    val maxTs = events.map(_.ts).max
    val batchEnd = maxTs + DayMillis

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L)

    val results = streamProcessorAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "stream_idle")
  }

  it should "match naive with all aggregation types (14 day sim)" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows),
      Builders.Aggregation(Operation.MIN, "num", AllWindows),
      Builders.Aggregation(Operation.MAX, "num", AllWindows),
      Builders.Aggregation(Operation.LAST, "num", AllWindows),
      Builders.Aggregation(Operation.FIRST, "num", AllWindows)
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)

    val results = streamProcessorAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "stream_multi_agg")
  }

  it should "handle day boundary transitions across multiple days" in {
    val (events, schema) = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 18 * 3600 * 1000L,
      batchEnd + 30 * 3600 * 1000L,
      batchEnd + 42 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = streamProcessorAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "stream_day_boundary")
  }

  it should "handle multi-day watermark gap (3+ day idle then resume)" in {
    // Events span 14 days with a 3-day gap. Tests that advanceWatermark handles
    // multi-day jumps correctly (clears largeYesterdayIr instead of carrying stale data).
    // Batch is set fresh (1 day before query) so we stay within 2d staleness tolerance.
    val (allEvents, schema) = generateEvents(14, 20000)
    val maxTs = allEvents.map(_.ts).max
    val gapEnd = TsUtils.round(maxTs - 2 * DayMillis, DayMillis)
    val gapStart = gapEnd - 3 * DayMillis

    // Remove events in the gap to simulate idle entity
    val events = allEvents.filter(e => e.ts < gapStart || e.ts >= gapEnd)

    // Batch is fresh: 1 day before query time
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows)
    )

    val queryTimes = Array(
      batchEnd + 6 * 3600 * 1000L,
      batchEnd + 14 * 3600 * 1000L
    ).filter(_ <= maxTs)

    val results = streamProcessorAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    compareResults(results, naive, queryTimes, "stream_multi_day_gap")
  }

  it should "not update cached small windows for a retained late event outside the as-of horizon" in {
    val oneHour = new Window(1, TimeUnit.HOURS)
    val oneDay = new Window(1, TimeUnit.DAYS)
    val threeDays = new Window(3, TimeUnit.DAYS)
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(oneHour, oneDay, threeDays)))
    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new MegaTileStreamProcessor(megaTileAgg, new InMemoryTileStore(megaTileAgg.windowedAggregator))

    val todayStart = toMillis("2025-07-22T00:00:00Z")
    val asOfTs = toMillis("2025-07-22T00:10:00Z")

    val currentEvent = TestRow(todayStart, 1L)
    val currentResult = processor.onEvent(currentEvent, currentEvent.ts, asOfTs)
    val currentFinalized = finalizeEntry(megaTileAgg, currentResult.todayEntry)
    assertEquals("1h sum after current event", 1L, currentFinalized(0))
    assertEquals("1d sum after current event", 1L, currentFinalized(1))

    val retainedButOutsideOneHour = TestRow(toMillis("2025-07-21T00:00:00Z"), 1L)
    val lateResult = processor.onEvent(retainedButOutsideOneHour, retainedButOutsideOneHour.ts, asOfTs)
    val lateFinalized = finalizeEntry(megaTileAgg, lateResult.todayEntry)

    assertEquals("1h sum should exclude stale retained late event", 1L, lateFinalized(0))
    assertEquals("1d sum should still include retained late event", 2L, lateFinalized(1))
    assertNotNull("large-window yesterday entry should still be emitted", lateResult.yesterdayEntry)
  }

  it should "update cached small windows for a retained late event inside the as-of horizon" in {
    val oneHour = new Window(1, TimeUnit.HOURS)
    val oneDay = new Window(1, TimeUnit.DAYS)
    val threeDays = new Window(3, TimeUnit.DAYS)
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(oneHour, oneDay, threeDays)))
    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new MegaTileStreamProcessor(megaTileAgg, new InMemoryTileStore(megaTileAgg.windowedAggregator))

    val todayStart = toMillis("2025-07-22T00:00:00Z")
    val asOfTs = toMillis("2025-07-22T00:10:00Z")

    val currentEvent = TestRow(todayStart, 1L)
    processor.onEvent(currentEvent, currentEvent.ts, asOfTs)

    val retainedInsideOneHour = TestRow(toMillis("2025-07-21T23:30:00Z"), 1L)
    val lateResult = processor.onEvent(retainedInsideOneHour, retainedInsideOneHour.ts, asOfTs)
    val lateFinalized = finalizeEntry(megaTileAgg, lateResult.todayEntry)

    assertEquals("1h sum should include retained late event inside as-of horizon", 2L, lateFinalized(0))
    assertEquals("1d sum should include retained late event", 2L, lateFinalized(1))
    assertNotNull("large-window yesterday entry should still be emitted", lateResult.yesterdayEntry)
  }

  it should "rebuild cached small windows when watermark advances into a new day" in {
    val oneHour = new Window(1, TimeUnit.HOURS)
    val oneDay = new Window(1, TimeUnit.DAYS)
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(oneHour, oneDay)))
    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new MegaTileStreamProcessor(megaTileAgg, new InMemoryTileStore(megaTileAgg.windowedAggregator))

    // Before midnight, this event is inside both the 1h and 1d small-window caches.
    val yesterdayEvent = TestRow(toMillis("2025-07-22T22:00:00Z"), 10L)
    val yesterdayAsOfTs = toMillis("2025-07-22T23:00:00Z")
    val yesterdayResult = processor.onEvent(yesterdayEvent, yesterdayEvent.ts, yesterdayAsOfTs)
    val yesterdayFinalized = finalizeEntry(megaTileAgg, yesterdayResult.todayEntry)

    assertEquals("sanity: previous-day 1h cache contains the event before rollover", 10L, yesterdayFinalized(0))
    assertEquals("sanity: previous-day 1d cache contains the event before rollover", 10L, yesterdayFinalized(1))

    // Day rollover advances the as-of time without a new event, so cachedSmallWindowIr
    // must be rebuilt here instead of waiting for the next eviction.
    processor.advanceWatermark(toMillis("2025-07-23T00:00:00Z"))
    val todayFinalized = finalizeEntry(megaTileAgg, processor.packTodayEntry())

    // After midnight, the 1h window no longer includes 22:00, but the 1d window still does.
    assertNull("new-day 1h cache should not carry stale previous-day value", todayFinalized(0))
    assertEquals("new-day 1d cache should be rebuilt from retained tiles", 10L, todayFinalized(1))
  }

  it should "preserve no-batch values when packing yesterday after rollover" in {
    val oneHour = new Window(1, TimeUnit.HOURS)
    val threeDays = new Window(3, TimeUnit.DAYS)
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(oneHour, threeDays)))
    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new MegaTileStreamProcessor(megaTileAgg, new InMemoryTileStore(megaTileAgg.windowedAggregator))

    val midnight = toMillis("2025-07-23T00:00:00Z")
    val beforeRollover = TestRow(toMillis("2025-07-22T23:30:00Z"), 10L)
    processor.onEvent(beforeRollover, beforeRollover.ts, midnight)
    processor.advanceWatermark(midnight)

    val latePreviousDay = TestRow(toMillis("2025-07-22T22:00:00Z"), 5L)
    val result = processor.onEvent(latePreviousDay, latePreviousDay.ts, midnight + 5 * 60 * 1000L)
    val yesterdayFinalized = finalizeEntry(megaTileAgg, result.yesterdayEntry)

    // Late previous-day events must update yesterday's batch-backed columns because serving merges
    // daily contributions across N days. No-batch columns are not merged: serving picks the newest
    // available daily row, so small-window updates for the query path belong to today's row instead.
    assertEquals("yesterday 1h no-batch snapshot should survive the late-row overwrite", 10L, yesterdayFinalized(0))
    assertEquals("yesterday 3d batch-backed column should include the late event", 15L, yesterdayFinalized(1))
  }

  it should "not update previous-day no-batch columns for late events when serving picks today" in {
    val oneHour = new Window(1, TimeUnit.HOURS)
    val threeDays = new Window(3, TimeUnit.DAYS)
    val aggregations = Seq(Builders.Aggregation(Operation.SUM, "num", Seq(oneHour, threeDays)))
    val schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType)
    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val processor = new MegaTileStreamProcessor(megaTileAgg, new InMemoryTileStore(megaTileAgg.windowedAggregator))
    val merger = new MegaTileMerger(megaTileAgg)

    val yesterdayStart = toMillis("2025-07-22T00:00:00Z")
    val todayStart = yesterdayStart + DayMillis
    val queryTs = toMillis("2025-07-23T00:10:00Z")

    val beforeRollover = TestRow(toMillis("2025-07-22T23:30:00Z"), 10L)
    processor.onEvent(beforeRollover, beforeRollover.ts, todayStart)
    processor.advanceWatermark(todayStart)

    val todayEvent = TestRow(toMillis("2025-07-23T00:01:00Z"), 7L)
    processor.onEvent(todayEvent, todayEvent.ts, queryTs)

    val latePreviousDay = TestRow(toMillis("2025-07-22T22:00:00Z"), 5L)
    val lateResult = processor.onEvent(latePreviousDay, latePreviousDay.ts, queryTs)
    val yesterdayFinalized = finalizeEntry(megaTileAgg, lateResult.yesterdayEntry)
    assertEquals("late event should not mutate frozen previous-day 1h snapshot", 10L, yesterdayFinalized(0))
    assertEquals("late event should still update previous-day 3d contribution", 15L, yesterdayFinalized(1))

    val served =
      merger.merge(null, Seq(todayStart -> processor.packTodayEntry(), yesterdayStart -> lateResult.yesterdayEntry),
                   queryTs, yesterdayStart)
    assertEquals("serving should choose today's self-contained 1h row, not merge yesterday", 17L, served(0))
    assertEquals("batch-backed column should merge today and yesterday daily contributions", 22L, served(1))
  }

  it should "match naive with complex aggregations (buckets, approx_unique, histogram, last_k)" in {
    val (events, schema) = generateEventsWithCategory(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      // Bucketed: produces MapType(StringType, irType) IR
      Builders.Aggregation(Operation.SUM, "num", AllWindows, buckets = Seq("category")),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows, buckets = Seq("category")),
      // Sketch-based: BinaryType IR (CPC sketch serde)
      Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT, "num", AllWindows),
      // Map IR: MapType(StringType, IntType)
      Builders.Aggregation(Operation.HISTOGRAM, "category", AllWindows),
      // Collection IR: ListType
      Builders.Aggregation(Operation.LAST_K, "num", AllWindows, argMap = Map("k" -> "3"))
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)

    val results = streamProcessorAggregate(events, queryTimes, aggregations, schema, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema)
    // 5% tolerance for APPROX_UNIQUE_COUNT (CPC sketch merge imprecision)
    compareResults(results, naive, queryTimes, "stream_complex_aggs", sketchTolerance = 0.05)
  }
}
