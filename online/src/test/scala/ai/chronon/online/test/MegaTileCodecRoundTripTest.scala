package ai.chronon.online.test

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.test.NaiveAggregator
import ai.chronon.aggregator.windowing.{InMemoryTileStore, TileStore, _}
import ai.chronon.api._
import ai.chronon.api.Extensions.{AggregationOps, WindowOps}
import ai.chronon.online.MegaTileCodec
import ai.chronon.online.serde.ArrayRow
import com.google.gson.Gson
import org.junit.Assert._
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory

import java.util
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

/**
  * Tests MegaTileStreamProcessor with serde round-trips at state boundaries,
  * simulating what MegaTileProcessFunction does in Flink (persist → restore).
  * Catches bugs where encode → decode doesn't preserve state.
  */
class MegaTileCodecRoundTripTest extends AnyFlatSpec {
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

  val Schema: Seq[(String, DataType)] = Seq("ts" -> LongType, "num" -> LongType, "amount" -> DoubleType)
  val SchemaWithCategory: Seq[(String, DataType)] =
    Seq("ts" -> LongType, "num" -> LongType, "amount" -> DoubleType, "category" -> StringType)

  private val categories = Array("alpha", "beta", "gamma", "delta", "epsilon")

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

  def generateEvents(daySpan: Int, count: Int): Array[Row] = {
    val rng = new Random(42)
    val baseTs = 1774000000000L
    val spanMillis = daySpan.toLong * DayMillis
    (0 until count).map { _ =>
      val ts = baseTs + (rng.nextDouble() * spanMillis).toLong
      val num = rng.nextInt(1000).toLong
      val amount = rng.nextDouble() * 500.0
      new ArrayRow(Array(ts, num, amount), ts): Row
    }.toArray
  }

  def generateEventsWithCategory(daySpan: Int, count: Int): Array[Row] = {
    val rng = new Random(42)
    val baseTs = 1774000000000L
    val spanMillis = daySpan.toLong * DayMillis
    (0 until count).map { _ =>
      val ts = baseTs + (rng.nextDouble() * spanMillis).toLong
      val num = rng.nextInt(1000).toLong
      val amount = rng.nextDouble() * 500.0
      val cat = categories(rng.nextInt(categories.length))
      new ArrayRow(Array(ts, num, amount, cat), ts): Row
    }.toArray
  }

  def naiveAggregate(allEvents: Array[Row],
                     queryTimes: Array[Long],
                     aggregations: Seq[Aggregation],
                     schema: Seq[(String, DataType)] = Schema): Array[Array[Any]] = {
    val unpackedParts = aggregations.flatMap(_.unpack)
    val unpacked = unpackedParts.map(_.window).toArray
    val tailHops = unpacked.map(w => FiveMinuteResolution.calculateTailHop(w))
    val rowAgg = new RowAggregator(schema, unpackedParts)
    val naiveAgg = new NaiveAggregator(rowAgg, unpacked, tailHops)
    naiveAgg.aggregate(allEvents, queryTimes).map(ir => rowAgg.finalize(ir))
  }

  def buildGroupBy(aggregations: Seq[Aggregation]): GroupBy = {
    import scala.collection.JavaConverters._
    val gb = new GroupBy()
    gb.setAggregations(aggregations.asJava)
    val meta = new MetaData()
    meta.setName("test_mega_tile_codec")
    gb.setMetaData(meta)
    gb
  }

  /** TileStore that encodes/decodes on every access, simulating Flink's MapState/ValueState with codec. */
  class SerdeTileStore(windowedAgg: RowAggregator, codec: MegaTileCodec) extends TileStore {
    private val tileBytes = mutable.Map[(Long, Long), Array[Byte]]()
    private var cachedSmallBytes: Array[Byte] = codec.encode(windowedAgg.init)
    private var largeTodayBytes: Array[Byte] = codec.encode(windowedAgg.init)
    private var largeYesterdayBytes: Array[Byte] = codec.encode(windowedAgg.init)
    private var dayStart: Long = -1L
    private var earliest: Long = Long.MaxValue

    override def getTile(h: Long, t: Long): Array[Any] = tileBytes.get((h, t)).map(codec.decodeBaseIr).orNull
    override def putTile(h: Long, t: Long, ir: Array[Any]): Unit = tileBytes((h, t)) = codec.encodeBaseIr(ir)
    override def removeTile(h: Long, t: Long): Unit = tileBytes.remove((h, t))
    override def tileIterator: Iterator[(Long, Long, Array[Any])] =
      tileBytes.iterator.map { case ((h, t), b) => (h, t, codec.decodeBaseIr(b)) }

    override def getCachedSmallWindowIr: Array[Any] = codec.decode(cachedSmallBytes)
    override def putCachedSmallWindowIr(ir: Array[Any]): Unit = cachedSmallBytes = codec.encode(ir)
    override def getLargeTodayIr: Array[Any] = codec.decode(largeTodayBytes)
    override def putLargeTodayIr(ir: Array[Any]): Unit = largeTodayBytes = codec.encode(ir)
    override def getLargeYesterdayIr: Array[Any] = codec.decode(largeYesterdayBytes)
    override def putLargeYesterdayIr(ir: Array[Any]): Unit = largeYesterdayBytes = codec.encode(ir)
    override def getCurrentDayStart: Long = dayStart
    override def putCurrentDayStart(ts: Long): Unit = dayStart = ts
    override def getEarliestTileStart: Long = earliest
    override def putEarliestTileStart(ts: Long): Unit = earliest = ts
  }

  def streamProcessorWithSerdeAggregate(allEvents: Array[Row],
                                         queryTimes: Array[Long],
                                         aggregations: Seq[Aggregation],
                                         batchEnd: Long,
                                         schema: Seq[(String, DataType)] = Schema): Array[Array[Any]] = {

    val megaTileAgg = new MegaTileAggregator(aggregations, schema, tailBufferMillis = TailBufferMillis)
    val codec = new MegaTileCodec(buildGroupBy(aggregations), schema)
    // SerdeTileStore encodes/decodes on every access — tests the codec round-trip inline
    val store = new SerdeTileStore(megaTileAgg.windowedAggregator, codec)
    val processor = new MegaTileStreamProcessor(megaTileAgg, store)
    val merger = new MegaTileMerger(megaTileAgg)

    val batchEvents = allEvents.filter(_.ts < batchEnd)
    val onlineAgg = new SawtoothOnlineAggregator(batchEnd, aggregations, schema, tailBufferMillis = TailBufferMillis)
    var batchIr = onlineAgg.init
    batchEvents.foreach(row => batchIr = onlineAgg.update(batchIr, row))
    val finalBatchIr = onlineAgg.finalizeSnapshot(batchIr)

    val sortedEvents = allEvents.sortBy(_.ts)
    val sortedQueries = queryTimes.sorted

    val evictionInterval = processor.minSmallWindowTileSize
    var nextEvictionTs = Long.MaxValue
    val kvStore = mutable.Map[Long, Array[Any]]()
    var eventIdx = 0
    val resultsByQueryTs = mutable.Map[Long, Array[Any]]()

    // Serde happens on every TileStore access (SerdeTileStore), so no explicit round-trip needed.
    def firePendingEvictions(upToTs: Long): Unit = {
      if (!processor.hasSmallWindows) return
      while (nextEvictionTs <= upToTs) {
        processor.advanceWatermark(nextEvictionTs)
        val r = processor.onEviction(nextEvictionTs)
        if (r.todayEntry != null) kvStore(r.todayStart) = r.todayEntry
        nextEvictionTs += evictionInterval
      }
    }

    for (queryTs <- sortedQueries) {
      while (eventIdx < sortedEvents.length && sortedEvents(eventIdx).ts <= queryTs) {
        val event = sortedEvents(eventIdx)
        firePendingEvictions(event.ts)
        processor.advanceWatermark(event.ts)

        val result = processor.onEvent(event, event.ts, queryTs)
        if (result.todayEntry != null) kvStore(result.todayStart) = result.todayEntry
        if (result.yesterdayEntry != null) kvStore(result.yesterdayStart) = result.yesterdayEntry

        if (nextEvictionTs == Long.MaxValue && processor.hasSmallWindows) {
          nextEvictionTs = TsUtils.round(event.ts, evictionInterval) + evictionInterval
        }

        eventIdx += 1
      }

      firePendingEvictions(queryTs)
      processor.advanceWatermark(queryTs)

      val (todayStart, yesterdayStart) = merger.streamingDayKeys(queryTs)
      val todayIr = processor.packTodayEntry()
      val yesterdayIr = kvStore.getOrElse(yesterdayStart, null)
      resultsByQueryTs(queryTs) = merger.merge(finalBatchIr, todayIr, yesterdayIr, todayStart, queryTs, batchEnd)
    }

    queryTimes.map(resultsByQueryTs)
  }

  it should "match naive with serde round-trips (batch fresh)" in {
    val events = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val queryTimes = Array(batchEnd + 6 * 3600 * 1000L, batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)
    val results = streamProcessorWithSerdeAggregate(events, queryTimes, aggregations, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations)
    assertEquals("result count", naive.length, results.length)
    for (i <- queryTimes.indices) {
      assertTrue(s"serde_fresh: mismatch at query ${queryTimes(i)}\n  expected: ${gson.toJson(naive(i))}\n  got:      ${gson.toJson(results(i))}",
                 approxEqual(results(i), naive(i)))
    }
  }

  it should "match naive with serde round-trips (all agg types)" in {
    val events = generateEvents(14, 20000)
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
    val results = streamProcessorWithSerdeAggregate(events, queryTimes, aggregations, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations)
    assertEquals("result count", naive.length, results.length)
    for (i <- queryTimes.indices) {
      assertTrue(s"serde_all_agg: mismatch at query ${queryTimes(i)}\n  expected: ${gson.toJson(naive(i))}\n  got:      ${gson.toJson(results(i))}",
                 approxEqual(results(i), naive(i)))
    }
  }

  it should "handle key switching (Flink reuses processor across keys)" in {
    // Simulates Flink's key reuse: process events for key A, then reinitialize
    // processor (simulating key switch to B with empty state), verify B's state
    // is clean by comparing against the normal serde round-trip test for the same data.
    val events = generateEvents(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      Builders.Aggregation(Operation.SUM, "num", AllWindows),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows),
      Builders.Aggregation(Operation.AVERAGE, "amount", AllWindows)
    )

    val megaTileAgg = new MegaTileAggregator(aggregations, Schema, tailBufferMillis = TailBufferMillis)

    // Process key A events — accumulate state in a shared store
    val storeA = new InMemoryTileStore(megaTileAgg.windowedAggregator)
    val processorA = new MegaTileStreamProcessor(megaTileAgg, storeA)
    for (event <- events.sortBy(_.ts)) {
      processorA.advanceWatermark(event.ts)
      val smallWindowAsOfTs = TsUtils.round(event.ts, processorA.minSmallWindowTileSize) + processorA.minSmallWindowTileSize
      processorA.onEvent(event, event.ts, smallWindowAsOfTs)
    }
    // Verify key A has non-empty state
    assertTrue("key A should have tiles", storeA.tiles.nonEmpty)

    // Key switch to B: fresh store (what Flink's keyed state scoping does)
    val storeB = new InMemoryTileStore(megaTileAgg.windowedAggregator)
    val processorB = new MegaTileStreamProcessor(megaTileAgg, storeB)
    // Verify key B state is clean
    assertTrue("key B tiles should be empty", storeB.tiles.isEmpty)
    assertEquals("key B currentDayStart should be -1", -1L, storeB.currentDayStart)
    assertEquals("key B earliestTileStart should be MaxValue", Long.MaxValue, storeB.earliestTileStart)

    // Process same events through B with serde round-trips — should match naive
    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)
    val results = streamProcessorWithSerdeAggregate(events, queryTimes, aggregations, batchEnd)
    val naive = naiveAggregate(events, queryTimes, aggregations)
    assertEquals("result count", naive.length, results.length)
    for (i <- queryTimes.indices) {
      assertTrue(
        s"key_switch: mismatch at query ${queryTimes(i)}\n  expected: ${gson.toJson(naive(i))}\n  got:      ${gson.toJson(results(i))}",
        approxEqual(results(i), naive(i)))
    }
  }

  it should "match naive with complex aggregations through serde (buckets, approx_unique, histogram, last_k)" in {
    val events = generateEventsWithCategory(14, 20000)
    val maxTs = events.map(_.ts).max
    val batchEnd = TsUtils.round(maxTs - DayMillis, DayMillis)

    val aggregations = Seq(
      // Bucketed: MapType(StringType, irType) — tests map serde
      Builders.Aggregation(Operation.SUM, "num", AllWindows, buckets = Seq("category")),
      Builders.Aggregation(Operation.COUNT, "num", AllWindows, buckets = Seq("category")),
      // Sketch-based: BinaryType IR (CPC sketch) — tests binary serde
      Builders.Aggregation(Operation.APPROX_UNIQUE_COUNT, "num", AllWindows),
      // Map IR: MapType(StringType, IntType) — tests map serde
      Builders.Aggregation(Operation.HISTOGRAM, "category", AllWindows),
      // Collection IR: ListType — tests list serde
      Builders.Aggregation(Operation.LAST_K, "num", AllWindows, argMap = Map("k" -> "3"))
    )

    val queryTimes = Array(batchEnd + 14 * 3600 * 1000L).filter(_ <= maxTs)
    val results = streamProcessorWithSerdeAggregate(events, queryTimes, aggregations, batchEnd,
                                                     schema = SchemaWithCategory)
    val naive = naiveAggregate(events, queryTimes, aggregations, schema = SchemaWithCategory)
    assertEquals("result count", naive.length, results.length)
    for (i <- queryTimes.indices) {
      // 5% tolerance for APPROX_UNIQUE_COUNT (CPC sketch merge imprecision)
      assertTrue(
        s"serde_complex_aggs: mismatch at query ${queryTimes(i)}\n  expected: ${gson.toJson(naive(i))}\n  got:      ${gson.toJson(results(i))}",
        approxEqual(results(i), naive(i), sketchTolerance = 0.05))
    }
  }

  it should "decode mega tile bytes safely across threads" in {
    val aggregations = Seq(
      Builders.Aggregation(Operation.HISTOGRAM, "category", AllWindows),
      Builders.Aggregation(Operation.LAST_K, "num", AllWindows, argMap = Map("k" -> "3"))
    )
    val codec = new MegaTileCodec(buildGroupBy(aggregations), SchemaWithCategory)

    def histogramIr(): util.HashMap[String, java.lang.Long] = {
      val ir = new util.HashMap[String, java.lang.Long]()
      ir.put("alpha", Long.box(1L))
      ir.put("beta", Long.box(2L))
      ir
    }

    def lastKIr(): util.ArrayList[util.ArrayList[Any]] = {
      val ir = new util.ArrayList[util.ArrayList[Any]]()
      ir.add(ai.chronon.aggregator.base.TimeTuple.make(1775001000000L, Long.box(10L)))
      ir.add(ai.chronon.aggregator.base.TimeTuple.make(1775000400000L, Long.box(9L)))
      ir.add(ai.chronon.aggregator.base.TimeTuple.make(1774999800000L, Long.box(8L)))
      ir
    }

    val ir = Array[Any](
      histogramIr(),
      histogramIr(),
      histogramIr(),
      histogramIr(),
      histogramIr(),
      histogramIr(),
      histogramIr(),
      lastKIr(),
      lastKIr(),
      lastKIr(),
      lastKIr(),
      lastKIr(),
      lastKIr(),
      lastKIr()
    )

    val bytes = codec.encode(ir)
    assertTrue(approxEqual(ir, codec.decode(bytes)))

    val executor = java.util.concurrent.Executors.newFixedThreadPool(8)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)
    try {
      val decoded = Await.result(
        Future.sequence((0 until 2000).map(_ => Future(codec.decode(bytes)))),
        30.seconds
      )
      decoded.foreach(result => assertTrue(approxEqual(ir, result)))
    } finally {
      executor.shutdownNow()
    }
  }
}
