package ai.chronon.aggregator.windowing

import ai.chronon.api.Row
import ai.chronon.api.TsUtils

import scala.collection.mutable

/** Pure Scala state manager for the MegaTile streaming pipeline.
  * All state access goes through a TileStore — Flink backs it with MapState/ValueState + codec,
  * tests back it with InMemoryTileStore. No bulk restore/persist cycle; reads are lazy,
  * writes are immediate to the touched entries only.
  *
  * State layout:
  *   - Small window tiles: per-tier base IRs. Source of truth for eviction rebuilds.
  *   - cachedSmallWindowIr: sawtooth running sum, corrected on eviction.
  *   - Large window today IR and previous-day IR: large columns are per-day accumulators;
  *     no-batch columns in previous-day IR are frozen at adjacent day rollover.
  *   - Day transitions are watermark-driven (advanceWatermark), not event-driven.
  */
class MegaTileStreamProcessor(val megaTileAgg: MegaTileAggregator, val store: TileStore) {

  val DayMillis: Long = 24 * 3600 * 1000L

  private val windowedAgg = megaTileAgg.windowedAggregator
  private val baseAgg = megaTileAgg.baseAggregator
  private val isNoBatch = megaTileAgg.isNoBatch
  private val columnHopSize = megaTileAgg.columnHopSize

  val smallWindowTiers: Set[Long] = {
    val tiers = mutable.Set.empty[Long]
    var col = 0
    while (col < windowedAgg.length) {
      if (isNoBatch(col)) tiers += columnHopSize(col)
      col += 1
    }
    tiers.toSet
  }

  val hasSmallWindows: Boolean = smallWindowTiers.nonEmpty
  val minSmallWindowTileSize: Long = if (hasSmallWindows) smallWindowTiers.min else DayMillis

  /** Ingest an event at eventTs, but bound cached small-window emission as of smallWindowAsOfTs.
    * Base tile state still uses eventTs so retained late events remain available for future rebuilds.
    */
  def onEvent(row: Row, eventTs: Long, smallWindowAsOfTs: Long): EmitResult = {
    var currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) {
      currentDayStart = TsUtils.round(eventTs, DayMillis)
      store.putCurrentDayStart(currentDayStart)
    }

    var todayDirty = false
    var yesterdayDirty = false

    // Update tiles + cachedSmallWindowIr for small-window tiers
    if (hasSmallWindows) {
      val acceptedTileStarts = mutable.Map.empty[Long, Long]
      val tileStarts = megaTileAgg.tileStartsForEvent(eventTs)
      for ((hopSize, tileStart) <- tileStarts) {
        if (smallWindowTiers.contains(hopSize)) {
          val floor = megaTileAgg.retentionFloor(hopSize, eventTs, currentDayStart)
          val ceiling = currentDayStart + 2 * DayMillis
          if (tileStart >= floor && tileStart < ceiling) {
            val existing = store.getTile(hopSize, tileStart)
            val ir = if (existing != null) existing else baseAgg.init
            baseAgg.update(ir, row)
            store.putTile(hopSize, tileStart, ir)
            val earliest = store.getEarliestTileStart
            if (tileStart < earliest) store.putEarliestTileStart(tileStart)
            acceptedTileStarts(hopSize) = tileStart
          }
        }
      }

      // Sawtooth: merge event into the running IR only for columns whose tile belongs
      // in the as-of window being emitted. The base tile remains retained even when
      // a late event is outside a smaller current serving horizon.
      if (acceptedTileStarts.nonEmpty) {
        var cachedIr: Array[Any] = null
        var cachedIrUpdated = false
        var col = 0
        while (col < windowedAgg.length) {
          if (isNoBatch(col)) {
            val hopSize = columnHopSize(col)
            acceptedTileStarts.get(hopSize).foreach { tileStart =>
              val effectiveStart = megaTileAgg.effectiveStart(col, smallWindowAsOfTs, currentDayStart)
              // The retained base tile can accept older late events, but the sawtooth cache represents
              // the value emitted as of smallWindowAsOfTs.
              if (tileStart >= effectiveStart && tileStart < smallWindowAsOfTs) {
                if (cachedIr == null) cachedIr = store.getCachedSmallWindowIr
                windowedAgg.columnAggregators(col).update(cachedIr, row)
                cachedIrUpdated = true
              }
            }
          }
          col += 1
        }
        if (cachedIrUpdated) {
          store.putCachedSmallWindowIr(cachedIr)
          todayDirty = true
        }
      }
    }

    // Update large window IR — route by event day
    val todayStart = currentDayStart
    val nextDayStart = todayStart + DayMillis
    val yesterdayStart = todayStart - DayMillis

    if (eventTs >= nextDayStart) {
      // Future event — clamp to today. Day transitions are watermark-driven.
      val ir = store.getLargeTodayIr
      updateLargeWindowColumns(ir, row)
      store.putLargeTodayIr(ir)
      todayDirty = true
    } else if (eventTs >= todayStart) {
      val ir = store.getLargeTodayIr
      updateLargeWindowColumns(ir, row)
      store.putLargeTodayIr(ir)
      todayDirty = true
    } else if (eventTs >= yesterdayStart) {
      // Late event from yesterday — within 2d tolerance
      val ir = store.getLargeYesterdayIr
      updateLargeWindowColumns(ir, row)
      store.putLargeYesterdayIr(ir)
      yesterdayDirty = true
    }

    EmitResult(
      todayEntry = if (todayDirty) packTodayEntry() else null,
      todayStart = todayStart,
      yesterdayEntry = if (yesterdayDirty) packYesterdayEntry() else null,
      yesterdayStart = yesterdayStart
    )
  }

  def advanceWatermark(watermarkTs: Long): Unit = {
    val currentDayStart = store.getCurrentDayStart
    if (currentDayStart == -1L) return
    val wmDay = TsUtils.round(watermarkTs, DayMillis)
    if (wmDay > currentDayStart) {
      val newYesterday =
        if (wmDay == currentDayStart + DayMillis) previousDayIrForAdjacentRollover()
        else windowedAgg.init
      store.putLargeYesterdayIr(newYesterday)
      store.putLargeTodayIr(windowedAgg.init)
      store.putCurrentDayStart(wmDay)
      // cachedSmallWindowIr is as-of sensitive. Day rollover changes each small window's
      // effective start, so rebuild from retained tiles instead of carrying yesterday's cache forward.
      rebuildCachedSmallWindowIr(watermarkTs, wmDay)
    }
  }

  def onEviction(timerTs: Long): EmitResult = {
    val earliest = store.getEarliestTileStart
    val currentDayStart = store.getCurrentDayStart
    if (!hasSmallWindows || earliest == Long.MaxValue) {
      return EmitResult(null, currentDayStart, null, currentDayStart - DayMillis)
    }

    val todayStart = currentDayStart

    rebuildCachedSmallWindowIr(timerTs, todayStart)

    EmitResult(
      todayEntry = packTodayEntry(),
      todayStart = todayStart,
      yesterdayEntry = null,
      yesterdayStart = todayStart - DayMillis
    )
  }

  def packTodayEntry(): Array[Any] = {
    val cachedIr = store.getCachedSmallWindowIr
    val largeIr = store.getLargeTodayIr
    val entry = new Array[Any](windowedAgg.length)
    var col = 0
    while (col < windowedAgg.length) {
      entry(col) = if (isNoBatch(col)) cachedIr(col) else largeIr(col)
      col += 1
    }
    entry
  }

  def packYesterdayEntry(): Array[Any] = {
    val yesterdayIr = store.getLargeYesterdayIr
    val entry = new Array[Any](windowedAgg.length)
    var col = 0
    while (col < windowedAgg.length) {
      entry(col) = yesterdayIr(col)
      col += 1
    }
    entry
  }

  private def previousDayIrForAdjacentRollover(): Array[Any] = {
    val previousDayIr = windowedAgg.clone(store.getLargeTodayIr)
    val cachedSmallIr = store.getCachedSmallWindowIr
    var col = 0
    while (col < windowedAgg.length) {
      if (isNoBatch(col)) {
        previousDayIr(col) = windowedAgg.columnAggregators(col).clone(cachedSmallIr(col))
      }
      col += 1
    }
    previousDayIr
  }

  private def updateLargeWindowColumns(ir: Array[Any], row: Row): Unit = {
    var col = 0
    while (col < windowedAgg.length) {
      if (!isNoBatch(col)) {
        windowedAgg.columnAggregators(col).update(ir, row)
      }
      col += 1
    }
  }

  private def rebuildCachedSmallWindowIr(asOfTs: Long, todayStart: Long): Unit = {
    if (!hasSmallWindows) return

    // Classify stale vs retained tiles in one iterator pass. Flink-backed TileStore decodes values
    // during iteration, so a second full scan would deserialize every retained tile again.
    val staleEntries = mutable.ArrayBuffer.empty[(Long, Long)]
    val tiles: Map[Long, mutable.Map[Long, Array[Any]]] =
      smallWindowTiers.map(hop => hop -> mutable.Map.empty[Long, Array[Any]]).toMap
    var newEarliest = Long.MaxValue
    val iter = store.tileIterator
    while (iter.hasNext) {
      val (hopSize, tileStart, ir) = iter.next()
      if (smallWindowTiers.contains(hopSize)) {
        val floor = megaTileAgg.retentionFloor(hopSize, asOfTs, todayStart)
        if (tileStart < floor) {
          staleEntries += ((hopSize, tileStart))
        } else {
          tiles(hopSize)(tileStart) = ir
          if (tileStart < newEarliest) newEarliest = tileStart
        }
      }
    }
    staleEntries.foreach { case (h, t) => store.removeTile(h, t) }
    store.putEarliestTileStart(newEarliest)

    val rebuiltIr = megaTileAgg.buildMegaTileIr(tiles, now = asOfTs, batchEnd = todayStart)
    store.putCachedSmallWindowIr(rebuiltIr)
  }
}

case class EmitResult(
    todayEntry: Array[Any],
    todayStart: Long,
    yesterdayEntry: Array[Any],
    yesterdayStart: Long
)
