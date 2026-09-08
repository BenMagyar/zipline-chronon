package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.RowAggregator

import scala.collection.mutable

/** In-memory TileStore backed by mutable maps. No serde overhead.
  * Used for testing and for the MegaTileStreamProcessorTest simulation.
  */
class InMemoryTileStore(windowedAgg: RowAggregator) extends TileStore {
  val tiles: mutable.Map[(Long, Long), Array[Any]] = mutable.Map.empty

  var cachedSmallWindowIr: Array[Any] = windowedAgg.init
  var largeTodayIr: Array[Any] = windowedAgg.init
  var largeYesterdayIr: Array[Any] = windowedAgg.init
  var currentDayStart: Long = -1L
  var earliestTileStart: Long = Long.MaxValue

  override def getTile(hopSize: Long, tileStart: Long): Array[Any] = tiles.getOrElse((hopSize, tileStart), null)
  override def putTile(hopSize: Long, tileStart: Long, ir: Array[Any]): Unit = tiles((hopSize, tileStart)) = ir
  override def removeTile(hopSize: Long, tileStart: Long): Unit = tiles.remove((hopSize, tileStart))
  override def tileIterator: Iterator[(Long, Long, Array[Any])] =
    tiles.iterator.map { case ((h, t), ir) => (h, t, ir) }

  override def getCachedSmallWindowIr: Array[Any] = cachedSmallWindowIr
  override def putCachedSmallWindowIr(ir: Array[Any]): Unit = cachedSmallWindowIr = ir

  override def getLargeTodayIr: Array[Any] = largeTodayIr
  override def putLargeTodayIr(ir: Array[Any]): Unit = largeTodayIr = ir

  override def getLargeYesterdayIr: Array[Any] = largeYesterdayIr
  override def putLargeYesterdayIr(ir: Array[Any]): Unit = largeYesterdayIr = ir

  override def getCurrentDayStart: Long = currentDayStart
  override def putCurrentDayStart(ts: Long): Unit = currentDayStart = ts

  override def getEarliestTileStart: Long = earliestTileStart
  override def putEarliestTileStart(ts: Long): Unit = earliestTileStart = ts
}
