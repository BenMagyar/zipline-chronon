package ai.chronon.aggregator.windowing

/** Abstraction over tile and IR state storage for MegaTileStreamProcessor.
  *
  * Flink implements this with MapState/ValueState + codec (serde on access).
  * Tests implement with mutable.Map (in-memory, no serde).
  * The processor calls these inline — no bulk restore/persist cycle.
  */
trait TileStore {
  def getTile(hopSize: Long, tileStart: Long): Array[Any]
  def putTile(hopSize: Long, tileStart: Long, ir: Array[Any]): Unit
  def removeTile(hopSize: Long, tileStart: Long): Unit
  def tileIterator: Iterator[(Long, Long, Array[Any])]

  def getCachedSmallWindowIr: Array[Any]
  def putCachedSmallWindowIr(ir: Array[Any]): Unit

  def getLargeTodayIr: Array[Any]
  def putLargeTodayIr(ir: Array[Any]): Unit

  def getLargeYesterdayIr: Array[Any]
  def putLargeYesterdayIr(ir: Array[Any]): Unit

  def getCurrentDayStart: Long
  def putCurrentDayStart(ts: Long): Unit

  def getEarliestTileStart: Long
  def putEarliestTileStart(ts: Long): Unit
}
