package ai.chronon.online.test

import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Builders, GroupBy, GroupByServingInfo, Operation, TimeUnit, Window}
import ai.chronon.online.GroupByServingInfoParsed
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, TimeUnit => JavaTimeUnit}

class GroupByServingInfoParsedTest extends AnyFlatSpec with Matchers {

  private def utc(s: String): Long = java.time.Instant.parse(s).toEpochMilli

  // eager members of the parsed class only need a groupBy to exist, not a full conf
  private def servingInfo: GroupByServingInfo = new GroupByServingInfo().setGroupBy(new GroupBy())

  "batchEndTsMillis" should "prefer the explicit watermark and fall back to ds parsing" in {
    // explicit thrift watermark wins, even over a deliberately inconsistent ds
    val explicit = servingInfo
      .setBatchEndTs(utc("2026-06-03T04:00:00Z"))
      .setBatchEndDate("1999-01-01")
      .setDateFormat("yyyy-MM-dd")
    new GroupByServingInfoParsed(explicit).batchEndTsMillis should be(utc("2026-06-03T04:00:00Z"))

    // uploads from older versions: daily ds parse
    val daily = servingInfo
      .setBatchEndDate("2026-06-03")
      .setDateFormat("yyyy-MM-dd")
    new GroupByServingInfoParsed(daily).batchEndTsMillis should be(utc("2026-06-03T00:00:00Z"))

    // sub-daily ds values parse via the partition interval and offset fields
    val subDaily = servingInfo
      .setBatchEndDate("2026-06-03-04-00")
      .setDateFormat("yyyy-MM-dd-HH-mm")
      .setPartitionInterval(new Window(3, TimeUnit.HOURS))
      .setPartitionOffset(new Window(1, TimeUnit.HOURS))
    new GroupByServingInfoParsed(subDaily).batchEndTsMillis should be(utc("2026-06-03T04:00:00Z"))
  }

  "streamingTileGrid" should "keep the native tile size and align daily tiles to the partition offset" in {
    val daily = new GroupByServingInfoParsed(
      servingInfo
        .setPartitionInterval(new Window(1, TimeUnit.DAYS))
        .setPartitionOffset(new Window(1, TimeUnit.HOURS)))

    daily.streamingTileGrid.spanMillis should be(new Window(1, TimeUnit.DAYS).millis)
    daily.streamingTileGrid.offsetMillis should be(new Window(1, TimeUnit.HOURS).millis)
    daily.streamingTileGrid.leftBound(utc("2026-06-03T00:30:00Z")) should be(utc("2026-06-02T01:00:00Z"))
    daily.streamingTileGrid.leftBound(utc("2026-06-03T01:30:00Z")) should be(utc("2026-06-03T01:00:00Z"))

    val hourlyGroupBy = new GroupBy().setAggregations(
      Seq(Builders.Aggregation(Operation.SUM, "value", Seq(new Window(1, TimeUnit.DAYS)))).toJava)
    val hourly = new GroupByServingInfoParsed(
      new GroupByServingInfo()
        .setGroupBy(hourlyGroupBy)
        .setPartitionInterval(new Window(1, TimeUnit.DAYS))
        .setPartitionOffset(new Window(1, TimeUnit.HOURS)))

    hourly.streamingTileGrid.spanMillis should be(new Window(1, TimeUnit.HOURS).millis)
    hourly.streamingTileGrid.offsetMillis should be(0L)
  }

  "codec accessors" should "reuse a codec on one thread without sharing mutable codec state across threads" in {
    val parsed = GroupByDerivationsTest.makeTestGroupByServingInfoParsed()
    val currentThreadCodec = parsed.keyCodec

    (parsed.keyCodec eq currentThreadCodec) shouldBe true

    val otherThreadCodec = new AtomicReference[ai.chronon.online.serde.AvroCodec]()
    val executor = Executors.newSingleThreadExecutor()
    try {
      executor.submit(new Runnable {
        override def run(): Unit = otherThreadCodec.set(parsed.keyCodec)
      }).get(10, JavaTimeUnit.SECONDS)
    } finally {
      executor.shutdownNow()
    }

    otherThreadCodec.get() should not be theSameInstanceAs(currentThreadCodec)
    otherThreadCodec.get().schemaStr shouldBe currentThreadCodec.schemaStr
  }
}
