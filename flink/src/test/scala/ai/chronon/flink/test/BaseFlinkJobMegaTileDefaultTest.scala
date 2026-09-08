package ai.chronon.flink.test

import ai.chronon.flink.BaseFlinkJob
import ai.chronon.flink.types.WriteResponse
import ai.chronon.online.GroupByServingInfoParsed
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Custom subclass that only implements runTiledGroupByJob — simulates a vendor / fork
  * subclass that pre-dates MegaTile and doesn't opt into mega-tiled execution.
  */
private class LegacyOnlyFlinkJob(val groupByName: String) extends BaseFlinkJob {
  override def groupByServingInfoParsed: GroupByServingInfoParsed = null
  override def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = null
}

class BaseFlinkJobMegaTileDefaultTest extends AnyFlatSpec with Matchers {

  it should "default runMegaTiledGroupByJob impl throws so MegaTile opt-in is explicit" in {
    val job = new LegacyOnlyFlinkJob("legacy_test_groupby")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val ex = the[UnsupportedOperationException] thrownBy job.runMegaTiledGroupByJob(env)
    ex.getMessage should include("LegacyOnlyFlinkJob")
    ex.getMessage should include("legacy_test_groupby")
    ex.getMessage should include("STREAMING_MEGATILES")
  }

  it should "let subclasses that implement runMegaTiledGroupByJob keep working" in {
    val sentinel: DataStream[WriteResponse] = null
    val job = new BaseFlinkJob {
      override def groupByName: String = "implements_megatile"
      override def groupByServingInfoParsed: GroupByServingInfoParsed = null
      override def runTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = sentinel
      override def runMegaTiledGroupByJob(env: StreamExecutionEnvironment): DataStream[WriteResponse] = sentinel
    }
    noException should be thrownBy job.runMegaTiledGroupByJob(StreamExecutionEnvironment.getExecutionEnvironment)
  }
}
