package ai.chronon.flink.test

import ai.chronon.flink.source.BatchIrSourceBuilder
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.{
  Boundedness,
  Source,
  SourceReader,
  SourceReaderContext,
  SourceSplit,
  SplitEnumerator,
  SplitEnumeratorContext
}
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.data.RowData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Shaped like IcebergSource: generic in T, not ResultTypeQueryable. Never executed — only Flink's type extraction
  * at graph construction is exercised, which is where the Iceberg source failed.
  */
class ErasedSource[T] extends Source[T, SourceSplit, Void] {
  override def getBoundedness: Boundedness = Boundedness.CONTINUOUS_UNBOUNDED
  override def createReader(ctx: SourceReaderContext): SourceReader[T, SourceSplit] =
    throw new UnsupportedOperationException
  override def createEnumerator(ctx: SplitEnumeratorContext[SourceSplit]): SplitEnumerator[SourceSplit, Void] =
    throw new UnsupportedOperationException
  override def restoreEnumerator(ctx: SplitEnumeratorContext[SourceSplit],
                                 checkpoint: Void): SplitEnumerator[SourceSplit, Void] =
    throw new UnsupportedOperationException
  override def getSplitSerializer: SimpleVersionedSerializer[SourceSplit] = throw new UnsupportedOperationException
  override def getEnumeratorCheckpointSerializer: SimpleVersionedSerializer[Void] =
    throw new UnsupportedOperationException
}

class BatchIrSourceBuilderTest extends AnyFlatSpec with Matchers {

  private val watermark = WatermarkStrategy.noWatermarks[RowData]()

  "a generic Source attached with the 3-arg fromSource" should "fail type extraction at the first downstream operator" in {
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    val stream = env.fromSource(new ErasedSource[RowData], watermark, "erased")
    an[InvalidTypesException] should be thrownBy stream.map(row => row)
  }

  "BatchIrSourceBuilder.attachRowDataSource" should "give the stream RowData type information" in {
    val env = StreamExecutionEnvironment.createLocalEnvironment(1)
    val stream = BatchIrSourceBuilder.attachRowDataSource(env, new ErasedSource[RowData], watermark, "typed")
    stream.getType shouldBe TypeInformation.of(classOf[RowData])
    noException should be thrownBy stream.map(row => row)
  }
}
