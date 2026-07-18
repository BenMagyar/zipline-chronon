package ai.chronon.online.fetcher

import ai.chronon.api.{LongType, StringType, StructField, StructType}
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import org.apache.avro.generic.GenericRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.util.concurrent.{Callable, CyclicBarrier, Executors, TimeUnit}

class FetcherCodecConcurrencyTest extends AnyFlatSpec {

  private class ThreadBoundAvroCodec(schemaStr: String, ownerThread: Thread) extends AvroCodec(schemaStr) {
    private def requireOwnerThread(): Unit =
      if (Thread.currentThread() ne ownerThread) {
        throw new IllegalStateException("The cached AvroCodec was used from a different thread")
      }

    override def encodeBinary(record: GenericRecord): Array[Byte] = {
      requireOwnerThread()
      super.encodeBinary(record)
    }

    override def decodeRow(bytes: Array[Byte]): Array[Any] = {
      requireOwnerThread()
      super.decodeRow(bytes)
    }
  }

  it should "resolve cached codecs before encoding on a worker thread" in {
    val schema = StructType(
      "concurrent_fetcher_encode",
      Array(StructField("id", LongType), StructField("value", StringType))
    )
    val schemaStr = AvroConversions.fromChrononSchema(schema).toString
    val cachedCodec = new ThreadBoundAvroCodec(schemaStr, Thread.currentThread())
    val workerCount = 4
    val executor = Executors.newFixedThreadPool(workerCount)
    val startBarrier = new CyclicBarrier(workerCount)

    try {
      val futures = (0 until workerCount).map { workerIndex =>
        executor.submit(new Callable[AvroCodec] {
          override def call(): AvroCodec = {
            startBarrier.await(10, TimeUnit.SECONDS)
            val expectedId = Long.box(workerIndex.toLong)
            val expectedValue = s"value-$workerIndex"
            val localCodec = Fetcher.codecForCurrentThread(cachedCodec)
            val data = schema.castArr(Map("id" -> expectedId, "value" -> expectedValue))
            val record =
              AvroConversions.fromChrononRow(data, schema, localCodec.schema).asInstanceOf[GenericRecord]
            val decoded = localCodec.decodeMap(localCodec.encodeBinary(record))

            decoded("id") shouldEqual expectedId
            decoded("value") shouldEqual expectedValue
            localCodec
          }
        })
      }

      val codecs = futures.map(_.get(10, TimeUnit.SECONDS))
      codecs.foreach(_ should not be theSameInstanceAs(cachedCodec))
      codecs.combinations(2).foreach { pair =>
        pair.head should not be theSameInstanceAs(pair.last)
      }
    } finally {
      executor.shutdownNow()
    }
  }
}
