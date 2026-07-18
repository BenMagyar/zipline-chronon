package ai.chronon.online.test

import ai.chronon.online.LoggableResponse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.util.concurrent.{Callable, CyclicBarrier, Executors, TimeUnit}

class LoggableResponseSerDeSpec extends AnyFlatSpec {

  it should "correct handle loggable response round trip" in {
    val keyBytes = "testKey".getBytes("UTF-8")
    val valueBytes = "testValue".getBytes("UTF-8")
    val joinName = "test.join"
    val schemaHash = "abcd"
    val loggableResponse = LoggableResponse(keyBytes, valueBytes, joinName, 123L, schemaHash)

    val avroBytes = LoggableResponse.toAvroBytes(loggableResponse)
    avroBytes should not be null

    val deserializedResponse = LoggableResponse.fromAvroBytes(avroBytes)
    deserializedResponse should not be null
    deserializedResponse.keyBytes shouldEqual keyBytes
    deserializedResponse.valueBytes shouldEqual valueBytes
    deserializedResponse.joinName shouldEqual joinName
    deserializedResponse.tsMillis shouldEqual 123L
    deserializedResponse.schemaHash shouldEqual schemaHash
  }

  it should "tack on schema registry bytes when schema ID is provided" in {
    val keyBytes = "testKey".getBytes("UTF-8")
    val valueBytes = "testValue".getBytes("UTF-8")
    val joinName = "test.join"
    val schemaHash = "abcd"
    val schemaId = 42
    val loggableResponse = LoggableResponse(keyBytes, valueBytes, joinName, 123L, schemaHash)
    val avroBytes = LoggableResponse.toAvroBytes(loggableResponse)
    val avroBytesWithSchemaId = LoggableResponse.prependSchemaRegistryBytes(schemaId, avroBytes)
    avroBytesWithSchemaId should not be null

    val skippedAvroBytes = avroBytesWithSchemaId.drop(5) // drop the first 5 bytes (magic byte + schema ID)
    val deserializedResponse = LoggableResponse.fromAvroBytes(skippedAvroBytes)
    deserializedResponse should not be null
    deserializedResponse.keyBytes shouldEqual keyBytes
    deserializedResponse.valueBytes shouldEqual valueBytes
    deserializedResponse.joinName shouldEqual joinName
    deserializedResponse.tsMillis shouldEqual 123L
    deserializedResponse.schemaHash shouldEqual schemaHash
  }

  it should "serialize loggable responses safely across threads" in {
    val workerCount = 8
    val iterations = 250
    val executor = Executors.newFixedThreadPool(workerCount)
    val startBarrier = new CyclicBarrier(workerCount)

    try {
      val futures = (0 until workerCount).map { workerIndex =>
        executor.submit(new Callable[Unit] {
          override def call(): Unit = {
            startBarrier.await(10, TimeUnit.SECONDS)

            (0 until iterations).foreach { iteration =>
              val marker = workerIndex * iterations + iteration
              val keyBytes = Array.tabulate[Byte](2048)(index => (marker + index).toByte)
              val valueBytes = Array.tabulate[Byte](4096)(index => (marker * 3 + index).toByte)
              val response = LoggableResponse(
                keyBytes,
                valueBytes,
                s"test.join.$workerIndex.$iteration",
                marker.toLong,
                s"schema-$marker"
              )

              val decoded = LoggableResponse.fromAvroBytes(LoggableResponse.toAvroBytes(response))
              decoded.keyBytes shouldEqual response.keyBytes
              decoded.valueBytes shouldEqual response.valueBytes
              decoded.joinName shouldEqual response.joinName
              decoded.tsMillis shouldEqual response.tsMillis
              decoded.schemaHash shouldEqual response.schemaHash
            }
          }
        })
      }

      futures.foreach(_.get(30, TimeUnit.SECONDS))
    } finally {
      executor.shutdownNow()
    }
  }
}
