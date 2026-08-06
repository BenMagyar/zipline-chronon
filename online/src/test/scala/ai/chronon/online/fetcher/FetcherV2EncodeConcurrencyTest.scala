package ai.chronon.online.fetcher

import ai.chronon.api.Extensions.JoinOps
import ai.chronon.api.{Builders, Constants, LongType, StringType, StructField, StructType}
import ai.chronon.online.KVStore.{GetRequest, GetResponse, PutRequest}
import ai.chronon.online.fetcher.Fetcher.{AvroResponseValue, Request, Response}
import ai.chronon.online.metrics.{Metrics, TTLCache}
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import ai.chronon.online.{JoinCodec, KVStore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.Base64
import java.util.concurrent.{CyclicBarrier, Executors, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Success, Try}

class FetcherV2EncodeConcurrencyTest extends AnyFlatSpec with Matchers {

  private val JoinName = "test.v2_encode_concurrency"
  private val keySchema = StructType("v2_encode_concurrency_key", Array(StructField("id", LongType)))
  private val valueSchema = StructType(
    "v2_encode_concurrency_value",
    Array(StructField("score", LongType), StructField("label", StringType)))

  private val Producers = 8
  private val ProducerThreads = 2
  private val RowsPerProducer = 50
  private val Rounds = 20

  private class NoOpKvStore extends KVStore {
    override def create(dataset: String): Unit = ()
    override def multiGet(requests: Seq[GetRequest]): Future[Seq[GetResponse]] = Future.successful(Seq.empty)
    override def multiPut(requests: Seq[PutRequest]): Future[Seq[Boolean]] = Future.successful(Seq.empty)
    override def bulkPut(sourceOfflineTable: String, destinationOnlineDataSet: String, partition: String): Unit = ()
  }

  private def joinCodec(joinName: String = JoinName): JoinCodec = {
    val join = Builders.Join(metaData = Builders.MetaData(name = joinName))
    JoinCodec(
      new JoinOps(join),
      keySchema,
      valueSchema,
      AvroCodec.of(AvroConversions.fromChrononSchema(keySchema).toString),
      AvroCodec.of(AvroConversions.fromChrononSchema(valueSchema).toString),
      Array.empty
    )
  }

  private class StaticCodecCache(codecs: Map[String, Try[JoinCodec]])
      extends TTLCache[String, Try[JoinCodec]](
        name => codecs(name),
        name => Metrics.Context(Metrics.Environment.JoinSchemaFetching, join = name)
      ) {
    override def apply(name: String): Try[JoinCodec] = codecs(name)
  }

  private class TestFetcher(codecCache: StaticCodecCache)
      extends Fetcher(new NoOpKvStore, Constants.MetadataDataset) {
    override lazy val joinCodecCache: TTLCache[String, Try[JoinCodec]] = codecCache
  }

  private def expectedValues(producer: Int, row: Int): Map[String, AnyRef] =
    Map("score" -> Long.box((producer * 1000 + row).toLong), "label" -> s"producer$producer-row$row")

  "Fetcher v2 response encoding" should "stay correct when batches are forced on threads other than the encoder's" in {
    val codec = joinCodec()
    val fetcher = new TestFetcher(new StaticCodecCache(Map(JoinName -> Success(codec))))
    val resolved = fetcher.resolveJoinCodecs(Seq(JoinName), joinConf = None)

    val producerPool = Executors.newFixedThreadPool(ProducerThreads)
    val forcePool = Executors.newFixedThreadPool(Producers)
    val producerEc = ExecutionContext.fromExecutorService(producerPool)
    val forceEc = ExecutionContext.fromExecutorService(forcePool)

    try {
      val problems = (0 until Rounds).flatMap { round =>
        val encodedF = Future.sequence((0 until Producers).map { producer =>
          Future {
            val responses = (0 until RowsPerProducer).map { row =>
              val values = expectedValues(producer, row)
              Response(Request(JoinName, Map("id" -> values("score"))), Success(values))
            }
            producer -> fetcher.encodeJoinResponses(responses, FeaturesResponseType.AvroString, resolved)
          }(producerEc)
        })
        val encoded = Await.result(encodedF, 60.seconds)

        val barrier = new CyclicBarrier(Producers)
        val forcedF = Future.sequence(encoded.map { case (producer, lazyResponses) =>
          Future {
            barrier.await(60, TimeUnit.SECONDS)
            producer -> Try(lazyResponses.toVector)
          }(forceEc)
        })
        val forced = Await.result(forcedF, 60.seconds)

        val decoder = Fetcher.codecForCurrentThread(codec.valueCodec)
        forced.flatMap {
          case (producer, scala.util.Failure(exception)) =>
            Seq(s"round $round producer $producer failed to force: $exception")
          case (producer, Success(responses)) =>
            responses.zipWithIndex.flatMap { case (response, row) =>
              val decoded = Try {
                response.value match {
                  case AvroResponseValue.AvroString(result) =>
                    decoder.decodeMap(Base64.getDecoder.decode(result.get))
                  case other => sys.error(s"expected an Avro string response, got $other")
                }
              }
              val expected = expectedValues(producer, row)
              decoded match {
                case Success(actual) if actual == expected => Seq.empty
                case Success(actual) =>
                  Seq(s"round $round producer $producer row $row decoded $actual, expected $expected")
                case scala.util.Failure(exception) =>
                  Seq(s"round $round producer $producer row $row failed to decode: $exception")
              }
            }
        }
      }

      withClue(s"${problems.size} corrupted rows, first few:\n${problems.take(10).mkString("\n")}\n") {
        problems shouldBe empty
      }
    } finally {
      producerEc.shutdown()
      forceEc.shutdown()
    }
  }
}
