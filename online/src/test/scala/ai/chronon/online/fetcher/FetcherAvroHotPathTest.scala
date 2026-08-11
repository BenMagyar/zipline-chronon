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
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CyclicBarrier, Executors, TimeUnit}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Success, Try}

class FetcherAvroHotPathTest extends AnyFlatSpec with Matchers {

  private val JoinName = "test.avro_hot_path"
  private val keySchema = StructType("avro_hot_path_key", Array(StructField("id", LongType)))
  private val valueSchema = StructType(
    "avro_hot_path_value",
    Array(StructField("score", LongType), StructField("label", StringType)))

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

  private class CountingCodecCache(codecs: Map[String, Try[JoinCodec]])
      extends TTLCache[String, Try[JoinCodec]](
        name => codecs(name),
        name => Metrics.Context(Metrics.Environment.JoinSchemaFetching, join = name)
      ) {
    val lookups = new AtomicInteger(0)

    override def apply(name: String): Try[JoinCodec] = {
      lookups.incrementAndGet()
      codecs(name)
    }
  }

  private class TestFetcher(codecCache: CountingCodecCache)
      extends Fetcher(new NoOpKvStore, Constants.MetadataDataset) {
    override lazy val joinCodecCache: TTLCache[String, Try[JoinCodec]] = codecCache
  }

  private class DecodeRejectingCodec(schema: String) extends AvroCodec(schema) {
    override def decodeRow(bytes: Array[Byte]): Array[Any] =
      throw new AssertionError("online response encoding must not immediately decode its own output")
  }

  "Fetcher response encoding" should "produce round-trippable Avro without decoding it on the write path" in {
    val codec = new DecodeRejectingCodec(AvroConversions.fromChrononSchema(valueSchema).toString)
    val fetcher = new TestFetcher(new CountingCodecCache(Map(JoinName -> Success(joinCodec()))))
    val values = Map("score" -> Long.box(42L), "label" -> "fast")

    val bytes = fetcher.encode(valueSchema, codec, values)
    val decoded = new AvroCodec(codec.schemaStr).decodeMap(bytes)

    decoded shouldBe values
  }

  it should "resolve a cached join codec once per distinct join in a batch" in {
    val otherJoin = "test.avro_hot_path_other"
    val cache = new CountingCodecCache(
      Map(JoinName -> Success(joinCodec()), otherJoin -> Success(joinCodec(otherJoin))))
    val fetcher = new TestFetcher(cache)

    val resolved = fetcher.resolveJoinCodecs(
      Seq.fill(50)(JoinName) ++ Seq.fill(50)(otherJoin),
      joinConf = None
    )

    resolved.keySet shouldBe Set(JoinName, otherJoin)
    cache.lookups.get() shouldBe 2
  }

  it should "reuse one response encoder for a same-join batch and preserve Avro string values" in {
    val codec = joinCodec()
    val cache = new CountingCodecCache(Map(JoinName -> Success(codec)))
    val fetcher = new TestFetcher(cache)
    val responses = (0 until 50).map { index =>
      val values = Map("score" -> Long.box(index.toLong), "label" -> s"label-$index")
      Response(Request(JoinName, Map("id" -> Long.box(index.toLong))), Success(values))
    }

    val resolved = fetcher.resolveJoinCodecs(responses.map(_.request.name), joinConf = None)
    val encoded = fetcher.encodeJoinResponses(responses, FeaturesResponseType.AvroString, resolved)
    val encodedBytes = fetcher.encodeJoinResponses(responses, FeaturesResponseType.AvroBytes, resolved)

    cache.lookups.get() shouldBe 1
    encoded shouldBe a[Vector[_]]
    encodedBytes shouldBe a[Vector[_]]
    encoded should have size 50
    val decoder = Fetcher.codecForCurrentThread(codec.valueCodec)
    encoded.zipWithIndex.foreach { case (response, index) =>
      val value = response.value match {
        case AvroResponseValue.AvroString(result) => result.get
        case other                                => fail(s"Expected an Avro string response, got $other")
      }
      val decoded = decoder.decodeMap(Base64.getDecoder.decode(value))
      decoded shouldBe Map("score" -> Long.box(index.toLong), "label" -> s"label-$index")
      response.errors shouldBe Success(Map.empty[String, String])
    }
  }

  it should "finish encoding a strict batch on the thread that resolved its codec" in {
    val codec = joinCodec()
    val fetcher = new TestFetcher(new CountingCodecCache(Map(JoinName -> Success(codec))))
    val resolved = fetcher.resolveJoinCodecs(Seq(JoinName), joinConf = None)
    val producers = 8
    val rowsPerProducer = 50

    val producerEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))
    val observerEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(producers))

    try {
      val encoded = Await.result(
        Future.sequence((0 until producers).map { producer =>
          Future {
            val responses = (0 until rowsPerProducer).map { row =>
              val values = Map(
                "score" -> Long.box((producer * 1000 + row).toLong),
                "label" -> s"producer$producer-row$row"
              )
              Response(Request(JoinName, Map("id" -> values("score"))), Success(values))
            }
            val batch = fetcher.encodeJoinResponses(responses, FeaturesResponseType.AvroString, resolved)
            withClue("response encoding must complete before its Future does") {
              batch shouldBe a[Vector[_]]
            }
            producer -> batch
          }(producerEc)
        }),
        60.seconds
      )

      val barrier = new CyclicBarrier(producers)
      val observed = Await.result(
        Future.sequence(encoded.map { case (producer, batch) =>
          Future {
            barrier.await(60, TimeUnit.SECONDS)
            producer -> batch.toVector
          }(observerEc)
        }),
        60.seconds
      )

      val decoder = Fetcher.codecForCurrentThread(codec.valueCodec)
      observed.foreach { case (producer, responses) =>
        responses.zipWithIndex.foreach { case (response, row) =>
          val encodedValue = response.value match {
            case AvroResponseValue.AvroString(result) => result.get
            case other                                => fail(s"Expected an Avro string response, got $other")
          }
          decoder.decodeMap(Base64.getDecoder.decode(encodedValue)) shouldBe Map(
            "score" -> Long.box((producer * 1000 + row).toLong),
            "label" -> s"producer$producer-row$row"
          )
        }
      }
    } finally {
      producerEc.shutdown()
      observerEc.shutdown()
    }
  }
}
