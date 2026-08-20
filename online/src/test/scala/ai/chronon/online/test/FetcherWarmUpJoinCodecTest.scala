package ai.chronon.online.test

import ai.chronon.api.Extensions.{GroupByOps, JoinOps}
import ai.chronon.api.{Builders, Constants, LongType, StructField, StructType, ThriftJsonCodec}
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.metrics.{Metrics, TTLCache}
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import ai.chronon.online.{InMemoryKvStore, JoinCodec}
import com.linkedin.avro.fastserde.FastSerdeCache
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Await
import scala.concurrent.duration.{DurationInt, DurationLong}
import scala.util.{Success, Try}

class FetcherWarmUpJoinCodecTest extends AnyFlatSpec with Matchers {

  private def putString(kvStore: InMemoryKvStore, key: String, value: String, dataset: String): Unit = {
    Await.result(kvStore.put(PutRequest(key.getBytes(Constants.UTF8), value.getBytes(Constants.UTF8), dataset)), 1.second)
  }

  // An isolated FastSerdeCache (not the JVM-wide singleton) so tests don't collide with each other's schema
  // fingerprints, plus a counting Executor standing in for FastSerdeCache's own compile pool: since every
  // FastSerdeCache query is itself a registration, "was X already touched by warmUpJoinCodec" is only
  // observable by checking whether re-querying X causes a *new* submission (count goes up - it wasn't there
  // yet) or not (count stays flat - it was already registered). Running the submitted task inline keeps the
  // check synchronous, so there's nothing to poll for in these tests.
  private def newTrackedCache(): (FastSerdeCache, AtomicInteger) = {
    val submitted = new AtomicInteger(0)
    val executor = new Executor {
      override def execute(command: Runnable): Unit = {
        submitted.incrementAndGet()
        command.run()
      }
    }
    (new FastSerdeCache(executor), submitted)
  }

  // True if schema was already registered with cache (querying it didn't submit a new compile job).
  private def wasAlreadyTouched(cache: FastSerdeCache, submitted: AtomicInteger, schema: org.apache.avro.Schema, isSerializer: Boolean): Boolean = {
    val before = submitted.get()
    if (isSerializer) cache.getFastGenericSerializer(schema) else cache.getFastGenericDeserializer(schema, schema)
    submitted.get() == before
  }

  private val JoinName = "warmup_test.tracking_join"
  private val keySchema = StructType("warmup_key", Array(StructField("id", LongType)))
  private val baseValueSchema = StructType("warmup_base_value", Array(StructField("raw_score", LongType)))

  private def trackingJoinCodec(): JoinCodec = {
    val join = Builders.Join(metaData = Builders.MetaData(name = JoinName))
    val keyCodec = AvroCodec.of(AvroConversions.fromChrononSchema(keySchema).toString)
    val baseValueCodec = AvroCodec.of(AvroConversions.fromChrononSchema(baseValueSchema).toString)
    JoinCodec(new JoinOps(join), keySchema, baseValueSchema, keyCodec, baseValueCodec, Array.empty)
    // valueCodec is computed by JoinCodec itself (its own `valueSchema`/`valueCodec` lazy vals) from
    // baseValueSchema - this join has no derivations configured, so valueSchema just wraps baseValueSchema's
    // fields under a new name ("join_derived_<cleanName>" vs baseValueSchema's own name), giving valueCodec a
    // distinct underlying schema from baseValueCodec despite identical fields - letting the test tell apart
    // "touched" (valueCodec) from "never touched" (baseValueCodec).
  }

  private class TestFetcher(joinCodec: JoinCodec)
      extends Fetcher(InMemoryKvStore.build(s"FetcherWarmUpJoinCodecTest_tracking_${System.nanoTime()}"), Constants.MetadataDataset) {
    override lazy val joinCodecCache: TTLCache[String, Try[JoinCodec]] =
      new TTLCache[String, Try[JoinCodec]](
        _ => Success(joinCodec),
        name => Metrics.Context(Metrics.Environment.JoinSchemaFetching, join = name)
      )
  }

  it should "touch only the join-level codec directions actually used on the serving path" in {
    val codec = trackingJoinCodec()
    val fetcher = new TestFetcher(codec)
    val (cache, submitted) = newTrackedCache()

    val result = fetcher.warmUpJoinCodec(JoinName, cache = cache)
    result.isSuccess shouldBe true

    // keyCodec: serializer touched (logging key hash), deserializer never used
    wasAlreadyTouched(cache, submitted, codec.keyCodec.schema, isSerializer = true) shouldBe true
    wasAlreadyTouched(cache, submitted, codec.keyCodec.schema, isSerializer = false) shouldBe false

    // valueCodec (derived): serializer touched (fetchJoinV2/encodeJoinResponses + logging), deserializer never used
    wasAlreadyTouched(cache, submitted, codec.valueCodec.schema, isSerializer = true) shouldBe true
    wasAlreadyTouched(cache, submitted, codec.valueCodec.schema, isSerializer = false) shouldBe false

    // baseValueCodec: never used anywhere on the serving path, so neither direction should be touched
    wasAlreadyTouched(cache, submitted, codec.baseValueCodec.schema, isSerializer = true) shouldBe false
    wasAlreadyTouched(cache, submitted, codec.baseValueCodec.schema, isSerializer = false) shouldBe false
  }

  // The waitForCompileMillis/deadline mechanics themselves (respects the deadline, doesn't wait when 0, never
  // blocks the calling thread, etc.) are pure AvroCodec.warmUp behavior - see AvroCodecWarmUpTest, which
  // exercises them directly without any Fetcher/Join/GroupBy involvement, including the non-blocking-call and
  // many-concurrent-waits properties. What's worth covering here is just that warmUpJoinCodec's Try[Future[Unit]]
  // wiring - the outer Try for resolve+trigger, the inner Future for the wait - is plumbed through correctly.
  it should "return an awaitable Future reflecting the wait, through the full Try[Future[Unit]] signature" in {
    val codec = trackingJoinCodec()
    val fetcher = new TestFetcher(codec)
    val (cache, _) = newTrackedCache() // synchronous executor - compiles settle immediately once touched

    val waitForCompileMillis = 2000L
    val result = fetcher.warmUpJoinCodec(JoinName, waitForCompileMillis = waitForCompileMillis, cache = cache)

    result.isSuccess shouldBe true
    // Await.result's own timeout must exceed the wait's *own* configured deadline - the Future can't settle
    // before that regardless of whether the underlying compiles finish, so this needs headroom above it.
    noException should be thrownBy Await.result(result.get, (waitForCompileMillis + 2000L).millis)
  }

  it should "resolve, touch, and warm up a real join with a real groupBy end-to-end without throwing" in {
    val kvStore = InMemoryKvStore.build(s"FetcherWarmUpJoinCodecTest_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    servingInfo.groupBy.metaData.setOnline(true)
    val batchDataset = new GroupByOps(servingInfo.groupBy).batchDataset
    kvStore.create(batchDataset)

    putString(kvStore, servingInfo.groupBy.keyNameForKvStore, ThriftJsonCodec.toJsonStr(servingInfo.groupBy), Constants.MetadataDataset)
    putString(kvStore, Constants.GroupByServingInfoKey, ThriftJsonCodec.toJsonStr(servingInfo), batchDataset)

    val joinName = s"warmup_test.join_${System.nanoTime()}"
    val join = Builders.Join(
      metaData = Builders.MetaData(name = joinName),
      joinParts = Seq(Builders.JoinPart(groupBy = servingInfo.groupBy, keyMapping = Map("id" -> "id")))
    )

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    Await.result(fetcher.metadataStore.putJoinConf(join), 1.second)
    val (cache, submitted) = newTrackedCache()

    // Resolves the join conf + the groupBy's serving info via real (in-memory) KV lookups, builds the join
    // codec, and touches every join- and groupBy-level codec in the direction it's actually used - all
    // without throwing.
    fetcher.warmUpJoinCodec(joinName, cache = cache).isSuccess shouldBe true

    val keySchema = new org.apache.avro.Schema.Parser().parse(servingInfo.keyAvroSchema)
    val selectedSchema = new org.apache.avro.Schema.Parser().parse(servingInfo.selectedAvroSchema)
    wasAlreadyTouched(cache, submitted, keySchema, isSerializer = true) shouldBe true
    wasAlreadyTouched(cache, submitted, keySchema, isSerializer = false) shouldBe false
    wasAlreadyTouched(cache, submitted, selectedSchema, isSerializer = false) shouldBe true
    wasAlreadyTouched(cache, submitted, selectedSchema, isSerializer = true) shouldBe false
  }

  it should "fail gracefully for an unknown join, without throwing" in {
    val kvStore = InMemoryKvStore.build(s"FetcherWarmUpJoinCodecTest_unknown_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)
    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)

    noException should be thrownBy fetcher.warmUpJoinCodec("does.not.exist")
    fetcher.warmUpJoinCodec("does.not.exist").isFailure shouldBe true
  }

  it should "still succeed for the join even when one of its groupBys has no servable info" in {
    val kvStore = InMemoryKvStore.build(s"FetcherWarmUpJoinCodecTest_missing_gb_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    servingInfo.groupBy.metaData.setOnline(true)
    // Create the batch dataset but never write the serving info - mirrors a groupBy that was referenced by
    // the join conf but never actually uploaded.
    kvStore.create(new GroupByOps(servingInfo.groupBy).batchDataset)

    val joinName = s"warmup_test.join_missing_gb_${System.nanoTime()}"
    val join = Builders.Join(
      metaData = Builders.MetaData(name = joinName),
      joinParts = Seq(Builders.JoinPart(groupBy = servingInfo.groupBy, keyMapping = Map("id" -> "id")))
    )

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    Await.result(fetcher.metadataStore.putJoinConf(join), 1.second)

    // The join's own codecs still resolve and get touched; the missing groupBy is skipped with a log line,
    // not a failure of the whole call.
    fetcher.warmUpJoinCodec(joinName).isSuccess shouldBe true
  }
}
