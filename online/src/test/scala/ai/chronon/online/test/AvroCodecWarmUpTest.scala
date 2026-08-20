package ai.chronon.online.test

import ai.chronon.api.{LongType, StructField, StructType}
import ai.chronon.online.serde.{AvroCodec, AvroConversions}
import com.linkedin.avro.fastserde.FastSerdeCache
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.DurationInt

class AvroCodecWarmUpTest extends AnyFlatSpec with Matchers {

  private def codecFor(name: String): AvroCodec =
    AvroCodec.of(AvroConversions.fromChrononSchema(StructType(name, Array(StructField("id", LongType)))).toString)

  // An isolated FastSerdeCache (not the JVM-wide singleton) so tests don't collide with each other's schema
  // fingerprints, plus a counting Executor standing in for FastSerdeCache's own compile pool: since every
  // FastSerdeCache query is itself a registration, "was X already touched by warmUp" is only observable by
  // checking whether re-querying X causes a *new* submission (count goes up - it wasn't there yet) or not
  // (count stays flat - it was already registered). Running the submitted task inline keeps the check
  // synchronous, so there's nothing to poll for in this test.
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

  private def wasAlreadyTouched(cache: FastSerdeCache, submitted: AtomicInteger, codec: AvroCodec, isSerializer: Boolean): Boolean = {
    val before = submitted.get()
    if (isSerializer) cache.getFastGenericSerializer(codec.schema) else cache.getFastGenericDeserializer(codec.schema, codec.schema)
    submitted.get() == before
  }

  // Runs submitted compile jobs on a delay, so these tests can tell apart "waited for the compile" from
  // "returned before it finished" by timing - the synchronous executor above compiles instantly either way,
  // which wouldn't exercise the wait/poll loop at all.
  private def newDelayedCache(delayMillis: Long): FastSerdeCache = {
    val executor = new Executor {
      override def execute(command: Runnable): Unit = {
        val t = new Thread(() => {
          Thread.sleep(delayMillis)
          command.run()
        })
        t.setDaemon(true)
        t.start()
      }
    }
    new FastSerdeCache(executor)
  }

  it should "touch writerCodecs as serializers and readerCodecs as deserializers, and nothing else" in {
    val (cache, submitted) = newTrackedCache()
    val writer = codecFor("warmup_writer_only")
    val reader = codecFor("warmup_reader_only")
    val untouched = codecFor("warmup_untouched")

    Await.result(AvroCodec.warmUp(writerCodecs = Seq(writer), readerCodecs = Seq(reader), cache = cache), 5.seconds)

    wasAlreadyTouched(cache, submitted, writer, isSerializer = true) shouldBe true
    wasAlreadyTouched(cache, submitted, writer, isSerializer = false) shouldBe false

    wasAlreadyTouched(cache, submitted, reader, isSerializer = false) shouldBe true
    wasAlreadyTouched(cache, submitted, reader, isSerializer = true) shouldBe false

    wasAlreadyTouched(cache, submitted, untouched, isSerializer = true) shouldBe false
    wasAlreadyTouched(cache, submitted, untouched, isSerializer = false) shouldBe false
  }

  it should "do nothing when both lists are empty" in {
    val (cache, submitted) = newTrackedCache()

    Await.result(AvroCodec.warmUp(writerCodecs = Seq.empty, readerCodecs = Seq.empty, cache = cache), 5.seconds)

    submitted.get() shouldBe 0
  }

  // The defining property of this design: calling warmUp never blocks, regardless of waitForCompileMillis -
  // only the *returned Future* takes time to resolve. The old Thread.sleep-based implementation would have
  // made this same call take ~5s to return; this proves the call itself is now always cheap.
  it should "return a Future immediately without blocking the calling thread, even when a wait is requested" in {
    val cache = newDelayedCache(delayMillis = 500L)
    val writer = codecFor("warmup_call_returns_immediately")

    val start = System.currentTimeMillis()
    val future = AvroCodec.warmUp(writerCodecs = Seq(writer), readerCodecs = Seq.empty, waitForCompileMillis = 5000L, cache = cache)
    val callReturnedAfterMillis = System.currentTimeMillis() - start

    callReturnedAfterMillis should be < 100L

    Await.result(future, 6.seconds)
  }

  it should "wait up to its own deadline, then give up, for a compile that's still in flight" in {
    // Compile job takes far longer than the wait budget below - simulates a slow or stuck compile.
    val cache = newDelayedCache(delayMillis = 10000L)
    val writer = codecFor("warmup_wait_deadline")

    val start = System.currentTimeMillis()
    Await.result(
      AvroCodec.warmUp(writerCodecs = Seq(writer), readerCodecs = Seq.empty, waitForCompileMillis = 300L, cache = cache),
      2.seconds
    )
    val elapsed = System.currentTimeMillis() - start

    // Never blocks past its own deadline, even though the compile is still in flight.
    elapsed should (be >= 300L and be < 2000L)
  }

  it should "return promptly without waiting when waitForCompileMillis is left at its 0 default" in {
    val cache = newDelayedCache(delayMillis = 10000L)
    val writer = codecFor("warmup_no_wait_default")

    val start = System.currentTimeMillis()
    Await.result(AvroCodec.warmUp(writerCodecs = Seq(writer), readerCodecs = Seq.empty, cache = cache), 1.second)
    val elapsed = System.currentTimeMillis() - start

    elapsed should be < 1000L
  }

  // The actual scalability property motivating a non-blocking wait: firing many concurrent waits costs
  // lightweight scheduled callbacks, not one held thread each - so N waits of the same duration complete in
  // ~that duration, not N times it (which a thread-per-wait design bounded by a small pool would exhibit).
  it should "let many concurrent waits resolve in ~wait-time, not N times it" in {
    val cache = newDelayedCache(delayMillis = 300L)
    val codecs = (1 to 50).map(i => codecFor(s"warmup_concurrent_$i"))

    val start = System.currentTimeMillis()
    val futures = codecs.map(codec =>
      AvroCodec.warmUp(writerCodecs = Seq(codec), readerCodecs = Seq.empty, waitForCompileMillis = 2000L, cache = cache))
    Await.result(Future.sequence(futures), 5.seconds)
    val elapsed = System.currentTimeMillis() - start

    // A fully serialized (thread-per-wait, one at a time) design would take 50 * 300ms = 15s; a design bounded
    // by a small worker pool (e.g. 20) would still take multiple rounds. 4s gives generous headroom for test
    // environment jitter while still being far below either of those - this only fails if waits are actually
    // being serialized in some way, not from ordinary scheduling noise.
    elapsed should be < 4000L
  }
}
