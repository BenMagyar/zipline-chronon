/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online.metrics

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import java.util.concurrent.{CountDownLatch, RejectedExecutionException, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

class TTLCacheTest extends AnyFlatSpec with Matchers {

  private def testContext: Metrics.Context = Metrics.Context(environment = "test")

  // Refreshes run on TTLCache's shared executor, so assertions on their effect must poll.
  private def awaitCond(timeoutMillis: Long = 2000L)(cond: => Boolean): Boolean = {
    val deadline = System.currentTimeMillis() + timeoutMillis
    while (System.currentTimeMillis() < deadline && !cond) Thread.sleep(10)
    cond
  }

  // A loader whose next result is controlled via an AtomicReference; counts invocations.
  private def controllableCache(now: AtomicLong,
                                calls: AtomicInteger,
                                result: AtomicReference[Try[String]]): TTLCache[String, Try[String]] =
    new TTLCache[String, Try[String]](
      f = { _ =>
        calls.incrementAndGet()
        result.get()
      },
      contextBuilder = _ => testContext,
      ttlMillis = 100L,
      nowFunc = () => now.get(),
      // jitter off so expiry is exactly ttlMillis and timing is deterministic in the test
      refreshJitterRatio = 0.0,
      isValid = (t: Try[String]) => t.isSuccess
    )

  "TTLCache" should "keep serving the last-known-good value when a refresh fails" in {
    val now = new AtomicLong(0L)
    val calls = new AtomicInteger(0)
    val result = new AtomicReference[Try[String]](Success("good"))
    val cache = controllableCache(now, calls, result)

    cache("k") shouldBe Success("good") // cold load caches Success
    val loadsAfterColdStart = calls.get()

    // refreshes now fail
    result.set(Failure(new RuntimeException("boom")))
    now.set(10000L) // push the entry well past its ttl

    cache("k") shouldBe Success("good") // returns stale-good synchronously and enqueues the async refresh
    awaitCond()(calls.get() > loadsAfterColdStart) shouldBe true // the refresh actually ran (and failed)

    now.set(20000L)
    cache("k") shouldBe Success("good") // still the good value, not the Failure — no poisoning
  }

  it should "recover and serve the fresh value once refreshes succeed again" in {
    val now = new AtomicLong(0L)
    val calls = new AtomicInteger(0)
    val result = new AtomicReference[Try[String]](Success("v1"))
    val cache = controllableCache(now, calls, result)

    cache("k") shouldBe Success("v1")
    val loadsAfterColdStart = calls.get()

    result.set(Failure(new RuntimeException("boom")))
    now.set(10000L)
    cache("k")
    awaitCond()(calls.get() > loadsAfterColdStart) shouldBe true
    cache("k") shouldBe Success("v1") // held stale through the failed refresh

    result.set(Success("v2"))
    now.set(20000L)
    awaitCond()(cache("k") == Success("v2")) shouldBe true // eventually picks up the fresh value
  }

  it should "surface the failure for a cold key that has never loaded successfully" in {
    // serve-stale needs a prior good value; a key that has never loaded cannot serve stale and must fail.
    val now = new AtomicLong(0L)
    val cache = new TTLCache[String, Try[String]](
      f = _ => Failure(new RuntimeException("cold miss")),
      contextBuilder = _ => testContext,
      nowFunc = () => now.get(),
      isValid = (t: Try[String]) => t.isSuccess
    )
    cache("cold").isFailure shouldBe true
  }

  it should "retry a failed cold load at the refresh cadence instead of caching it for the full ttl" in {
    // now starts well past ttl so a ts=0 (failed cold) entry reads as already-expired, mirroring wall-clock time.
    val now = new AtomicLong(1000L)
    val calls = new AtomicInteger(0)
    val result = new AtomicReference[Try[String]](Failure(new RuntimeException("cold boom")))
    val cache = controllableCache(now, calls, result)

    cache("k").isFailure shouldBe true // cold load fails; stamped as already-expired (ts = 0), not cached for ttl

    // backing store recovers; because the failed cold entry is stamped ts=0 it is eligible for re-fetch right away
    // via the normal apply() path, without waiting the full ttl.
    result.set(Success("good"))
    awaitCond()(cache("k") == Success("good")) shouldBe true
  }

  it should "leave a shared fetcher worker available for a Future awaited by the refresh creator" in {
    val sharedExecutor = FlexibleExecutionContext.buildExecutor
    val workerCount = sharedExecutor.getMaximumPoolSize
    workerCount should be > 1
    val blockerCount = workerCount - 1
    val blockersStarted = new CountDownLatch(blockerCount)
    val releaseBlockers = new CountDownLatch(1)
    val blockersFinished = new CountDownLatch(blockerCount)

    (0 until blockerCount).foreach { _ =>
      sharedExecutor.execute(new Runnable {
        override def run(): Unit = {
          blockersStarted.countDown()
          try releaseBlockers.await()
          finally blockersFinished.countDown()
        }
      })
    }

    try {
      blockersStarted.await(10, TimeUnit.SECONDS) shouldBe true

      val now = new AtomicLong(0L)
      val result = new AtomicReference[Try[String]](Success("v1"))
      val refreshThreadName = new AtomicReference[String]()
      val calls = new AtomicInteger(0)
      val cache = new TTLCache[String, Try[String]](
        f = { _ =>
          if (calls.incrementAndGet() == 1) {
            result.get()
          } else {
            refreshThreadName.set(Thread.currentThread().getName)
            Await.result(
              Future(result.get())(FlexibleExecutionContext.buildExecutionContext),
              1.second
            )
          }
        },
        contextBuilder = _ => testContext,
        nowFunc = () => now.get(),
        refreshIntervalMillis = 1L,
        refreshJitterRatio = 0.0,
        isValid = (value: Try[String]) => value.isSuccess
      )

      cache("k") shouldBe Success("v1")
      result.set(Success("v2"))
      now.set(2L)

      cache.refresh("k") shouldBe Success("v1")
      awaitCond()(cache("k") == Success("v2")) shouldBe true
      refreshThreadName.get() should startWith("chronon-ttl-cache-refresh-")
    } finally {
      releaseBlockers.countDown()
      blockersFinished.await(10, TimeUnit.SECONDS) shouldBe true
    }
  }

  it should "serve stale and allow retry when refresh submission is rejected" in {
    val now = new AtomicLong(0L)
    val result = new AtomicReference[Try[String]](Success("v1"))
    val calls = new AtomicInteger(0)
    val submissions = new AtomicInteger(0)
    val cache = new TTLCache[String, Try[String]](
      f = { _ =>
        calls.incrementAndGet()
        result.get()
      },
      contextBuilder = _ => testContext,
      ttlMillis = 100L,
      nowFunc = () => now.get(),
      refreshJitterRatio = 0.0,
      isValid = (value: Try[String]) => value.isSuccess
    ) {
      override private[metrics] def executeRefresh(task: Runnable): Unit = {
        if (submissions.incrementAndGet() == 1) throw new RejectedExecutionException("test rejection")
        task.run()
      }
    }

    cache("k") shouldBe Success("v1")
    result.set(Success("v2"))
    now.set(1000L)

    cache("k") shouldBe Success("v1")
    calls.get() shouldBe 1
    submissions.get() shouldBe 1

    cache("k") shouldBe Success("v1")
    calls.get() shouldBe 2
    submissions.get() shouldBe 2
    cache("k") shouldBe Success("v2")
  }

  it should "reject an out-of-range refreshJitterRatio at construction" in {
    def build(ratio: Double): TTLCache[String, Try[String]] =
      new TTLCache[String, Try[String]](
        f = _ => Success("v"),
        contextBuilder = _ => testContext,
        refreshJitterRatio = ratio
      )

    build(0.0) // lower bound is valid
    build(0.5) // in range is valid
    an[IllegalArgumentException] should be thrownBy build(1.0) // whole interval jittered away
    an[IllegalArgumentException] should be thrownBy build(2.0)
    an[IllegalArgumentException] should be thrownBy build(-0.1)
    an[IllegalArgumentException] should be thrownBy build(Double.NaN)
    an[IllegalArgumentException] should be thrownBy build(Double.PositiveInfinity)
  }
}
