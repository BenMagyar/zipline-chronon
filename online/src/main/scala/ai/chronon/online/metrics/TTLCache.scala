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

import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.function

object TTLCache {
  val DefaultTtlMillis: Long = 2 * 60 * 60 * 1000 // 2 hours
  private[TTLCache] val executor = FlexibleExecutionContext.buildExecutor
}
// can continuously grow, only used for schemas
// has two methods apply & refresh. Apply uses a longer ttl before updating than refresh
// Four 9's of availability is 8.64 secs of downtime per day. Batch uploads happen once per day
// we choose 8 secs as the refresh interval. Refresh is to be used when an exception happens and we want to re-fetch.
class TTLCache[I, O](f: I => O,
                     contextBuilder: I => Metrics.Context,
                     ttlMillis: Long = TTLCache.DefaultTtlMillis,
                     nowFunc: () => Long = { () => System.currentTimeMillis() },
                     refreshIntervalMillis: Long = 8 * 1000, // 8 seconds
                     onCreateFunc: Option[O => Unit] = None,
                     // A background refresh that fails - the creator throws, or returns a value rejected by
                     // isValid - must NOT evict a previously cached valid value. We keep serving the
                     // last-known-good instead. The default accepts everything, which is correct for creators
                     // that throw on failure (a throw never reaches the cache). Caches whose creator returns a
                     // Failure sentinel (e.g. Try) must pass `_.isSuccess`, otherwise a single backing-store
                     // blip during refresh poisons a good entry and every subsequent read serves the failure.
                     isValid: O => Boolean = (_: O) => true,
                     // Deterministic per-key jitter shrinks the effective interval by up to this fraction so
                     // entries loaded together (e.g. at startup) don't all expire on the same tick and stampede
                     // the backing store when they refresh.
                     refreshJitterRatio: Double = 0.1) {

  // Fail fast on a misconfigured ratio: >= 1.0 would jitter away the whole interval (refresh on every read).
  // The range check also rejects NaN/Infinity, since any comparison with NaN is false and the infinities fall
  // outside [0, 1). Reject at construction rather than silently degrading.
  require(
    refreshJitterRatio >= 0.0 && refreshJitterRatio < 1.0,
    s"refreshJitterRatio must be in [0, 1), got $refreshJitterRatio"
  )

  private def wrappedCreator(i: I): O = {
    val result = f(i)
    onCreateFunc.foreach(func => func(result))
    result
  }

  case class Entry(value: O, updatedAtMillis: Long, var markedForUpdate: AtomicBoolean = new AtomicBoolean(false))
  @transient implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  private val updateWhenNull =
    new function.BiFunction[I, Entry, Entry] {
      override def apply(t: I, u: Entry): Entry = {
        val now = nowFunc()
        if (u == null) {
          val result = wrappedCreator(t)
          // A failed cold load is stamped as already-expired (ts = 0) so the next read re-attempts it at the
          // refresh cadence instead of caching the failure for the full ttl (potentially hours). Once it loads
          // successfully it is stamped normally and cached for ttl.
          val ts = if (isValid(result)) now else 0L
          Entry(result, ts)
        } else {
          u
        }
      }
    }

  val cMap = new ConcurrentHashMap[I, Entry]()

  // Stable per-key offset in [0, intervalMillis * refreshJitterRatio) subtracted from the interval, so an
  // entry expires slightly early. Deterministic in the key so a given entry's cadence stays stable, and 0 when
  // the interval is 0 (force) so forced refreshes remain immediate.
  private def effectiveInterval(i: I, intervalMillis: Long): Long = {
    val jitterWindow = (intervalMillis * refreshJitterRatio).toLong
    if (jitterWindow <= 0 || jitterWindow >= intervalMillis) intervalMillis
    else intervalMillis - Math.floorMod(i.hashCode.toLong, jitterWindow)
  }

  // use the fact that cache update is not immediately necessary during regular reads
  // sync update would block the calling threads on every update
  private def asyncUpdateOnExpiry(i: I, intervalMillis: Long): O = {
    val entry = cMap.get(i)
    if (entry == null) {
      // block all concurrent callers of this key only on the very first read
      val entry = cMap.compute(i, updateWhenNull)
      contextBuilder(i).increment("cache.insert")
      entry.value
    } else {
      if (
        (nowFunc() - entry.updatedAtMillis > effectiveInterval(i, intervalMillis)) &&
        // CAS so that update is enqueued only once per expired entry
        entry.markedForUpdate.compareAndSet(false, true)
      ) {
        // enqueue async update and return old value
        TTLCache.executor.execute(new Runnable {
          override def run(): Unit = {
            try {
              val updated = wrappedCreator(i)
              if (isValid(updated)) {
                cMap.put(i, Entry(updated, nowFunc()))
                contextBuilder(i).increment("cache.update")
              } else {
                // refresh produced an invalid value (e.g. a Failure): keep the last-known-good entry and let a
                // later read re-enqueue the refresh once the interval elapses again.
                cMap.get(i).markedForUpdate.compareAndSet(true, false)
                contextBuilder(i).increment("cache.refresh_failure")
              }
            } catch {
              case ex: Exception =>
                // creator threw: keep serving the stale value, reset the mark so another read can retry
                cMap.get(i).markedForUpdate.compareAndSet(true, false)
                contextBuilder(i).increment("cache.refresh_failure")
                contextBuilder(i).incrementException(ex)
            }
          }
        })
      }
      entry.value
    }
  }

  def apply(i: I): O = asyncUpdateOnExpiry(i, ttlMillis)
  // manually refresh entry with a lower interval
  def refresh(i: I): O = asyncUpdateOnExpiry(i, refreshIntervalMillis)
  def force(i: I): O = asyncUpdateOnExpiry(i, 0)
}
