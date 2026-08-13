package ai.chronon.integrations.redis

import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{
  CompletableFuture,
  RejectedExecutionException,
  ScheduledFuture,
  ScheduledThreadPoolExecutor,
  ThreadFactory,
  TimeUnit
}

private[redis] final case class RedisReadAdmissionConfig(maxInflightBatches: Int,
                                                         maxQueuedBatches: Int,
                                                         queueTimeoutMillis: Long,
                                                         maxQueuedControlBatches: Int = 128,
                                                         controlQueueTimeoutMillis: Long = 2000L) {
  require(maxInflightBatches > 0, s"maxInflightBatches must be positive, got $maxInflightBatches")
  require(maxQueuedBatches >= 0, s"maxQueuedBatches must be non-negative, got $maxQueuedBatches")
  require(queueTimeoutMillis > 0L, s"queueTimeoutMillis must be positive, got $queueTimeoutMillis")
  require(
    maxQueuedControlBatches >= 0,
    s"maxQueuedControlBatches must be non-negative, got $maxQueuedControlBatches"
  )
  require(
    controlQueueTimeoutMillis > 0L,
    s"controlQueueTimeoutMillis must be positive, got $controlQueueTimeoutMillis"
  )
}

private[redis] sealed trait RedisReadClass {
  def metricName: String
}

private[redis] object RedisReadClass {
  case object Feature extends RedisReadClass {
    override val metricName: String = "feature"
  }

  case object Control extends RedisReadClass {
    override val metricName: String = "control"
  }
}

private[redis] abstract class RedisReadAdmissionRejectedException(message: String, val reason: String)
    extends RejectedExecutionException(message) {
  // These are expected overload results. Avoid allocating a stack while the process is already under pressure.
  override def fillInStackTrace(): Throwable = this
}

private[redis] final class RedisReadAdmissionQueueFullException(message: String)
    extends RedisReadAdmissionRejectedException(message, "queue_full")

private[redis] final class RedisReadAdmissionQueueTimeoutException(message: String)
    extends RedisReadAdmissionRejectedException(message, "queue_timeout")

private[redis] final class RedisReadAdmissionCooldownException(message: String)
    extends RedisReadAdmissionRejectedException(message, "timeout_cooldown")

private[redis] final class RedisReadGlideTimeoutException(message: String)
    extends RedisReadAdmissionRejectedException(message, "glide_timeout")

private[redis] final class RedisReadAdmissionClosedException(message: String)
    extends RedisReadAdmissionRejectedException(message, "controller_closed")

private[redis] final case class RedisReadAdmissionSnapshot(inflightBatches: Int, queuedBatches: Int)

private[redis] final case class RedisReadAdmissionCooldown(flushedBatches: Int, snapshot: RedisReadAdmissionSnapshot)

private[redis] final case class RedisReadAdmissionRequest(future: CompletableFuture[RedisReadAdmissionPermit],
                                                          queued: Boolean)

private[redis] object RedisReadAdmissionController {
  private val controllerIds = new AtomicInteger()

  private def newTimeoutScheduler(): ScheduledThreadPoolExecutor = {
    val controllerId = controllerIds.incrementAndGet()
    val scheduler = new ScheduledThreadPoolExecutor(
      1,
      new ThreadFactory {
        override def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(runnable, s"chronon-redis-read-admission-$controllerId")
          thread.setDaemon(true)
          thread
        }
      }
    )
    scheduler.setRemoveOnCancelPolicy(true)
    scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
    scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false)
    scheduler
  }

  /** Prevent callers from cancelling an already-reserved slot and leaking it before a permit is delivered. */
  private final class AdmissionFuture[T] extends CompletableFuture[T] {
    def succeed(value: T): Boolean = super.complete(value)
    def fail(error: Throwable): Boolean = super.completeExceptionally(error)

    override def complete(value: T): Boolean = false
    override def completeExceptionally(error: Throwable): Boolean = false
    override def cancel(mayInterruptIfRunning: Boolean): Boolean = false
    override def obtrudeValue(value: T): Unit =
      throw new UnsupportedOperationException("Redis admission futures cannot be externally completed")
    override def obtrudeException(error: Throwable): Unit =
      throw new UnsupportedOperationException("Redis admission futures cannot be externally completed")
  }
}

/** A bounded, asynchronous priority bulkhead around whole GLIDE ClusterBatch lifecycles.
  *
  * Queue entries contain only a promise and timestamps. No thread blocks while waiting, and no native GLIDE work is
  * created until a permit is handed off. Control reads share the same total capacity and are handed released slots
  * before feature reads. The owner must release each permit after planning, native execution,
  * completion-queue wait, and JVM response decoding have all terminated.
  */
private[redis] final class RedisReadAdmissionController(val config: RedisReadAdmissionConfig,
                                                        monotonicNanos: () => Long = () => System.nanoTime(),
                                                        timeoutScheduler: ScheduledThreadPoolExecutor =
                                                          RedisReadAdmissionController.newTimeoutScheduler())
    extends AutoCloseable {

  import RedisReadAdmissionController.AdmissionFuture

  private final class Waiter(val readClass: RedisReadClass, val enqueuedAtNanos: Long) {
    val future = new AdmissionFuture[RedisReadAdmissionPermit]()
    var timeout: ScheduledFuture[_] = _
    var settled = false
  }

  private var inflightBatches = 0
  private var closed = false
  private var cooldownUntilNanos = 0L
  private val featureWaiters = new ArrayDeque[Waiter]()
  private val controlWaiters = new ArrayDeque[Waiter]()

  def acquire(
      readClass: RedisReadClass = RedisReadClass.Feature
  ): Either[RedisReadAdmissionRejectedException, RedisReadAdmissionRequest] = synchronized {
    val now = monotonicNanos()
    if (closed) Left(new RedisReadAdmissionClosedException("Redis read admission controller is closed"))
    else if (readClass == RedisReadClass.Feature && now < cooldownUntilNanos)
      Left(
        new RedisReadAdmissionCooldownException(
          s"Redis read admission is cooling down for ${remainingCooldownMillis(now)}ms"))
    else if (canAdmitUnsafe()) immediateAdmission(readClass)
    else if (queueUnsafe(readClass).size() >= maxQueuedUnsafe(readClass)) Left(queueFullException(readClass))
    else enqueue(readClass)
  }

  def snapshot: RedisReadAdmissionSnapshot = synchronized {
    snapshotUnsafe()
  }

  /** Stops replacement work after GLIDE has timed out but may still be draining written native requests. */
  def openCooldown(cooldownMillis: Long): RedisReadAdmissionCooldown = {
    require(cooldownMillis > 0L, s"cooldownMillis must be positive, got $cooldownMillis")
    val (queued, result) = synchronized {
      if (closed) Vector.empty[Waiter] -> RedisReadAdmissionCooldown(0, snapshotUnsafe())
      else {
        cooldownUntilNanos = math.max(
          cooldownUntilNanos,
          monotonicNanos() + TimeUnit.MILLISECONDS.toNanos(cooldownMillis)
        )
        val builder = Vector.newBuilder[Waiter]
        while (!featureWaiters.isEmpty) {
          val waiter = featureWaiters.removeFirst()
          waiter.settled = true
          waiter.timeout.cancel(false)
          builder += waiter
        }
        val drained = builder.result()
        drained -> RedisReadAdmissionCooldown(drained.size, snapshotUnsafe())
      }
    }
    queued.foreach(
      _.future.fail(new RedisReadAdmissionCooldownException("Redis read admission entered timeout cooldown")))
    result
  }

  private def immediateAdmission(readClass: RedisReadClass): Right[Nothing, RedisReadAdmissionRequest] = {
    inflightBatches += 1
    val future = new AdmissionFuture[RedisReadAdmissionPermit]()
    future.succeed(new RedisReadAdmissionPermit(this, readClass, wasQueued = false, queueWaitMillis = 0L))
    Right(RedisReadAdmissionRequest(future, queued = false))
  }

  private def enqueue(
      readClass: RedisReadClass): Either[RedisReadAdmissionRejectedException, RedisReadAdmissionRequest] = {
    val waiter = new Waiter(readClass, monotonicNanos())
    val queue = queueUnsafe(readClass)
    queue.addLast(waiter)
    try {
      waiter.timeout = timeoutScheduler.schedule(
        new Runnable {
          override def run(): Unit = timeout(waiter)
        },
        queueTimeoutUnsafe(readClass),
        TimeUnit.MILLISECONDS
      )
      Right(RedisReadAdmissionRequest(waiter.future, queued = true))
    } catch {
      case _: RejectedExecutionException =>
        queue.removeLastOccurrence(waiter)
        waiter.settled = true
        Left(new RedisReadAdmissionClosedException("Redis read admission controller is closing"))
    }
  }

  private def timeout(waiter: Waiter): Unit = {
    val shouldFail = synchronized {
      if (waiter.settled || !queueUnsafe(waiter.readClass).remove(waiter)) false
      else {
        waiter.settled = true
        true
      }
    }
    if (shouldFail) {
      val waitedMillis = elapsedMillis(waiter.enqueuedAtNanos)
      waiter.future.fail(
        new RedisReadAdmissionQueueTimeoutException(
          s"Redis ${waiter.readClass.metricName} read admission timed out after ${waitedMillis}ms " +
            s"(limit ${queueTimeoutUnsafe(waiter.readClass)}ms)"))
    }
  }

  private[redis] def release(permit: RedisReadAdmissionPermit): Option[RedisReadAdmissionSnapshot] = {
    var handoff: (Waiter, RedisReadAdmissionPermit) = null
    val releasedSnapshot = synchronized {
      if (permit.released) None
      else {
        require(inflightBatches > 0, "Redis read admission invariant violated: releasing with no in-flight batches")
        permit.released = true
        inflightBatches -= 1

        val waiter = if (closed) null else nextWaiterUnsafe()
        if (waiter != null) {
          waiter.settled = true
          waiter.timeout.cancel(false)
          inflightBatches += 1
          handoff = waiter -> new RedisReadAdmissionPermit(
            this,
            waiter.readClass,
            wasQueued = true,
            queueWaitMillis = elapsedMillis(waiter.enqueuedAtNanos)
          )
        }
        Some(snapshotUnsafe())
      }
    }

    // CompletableFuture continuations may run inline; never execute user work while holding the controller lock.
    if (handoff != null) {
      val completed = handoff._1.future.succeed(handoff._2)
      require(completed, "Redis read admission invariant violated: queued permit future was already completed")
    }
    releasedSnapshot
  }

  override def close(): Unit = {
    val queued = synchronized {
      if (closed) Vector.empty[Waiter]
      else {
        closed = true
        val builder = Vector.newBuilder[Waiter]
        drainUnsafe(controlWaiters, builder)
        drainUnsafe(featureWaiters, builder)
        builder.result()
      }
    }
    timeoutScheduler.shutdownNow()
    queued.foreach(_.future.fail(new RedisReadAdmissionClosedException("Redis read admission controller was closed")))
  }

  private def elapsedMillis(startedAtNanos: Long): Long =
    math.max(0L, TimeUnit.NANOSECONDS.toMillis(monotonicNanos() - startedAtNanos))

  private def remainingCooldownMillis(nowNanos: Long): Long =
    math.max(0L, TimeUnit.NANOSECONDS.toMillis(cooldownUntilNanos - nowNanos))

  private def canAdmitUnsafe(): Boolean = inflightBatches < config.maxInflightBatches

  private def nextWaiterUnsafe(): Waiter =
    if (!controlWaiters.isEmpty) controlWaiters.removeFirst()
    else if (
      monotonicNanos() >= cooldownUntilNanos &&
      !featureWaiters.isEmpty
    ) featureWaiters.removeFirst()
    else null

  private def queueUnsafe(readClass: RedisReadClass): ArrayDeque[Waiter] = readClass match {
    case RedisReadClass.Feature => featureWaiters
    case RedisReadClass.Control => controlWaiters
  }

  private def maxQueuedUnsafe(readClass: RedisReadClass): Int = readClass match {
    case RedisReadClass.Feature => config.maxQueuedBatches
    case RedisReadClass.Control => config.maxQueuedControlBatches
  }

  private def queueTimeoutUnsafe(readClass: RedisReadClass): Long = readClass match {
    case RedisReadClass.Feature => config.queueTimeoutMillis
    case RedisReadClass.Control => config.controlQueueTimeoutMillis
  }

  private def drainUnsafe(queue: ArrayDeque[Waiter],
                          builder: scala.collection.mutable.Builder[Waiter, Vector[Waiter]]): Unit =
    while (!queue.isEmpty) {
      val waiter = queue.removeFirst()
      waiter.settled = true
      waiter.timeout.cancel(false)
      builder += waiter
    }

  private def queueFullException(readClass: RedisReadClass): RedisReadAdmissionQueueFullException =
    new RedisReadAdmissionQueueFullException(
      s"Redis ${readClass.metricName} read admission queue is full " +
        s"(${queueUnsafe(readClass).size()}/${maxQueuedUnsafe(readClass)} queued, " +
        s"$inflightBatches/${config.maxInflightBatches} total batches in flight)")

  private def snapshotUnsafe(): RedisReadAdmissionSnapshot =
    RedisReadAdmissionSnapshot(inflightBatches, featureWaiters.size() + controlWaiters.size())
}

private[redis] final class RedisReadAdmissionPermit private[redis] (controller: RedisReadAdmissionController,
                                                                    val readClass: RedisReadClass,
                                                                    val wasQueued: Boolean,
                                                                    val queueWaitMillis: Long) {
  private[redis] var released = false

  def release(): Option[RedisReadAdmissionSnapshot] = controller.release(this)
}
