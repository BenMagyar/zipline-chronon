package ai.chronon.integrations.redis

import ai.chronon.integrations.redis.GlideReadCommand.{Get, MGet, ZRangeByScore}
import ai.chronon.integrations.redis.GlideReadResult.{OptionalValues, Value, Values}
import ai.chronon.integrations.redis.RedisKVStoreConstants.{GlideExecutionClusterBatch, GlideExecutionDirect}
import glide.api.GlideClusterClient
import glide.api.models.commands.RangeOptions.{RangeByScore, ScoreBoundary}
import glide.api.models.commands.batch.{ClusterBatchOptions, ClusterBatchRetryStrategy}
import glide.api.models.{ClusterBatch, GlideString}

import java.util.ArrayDeque
import java.util.concurrent.{
  CompletableFuture,
  CompletionException,
  ExecutionException,
  Executors,
  RejectedExecutionException,
  ScheduledExecutorService,
  ScheduledFuture,
  ThreadFactory,
  TimeUnit,
  TimeoutException
}
import java.util.function.{BiConsumer, BiFunction, Function}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private[redis] sealed trait GlideReadCommand

private[redis] object GlideReadCommand {
  final case class Get(key: Array[Byte]) extends GlideReadCommand
  final case class MGet(keys: IndexedSeq[Array[Byte]]) extends GlideReadCommand
  final case class ZRangeByScore(key: Array[Byte], min: Double, max: Double) extends GlideReadCommand
}

private[redis] sealed trait GlideReadResult

private[redis] object GlideReadResult {
  final case class Value(value: Option[Array[Byte]]) extends GlideReadResult
  final case class OptionalValues(values: IndexedSeq[Option[Array[Byte]]]) extends GlideReadResult
  final case class Values(values: Seq[Array[Byte]]) extends GlideReadResult
}

private[redis] object GlideRedisReadClient {
  // These protect the benchmark-only direct path. Production reads use one cluster batch per Chronon multiGet.
  val InternalDirectMaxInflight = 1000
  val InternalDirectMaxPending = 100000
  val InternalDirectNativeInflightLimit = 10000
}

/** Binary-safe read-only adapter around GLIDE's cluster client.
  *
  * Direct mode issues one asynchronous GLIDE request per command. Cluster-batch mode sends one non-atomic
  * `ClusterBatch`, allowing GLIDE to split commands across cluster nodes while preserving response order. Both modes
  * normalize command-level failures into `Try` so callers receive exactly one result per input command.
  */
private[redis] final class GlideRedisReadClient private (
    client: GlideClusterClient,
    executionMode: String,
    directMaxInflight: Int,
    directMaxPending: Int,
    directQueueTimeoutMs: Int,
    directCommandRunnerOverride: Option[GlideReadCommand => CompletableFuture[Try[GlideReadResult]]],
    clusterBatchRunnerOverride: Option[
      IndexedSeq[GlideReadCommand] => CompletableFuture[IndexedSeq[Try[GlideReadResult]]]],
    closeAction: () => Unit)
    extends AutoCloseable {

  private[redis] def this(client: GlideClusterClient,
                          executionMode: String,
                          directMaxInflight: Int,
                          directMaxPending: Int,
                          directQueueTimeoutMs: Int) =
    this(client,
         executionMode,
         directMaxInflight,
         directMaxPending,
         directQueueTimeoutMs,
         None,
         None,
         () => client.close())

  private[redis] def this(executionMode: String,
                          directMaxInflight: Int,
                          directMaxPending: Int,
                          directQueueTimeoutMs: Int,
                          directCommandRunner: GlideReadCommand => CompletableFuture[Try[GlideReadResult]],
                          closeAction: () => Unit) =
    this(null,
         executionMode,
         directMaxInflight,
         directMaxPending,
         directQueueTimeoutMs,
         Some(directCommandRunner),
         None,
         closeAction)

  private[redis] def this(
      clusterBatchRunner: IndexedSeq[GlideReadCommand] => CompletableFuture[IndexedSeq[Try[GlideReadResult]]],
      closeAction: () => Unit) =
    this(null, GlideExecutionClusterBatch, 1, 1, 1, None, Some(clusterBatchRunner), closeAction)

  require(executionMode == GlideExecutionDirect || executionMode == GlideExecutionClusterBatch,
          s"Unsupported GLIDE execution mode: $executionMode")
  require(directMaxInflight > 0, s"GLIDE direct max inflight must be positive, got $directMaxInflight")
  require(
    directMaxPending >= directMaxInflight,
    s"GLIDE direct max pending ($directMaxPending) must be at least direct max inflight ($directMaxInflight)"
  )
  require(directQueueTimeoutMs > 0, s"GLIDE direct queue timeout must be positive, got $directQueueTimeoutMs")
  require(
    directCommandRunnerOverride.isEmpty || executionMode == GlideExecutionDirect,
    "A GLIDE direct command runner can only be injected in direct execution mode"
  )
  require(
    clusterBatchRunnerOverride.isEmpty || executionMode == GlideExecutionClusterBatch,
    "A GLIDE cluster-batch runner can only be injected in cluster-batch execution mode"
  )

  private final class PendingDirectCommand(val command: GlideReadCommand,
                                           val result: CompletableFuture[Try[GlideReadResult]],
                                           val enqueuedAtNanos: Long)

  private val directStateLock = new AnyRef
  private val pendingDirectCommands = new ArrayDeque[PendingDirectCommand]()
  private val directQueueTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(directQueueTimeoutMs.toLong)
  private val directQueueTimeoutExecutor: Option[ScheduledExecutorService] =
    if (executionMode == GlideExecutionDirect)
      Some(Executors.newSingleThreadScheduledExecutor(new ThreadFactory {
        override def newThread(runnable: Runnable): Thread = {
          val thread = new Thread(runnable, "chronon-glide-direct-queue-timeout")
          thread.setDaemon(true)
          thread
        }
      }))
    else None
  private var activeDirectCommands = 0
  private var drainingDirectCommands = false
  private var scheduledQueueHead: PendingDirectCommand = null
  private var scheduledQueueTimeout: ScheduledFuture[_] = null
  @volatile private var closed = false

  private[redis] def usesClusterBatch: Boolean = executionMode == GlideExecutionClusterBatch

  private val clusterBatchOptions = ClusterBatchOptions
    .builder()
    .retryStrategy(
      ClusterBatchRetryStrategy
        .builder()
        .retryServerError(true)
        .retryConnectionError(true)
        .build())
    .build()

  def execute(commands: IndexedSeq[GlideReadCommand]): CompletableFuture[IndexedSeq[Try[GlideReadResult]]] =
    if (commands.isEmpty) {
      CompletableFuture.completedFuture(Vector.empty)
    } else {
      executionMode match {
        case GlideExecutionDirect       => executeDirect(commands)
        case GlideExecutionClusterBatch => executeClusterBatch(commands)
      }
    }

  private def executeDirect(
      commands: IndexedSeq[GlideReadCommand]): CompletableFuture[IndexedSeq[Try[GlideReadResult]]] = {
    val commandFutures = commands.map(enqueueDirectCommand)
    CompletableFuture
      .allOf(commandFutures.map(_.asInstanceOf[CompletableFuture[_]]): _*)
      .thenApply[IndexedSeq[Try[GlideReadResult]]](new Function[Void, IndexedSeq[Try[GlideReadResult]]] {
        override def apply(ignored: Void): IndexedSeq[Try[GlideReadResult]] = commandFutures.map(_.join())
      })
  }

  private def enqueueDirectCommand(command: GlideReadCommand): CompletableFuture[Try[GlideReadResult]] = {
    val result = new CompletableFuture[Try[GlideReadResult]]()
    val rejection = directStateLock.synchronized {
      if (closed) Some(closedError())
      else if (activeDirectCommands + pendingDirectCommands.size() >= directMaxPending) {
        Some(queueFullError())
      } else {
        pendingDirectCommands.addLast(new PendingDirectCommand(command, result, System.nanoTime()))
        None
      }
    }
    rejection match {
      case Some(error) => result.complete(Failure(error))
      case None        => drainDirectCommands()
    }
    result
  }

  /** Starts queued commands without occupying a worker while they wait for an admission slot.
    *
    * The draining flag also prevents recursive submission when GLIDE completes a future synchronously.
    */
  private def drainDirectCommands(): Unit = {
    val acquiredDrainer = directStateLock.synchronized {
      if (drainingDirectCommands) false
      else {
        drainingDirectCommands = true
        true
      }
    }
    if (!acquiredDrainer) return

    var keepDraining = true
    while (keepDraining) {
      val next = directStateLock.synchronized {
        if (closed || pendingDirectCommands.isEmpty) {
          drainingDirectCommands = false
          clearScheduledQueueTimeoutLocked()
          None
        } else {
          val head = pendingDirectCommands.peekFirst()
          if (queueWaitNanos(head) >= directQueueTimeoutNanos) {
            pendingDirectCommands.removeFirst()
            clearScheduledQueueTimeoutLocked()
            Some(head -> true)
          } else if (activeDirectCommands < directMaxInflight) {
            pendingDirectCommands.removeFirst()
            clearScheduledQueueTimeoutLocked()
            activeDirectCommands += 1
            Some(head -> false)
          } else {
            drainingDirectCommands = false
            scheduleQueueTimeoutLocked(head)
            None
          }
        }
      }
      next match {
        case Some((pending, true))  => pending.result.complete(Failure(queueTimeoutError(pending)))
        case Some((pending, false)) => startDirectCommand(pending)
        case None                   => keepDraining = false
      }
    }
  }

  private def scheduleQueueTimeoutLocked(head: PendingDirectCommand): Unit = {
    if (scheduledQueueHead ne head) {
      clearScheduledQueueTimeoutLocked()
      scheduledQueueHead = head
      val remainingNanos = math.max(0L, directQueueTimeoutNanos - queueWaitNanos(head))
      scheduledQueueTimeout = directQueueTimeoutExecutor.get.schedule(
        new Runnable {
          override def run(): Unit = {
            val shouldDrain = directStateLock.synchronized {
              if (scheduledQueueHead eq head) {
                scheduledQueueHead = null
                scheduledQueueTimeout = null
                true
              } else false
            }
            if (shouldDrain) drainDirectCommands()
          }
        },
        remainingNanos,
        TimeUnit.NANOSECONDS
      )
    }
  }

  private def clearScheduledQueueTimeoutLocked(): Unit = {
    if (scheduledQueueTimeout != null) scheduledQueueTimeout.cancel(false)
    scheduledQueueTimeout = null
    scheduledQueueHead = null
  }

  private def queueWaitNanos(pending: PendingDirectCommand): Long =
    System.nanoTime() - pending.enqueuedAtNanos

  private def startDirectCommand(pending: PendingDirectCommand): Unit = {
    val commandResult =
      if (closed) CompletableFuture.completedFuture[Try[GlideReadResult]](Failure(closedError()))
      else runDirectCommand(pending.command)
    commandResult.whenComplete(new BiConsumer[Try[GlideReadResult], Throwable] {
      override def accept(result: Try[GlideReadResult], error: Throwable): Unit = {
        directStateLock.synchronized {
          activeDirectCommands -= 1
        }
        if (error == null) pending.result.complete(result)
        else pending.result.complete(Failure(unwrap(error)))
        drainDirectCommands()
      }
    })
  }

  private def runDirectCommand(command: GlideReadCommand): CompletableFuture[Try[GlideReadResult]] =
    directCommandRunnerOverride match {
      case Some(runner) =>
        try runner(command)
        catch {
          case NonFatal(error) => CompletableFuture.completedFuture(Failure(error))
        }
      case None => executeDirectCommand(command)
    }

  private def executeDirectCommand(command: GlideReadCommand): CompletableFuture[Try[GlideReadResult]] =
    capture {
      command match {
        case Get(key) =>
          client
            .get(GlideString.of(key))
            .thenApply[GlideReadResult](new Function[GlideString, GlideReadResult] {
              override def apply(value: GlideString): GlideReadResult =
                Value(Option(value).map(_.getBytes))
            })
        case MGet(keys) =>
          client
            .mget(glideStrings(keys))
            .thenApply[GlideReadResult](new Function[Array[GlideString], GlideReadResult] {
              override def apply(values: Array[GlideString]): GlideReadResult =
                OptionalValues(normalizeMGetValues(keys.size, values))
            })
        case ZRangeByScore(key, min, max) =>
          client
            .zrange(GlideString.of(key), scoreRange(min, max))
            .thenApply[GlideReadResult](new Function[Array[GlideString], GlideReadResult] {
              override def apply(values: Array[GlideString]): GlideReadResult =
                Values(Option(values).fold(Vector.empty[Array[Byte]])(_.iterator.map(_.getBytes).toVector))
            })
      }
    }

  private def executeClusterBatch(
      commands: IndexedSeq[GlideReadCommand]): CompletableFuture[IndexedSeq[Try[GlideReadResult]]] =
    clusterBatchRunnerOverride match {
      case Some(runner) =>
        try runner(commands)
        catch {
          case NonFatal(error) => completedFailures(commands.size, error)
        }
      case None => executeNativeClusterBatch(commands)
    }

  private def executeNativeClusterBatch(
      commands: IndexedSeq[GlideReadCommand]): CompletableFuture[IndexedSeq[Try[GlideReadResult]]] =
    try {
      val batch = new ClusterBatch(false).withBinaryOutput()
      commands.foreach {
        case Get(key)                     => batch.get(GlideString.of(key))
        case MGet(keys)                   => batch.mget(glideStrings(keys))
        case ZRangeByScore(key, min, max) => batch.zrange(GlideString.of(key), scoreRange(min, max))
      }

      client
        .exec(batch, false, clusterBatchOptions)
        .handle[IndexedSeq[Try[GlideReadResult]]](
          new BiFunction[Array[Object], Throwable, IndexedSeq[Try[GlideReadResult]]] {
            override def apply(responses: Array[Object], error: Throwable): IndexedSeq[Try[GlideReadResult]] =
              if (error != null) commands.map(_ => Failure(unwrap(error)))
              else normalizeBatchResponses(commands, responses)
          })
    } catch {
      case NonFatal(error) => completedFailures(commands.size, error)
    }

  private def normalizeBatchResponses(commands: IndexedSeq[GlideReadCommand],
                                      responses: Array[Object]): IndexedSeq[Try[GlideReadResult]] = {
    if (responses == null || responses.length != commands.length) {
      val actual = Option(responses).fold("null")(_.length.toString)
      completedResultFailures(
        commands.size,
        new IllegalStateException(s"GLIDE cluster batch returned $actual responses for ${commands.length} commands"))
    } else {
      commands.zip(responses).map {
        case (_, error: Throwable)        => Failure(error)
        case (Get(_), null)               => Success(Value(None))
        case (Get(_), value: GlideString) => Success(Value(Some(value.getBytes)))
        case (MGet(keys), values: Array[_]) =>
          Try(OptionalValues(normalizeMGetValues(keys.size, values)))
        case (ZRangeByScore(_, _, _), values: Array[_]) =>
          Try {
            val binaryValues = values.iterator.map {
              case value: GlideString => value.getBytes
              case other =>
                throw new IllegalStateException(
                  s"GLIDE ZRANGE batch response contained ${typeName(other)} instead of GlideString")
            }.toVector
            Values(binaryValues)
          }
        case (command, response) =>
          Failure(
            new IllegalStateException(
              s"Unexpected GLIDE response ${typeName(response)} for ${command.getClass.getSimpleName}"))
      }
    }
  }

  private def capture(operation: => CompletableFuture[GlideReadResult]): CompletableFuture[Try[GlideReadResult]] =
    try {
      operation.handle[Try[GlideReadResult]](new BiFunction[GlideReadResult, Throwable, Try[GlideReadResult]] {
        override def apply(result: GlideReadResult, error: Throwable): Try[GlideReadResult] =
          if (error == null) Success(result) else Failure(unwrap(error))
      })
    } catch {
      case NonFatal(error) => CompletableFuture.completedFuture(Failure(error))
    }

  private def scoreRange(min: Double, max: Double): RangeByScore =
    new RangeByScore(new ScoreBoundary(min), new ScoreBoundary(max))

  private def glideStrings(values: IndexedSeq[Array[Byte]]): Array[GlideString] =
    values.iterator.map(GlideString.of).toArray

  private def normalizeMGetValues(expected: Int, values: Array[_]): IndexedSeq[Option[Array[Byte]]] = {
    if (values == null || values.length != expected) {
      val actual = Option(values).fold("null")(_.length.toString)
      throw new IllegalStateException(s"GLIDE MGET returned $actual values for $expected keys")
    }
    values.iterator.map {
      case null               => None
      case value: GlideString => Some(value.getBytes)
      case other =>
        throw new IllegalStateException(s"GLIDE MGET response contained ${typeName(other)} instead of GlideString")
    }.toVector
  }

  private def completedFailures(count: Int, error: Throwable): CompletableFuture[IndexedSeq[Try[GlideReadResult]]] =
    CompletableFuture.completedFuture(completedResultFailures(count, error))

  private def completedResultFailures(count: Int, error: Throwable): IndexedSeq[Try[GlideReadResult]] =
    Vector.fill(count)(Failure(error))

  private def unwrap(error: Throwable): Throwable = error match {
    case wrapper: CompletionException if wrapper.getCause != null => unwrap(wrapper.getCause)
    case wrapper: ExecutionException if wrapper.getCause != null  => unwrap(wrapper.getCause)
    case other                                                    => other
  }

  private def typeName(value: Any): String = Option(value).fold("null")(_.getClass.getName)

  private def closedError(): IllegalStateException = new IllegalStateException("GLIDE read client is closed")

  private def queueFullError(): RejectedExecutionException =
    new RejectedExecutionException(
      s"GLIDE direct command queue is full ($directMaxPending active and queued commands maximum)")

  private def queueTimeoutError(pending: PendingDirectCommand): TimeoutException = {
    val waitedMs = TimeUnit.NANOSECONDS.toMillis(queueWaitNanos(pending))
    new TimeoutException(
      s"GLIDE direct command waited ${waitedMs}ms in the admission queue (timeout ${directQueueTimeoutMs}ms)")
  }

  override def close(): Unit = {
    val (shouldClose, queued) = directStateLock.synchronized {
      if (closed) {
        false -> Vector.empty[PendingDirectCommand]
      } else {
        closed = true
        clearScheduledQueueTimeoutLocked()
        val queued = Vector.newBuilder[PendingDirectCommand]
        while (!pendingDirectCommands.isEmpty) queued += pendingDirectCommands.removeFirst()
        true -> queued.result()
      }
    }
    queued.foreach(_.result.complete(Failure(closedError())))
    if (shouldClose) {
      directQueueTimeoutExecutor.foreach(_.shutdownNow())
      closeAction()
    }
  }
}
