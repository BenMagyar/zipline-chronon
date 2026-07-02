package ai.chronon.flink

import ai.chronon.flink.types.AvroCodecOutput
import ai.chronon.flink.types.WriteResponse
import ai.chronon.online.Api
import ai.chronon.online.KVStore
import ai.chronon.online.KVStore.PutRequest
import com.codahale.metrics.ExponentiallyDecayingReservoir
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.{Counter, Histogram}
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

object AsyncKVStoreWriter {
  val kvStoreConcurrency = 10
  val defaultTimeoutMillis = 1000L

  def withUnorderedWaits(inputDS: DataStream[AvroCodecOutput],
                         kvStoreWriterFn: RichAsyncFunction[AvroCodecOutput, WriteResponse],
                         featureGroupName: String,
                         timeoutMillis: Long = defaultTimeoutMillis,
                         capacity: Int = kvStoreConcurrency): DataStream[WriteResponse] = {

    AsyncDataStream
      .unorderedWait(
        inputDS,
        kvStoreWriterFn,
        timeoutMillis,
        TimeUnit.MILLISECONDS,
        capacity
      )
      .uid(s"kvstore-writer-async-$featureGroupName")
      .name(
        s"async kvstore writes for $featureGroupName(concurrency per task = $capacity, timeout = $timeoutMillis ms)")
      .setParallelism(inputDS.getParallelism)
  }

  private val ExecutionContextInstance: ExecutionContext = new DirectExecutionContext
}

/** Async Flink writer function to help us write to the KV store.
  * @param onlineImpl - Instantiation of the Chronon API to help create KV store objects
  * @param featureGroupName Name of the FG we're writing to
  */
class AsyncKVStoreWriter(onlineImpl: Api, featureGroupName: String, enableDebug: Boolean = false)
    extends RichAsyncFunction[AvroCodecOutput, WriteResponse]
    with FlinkLogging {

  @transient private var kvStore: KVStore = _

  @transient private var errorCounter: Counter = _
  @transient private var successCounter: Counter = _
  @transient private var putTimeHistogram: Histogram = _

  // The context used for the future callbacks
  implicit lazy val executor: ExecutionContext = AsyncKVStoreWriter.ExecutionContextInstance

  // One may want to use different KV stores depending on whether tiling is on.
  // The untiled version of Chronon works on "append" store semantics, and the tiled version works on "overwrite".
  protected def getKVStore: KVStore = {
    onlineImpl.genKvStore
  }

  override def open(configuration: Configuration): Unit = {
    val group = getRuntimeContext.getMetricGroup
      .addGroup("chronon")
      .addGroup("feature_group", featureGroupName)
    errorCounter = group.counter("kvstore_writer.errors")
    successCounter = group.counter("kvstore_writer.successes")
    putTimeHistogram = group.histogram(
      "multiput_time",
      new DropwizardHistogramWrapper(
        new com.codahale.metrics.Histogram(new ExponentiallyDecayingReservoir())
      )
    )

    kvStore = getKVStore
  }

  override def timeout(input: AvroCodecOutput, resultFuture: ResultFuture[WriteResponse]): Unit = {
    logThrottled(ERROR, "kvstore_timeout", s"Timed out writing to KV Store for object: $input")
    errorCounter.inc()
    resultFuture.complete(
      util.Arrays.asList[WriteResponse](
        new WriteResponse(input.keyBytes,
                          input.valueBytes,
                          input.dataset,
                          input.tsMillis,
                          status = false,
                          input.startProcessingTime)))
  }

  override def asyncInvoke(input: AvroCodecOutput, resultFuture: ResultFuture[WriteResponse]): Unit = {
    val putRequest = PutRequest(input.keyBytes, input.valueBytes, input.dataset, Some(input.tsMillis))

    if (enableDebug) {
      log(
        INFO,
        s"""
           |Writing to KVStore with request:
           |dataset=${putRequest.dataset}
           |tsMillis=${putRequest.tsMillis}
           |keyBytes=${java.util.Base64.getEncoder.encodeToString(putRequest.keyBytes)}
           |valueBytes=${java.util.Base64.getEncoder.encodeToString(putRequest.valueBytes)}
        """.stripMargin
      )
    }

    val startTime = System.currentTimeMillis()
    val resultFutureRequested: Future[Seq[Boolean]] = kvStore.multiPut(Seq(putRequest))
    resultFutureRequested.onComplete {
      case Success(l) =>
        putTimeHistogram.update(System.currentTimeMillis() - startTime)
        val succeeded = l.forall(identity)
        if (succeeded) {
          successCounter.inc()
        } else {
          errorCounter.inc()
          logThrottled(ERROR, "kvstore_write_fail", s"Failed to write to KVStore for object: $input")
        }
        resultFuture.complete(
          util.Arrays.asList[WriteResponse](
            new WriteResponse(input.keyBytes,
                              input.valueBytes,
                              input.dataset,
                              input.tsMillis,
                              succeeded,
                              input.startProcessingTime)))
      case Failure(exception) =>
        // this should be rare and indicates we have an uncaught exception
        // in the KVStore - we log the exception and skip the object to
        // not fail the app
        putTimeHistogram.update(System.currentTimeMillis() - startTime)
        errorCounter.inc()
        logThrottled(ERROR, "kvstore_exception", s"Caught exception writing to KVStore for object: $input", exception)
        resultFuture.complete(
          util.Arrays.asList[WriteResponse](
            new WriteResponse(input.keyBytes,
                              input.valueBytes,
                              input.dataset,
                              input.tsMillis,
                              status = false,
                              input.startProcessingTime)))
    }
  }
}
