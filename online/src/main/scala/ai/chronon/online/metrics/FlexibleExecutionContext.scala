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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ArrayBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContextExecutor

object FlexibleExecutionContext {
  val ThreadPoolSizeProperty = "ai.chronon.threadpool.size"
  val QueueCapacityProperty = "ai.chronon.threadpool.queue.capacity"
  val KeepAliveSecondsProperty = "ai.chronon.threadpool.keepalive.seconds"

  private val DefaultQueueCapacity = 10000
  private val DefaultKeepAliveSeconds = 600

  private def readPositiveInt(prop: String, default: Int): Int = {
    val v = Option(System.getProperty(prop))
      .flatMap(raw => scala.util.Try(raw.trim.toInt).toOption)
      .getOrElse(default)
    if (v > 0) v else default
  }

  private def readNonNegativeInt(prop: String, default: Int): Int = {
    val v = Option(System.getProperty(prop))
      .flatMap(raw => scala.util.Try(raw.trim.toInt).toOption)
      .getOrElse(default)
    if (v >= 0) v else default
  }

  private def buildThreadFactory(threadPrefix: String, daemonSetting: Option[Boolean]): ThreadFactory =
    new ThreadFactory {
      private val instanceId = java.util.UUID.randomUUID().toString.take(8)
      private val counter = new AtomicInteger(0)

      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setName(s"$threadPrefix-$instanceId-${counter.incrementAndGet()}")
        daemonSetting.foreach(t.setDaemon)
        // Set the context class loader if missing for libs like Spark (used in catalyst util) that rely on it to find classes
        if (t.getContextClassLoader == null) {
          t.setContextClassLoader(getClass.getClassLoader)
        }

        t
      }
    }

  // Create a thread factory so that we can name the threads for easier debugging
  // None preserves the existing factory's inherited daemon status.
  val threadFactory: ThreadFactory = buildThreadFactory("chronon-fetcher", daemonSetting = None)

  private def newExecutor(poolSize: Int,
                          queueCapacity: Int,
                          keepAliveSeconds: Long,
                          threadFactory: ThreadFactory,
                          metricsContext: Metrics.Context): ThreadPoolExecutor =
    new InstrumentedThreadPoolExecutor(
      poolSize, // corePoolSize
      poolSize, // maxPoolSize
      keepAliveSeconds, // keepAliveTime
      TimeUnit.SECONDS, // keep alive time units
      new ArrayBlockingQueue[Runnable](queueCapacity),
      threadFactory,
      metricsContext = metricsContext
    )

  def buildExecutor(
      poolSize: Int,
      queueCapacity: Int,
      threadPrefix: String,
      metricsContext: Metrics.Context,
      daemonThreads: Boolean
  ): ThreadPoolExecutor = {
    require(poolSize > 0, s"poolSize must be positive, got $poolSize")
    require(queueCapacity > 0, s"queueCapacity must be positive, got $queueCapacity")
    newExecutor(poolSize,
                queueCapacity,
                keepAliveSeconds = 0L,
                buildThreadFactory(threadPrefix, daemonSetting = Some(daemonThreads)),
                metricsContext)
  }

  def buildExecutor(metricsContext: Metrics.Context): ThreadPoolExecutor = {
    val cores = Runtime.getRuntime.availableProcessors()
    val defaultPoolSize = cores * 4
    val poolSize = readPositiveInt(ThreadPoolSizeProperty, defaultPoolSize)
    val queueCapacity = readPositiveInt(QueueCapacityProperty, DefaultQueueCapacity)
    val keepAliveSeconds = readNonNegativeInt(KeepAliveSecondsProperty, DefaultKeepAliveSeconds)
    newExecutor(poolSize, queueCapacity, keepAliveSeconds, threadFactory, metricsContext)
  }

  lazy val buildExecutor: ThreadPoolExecutor =
    buildExecutor(InstrumentedThreadPoolExecutor.DefaultMetricsContext)

  def buildExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(buildExecutor)

  /** Creates a fresh executor for callers that need non-default threadpool metric ownership. */
  def buildExecutionContext(metricsContext: Metrics.Context): ExecutionContextExecutor =
    ExecutionContext.fromExecutor(buildExecutor(metricsContext))
}
