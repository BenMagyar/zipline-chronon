package ai.chronon.online.metrics

import org.slf4j.LoggerFactory

import java.util.concurrent.{
  BlockingQueue,
  Executors,
  ScheduledExecutorService,
  ThreadFactory,
  ThreadPoolExecutor,
  TimeUnit
}

object InstrumentedThreadPoolExecutor {
  lazy val DefaultMetricsContext: Metrics.Context =
    Metrics.Context(Metrics.Environment.Fetcher).withSuffix("threadpool")
}

class InstrumentedThreadPoolExecutor(corePoolSize: Int,
                                     maximumPoolSize: Int,
                                     keepAliveTime: Long,
                                     unit: TimeUnit,
                                     workQueue: BlockingQueue[Runnable],
                                     threadFactory: ThreadFactory,
                                     metricsIntervalSeconds: Int = 15,
                                     protected val metricsContext: Metrics.Context =
                                       InstrumentedThreadPoolExecutor.DefaultMetricsContext)
    extends ThreadPoolExecutor(
      corePoolSize,
      maximumPoolSize,
      keepAliveTime,
      unit,
      workQueue,
      threadFactory
    ) {
  // Reporter for periodic metrics
  private val metricsReporter: ScheduledExecutorService = buildMetricsScheduledExecutor()

  private val logger = LoggerFactory.getLogger(classOf[InstrumentedThreadPoolExecutor])

  // Schedule periodic metrics collection to capture sizes of the queue and the pool
  private def buildMetricsScheduledExecutor(): ScheduledExecutorService = {
    val reporter = Executors.newSingleThreadScheduledExecutor(r => {
      val thread = new Thread(r)
      thread.setDaemon(true)
      thread.setName(s"metrics-reporter")
      thread
    })

    reporter.scheduleAtFixedRate(
      () => {
        try {
          // Report queue size
          metricsContext.gauge("queue_size", getQueue.size())

          // Report pool sizes directly from the executor
          metricsContext.gauge("active_threads", getActiveCount)
          metricsContext.gauge("pool_size", getPoolSize)
          metricsContext.gauge("core_pool_size", getCorePoolSize)
          metricsContext.gauge("maximum_pool_size", getMaximumPoolSize)
          metricsContext.gauge("largest_pool_size", getLargestPoolSize)

          // Task counts from executor
          metricsContext.gauge("completed_task_count", getCompletedTaskCount)
          metricsContext.gauge("task_count", getTaskCount)
        } catch {
          case e: Exception =>
            logger.warn(s"Error reporting threadpool metrics - $e")
        }
      },
      60,
      metricsIntervalSeconds,
      TimeUnit.SECONDS
    )

    reporter
  }

  // Wrapper on the Executor's execute method to capture metrics on task wait and execution times
  override def execute(command: Runnable): Unit = {
    val submitTime = System.currentTimeMillis()

    val instrumentedTask = new Runnable {
      override def run(): Unit = {
        val startTime = System.currentTimeMillis()
        val waitTime = startTime - submitTime

        // Record wait time
        metricsContext.distribution("wait_time_ms", waitTime)

        command.run()
        val endTime = System.currentTimeMillis()
        val execTime = endTime - startTime
        val totalTime = endTime - submitTime

        // Record timing metrics
        metricsContext.distribution("execution_time_ms", execTime)
        metricsContext.distribution("total_time_ms", totalTime)
      }
    }

    super.execute(instrumentedTask)
  }

  // Clean up resources on shutdown
  override def shutdown(): Unit = {
    metricsReporter.shutdown()
    super.shutdown()
  }

  override def shutdownNow(): java.util.List[Runnable] = {
    metricsReporter.shutdownNow()
    super.shutdownNow()
  }

}
