package ai.chronon.online.test

import ai.chronon.online.metrics.{FlexibleExecutionContext, Metrics}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, ThreadPoolExecutor, TimeUnit}

class FlexibleExecutionContextTest extends AnyFlatSpec {

  it should "use default pool size of cores * 4 when no system property is set" in {
    val executor = FlexibleExecutionContext.buildExecutor
    val expectedPoolSize = Runtime.getRuntime.availableProcessors() * 4
    assert(executor.getCorePoolSize == expectedPoolSize)
    assert(executor.getMaximumPoolSize == expectedPoolSize)
    assert(executor.getKeepAliveTime(TimeUnit.SECONDS) == 600)
  }

  it should "have a bounded blocking queue" in {
    val executor = FlexibleExecutionContext.buildExecutor
    assert(executor.getQueue.isInstanceOf[ArrayBlockingQueue[_]])
    val queue = executor.getQueue.asInstanceOf[ArrayBlockingQueue[_]]
    assert(queue.remainingCapacity() == 10000)
  }

  it should "expose configurable property key constants" in {
    assert(FlexibleExecutionContext.ThreadPoolSizeProperty == "ai.chronon.threadpool.size")
    assert(FlexibleExecutionContext.QueueCapacityProperty == "ai.chronon.threadpool.queue.capacity")
    assert(FlexibleExecutionContext.KeepAliveSecondsProperty == "ai.chronon.threadpool.keepalive.seconds")
  }

  it should "build an explicitly sized executor with named daemon threads" in {
    val executor = FlexibleExecutionContext.buildExecutor(
      poolSize = 3,
      queueCapacity = 7,
      threadPrefix = "custom-pool",
      metricsContext = Metrics.Context(environment = "test"),
      daemonThreads = true
    )
    val worker = new AtomicReference[Thread]()
    val taskFinished = new CountDownLatch(1)

    try {
      executor.execute(new Runnable {
        override def run(): Unit = {
          worker.set(Thread.currentThread())
          taskFinished.countDown()
        }
      })

      assert(taskFinished.await(10, TimeUnit.SECONDS))
      assert(executor.getCorePoolSize == 3)
      assert(executor.getMaximumPoolSize == 3)
      assert(executor.getQueue.asInstanceOf[ArrayBlockingQueue[_]].remainingCapacity() == 7)
      assert(worker.get().getName.startsWith("custom-pool-"))
      assert(worker.get().isDaemon)
    } finally {
      executor.shutdownNow()
    }
  }
}
