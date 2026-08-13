package ai.chronon.integrations.redis

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, ExecutionException, Executors, TimeUnit}

class RedisReadAdmissionControllerTest extends AnyFlatSpec with Matchers {

  private final class TestClock(private var nanos: Long = 0L) {
    def now(): Long = synchronized(nanos)
    def advanceMillis(millis: Long): Unit = synchronized {
      nanos += TimeUnit.MILLISECONDS.toNanos(millis)
    }
  }

  private def controller(maxInflight: Int = 2,
                         maxQueued: Int = 2,
                         queueTimeoutMillis: Long = 10000L,
                         maxQueuedControl: Int = 128,
                         controlQueueTimeoutMillis: Long = 2000L,
                         clock: TestClock = new TestClock): RedisReadAdmissionController =
    new RedisReadAdmissionController(
      RedisReadAdmissionConfig(
        maxInflight,
        maxQueued,
        queueTimeoutMillis,
        maxQueuedControl,
        controlQueueTimeoutMillis
      ),
      () => clock.now()
    )

  private def request(controller: RedisReadAdmissionController,
                      readClass: RedisReadClass = RedisReadClass.Feature): RedisReadAdmissionRequest =
    controller.acquire(readClass) match {
      case Right(admission) => admission
      case Left(error)      => fail(s"Expected admission, got ${error.reason}: ${error.getMessage}")
    }

  private def permit(request: RedisReadAdmissionRequest): RedisReadAdmissionPermit =
    request.future.get(1L, TimeUnit.SECONDS)

  "Redis read admission" should "hand permits to queued batches in FIFO order" in {
    val clock = new TestClock
    val admission = controller(maxInflight = 1, maxQueued = 3, clock = clock)
    try {
      val active = permit(request(admission))
      val first = request(admission)
      clock.advanceMillis(10L)
      val second = request(admission)
      clock.advanceMillis(10L)
      val third = request(admission)

      first.queued shouldBe true
      second.queued shouldBe true
      third.queued shouldBe true
      admission.snapshot shouldBe RedisReadAdmissionSnapshot(1, 3)

      clock.advanceMillis(30L)
      active.release() shouldBe Some(RedisReadAdmissionSnapshot(1, 2))
      val firstPermit = permit(first)
      firstPermit.wasQueued shouldBe true
      firstPermit.queueWaitMillis shouldBe 50L
      second.future.isDone shouldBe false

      firstPermit.release() shouldBe Some(RedisReadAdmissionSnapshot(1, 1))
      val secondPermit = permit(second)
      secondPermit.queueWaitMillis shouldBe 40L
      third.future.isDone shouldBe false

      secondPermit.release() shouldBe Some(RedisReadAdmissionSnapshot(1, 0))
      val thirdPermit = permit(third)
      thirdPermit.queueWaitMillis shouldBe 30L
      thirdPermit.release() shouldBe Some(RedisReadAdmissionSnapshot(0, 0))
    } finally admission.close()
  }

  it should "fail a full queue immediately without allocating a stack trace" in {
    val admission = controller(maxInflight = 1, maxQueued = 1)
    try {
      val active = permit(request(admission))
      val queued = request(admission)

      val rejection = admission.acquire().left.get
      rejection shouldBe a[RedisReadAdmissionQueueFullException]
      rejection.reason shouldBe "queue_full"
      rejection.getStackTrace shouldBe empty
      admission.snapshot shouldBe RedisReadAdmissionSnapshot(1, 1)

      active.release()
      permit(queued).release()
    } finally admission.close()
  }

  it should "time out a queued batch without consuming an in-flight slot" in {
    val admission = controller(maxInflight = 1, maxQueued = 1, queueTimeoutMillis = 20L)
    try {
      val active = permit(request(admission))
      val queued = request(admission)

      val failure = intercept[ExecutionException](queued.future.get(2L, TimeUnit.SECONDS)).getCause
      failure shouldBe a[RedisReadAdmissionQueueTimeoutException]
      failure.asInstanceOf[RedisReadAdmissionRejectedException].reason shouldBe "queue_timeout"
      admission.snapshot shouldBe RedisReadAdmissionSnapshot(1, 0)

      active.release() shouldBe Some(RedisReadAdmissionSnapshot(0, 0))
    } finally admission.close()
  }

  it should "release a lifecycle permit exactly once" in {
    val admission = controller(maxInflight = 1, maxQueued = 0)
    try {
      val acquired = request(admission)
      acquired.queued shouldBe false
      acquired.future.cancel(true) shouldBe false
      val active = permit(acquired)
      active.wasQueued shouldBe false
      active.queueWaitMillis shouldBe 0L

      active.release() shouldBe Some(RedisReadAdmissionSnapshot(0, 0))
      active.release() shouldBe None
    } finally admission.close()
  }

  it should "flush queued batches on close while allowing active work to finish" in {
    val admission = controller(maxInflight = 1, maxQueued = 2)
    val active = permit(request(admission))
    val queued = request(admission)

    admission.close()
    val queuedFailure = intercept[ExecutionException](queued.future.get(1L, TimeUnit.SECONDS)).getCause
    queuedFailure shouldBe a[RedisReadAdmissionClosedException]
    admission.snapshot shouldBe RedisReadAdmissionSnapshot(1, 0)
    admission.acquire().left.get.reason shouldBe "controller_closed"
    active.release() shouldBe Some(RedisReadAdmissionSnapshot(0, 0))
    admission.close()
  }

  it should "flush queued batches and reject new work during a timeout cooldown" in {
    val clock = new TestClock
    val admission = controller(maxInflight = 1, maxQueued = 2, clock = clock)
    try {
      val active = permit(request(admission))
      val firstQueued = request(admission)
      val secondQueued = request(admission)

      val cooldown = admission.openCooldown(5000L)
      cooldown.flushedBatches shouldBe 2
      cooldown.snapshot shouldBe RedisReadAdmissionSnapshot(1, 0)
      Seq(firstQueued, secondQueued).foreach { queued =>
        val failure = intercept[ExecutionException](queued.future.get(1L, TimeUnit.SECONDS)).getCause
        failure shouldBe a[RedisReadAdmissionCooldownException]
        failure.asInstanceOf[RedisReadAdmissionRejectedException].reason shouldBe "timeout_cooldown"
      }
      admission.acquire().left.get.reason shouldBe "timeout_cooldown"

      // The timed-out active request still owns its lifecycle slot until its caller explicitly releases it.
      active.release() shouldBe Some(RedisReadAdmissionSnapshot(0, 0))
      clock.advanceMillis(5000L)
      permit(request(admission)).release() shouldBe Some(RedisReadAdmissionSnapshot(0, 0))
    } finally admission.close()
  }

  it should "hand a released slot to queued control work before queued feature work" in {
    val admission = controller(maxInflight = 2, maxQueued = 1, maxQueuedControl = 1)
    try {
      val activeFeatureOne = permit(request(admission))
      val activeFeatureTwo = permit(request(admission))
      val queuedFeature = request(admission)
      val queuedControl = request(admission, RedisReadClass.Control)

      activeFeatureOne.release()
      val nextControl = permit(queuedControl)
      queuedFeature.future.isDone shouldBe false

      activeFeatureTwo.release()
      val nextFeature = permit(queuedFeature)
      nextControl.release()
      nextFeature.release()
      admission.snapshot shouldBe RedisReadAdmissionSnapshot(0, 0)
    } finally admission.close()
  }

  it should "flush only feature waiters during cooldown and continue necessary control work" in {
    val clock = new TestClock
    val admission = controller(
      maxInflight = 2,
      maxQueued = 1,
      maxQueuedControl = 1,
      clock = clock
    )
    try {
      val activeFeature = permit(request(admission))
      val activeControl = permit(request(admission, RedisReadClass.Control))
      val queuedFeature = request(admission)
      val queuedControl = request(admission, RedisReadClass.Control)

      admission.openCooldown(5000L).flushedBatches shouldBe 1
      intercept[ExecutionException](queuedFeature.future.get(1L, TimeUnit.SECONDS)).getCause shouldBe
        a[RedisReadAdmissionCooldownException]
      queuedControl.future.isDone shouldBe false

      activeControl.release()
      val admittedControl = permit(queuedControl)
      admission.acquire().left.get.reason shouldBe "timeout_cooldown"

      activeFeature.release()
      admittedControl.release()
      admission.snapshot shouldBe RedisReadAdmissionSnapshot(0, 0)
    } finally admission.close()
  }

  it should "never exceed either bound under concurrent acquisition" in {
    val maxInflight = 8
    val maxQueued = 16
    val attempts = 64
    val admission = controller(maxInflight, maxQueued)
    val accepted = new ConcurrentLinkedQueue[RedisReadAdmissionRequest]()
    val rejected = new ConcurrentLinkedQueue[RedisReadAdmissionRejectedException]()
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(attempts)
    val executor = Executors.newFixedThreadPool(16)

    try {
      (1 to attempts).foreach { _ =>
        executor.execute(new Runnable {
          override def run(): Unit = {
            try {
              start.await()
              admission.acquire() match {
                case Right(request) => accepted.add(request)
                case Left(error)    => rejected.add(error)
              }
            } finally done.countDown()
          }
        })
      }

      start.countDown()
      done.await(5L, TimeUnit.SECONDS) shouldBe true
      accepted.size() shouldBe maxInflight + maxQueued
      rejected.size() shouldBe attempts - maxInflight - maxQueued
      admission.snapshot shouldBe RedisReadAdmissionSnapshot(maxInflight, maxQueued)

      val iterator = accepted.iterator()
      val immediate = Vector.newBuilder[RedisReadAdmissionPermit]
      val queued = Vector.newBuilder[RedisReadAdmissionRequest]
      while (iterator.hasNext) {
        val next = iterator.next()
        if (next.queued) queued += next else immediate += permit(next)
      }
      immediate.result().foreach(_.release())
      queued.result().foreach(request => permit(request).release())
      admission.snapshot shouldBe RedisReadAdmissionSnapshot(0, 0)
    } finally {
      admission.close()
      executor.shutdownNow()
      executor.awaitTermination(5L, TimeUnit.SECONDS)
    }
  }
}
