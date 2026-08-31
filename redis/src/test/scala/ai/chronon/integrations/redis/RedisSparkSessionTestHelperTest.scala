package ai.chronon.integrations.redis

import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.{never, times, verify}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class RedisSparkSessionTestHelperTest extends AnyFlatSpec with Matchers with MockitoSugar {
  "Redis Spark session management" should "leave borrowed sessions running after the final release" in {
    val borrowed = mock[SparkSession]
    var createCalls = 0
    val manager = new RedisSparkSessionManager(
      () => Some(borrowed),
      _ => {
        createCalls += 1
        mock[SparkSession]
      }
    )

    manager.acquire("first") shouldBe borrowed
    manager.acquire("second") shouldBe borrowed
    createCalls shouldBe 0

    manager.release()
    manager.release()

    verify(borrowed, never()).stop()
  }

  it should "stop an owned session only after the final release" in {
    val first = mock[SparkSession]
    val second = mock[SparkSession]
    val sessions = Iterator(first, second)
    val manager = new RedisSparkSessionManager(() => None, _ => sessions.next())

    manager.acquire("first") shouldBe first
    manager.acquire("second") shouldBe first
    manager.release()
    verify(first, never()).stop()

    manager.release()
    verify(first, times(1)).stop()

    manager.acquire("replacement") shouldBe second
    manager.release()
    verify(second, times(1)).stop()
  }
}
