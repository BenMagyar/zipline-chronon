package ai.chronon.integrations.redis

import org.apache.spark.sql.SparkSession

import java.nio.file.Files

private[redis] final class RedisSparkSessionManager(existingSession: () => Option[SparkSession],
                                                    createSession: String => SparkSession) {
  private var spark: SparkSession = _
  private var references = 0
  private var owned = false

  def acquire(appName: String): SparkSession = synchronized {
    if (spark == null) {
      existingSession() match {
        case Some(existing) =>
          spark = existing
          owned = false
        case None =>
          spark = createSession(appName)
          owned = true
      }
    }
    references += 1
    spark
  }

  def release(): Unit = {
    val sessionToStop = synchronized {
      if (references > 0) references -= 1
      if (references == 0 && spark != null) {
        val session = spark
        val stop = owned
        spark = null
        owned = false
        if (stop) Some(session) else None
      } else None
    }
    sessionToStop.foreach(_.stop())
  }
}

private[redis] object RedisSparkSessionTestHelper {
  private def runningSession: Option[SparkSession] =
    SparkSession.getActiveSession.filterNot(_.sparkContext.isStopped).orElse {
      SparkSession.getDefaultSession.filterNot(_.sparkContext.isStopped)
    }

  private def createSession(appName: String): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    SparkSession
      .builder()
      .appName(appName)
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("redis-spark-warehouse").toString)
      .enableHiveSupport()
      .getOrCreate()
  }

  private val manager = new RedisSparkSessionManager(
    () => runningSession,
    createSession
  )

  def acquire(appName: String): SparkSession = manager.acquire(appName)

  def release(): Unit = manager.release()
}
