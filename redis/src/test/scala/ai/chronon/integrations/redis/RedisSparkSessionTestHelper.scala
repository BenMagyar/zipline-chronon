package ai.chronon.integrations.redis

import org.apache.spark.sql.SparkSession

import java.nio.file.Files

private[redis] object RedisSparkSessionTestHelper {
  private var spark: SparkSession = _
  private var references = 0

  def acquire(appName: String): SparkSession = synchronized {
    if (spark == null) {
      spark = SparkSession
        .builder()
        .appName(appName)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", Files.createTempDirectory("redis-spark-warehouse").toString)
        .enableHiveSupport()
        .getOrCreate()
    }
    references += 1
    spark
  }

  def release(): Unit = synchronized {
    if (references > 0) references -= 1
    if (references == 0 && spark != null) {
      val session = spark
      spark = null
      session.stop()
    }
  }
}
