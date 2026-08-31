package ai.chronon.integrations.redis

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import redis.clients.jedis.JedisCluster

class RedisBatchModeDispatchTest extends AnyFlatSpec with Matchers {

  private final class RecordingStore(mode: => RedisBatchMode)
      extends RedisKVStoreImpl(null.asInstanceOf[JedisCluster], Map.empty, _ => mode) {
    var fullSnapshotUpload: Option[(String, String, String)] = None
    var incrementalUpload: Option[(String, String, String, ConditionalObjectWriter)] = None

    override protected[redis] def bulkPutFullSnapshot(sourceOfflineTable: String,
                                                      destinationOnlineDataSet: String,
                                                      partition: String,
                                                      uploadConf: Map[String, String]): String = {
      fullSnapshotUpload = Some((sourceOfflineTable, destinationOnlineDataSet, partition))
      "full snapshot"
    }

    override protected[redis] def bulkPutIncremental(sourceOfflineTable: String,
                                                     destinationOnlineDataSet: String,
                                                     partition: String,
                                                     writer: ConditionalObjectWriter,
                                                     uploadConf: Map[String, String]): String = {
      incrementalUpload = Some((sourceOfflineTable, destinationOnlineDataSet, partition, writer))
      "incremental"
    }
  }

  "Redis full-snapshot mode" should "select only the full-snapshot bulk uploader" in {
    val store = new RecordingStore(RedisBatchMode.FullSnapshot)

    store.bulkPut("source_table", "feature_group", "2024-10-05")

    store.fullSnapshotUpload shouldBe Some(("source_table", "feature_group", "2024-10-05"))
    store.incrementalUpload shouldBe None
  }

  it should "remain unresolved until a batch write uses the upload setting" in {
    var resolutions = 0
    val store = new RecordingStore({
      resolutions += 1
      RedisBatchMode.FullSnapshot
    })

    resolutions shouldBe 0
    store.create("metadata")
    resolutions shouldBe 0
    store.bulkPut("source_table", "feature_group", "2024-10-05")
    resolutions shouldBe 1
  }

  "Redis incremental mode" should "select only the incremental uploader with its conditional writer" in {
    val writer = HadoopConditionalObjectWriter
    val store = new RecordingStore(RedisBatchMode.Incremental(writer))

    store.bulkPut("source_table", "feature_group", "2024-10-05")

    store.fullSnapshotUpload shouldBe None
    val upload = store.incrementalUpload.get
    upload._1 shouldBe "source_table"
    upload._2 shouldBe "feature_group"
    upload._3 shouldBe "2024-10-05"
    upload._4 should be theSameInstanceAs writer
  }
}
