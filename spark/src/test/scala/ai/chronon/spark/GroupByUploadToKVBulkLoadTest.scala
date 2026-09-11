package ai.chronon.spark

import ai.chronon.api.Extensions.{WindowOps, WindowUtils}
import ai.chronon.api.PartitionSpec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GroupByUploadToKVBulkLoadTest extends AnyFlatSpec with Matchers {

  "buildBulkPutRequest" should "use an explicit publication timestamp" in {
    val request = Driver.GroupByUploadToKVBulkLoad.buildBulkPutRequest(
      "test.upload",
      "test_group_by",
      "2026-09-02",
      Some(1788557000000L),
      PartitionSpec.daily
    )

    request.publicationTimestampMillis shouldBe 1788557000000L
  }

  it should "use the daily partition end when no timestamp is supplied" in {
    val request = Driver.GroupByUploadToKVBulkLoad.buildBulkPutRequest(
      "test.upload",
      "test_group_by",
      "2026-09-02",
      None,
      PartitionSpec.daily
    )

    request.publicationTimestampMillis shouldBe PartitionSpec.daily.partitionEndMillis("2026-09-02")
  }

  it should "use the configured sub-daily partition end when no timestamp is supplied" in {
    val partitionSpec =
      PartitionSpec("ds", "yyyy-MM-dd-HH-mm", 3 * WindowUtils.Hour.millis, WindowUtils.Hour.millis)
    val request = Driver.GroupByUploadToKVBulkLoad.buildBulkPutRequest(
      "test.upload",
      "test_group_by",
      "2026-09-02-01-00",
      None,
      partitionSpec
    )

    request.publicationTimestampMillis shouldBe partitionSpec.partitionEndMillis("2026-09-02-01-00")
  }

  it should "reject publication timestamps outside the supported epoch-millisecond range" in {
    an[IllegalArgumentException] should be thrownBy Driver.GroupByUploadToKVBulkLoad.buildBulkPutRequest(
      "test.upload",
      "test_group_by",
      "2026-09-02",
      Some(253402300800000L)
    )
  }
}
