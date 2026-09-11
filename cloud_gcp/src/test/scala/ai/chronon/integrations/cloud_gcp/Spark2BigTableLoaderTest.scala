package ai.chronon.integrations.cloud_gcp

import ai.chronon.online.KVStore.BulkPutRequest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Spark2BigTableLoaderTest extends AnyFlatSpec with Matchers {

  private val requiredArgs = Seq(
    "--dataset",
    "test_group_by",
    "--end-ds",
    "2026-06-11",
    "--table-name",
    "test.upload",
    "--project-id",
    "test-project",
    "--instance-id",
    "test-instance"
  )

  "writeTimestamp" should "prefer an explicit timestamp" in {
    Spark2BigTableLoader.writeTimestamp(Some(1788541200000L), throw new RuntimeException("unused fallback")) shouldBe
      1788541200000L
  }

  it should "fall back to the partition end" in {
    Spark2BigTableLoader.writeTimestamp(None, 1788541300000L) shouldBe 1788541300000L
  }

  it should "reject timestamps outside the supported epoch-millisecond range" in {
    an[IllegalArgumentException] should be thrownBy Spark2BigTableLoader.writeTimestamp(Some(-1L), 1788541300000L)
    an[IllegalArgumentException] should be thrownBy Spark2BigTableLoader.writeTimestamp(Some(253402300800000L),
                                                                                        1788541300000L)
  }

  "Conf" should "parse an explicit timestamp" in {
    val conf = new Spark2BigTableLoader.Conf(requiredArgs ++ Seq("--timestamp", "1788541200000"))

    conf.timestamp.toOption shouldBe Some(1788541200000L)
  }

  it should "leave the timestamp unset by default" in {
    val conf = new Spark2BigTableLoader.Conf(requiredArgs)

    conf.timestamp.toOption shouldBe None
  }

  "sparkLoaderArgs" should "forward the publication timestamp" in {
    val request = BulkPutRequest("test.upload", "test_group_by", "2026-06-11", 1788541200000L)

    BigTableKVStore.sparkLoaderArgs(request, "test-project", "test-instance") should contain theSameElementsInOrderAs
      Seq(
        "--table-name",
        "test.upload",
        "--dataset",
        "test_group_by",
        "--end-ds",
        "2026-06-11",
        "--project-id",
        "test-project",
        "--instance-id",
        "test-instance",
        "--timestamp",
        "1788541200000"
      )
  }
}
