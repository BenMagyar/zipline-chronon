package ai.chronon.online.test

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.{Constants, ThriftJsonCodec}
import ai.chronon.online.{GroupByServingInfoParsed, InMemoryKvStore, JavaGroupByStatusResponse}
import ai.chronon.online.KVStore.PutRequest
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.serde.AvroCodec
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class FetcherSchemaTest extends AnyFlatSpec with Matchers {

  private def putString(kvStore: InMemoryKvStore,
                        key: String,
                        value: String,
                        dataset: String,
                        tsMillis: Option[Long] = None): Unit = {
    Await.result(
      kvStore.put(PutRequest(key.getBytes(Constants.UTF8), value.getBytes(Constants.UTF8), dataset, tsMillis)),
      1.second)
  }

  it should "fetch schema for online groupBys" in {
    val kvStore = InMemoryKvStore.build(s"FetcherSchemaTest_online_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    servingInfo.groupBy.metaData.setOnline(true)
    val groupByName = servingInfo.groupBy.metaData.name
    val batchDataset = new GroupByOps(servingInfo.groupBy).batchDataset
    kvStore.create(batchDataset)

    putString(kvStore, servingInfo.groupBy.keyNameForKvStore, ThriftJsonCodec.toJsonStr(servingInfo.groupBy), Constants.MetadataDataset)
    putString(kvStore, Constants.GroupByServingInfoKey, ThriftJsonCodec.toJsonStr(servingInfo), batchDataset)

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    val response = fetcher.fetchGroupBySchema(groupByName).get

    response.groupByName shouldBe groupByName
    response.keySchema shouldBe servingInfo.keyAvroSchema
    response.inputSchema shouldBe servingInfo.inputAvroSchema
    response.selectedSchema shouldBe servingInfo.selectedAvroSchema

    val valueCodec = AvroCodec.of(response.valueSchema)
    valueCodec.fieldNames.toSet shouldBe Set("int_val", "id_last2_1d", "id2_last2_1d")
  }

  it should "fetch current status directly from batch serving info" in {
    val kvStore = InMemoryKvStore.build(s"FetcherSchemaTest_status_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    servingInfo.groupBy.metaData.setOnline(true)
    servingInfo.setBatchEndDate("2026-06-03-04-00")
    servingInfo.setDateFormat("yyyy-MM-dd-HH-mm")
    servingInfo.setBatchEndTs(1780459200000L)
    val groupByName = servingInfo.groupBy.metaData.name
    val batchDataset = new GroupByOps(servingInfo.groupBy).batchDataset
    kvStore.create(batchDataset)

    putString(kvStore,
              servingInfo.groupBy.keyNameForKvStore,
              ThriftJsonCodec.toJsonStr(servingInfo.groupBy),
              Constants.MetadataDataset)
    putString(kvStore,
              Constants.GroupByServingInfoKey,
              ThriftJsonCodec.toJsonStr(servingInfo),
              batchDataset,
              Some(1L))

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    val firstResponse = fetcher.fetchGroupByStatus(groupByName).get

    firstResponse.groupByName shouldBe groupByName
    firstResponse.batchEndDate shouldBe servingInfo.batchEndDate
    firstResponse.batchEndTs shouldBe new GroupByServingInfoParsed(servingInfo).batchEndTsMillis

    servingInfo.setBatchEndDate("2026-07-23")
    servingInfo.setBatchEndTs(1784764800000L)
    putString(kvStore,
              Constants.GroupByServingInfoKey,
              ThriftJsonCodec.toJsonStr(servingInfo),
              batchDataset,
              Some(2L))

    val refreshedResponse = fetcher.fetchGroupByStatus(groupByName).get
    refreshedResponse.batchEndDate shouldBe "2026-07-23"
    refreshedResponse.batchEndTs shouldBe 1784764800000L
  }

  it should "return a user-facing status error for offline groupBys" in {
    val kvStore = InMemoryKvStore.build(s"FetcherSchemaTest_status_offline_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    servingInfo.groupBy.metaData.setOnline(false)
    val groupByName = servingInfo.groupBy.metaData.name

    putString(kvStore,
              servingInfo.groupBy.keyNameForKvStore,
              ThriftJsonCodec.toJsonStr(servingInfo.groupBy),
              Constants.MetadataDataset)

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    val failure = fetcher.fetchGroupByStatus(groupByName).failed.get

    failure shouldBe a[IllegalArgumentException]
    failure.getMessage should include("online=True")
    failure.getMessage should include("upload the GroupBy")
  }

  it should "retain the two-field status response API" in {
    val scalaResponse = Fetcher.GroupByStatusResponse("legacy_group_by", "2026-05-20")
    val javaResponse = new JavaGroupByStatusResponse("legacy_group_by", "2026-05-20")

    scalaResponse.productArity shouldBe 2
    scalaResponse.batchEndTs shouldBe null
    javaResponse.batchEndTs shouldBe null
  }

  it should "include batchEndTs in status response value semantics" in {
    val batchEndTs = java.lang.Long.valueOf(1779235200000L)
    val response = Fetcher.GroupByStatusResponse("test_group_by", "2026-05-20", batchEndTs)
    val equalResponse = Fetcher.GroupByStatusResponse("test_group_by", "2026-05-20", batchEndTs)
    val differentWatermark = Fetcher.GroupByStatusResponse("test_group_by", "2026-05-20", batchEndTs + 1L)

    response shouldBe equalResponse
    response.hashCode() shouldBe equalResponse.hashCode()
    response.hashCode() shouldBe (response.groupByName, response.batchEndDate, response.batchEndTs).hashCode()
    response should not be differentWatermark
    response.toString shouldBe "GroupByStatusResponse(test_group_by,2026-05-20,1779235200000)"

    val copied = response.copy(batchEndDate = "2026-05-21")
    copied.groupByName shouldBe response.groupByName
    copied.batchEndDate shouldBe "2026-05-21"
    copied.batchEndTs shouldBe batchEndTs

    response.copy(batchEndTs = batchEndTs + 1L) shouldBe differentWatermark
  }

  // A GroupBy that is only a join dependency is uploaded to <NAME>_BATCH (so the join can fetch it) but
  // carries online=false in its own conf. The schema must still resolve - the batch serving info is the
  // signal, not the conf's online flag - otherwise the endpoint 400s for GroupBys that are servable.
  it should "fetch schema when the batch serving info exists but the conf is marked offline" in {
    val kvStore = InMemoryKvStore.build(s"FetcherSchemaTest_offline_conf_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    servingInfo.groupBy.metaData.setOnline(false)
    val groupByName = servingInfo.groupBy.metaData.name
    val batchDataset = new GroupByOps(servingInfo.groupBy).batchDataset
    kvStore.create(batchDataset)

    putString(kvStore, Constants.GroupByServingInfoKey, ThriftJsonCodec.toJsonStr(servingInfo), batchDataset)

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    val response = fetcher.fetchGroupBySchema(groupByName).get

    response.groupByName shouldBe groupByName
    response.keySchema shouldBe servingInfo.keyAvroSchema
    response.inputSchema shouldBe servingInfo.inputAvroSchema
    response.selectedSchema shouldBe servingInfo.selectedAvroSchema
  }

  it should "fail schema fetch when the groupBy has no batch upload" in {
    val kvStore = InMemoryKvStore.build(s"FetcherSchemaTest_no_upload_${System.nanoTime()}")
    kvStore.create(Constants.MetadataDataset)

    val servingInfo = GroupByDerivationsTest.makeTestGroupByServingInfoParsed().groupByServingInfo
    val groupByName = servingInfo.groupBy.metaData.name
    // create the batch dataset but never write the serving info - mirrors a GroupBy that was never uploaded
    kvStore.create(new GroupByOps(servingInfo.groupBy).batchDataset)

    val fetcher = new Fetcher(kvStore, Constants.MetadataDataset)
    val failure = fetcher.fetchGroupBySchema(groupByName).failed.get

    failure.getMessage should include("group by serving info")
  }
}
