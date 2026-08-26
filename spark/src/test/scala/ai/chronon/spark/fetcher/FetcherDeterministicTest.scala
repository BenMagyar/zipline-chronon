/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.fetcher

import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.spark.catalog.TableUtils
import ai.chronon.spark.utils.SparkTestBase
import org.slf4j.{Logger, LoggerFactory}

import java.util.TimeZone
import java.util.concurrent.Executors
import scala.concurrent.duration.{Duration, SECONDS}
import scala.concurrent.{Await, ExecutionContext}

class FetcherDeterministicTest extends SparkTestBase {

  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private val tableUtils = TableUtils(spark)
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

  it should "test temporal fetch join deterministic" in {
    val namespace = "deterministic_fetch"
    val joinConf = FetcherTestUtil.generateMutationData(namespace, tableUtils, spark)
    FetcherTestUtil.compareTemporalFetch(joinConf,
                                         "2021-04-10",
                                         namespace,
                                         consistencyCheck = false,
                                         dropDsOnWrite = true)(spark)
  }

  // Regression guard: KVUploadNodeRunner strips executionInfo before writing the Join to KV.
  // This test proves the fetch path (which reads the Join back out of KV) works when executionInfo
  // is absent on the served Join and each of its joinParts' groupBy metaData.
  it should "test temporal fetch join with executionInfo stripped from KV metadata" in {
    import ai.chronon.api.Extensions.JoinOps
    val namespace = "deterministic_fetch_stripped"
    val joinConf = FetcherTestUtil.generateMutationData(namespace, tableUtils, spark)
    FetcherTestUtil.compareTemporalFetch(
      joinConf,
      "2021-04-10",
      namespace,
      consistencyCheck = false,
      dropDsOnWrite = true,
      preUploadTransform = _.withoutExecutionInfo)(spark)
  }

  it should "test fetchJoinV2 with different response types" in {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))
    val namespace = "fetch_join_v2_test"
    val joinConf = FetcherTestUtil.generateMutationData(namespace, tableUtils, spark)

    // Set up the mock API with data
    import ai.chronon.api.Constants.MetadataDataset
      import ai.chronon.online.fetcher.{FetchContext, MetadataStore}
      import ai.chronon.spark.utils.{MockApi, OnlineUtils}

    val kvStoreFunc = () => OnlineUtils.buildInMemoryKVStore("FetchJoinV2Test")
    val inMemoryKvStore = kvStoreFunc()
    val mockApi = new MockApi(kvStoreFunc, namespace)

    // Serve the data for fetching
    import ai.chronon.api.ScalaJavaConversions._
    OnlineUtils.serve(tableUtils,
                      inMemoryKvStore,
                      kvStoreFunc,
                      namespace,
                      "2021-04-10",
                      joinConf.joinParts.toScala.head.groupBy,
                      dropDsOnWrite = true)

    // Set up metadata
    val metadataStore = new MetadataStore(FetchContext(inMemoryKvStore))
    inMemoryKvStore.create(MetadataDataset)
    Await.result(metadataStore.putJoinConf(joinConf), Duration(30, SECONDS))

    // Create test requests
    val requests = Array(
      Request(joinConf.metaData.name, Map("listing_id" -> java.lang.Long.valueOf(595125622443733822L)), Some(1618056600000L)), // 2021-04-10 09:00:00
      Request(joinConf.metaData.name, Map("listing_id" -> java.lang.Long.valueOf(1L)), Some(1618056600000L))
    )

    // Test fetchJoinV2
    FetcherTestUtil.testFetchJoinV2(spark, requests, mockApi, debug = true)
  }
}
