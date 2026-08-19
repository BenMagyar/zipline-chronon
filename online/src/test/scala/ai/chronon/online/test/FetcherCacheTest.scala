package ai.chronon.online

import ai.chronon.aggregator.windowing.FinalBatchIr
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.GroupBy
import ai.chronon.online.fetcher.Fetcher.Request
import ai.chronon.online.fetcher.FetcherCache.BatchIrCache
import ai.chronon.online.fetcher.FetcherCache.BatchResponses
import ai.chronon.online.fetcher.FetcherCache.CachedMapBatchResponse
import ai.chronon.online.KVStore.TimedValue
import ai.chronon.online.metrics.Metrics.Context
import ai.chronon.online.fetcher.LambdaKvRequest
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito
import org.mockito.Mockito._
import org.mockito.stubbing.Stubber
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.JavaConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait MockitoHelper extends MockitoSugar {
  // We override doReturn to fix a known Java/Scala interoperability issue. Without this fix, we see "doReturn:
  // ambiguous reference to overloaded definition" errors. An alternative would be to use the 'mockito-scala' library.
  def doReturn(toBeReturned: Any): Stubber = {
    Mockito.doReturn(toBeReturned, Nil: _*)
  }
}

class FetcherCacheTest extends AnyFlatSpec with MockitoHelper {
  class TestableFetcherCache(cache: Option[BatchIrCache]) extends fetcher.FetcherCache {
    override val maybeBatchIrCache: Option[BatchIrCache] = cache
  }
  val batchIrCacheMaximumSize = 50

  it should "disable batch ir cache when disable system property is set" in {
    val disableProp = "ai.chronon.fetcher.batch_ir_cache_disabled"
    val originalValue = Option(System.getProperty(disableProp))
    try {
      System.setProperty(disableProp, "true")
      val instance = new fetcher.FetcherCache {}
      assertEquals(false, instance.isCacheSizeConfigured)
      assertEquals(None, instance.configuredBatchIrCacheSize)
      assertEquals(None, instance.maybeBatchIrCache)
    } finally {
      originalValue match {
        case Some(v) => System.setProperty(disableProp, v)
        case None    => System.clearProperty(disableProp)
      }
    }
  }

  it should "enable batch ir cache by default when disable system property is unset" in {
    val disableProp = "ai.chronon.fetcher.batch_ir_cache_disabled"
    val originalValue = Option(System.getProperty(disableProp))
    try {
      System.clearProperty(disableProp)
      val instance = new fetcher.FetcherCache {}
      assertEquals(true, instance.isCacheSizeConfigured)
      assert(instance.configuredBatchIrCacheSize.exists(_ > 0))
      assert(instance.maybeBatchIrCache.isDefined)
    } finally {
      originalValue.foreach(v => System.setProperty(disableProp, v))
    }
  }

  it should "batch ir cache correctly caches batch irs" in {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)
    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createBatchir(i: Int) =
      BatchResponses(FinalBatchIr(collapsed = Array(i), tailHops = Array(Array(Array(i)), Array(Array(i)))))
    def createCacheKey(i: Int) = BatchIrCache.Key(dataset, Map("key" -> i), batchEndTsMillis)

    // Create a bunch of test batchIrs and store them in cache
    val batchIrs: Map[BatchIrCache.Key, BatchIrCache.Value] =
      (0 until batchIrCacheMaximumSize).map(i => createCacheKey(i) -> createBatchir(i)).toMap
    batchIrCache.cache.putAll(batchIrs.asJava)

    // Check that the cache contains all the batchIrs we created
    batchIrs.foreach(entry => {
      val cachedBatchIr = batchIrCache.cache.getIfPresent(entry._1)
      assertEquals(cachedBatchIr, entry._2)
    })
  }

  it should "batch ir cache correctly caches map response" in {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)
    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createMapResponse(i: Int) =
      BatchResponses(Map("group_by_key" -> i.asInstanceOf[AnyRef]))
    def createCacheKey(i: Int) = BatchIrCache.Key(dataset, Map("key" -> i), batchEndTsMillis)

    // Create a bunch of test mapResponses and store them in cache
    val mapResponses: Map[BatchIrCache.Key, BatchIrCache.Value] =
      (0 until batchIrCacheMaximumSize).map(i => createCacheKey(i) -> createMapResponse(i)).toMap
    batchIrCache.cache.putAll(mapResponses.asJava)

    // Check that the cache contains all the mapResponses we created
    mapResponses.foreach(entry => {
      val cachedBatchIr = batchIrCache.cache.getIfPresent(entry._1)
      assertEquals(cachedBatchIr, entry._2)
    })
  }

  // Test that the cache keys are compared by equality, not by reference. In practice, this means that if two keys
  // have the same (dataset, keys, batchEndTsMillis), they will only be stored once in the cache.
  it should "batch ir cache keys are compared by equality" in {
    val cacheName = "test"
    val batchIrCache = new BatchIrCache(cacheName, batchIrCacheMaximumSize)

    val dataset = "TEST_GROUPBY_BATCH"
    val batchEndTsMillis = 1000L

    def createCacheValue(i: Int) =
      BatchResponses(FinalBatchIr(collapsed = Array(i), tailHops = Array(Array(Array(i)), Array(Array(i)))))
    def createCacheKey(i: Int) = BatchIrCache.Key(dataset, Map("key" -> i), batchEndTsMillis)

    assert(batchIrCache.cache.estimatedSize() == 0)
    batchIrCache.cache.put(createCacheKey(1), createCacheValue(1))
    assert(batchIrCache.cache.estimatedSize() == 1)
    // Create a second key object with the same values as the first key, make sure it's not stored separately
    batchIrCache.cache.put(createCacheKey(1), createCacheValue(1))
    assert(batchIrCache.cache.estimatedSize() == 1)
  }

  it should "get cached requests returns correct cached data when cache is enabled" in {
    val cacheName = "test"
    val testCache = Some(new BatchIrCache(cacheName, batchIrCacheMaximumSize))
    val fetcherCache = new TestableFetcherCache(testCache) {
      override def isCachingEnabled(groupBy: GroupBy) = true
    }

    // Prepare groupByRequestToKvRequest
    val batchEndTsMillis = 0L
    val keys = Map("key" -> "value")
    val eventTs = 1000L
    val dataset = "TEST_GROUPBY_BATCH"
    val mockGroupByServingInfoParsed = mock[GroupByServingInfoParsed]
    val mockContext = mock[metrics.Metrics.Context]
    val request = Request("req_name", keys, Some(eventTs), Some(mock[Context]))
    val getRequest = KVStore.GetRequest("key".getBytes, dataset, Some(eventTs))
    val requestMeta =
      LambdaKvRequest(mockGroupByServingInfoParsed, request, getRequest, Some(getRequest), Some(eventTs), mockContext)
    val groupByRequestToKvRequest: Seq[(Request, Try[LambdaKvRequest])] = Seq((request, Success(requestMeta)))

    // getCachedRequests should return an empty list when the cache is empty
    val cachedRequestBeforePopulating = fetcherCache.getCachedRequests(groupByRequestToKvRequest)
    assert(cachedRequestBeforePopulating.isEmpty)

    // Add a GetRequest and a FinalBatchIr
    val key = BatchIrCache.Key(getRequest.dataset, keys, batchEndTsMillis)
    val finalBatchIr = BatchResponses(FinalBatchIr(Array(1), Array(Array(Array(1)), Array(Array(1)))))
    testCache.get.cache.put(key, finalBatchIr)

    // getCachedRequests should return the GetRequest and FinalBatchIr we cached
    val cachedRequestsAfterAddingItem = fetcherCache.getCachedRequests(groupByRequestToKvRequest)
    assert(cachedRequestsAfterAddingItem.head._1 == getRequest)
    assert(cachedRequestsAfterAddingItem.head._2 == finalBatchIr)
  }

  it should "get cached requests does not cache when cache is disabled for group by" in {
    val testCache = new BatchIrCache("test", batchIrCacheMaximumSize)
    val spiedTestCache = spy[BatchIrCache](testCache)
    val fetcherCache = new TestableFetcherCache(Some(testCache)) {
      // Cache is enabled globally, but disabled for a specific groupBy
      override def isCachingEnabled(groupBy: GroupBy) = false
    }

    // Prepare groupByRequestToKvRequest
    val keys = Map("key" -> "value")
    val eventTs = 1000L
    val dataset = "TEST_GROUPBY_BATCH"
    val mockGroupByServingInfoParsed = mock[GroupByServingInfoParsed]
    val mockContext = mock[metrics.Metrics.Context]
    val request = Request("req_name", keys, Some(eventTs))
    val getRequest = KVStore.GetRequest("key".getBytes, dataset, Some(eventTs))
    val requestMeta =
      LambdaKvRequest(mockGroupByServingInfoParsed, request, getRequest, Some(getRequest), Some(eventTs), mockContext)
    val groupByRequestToKvRequest: Seq[(Request, Try[LambdaKvRequest])] = Seq((request, Success(requestMeta)))

    val cachedRequests = fetcherCache.getCachedRequests(groupByRequestToKvRequest)
    assert(cachedRequests.isEmpty)
    // Cache was never called
    verify(spiedTestCache, never()).cache
  }

  it should "get batch bytes returns latest timed value bytes if greater than batch end" in {
    val kvStoreResponse = Success(
      Seq(TimedValue(Array(1.toByte), 1000L), TimedValue(Array(2.toByte), 2000L))
    )
    val batchResponses = BatchResponses(kvStoreResponse)
    val batchBytes = batchResponses.getBatchBytes(1500L)
    assertArrayEquals(Array(2.toByte), batchBytes)
  }

  it should "get batch bytes returns null if latest timed value timestamp is less than batch end minus one day" in {
    val oneDayInMillis = 86400000L
    val kvStoreResponse = Success(
      Seq(TimedValue(Array(1.toByte), 1000L), TimedValue(Array(2.toByte), 1500L))
    )
    val batchResponses = BatchResponses(kvStoreResponse)
    // All timestamps (1000L, 1500L) are more than one day before the batch end time
    val batchBytes = batchResponses.getBatchBytes(oneDayInMillis + 2000L)
    assertNull(batchBytes)
  }

  it should "get batch bytes returns value if within one day of batch end even if before batch end" in {
    val oneDayInMillis = 86400000L
    val kvStoreResponse = Success(
      Seq(TimedValue(Array(1.toByte), oneDayInMillis - 10000L), TimedValue(Array(2.toByte), oneDayInMillis - 5000L))
    )
    val batchResponses = BatchResponses(kvStoreResponse)
    // The latest value (at oneDayInMillis - 5000L) is within one day of batch end (oneDayInMillis)
    val batchBytes = batchResponses.getBatchBytes(oneDayInMillis)
    assertArrayEquals(Array(2.toByte), batchBytes)
  }

  it should "get batch bytes returns null when cached batch response" in {
    val finalBatchIr = mock[FinalBatchIr]
    val batchResponses = BatchResponses(finalBatchIr)
    val batchBytes = batchResponses.getBatchBytes(1000L)
    assertNull(batchBytes)
  }

  it should "get batch bytes returns null when kv store batch response fails" in {
    val kvStoreResponse = Failure(new RuntimeException("KV Store error"))
    val batchResponses = BatchResponses(kvStoreResponse)
    val batchBytes = batchResponses.getBatchBytes(1000L)
    assertNull(batchBytes)
  }

  it should "get batch ir from batch response returns correct i rs with cache enabled" in {
    // Use a real cache
    val batchIrCache = new BatchIrCache("test_cache", batchIrCacheMaximumSize)

    // Create all necessary mocks
    val servingInfo = mock[GroupByServingInfoParsed]
    val groupByOps = mock[GroupByOps]
    val toBatchIr = mock[(Array[Byte], GroupByServingInfoParsed) => FinalBatchIr]
    when(servingInfo.groupByOps).thenReturn(groupByOps)
    when(groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.batchEndTsMillis).thenReturn(1000L)

    // Dummy data
    val batchBytes = Array[Byte](1, 1)
    val keys = Map("key" -> "value")
    val cacheKey = BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)

    val fetcherCache = new TestableFetcherCache(Some(batchIrCache))
    val spiedFetcherCache = Mockito.spy[TestableFetcherCache](fetcherCache)
    doReturn(true).when(spiedFetcherCache).isCachingEnabled(any())

    // 1. Cached BatchResponse returns the same IRs passed in
    val finalBatchIr1 = mock[FinalBatchIr]
    val cachedBatchResponse = BatchResponses(finalBatchIr1)
    val cachedIr =
      spiedFetcherCache.getBatchIrFromBatchResponse(cachedBatchResponse, batchBytes, servingInfo, toBatchIr, keys)
    assertEquals(finalBatchIr1, cachedIr)
    verify(toBatchIr, never())(any(classOf[Array[Byte]]), any()) // no decoding needed

    // 2. Un-cached BatchResponse has IRs added to cache
    val finalBatchIr2 = mock[FinalBatchIr]
    val kvStoreBatchResponses = BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))
    when(toBatchIr(any(), any())).thenReturn(finalBatchIr2)
    val uncachedIr =
      spiedFetcherCache.getBatchIrFromBatchResponse(kvStoreBatchResponses, batchBytes, servingInfo, toBatchIr, keys)
    assertEquals(finalBatchIr2, uncachedIr)
    assertEquals(batchIrCache.cache.getIfPresent(cacheKey), BatchResponses(finalBatchIr2)) // key was added
    verify(toBatchIr, times(1))(any(), any()) // decoding did happen
  }

  it should "get batch ir from batch response decodes batch bytes if cache disabled" in {
    // Set up mocks and dummy data
    val servingInfo = mock[GroupByServingInfoParsed]
    val batchBytes = Array[Byte](1, 2, 3)
    val keys = Map("key" -> "value")
    val finalBatchIr = mock[FinalBatchIr]
    val toBatchIr = mock[(Array[Byte], GroupByServingInfoParsed) => FinalBatchIr]
    val kvStoreBatchResponses = BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))

    val spiedFetcherCache = Mockito.spy[TestableFetcherCache](new TestableFetcherCache(None))
    when(toBatchIr(any(), any())).thenReturn(finalBatchIr)

    // When getBatchIrFromBatchResponse is called, it decodes the bytes and doesn't hit the cache
    val ir =
      spiedFetcherCache.getBatchIrFromBatchResponse(kvStoreBatchResponses, batchBytes, servingInfo, toBatchIr, keys)
    verify(toBatchIr, times(1))(batchBytes, servingInfo) // decoding did happen
    assertEquals(finalBatchIr, ir)
  }

  it should "get batch ir from batch response returns correct map response with cache enabled" in {
    // Use a real cache
    val batchIrCache = new BatchIrCache("test_cache", batchIrCacheMaximumSize)
    // Set up mocks and dummy data
    val servingInfo = mock[GroupByServingInfoParsed]
    val groupByOps = mock[GroupByOps]
    mock[serde.AvroCodec]
    when(servingInfo.groupByOps).thenReturn(groupByOps)
    when(groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.groupByOps.batchDataset).thenReturn("test_dataset")
    when(servingInfo.batchEndTsMillis).thenReturn(1000L)
    val batchBytes = Array[Byte](1, 2, 3)
    val keys = Map("key" -> "value")
    val cacheKey = BatchIrCache.Key(servingInfo.groupByOps.batchDataset, keys, servingInfo.batchEndTsMillis)

    val spiedFetcherCache = Mockito.spy[TestableFetcherCache](new TestableFetcherCache(Some(batchIrCache)))
    doReturn(true).when(spiedFetcherCache).isCachingEnabled(any())

    // 1. Cached BatchResponse returns the same Map responses passed in
    val mapResponse1 = mock[Map[String, AnyRef]]
    val cachedBatchResponse = BatchResponses(mapResponse1)
    val decodingFunction1 = (_: Array[Byte]) => {
      fail("Decoding function should not be called when batch response is cached")
      mapResponse1
    }
    val cachedMapResponse = spiedFetcherCache.getMapResponseFromBatchResponse(cachedBatchResponse,
                                                                              batchBytes,
                                                                              decodingFunction1,
                                                                              servingInfo,
                                                                              keys)
    assertEquals(mapResponse1, cachedMapResponse)

    // 2. Un-cached BatchResponse has Map responses added to cache
    val mapResponse2 = mock[Map[String, AnyRef]]
    val kvStoreBatchResponses = BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))
    def decodingFunction2 = (_: Array[Byte]) => mapResponse2
    val decodedMapResponse = spiedFetcherCache.getMapResponseFromBatchResponse(kvStoreBatchResponses,
                                                                               batchBytes,
                                                                               decodingFunction2,
                                                                               servingInfo,
                                                                               keys)
    assertEquals(mapResponse2, decodedMapResponse)
    assertEquals(batchIrCache.cache.getIfPresent(cacheKey), CachedMapBatchResponse(mapResponse2)) // key was added
  }

  it should "get map response from batch response decodes batch bytes if cache disabled" in {
    // Set up mocks and dummy data
    val servingInfo = mock[GroupByServingInfoParsed]
    val batchBytes = Array[Byte](1, 2, 3)
    val keys = Map("key" -> "value")
    val mapResponse = mock[Map[String, AnyRef]]
    val outputCodec = mock[serde.AvroCodec]
    val kvStoreBatchResponses = BatchResponses(Success(Seq(TimedValue(batchBytes, 1000L))))
    when(servingInfo.outputCodec).thenReturn(outputCodec)
    when(outputCodec.decodeMap(any())).thenReturn(mapResponse)

    val spiedFetcherCache = Mockito.spy[TestableFetcherCache](new TestableFetcherCache(None))

    // When getMapResponseFromBatchResponse is called, it decodes the bytes and doesn't hit the cache
    val decodedMapResponse = spiedFetcherCache.getMapResponseFromBatchResponse(kvStoreBatchResponses,
                                                                               batchBytes,
                                                                               servingInfo.outputCodec.decodeMap,
                                                                               servingInfo,
                                                                               keys)
    verify(servingInfo.outputCodec, times(1)).decodeMap(any()) // decoding did happen
    assertEquals(mapResponse, decodedMapResponse)
  }
}
