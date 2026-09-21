package ai.chronon.flink.chaining

import ai.chronon.api.Constants
import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.online.fetcher.Fetcher
import ai.chronon.online.Api
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.metrics.{Counter, Histogram, MetricGroup}
import org.apache.flink.metrics.groups.OperatorMetricGroup
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import java.util.concurrent.{CountDownLatch, TimeUnit}
import scala.concurrent.Promise

class ModelTransformsAsyncFunctionTest extends AnyFlatSpec with Matchers with MockitoSugar {

  val modelTransformsName = "models/test_model_transforms"
  val enableDebug = false

  private def setupFunctionWithMockedMetrics(function: ModelTransformsAsyncFunction): Unit = {
    val mockRuntimeContext = mock[RuntimeContext]
    val mockOperatorMetricGroup = mock[OperatorMetricGroup]
    val mockSubGroup = mock[MetricGroup]
    val mockCounter = mock[Counter]
    val mockNotFoundCounter = mock[Counter]
    val mockHistogram = mock[Histogram]

    when(mockRuntimeContext.getMetricGroup).thenReturn(mockOperatorMetricGroup)
    when(mockOperatorMetricGroup.addGroup("chronon")).thenReturn(mockSubGroup)
    when(mockSubGroup.addGroup(anyString(), anyString())).thenReturn(mockSubGroup)
    when(mockSubGroup.counter("model_fetch.successes")).thenReturn(mockCounter)
    when(mockSubGroup.counter("model_fetch.errors")).thenReturn(mockCounter)
    when(mockSubGroup.counter("model_fetch.not_found")).thenReturn(mockNotFoundCounter)
    when(mockSubGroup.histogram(anyString(), any())).thenReturn(mockHistogram)

    function.setRuntimeContext(mockRuntimeContext)
  }

  private def resultFutureCollector(latch: CountDownLatch,
                                    setResults: java.util.Collection[ProjectedEvent] => Unit): ResultFuture[ProjectedEvent] =
    new ResultFuture[ProjectedEvent] {
      override def complete(results: java.util.Collection[ProjectedEvent]): Unit = {
        setResults(results)
        latch.countDown()
      }
      override def completeExceptionally(throwable: Throwable): Unit = latch.countDown()
    }

  private def buildModelTransforms(models: java.util.List[ai.chronon.api.Model] = null,
                                   passthrough: java.util.List[String] = null): ai.chronon.api.ModelTransforms = {
    val mt = new ai.chronon.api.ModelTransforms()
    val metaData = new ai.chronon.api.MetaData()
    metaData.setName(modelTransformsName)
    mt.setMetaData(metaData)
    if (models != null) mt.setModels(models)
    if (passthrough != null) mt.setPassthroughFields(passthrough)
    mt
  }

  "ModelTransformsAsyncFunction" should "merge model response values into the event" in {
    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    when(mockApi.generateModelPlatformProvider).thenReturn(new TestModelPlatformProvider())
    when(mockApi.buildFetcher(debug = enableDebug)).thenReturn(mockFetcher)

    val response = Fetcher.Response(
      Fetcher.Request(modelTransformsName, Map("user_id" -> "123"), Some(1000L)),
      scala.util.Success(Map("test_model__score" -> 0.85, "user_id" -> "123").asInstanceOf[Map[String, AnyRef]])
    )
    val future = Promise[Seq[Fetcher.Response]]()
    future.success(Seq(response))
    when(mockFetcher.fetchModelTransforms(any(), any())).thenReturn(future.future)

    val mt = buildModelTransforms()
    val function = new ModelTransformsAsyncFunction(modelTransformsName, Some(mt), "testGB", mockApi, enableDebug)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())

    val event = ProjectedEvent(Map("user_id" -> "123", "price" -> 99.99, Constants.TimeColumn -> 1000L), 500L)
    val latch = new CountDownLatch(1)
    var result: ProjectedEvent = null
    function.asyncInvoke(event, resultFutureCollector(latch, c => result = c.iterator().next()))

    latch.await(5, java.util.concurrent.TimeUnit.SECONDS) should be(true)
    result should not be null
    result.startProcessingTimeMillis should be(500L)
    result.fields("user_id") should be("123")
    result.fields("price") should be(99.99)
    result.fields("test_model__score") should be(0.85)

    verify(mockFetcher).fetchModelTransforms(any(), any())
  }

  it should "swallow the event when the fetch fails" in {
    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    when(mockApi.generateModelPlatformProvider).thenReturn(new TestModelPlatformProvider())
    when(mockApi.buildFetcher(debug = enableDebug)).thenReturn(mockFetcher)

    val future = Promise[Seq[Fetcher.Response]]()
    future.failure(new RuntimeException("model fetch failed"))
    when(mockFetcher.fetchModelTransforms(any(), any())).thenReturn(future.future)

    val mt = buildModelTransforms()
    val function = new ModelTransformsAsyncFunction(modelTransformsName, Some(mt), "testGB", mockApi, enableDebug)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())

    val event = ProjectedEvent(Map("user_id" -> "123", Constants.TimeColumn -> 1000L), 500L)
    val latch = new CountDownLatch(1)
    var results: java.util.Collection[ProjectedEvent] = null
    function.asyncInvoke(event, resultFutureCollector(latch, c => results = c))

    latch.await(5, java.util.concurrent.TimeUnit.SECONDS) should be(true)
    results should not be null
    results.isEmpty should be(true)
  }

  it should "swallow the event when no response is returned" in {
    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    when(mockApi.generateModelPlatformProvider).thenReturn(new TestModelPlatformProvider())
    when(mockApi.buildFetcher(debug = enableDebug)).thenReturn(mockFetcher)

    val future = Promise[Seq[Fetcher.Response]]()
    future.success(Seq.empty)
    when(mockFetcher.fetchModelTransforms(any(), any())).thenReturn(future.future)

    val mt = buildModelTransforms()
    val function = new ModelTransformsAsyncFunction(modelTransformsName, Some(mt), "testGB", mockApi, enableDebug)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())

    val event = ProjectedEvent(Map("user_id" -> "123", Constants.TimeColumn -> 1000L), 500L)
    val latch = new CountDownLatch(1)
    var results: java.util.Collection[ProjectedEvent] = null
    function.asyncInvoke(event, resultFutureCollector(latch, c => results = c))

    latch.await(5, java.util.concurrent.TimeUnit.SECONDS) should be(true)
    results should not be null
    results.isEmpty should be(true)
  }

  it should "handle timeout gracefully" in {
    val mockApi = mock[Api]
    when(mockApi.generateModelPlatformProvider).thenReturn(new TestModelPlatformProvider())

    val mt = buildModelTransforms()
    val function = new ModelTransformsAsyncFunction(modelTransformsName, Some(mt), "testGB", mockApi, enableDebug)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())

    val event = ProjectedEvent(Map("user_id" -> "123", Constants.TimeColumn -> 1000L), 500L)
    val latch = new CountDownLatch(1)
    var results: java.util.Collection[ProjectedEvent] = null
    function.timeout(event, resultFutureCollector(latch, c => results = c))

    latch.await(5, java.util.concurrent.TimeUnit.SECONDS) should be(true)
    results should not be null
    results.isEmpty should be(true)
  }

  it should "swallow the event when the time column is missing" in {
    val mockApi = mock[Api]
    val mockFetcher = mock[Fetcher]
    when(mockApi.generateModelPlatformProvider).thenReturn(new TestModelPlatformProvider())
    when(mockApi.buildFetcher(debug = enableDebug)).thenReturn(mockFetcher)

    val mt = buildModelTransforms()
    val function = new ModelTransformsAsyncFunction(modelTransformsName, Some(mt), "testGB", mockApi, enableDebug)
    setupFunctionWithMockedMetrics(function)
    function.open(new Configuration())

    val event = ProjectedEvent(Map("user_id" -> "123"), 500L)
    val latch = new CountDownLatch(1)
    var results: java.util.Collection[ProjectedEvent] = null
    function.asyncInvoke(event, resultFutureCollector(latch, c => results = c))

    latch.await(5, java.util.concurrent.TimeUnit.SECONDS) should be(true)
    results should not be null
    results.isEmpty should be(true)
    // Fetcher should not be invoked when the ts is missing - we short-circuit to not_found.
    verify(mockFetcher, never()).fetchModelTransforms(any(), any())
  }

  it should "fail fast in open when the Api has no model platform provider" in {
    val mockApi = mock[Api]
    when(mockApi.generateModelPlatformProvider).thenReturn(null)

    val mt = buildModelTransforms()
    val function = new ModelTransformsAsyncFunction(modelTransformsName, Some(mt), "testGB", mockApi, enableDebug)
    setupFunctionWithMockedMetrics(function)

    an[IllegalArgumentException] should be thrownBy function.open(new Configuration())
  }
}

// placeholder provider to satisfy the non-null check in open(); no predict calls happen in these tests
class TestModelPlatformProvider extends ai.chronon.online.ModelPlatformProvider {
  override def getPlatform(modelBackend: ai.chronon.api.ModelBackend,
                           backendParams: Map[String, String]): ai.chronon.online.ModelPlatform =
    throw new UnsupportedOperationException("not used in tests")
}