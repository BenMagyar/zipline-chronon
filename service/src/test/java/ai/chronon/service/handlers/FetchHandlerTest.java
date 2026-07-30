package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.service.model.GetFeaturesResponse;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Failure;
import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Success;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class FetchHandlerTest {

    @Mock
    private JavaFetcher mockFetcher;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private HttpServerResponse response;

    @Mock
    RequestBody requestBody;

    private FetchHandler handler;
    private Vertx vertx;

    private static final String TEST_GROUP_BY = "test_groupby.v1";

    @Before
    public void setUp(TestContext context) {
        MockitoAnnotations.openMocks(this);
        vertx = Vertx.vertx();

        handler = new FetchHandler(mockFetcher, new FetchRouter.JoinFetcherFunction());

        // Set up common routing context behavior
        when(routingContext.vertx()).thenReturn(vertx);
        when(routingContext.response()).thenReturn(response);
        when(response.putHeader(anyString(), anyString())).thenReturn(response);
        when(response.setStatusCode(anyInt())).thenReturn(response);
        when(response.end(anyString())).thenReturn(Future.succeededFuture());
        when(routingContext.body()).thenReturn(requestBody);
        when(routingContext.pathParam("name")).thenReturn(TEST_GROUP_BY);
    }

    @After
    public void tearDown() throws Exception {
        if (vertx != null) {
            vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testFetchInitiationRunsOffEventLoopAndConcurrently(TestContext context) throws Exception {
        CountDownLatch fetchStarted = new CountDownLatch(2);
        CountDownLatch releaseFetch = new CountDownLatch(1);
        CountDownLatch fetchCompleted = new CountDownLatch(2);
        CountDownLatch responseWritten = new CountDownLatch(2);
        CountDownLatch eventLoopDrained = new CountDownLatch(1);
        AtomicBoolean ranOnEventLoop = new AtomicBoolean(false);
        AtomicInteger activeFetches = new AtomicInteger();
        AtomicInteger maxActiveFetches = new AtomicInteger();
        WorkerExecutor workerExecutor = vertx.createSharedWorkerExecutor("fetch-handler-init-test", 2);

        when(requestBody.asString()).thenReturn("[{\"user_id\":\"123\"}]");
        when(response.end(anyString())).thenAnswer(invocation -> {
            responseWritten.countDown();
            return Future.succeededFuture();
        });
        handler = new FetchHandler(mockFetcher, (fetcher, requests) -> {
            ranOnEventLoop.compareAndSet(false, Context.isOnEventLoopThread());
            int active = activeFetches.incrementAndGet();
            maxActiveFetches.accumulateAndGet(active, Math::max);
            fetchStarted.countDown();
            try {
                releaseFetch.await(5, TimeUnit.SECONDS);
                return CompletableFuture.completedFuture(Collections.emptyList());
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(exception);
            } finally {
                activeFetches.decrementAndGet();
                fetchCompleted.countDown();
            }
        }, workerExecutor);

        try {
            vertx.setTimer(50, ignored -> eventLoopDrained.countDown());
            vertx.runOnContext(ignored -> {
                handler.handle(routingContext);
                handler.handle(routingContext);
            });

            context.assertTrue(fetchStarted.await(2, TimeUnit.SECONDS));
            context.assertTrue(eventLoopDrained.await(1, TimeUnit.SECONDS));
            context.assertFalse(ranOnEventLoop.get());
            context.assertTrue(maxActiveFetches.get() >= 2);
        } finally {
            releaseFetch.countDown();
            context.assertTrue(fetchCompleted.await(2, TimeUnit.SECONDS));
            context.assertTrue(responseWritten.await(2, TimeUnit.SECONDS));
            workerExecutor.close();
        }
    }

    @Test
    public void testSuccessfulSingleRequest(TestContext context) {
        Async async = context.async();

        // Set up mocks
        String validRequestBody = "[{\"user_id\":\"123\"}]";
        when(requestBody.asString()).thenReturn(validRequestBody);

        Map<String, Object> keys = Collections.singletonMap("user_id", "123");
        JavaRequest request = new JavaRequest(TEST_GROUP_BY, keys);
        Map<String, Object> featureMap = Map.of(
            "feature_1", 12,
            "feature_2", 23.3,
            "feature_3", "USD"
        );
        JTry<Map<String, Object>> values = JTry.success(featureMap);
        JavaResponse mockResponse = new JavaResponse(request, values);

        CompletableFuture<List<JavaResponse>> futureResponse =
                CompletableFuture.completedFuture(Collections.singletonList(mockResponse));
        when(mockFetcher.fetchJoin(anyList())).thenReturn(futureResponse);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(200);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Verify response format
            JsonObject actualResponse = new JsonObject(responseCaptor.getValue());
            GetFeaturesResponse.Result expectedResult = GetFeaturesResponse.Result.builder().status(Success).entityKeys(keys).features(featureMap).build();
            validateSuccessfulResponse(actualResponse, Collections.singletonList(expectedResult), context);
            async.complete();
        });
    }

    @Test
    public void testRequestParseIssue(TestContext context) {
        Async async = context.async();

        // Set up mocks
        String invalidRequestBody = "[{\"user_id\"\"123\"}]";
        when(requestBody.asString()).thenReturn(invalidRequestBody);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(400);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Verify response format
            validateFailureResponse(responseCaptor.getValue(), context);
            async.complete();
        });
    }

    @Test
    public void testFetcherFutureFailure(TestContext context) {
        Async async = context.async();

        // Set up mocks
        String validRequestBody = "[{\"user_id\":\"123\"}]";
        when(requestBody.asString()).thenReturn(validRequestBody);

        CompletableFuture<List<JavaResponse>> futureResponse = new CompletableFuture<>();
        futureResponse.completeExceptionally(new RuntimeException("Error in KV store lookup"));
        when(mockFetcher.fetchJoin(anyList())).thenReturn(futureResponse);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(500);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Verify response format
            validateFailureResponse(responseCaptor.getValue(), context);
            async.complete();
        });
    }

    @Test
    public void testSuccessfulMultipleRequests(TestContext context) {
        Async async = context.async();

        // Set up mocks
        String validRequestBody = "[{\"user_id\":\"123\"}, {\"user_id\":\"456\"}]";
        when(requestBody.asString()).thenReturn(validRequestBody);

        Map<String, Object> keys1 = Collections.singletonMap("user_id", "123");
        JavaRequest request1 = new JavaRequest(TEST_GROUP_BY, keys1);

        Map<String, Object> keys2 = Collections.singletonMap("user_id", "456");
        JavaRequest request2 = new JavaRequest(TEST_GROUP_BY, keys2);

        Map<String, Object> featureMap1 = Map.of(
            "feature_1", 12,
            "feature_2", 23.3,
            "feature_3", "USD"
        );

        Map<String, Object> featureMap2 = Map.of(
            "feature_1", 24,
            "feature_2", 26.3,
            "feature_3", "CAD"
        );

        JTry<Map<String, Object>> values1 = JTry.success(featureMap1);
        JTry<Map<String, Object>> values2 = JTry.success(featureMap2);
        JavaResponse mockResponse1 = new JavaResponse(request1, values1);
        JavaResponse mockResponse2 = new JavaResponse(request2, values2);

        List<JavaResponse> mockResponseList = List.of(
            mockResponse1,
            mockResponse2
        );
        CompletableFuture<List<JavaResponse>> futureResponse =
                CompletableFuture.completedFuture(mockResponseList);
        when(mockFetcher.fetchJoin(anyList())).thenReturn(futureResponse);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(200);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Verify response format
            JsonObject actualResponse = new JsonObject(responseCaptor.getValue());
            GetFeaturesResponse.Result expectedResult1 = GetFeaturesResponse.Result.builder().status(Success).entityKeys(keys1).features(featureMap1).build();
            GetFeaturesResponse.Result expectedResult2 = GetFeaturesResponse.Result.builder().status(Success).entityKeys(keys2).features(featureMap2).build();

            List<GetFeaturesResponse.Result> expectedResultList = List.of(
                expectedResult1,
                expectedResult2
            );
            validateSuccessfulResponse(actualResponse, expectedResultList, context);
            async.complete();
        });
    }

    @Test
    public void testPartialSuccessfulRequests(TestContext context) {
        Async async = context.async();

        // Set up mocks
        String validRequestBody = "[{\"user_id\":\"123\"}, {\"user_id\":\"456\"}]";
        when(requestBody.asString()).thenReturn(validRequestBody);

        Map<String, Object> keys1 = Collections.singletonMap("user_id", "123");
        JavaRequest request1 = new JavaRequest(TEST_GROUP_BY, keys1);

        Map<String, Object> keys2 = Collections.singletonMap("user_id", "456");
        JavaRequest request2 = new JavaRequest(TEST_GROUP_BY, keys2);

        Map<String, Object> featureMap = Map.of(
            "feature_1", 12,
            "feature_2", 23.3,
            "feature_3", "USD"
        );

        JTry<Map<String, Object>> values1 = JTry.success(featureMap);
        JTry<Map<String, Object>> values2 = JTry.failure(new RuntimeException("some failure!"));
        JavaResponse mockResponse1 = new JavaResponse(request1, values1);
        JavaResponse mockResponse2 = new JavaResponse(request2, values2);

        List<JavaResponse> mockResponseList = List.of(
                mockResponse1,
                mockResponse2
        );
        CompletableFuture<List<JavaResponse>> futureResponse =
                CompletableFuture.completedFuture(mockResponseList);
        when(mockFetcher.fetchJoin(anyList())).thenReturn(futureResponse);

        // Capture the response that will be sent
        ArgumentCaptor<String> responseCaptor = ArgumentCaptor.forClass(String.class);

        // Trigger call
        handler.handle(routingContext);

        // Assert results
        vertx.setTimer(1000, id -> {
            verify(response).setStatusCode(200);
            verify(response).putHeader("content-type", "application/json");
            verify(response).end(responseCaptor.capture());

            // Verify response format
            JsonObject actualResponse = new JsonObject(responseCaptor.getValue());
            GetFeaturesResponse.Result expectedResult1 = GetFeaturesResponse.Result.builder().status(Success).entityKeys(keys1).features(featureMap).build();
            GetFeaturesResponse.Result expectedResult2 = GetFeaturesResponse.Result.builder().status(Failure).entityKeys(keys2).error("some failure!").build();
            List<GetFeaturesResponse.Result> expectedResultList = List.of(
                    expectedResult1,
                    expectedResult2
            );
            validateSuccessfulResponse(actualResponse, expectedResultList, context);
            async.complete();;
        });
    }

    private void validateFailureResponse(String jsonResponse, TestContext context) {
        JsonObject actualResponse = new JsonObject(jsonResponse);
        context.assertTrue(actualResponse.containsKey("errors"));

        String failureString = actualResponse.getJsonArray("errors").getString(0);
        context.assertNotNull(failureString);
    }

    private void validateSuccessfulResponse(JsonObject actualResponse, List<GetFeaturesResponse.Result> expectedResults, TestContext context) {
        context.assertTrue(actualResponse.containsKey("results"));
        context.assertEquals(actualResponse.getJsonArray("results").size(), expectedResults.size());

        JsonArray results = actualResponse.getJsonArray("results");
        for (int i = 0; i < expectedResults.size(); i++) {
            Map<String, Object> resultMap = results.getJsonObject(i).getMap();
            context.assertTrue(resultMap.containsKey("status"));
            context.assertEquals(resultMap.get("status"), expectedResults.get(i).getStatus().name());

            context.assertTrue(resultMap.containsKey("entityKeys"));
            Map<String, Object> returnedKeys = (Map<String, Object>) resultMap.get("entityKeys");
            context.assertEquals(expectedResults.get(i).getEntityKeys(), returnedKeys);

            if (expectedResults.get(i).getStatus().equals(Success)) {
                context.assertTrue(resultMap.containsKey("features"));
                Map<String, Object> returnedFeatureMap = (Map<String, Object>) resultMap.get("features");
                context.assertEquals(expectedResults.get(i).getFeatures(), returnedFeatureMap);
            } else {
                context.assertTrue(resultMap.containsKey("error"));
                String returnedErrorMsg = (String) resultMap.get("error");
                context.assertEquals(expectedResults.get(i).getError(), returnedErrorMsg);
            }
        }
    }
}
