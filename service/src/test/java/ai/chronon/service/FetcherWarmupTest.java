package ai.chronon.service;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.online.fetcher.FeaturesResponseType;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@RunWith(VertxUnitRunner.class)
public class FetcherWarmupTest {

    @Mock private JavaFetcher mockFetcher;
    @Mock private ConfigStore mockCfgStore;

    private Vertx vertx;

    private static final String PAYLOAD_USER = "[{\"user_id\":\"1\",\"listing_id\":5}]";
    private static final String PAYLOAD_LISTING = "[{\"listing_id\":5}]";

    private static final List<String> ALL_JOINS = List.of(
            "aws.demo.test_sched_v1__1",
            "aws.demo.test_adhoc_v2__1",
            "rec.model.v1"
    );

    private JavaResponse successResponse(String joinName) {
        JavaRequest req = new JavaRequest(joinName, Map.of("user_id", "1", "listing_id", 5));
        JavaResponse response = new JavaResponse(req, JTry.success(Map.of()));
        response.valueType = FeaturesResponseType.AvroString();
        response.valuesAvroString = JTry.success("encoded-features");
        response.errorsV2 = JTry.success(Map.of());
        return response;
    }

    private JavaResponse codecFailureResponse(String joinName, String message) {
        JavaRequest req = new JavaRequest(joinName, Map.of("user_id", "1", "listing_id", 5));
        JavaResponse response = new JavaResponse(req, JTry.success(Map.of()));
        response.valueType = FeaturesResponseType.AvroString();
        response.valuesAvroString = JTry.success("encoded-features");
        response.errorsV2 = JTry.success(Map.of("join_codec_fetch_exception", message));
        return response;
    }

    private JavaResponse avroFailureResponse(String joinName, String message) {
        JavaRequest req = new JavaRequest(joinName, Map.of("user_id", "1", "listing_id", 5));
        JavaResponse response = new JavaResponse(req, JTry.success(Map.of()));
        response.valueType = FeaturesResponseType.AvroString();
        response.valuesAvroString = JTry.failure(new RuntimeException(message));
        response.errorsV2 = JTry.success(Map.of());
        return response;
    }

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        vertx = Vertx.vertx();
    }

    @After
    public void tearDown(TestContext ctx) {
        vertx.close(ctx.asyncAssertSuccess());
    }

    @Test
    public void testNoOpWhenNoGroupsConfigured(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, never()).listJoins(anyBoolean());
                    verify(mockFetcher, never()).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testNoOpWhenPayloadInvalid(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", "{not valid json array")
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, never()).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testSingleGroupRegexFiltersJoins(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // Only aws.demo.test_sched_v1__1 matches — the other two do not
                    verify(mockFetcher).prepareJoinFastSerde(
                            List.of("aws.demo.test_sched_v1__1"), 60_000L);
                    verify(mockFetcher, times(1)).fetchJoinBase64Avro(anyList());
                    verify(mockFetcher, never()).fetchJoin(anyList());
                    async.complete();
                });
    }

    @Test
    public void testFastSerdePreparationRunsBeforeFetchWarmup(TestContext ctx) {
        Async async = ctx.async();
        String joinName = "aws.demo.test_sched_v1__1";

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenReturn(
                CompletableFuture.completedFuture(List.of(successResponse(joinName))));

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    InOrder order = inOrder(mockFetcher);
                    order.verify(mockFetcher).prepareJoinFastSerde(List.of(joinName), 60_000L);
                    order.verify(mockFetcher).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testWarmupRemainsIncompleteWhileFastSerdePreparationIsRunning(TestContext ctx) throws Exception {
        Async async = ctx.async(10_000);
        String joinName = "aws.demo.test_sched_v1__1";
        CountDownLatch preparationStarted = new CountDownLatch(1);
        CountDownLatch releasePreparation = new CountDownLatch(1);

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        doAnswer(invocation -> {
            preparationStarted.countDown();
            if (!releasePreparation.await(5, TimeUnit.SECONDS)) {
                throw new RuntimeException("test did not release FastSerde preparation");
            }
            return null;
        }).when(mockFetcher).prepareJoinFastSerde(anyList(), anyLong());
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenReturn(
                CompletableFuture.completedFuture(List.of(successResponse(joinName))));

        io.vertx.core.Future<Void> warmup = FetcherWarmup.run(vertx, mockFetcher, mockCfgStore);
        ctx.assertTrue(preparationStarted.await(2, TimeUnit.SECONDS));
        ctx.assertFalse(warmup.isComplete());
        verify(mockFetcher, never()).fetchJoinBase64Avro(anyList());

        warmup.onComplete(ar -> {
            ctx.assertTrue(ar.succeeded());
            verify(mockFetcher).fetchJoinBase64Avro(anyList());
            async.complete();
        });
        releasePreparation.countDown();
    }

    @Test
    public void testFastSerdePreparationFailureDoesNotBlockStartup(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        doThrow(new RuntimeException("compiler unavailable"))
                .when(mockFetcher).prepareJoinFastSerde(anyList(), anyLong());
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenReturn(
                CompletableFuture.completedFuture(List.of(successResponse("aws.demo.test_sched_v1__1"))));

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, never()).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testWorkerReuseOutcomePreservesWarmupFailure(TestContext ctx) {
        Async async = ctx.async();
        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        doThrow(new RuntimeException("compiler unavailable"))
                .when(mockFetcher).prepareJoinFastSerde(anyList(), anyLong());

        FetcherWarmup.runForWorkerReuse(vertx, mockFetcher, mockCfgStore, null)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.failed());
                    ctx.assertEquals("compiler unavailable", ar.cause().getMessage());
                    verify(mockFetcher, never()).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testWorkerReuseReportsOnlyMatchedWarmedJoins(TestContext ctx) {
        Async async = ctx.async();
        String joinName = "aws.demo.test_sched_v1__1";
        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenReturn(
                CompletableFuture.completedFuture(List.of(successResponse(joinName))));

        FetcherWarmup.runForWorkerReuse(vertx, mockFetcher, mockCfgStore, null)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    ctx.assertEquals(Set.of(joinName), ar.result().warmedJoinNames());
                    async.complete();
                });
    }

    @Test
    public void testNoMatchingJoins(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("nonexistent\\.prefix.*", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, never()).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testTimesPerJoin(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("rec\\.model.*", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(3);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // 1 matching join × 3 times
                    verify(mockFetcher, times(3)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testMultipleGroups(TestContext ctx) {
        Async async = ctx.async();

        // Group 0 matches test_sched, group 1 matches test_adhoc with a different payload
        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER),
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_adhoc_v2__1", PAYLOAD_LISTING)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // 1 join per group × 1 time = 2 total fetchJoin calls
                    verify(mockFetcher, times(2)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testMultipleGroupsWithTimesPerJoin(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER),
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_adhoc_v2__1", PAYLOAD_LISTING)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(5);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // 2 groups × 1 matching join each × 5 times = 10 total
                    verify(mockFetcher, times(10)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testInvalidGroupSkippedOtherGroupsStillRun(TestContext ctx) {
        Async async = ctx.async();

        // Group 0 has invalid payload — should be skipped; group 1 is valid and should run
        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", "{bad json"),
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_adhoc_v2__1", PAYLOAD_LISTING)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // Only group 1 runs
                    verify(mockFetcher, times(1)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testInvalidRegexSkippedOtherGroupsStillRun(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("[invalid", PAYLOAD_USER),
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_adhoc_v2__1", PAYLOAD_LISTING)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(1)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testFetchJoinFailureDoesNotBlockStartup(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        CompletableFuture<List<JavaResponse>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("KV store unavailable"));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenReturn(failedFuture);

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(6)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testResponseFailureIsRetried(TestContext ctx) {
        Async async = ctx.async();
        String joinName = "aws.demo.test_sched_v1__1";

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenReturn(
                CompletableFuture.completedFuture(List.of(avroFailureResponse(joinName, "Avro encode unavailable"))),
                CompletableFuture.completedFuture(List.of(codecFailureResponse(joinName, "codec unavailable"))),
                CompletableFuture.completedFuture(List.of(successResponse(joinName))));

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(3)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testSynchronousFetchInitiationFailureIsRetried(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList()))
                .thenThrow(new RuntimeException("synchronous codec failure"))
                .thenAnswer(inv -> {
                    List<JavaRequest> reqs = inv.getArgument(0);
                    return CompletableFuture.completedFuture(
                            Collections.singletonList(successResponse(reqs.get(0).name)));
                });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(2)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testFetchInitiationRunsOffEventLoop(TestContext ctx) {
        Async async = ctx.async();
        AtomicReference<String> initiationThread = new AtomicReference<>();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            initiationThread.set(Thread.currentThread().getName());
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        vertx.runOnContext(ignored -> FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    ctx.assertNotNull(initiationThread.get());
                    ctx.assertTrue(initiationThread.get().contains("worker-thread"));
                    ctx.assertFalse(initiationThread.get().contains("eventloop-thread"));
                    async.complete();
                }));
    }

    @Test
    public void testWarmupRequestsRunSequentially(TestContext ctx) {
        Async async = ctx.async();
        String joinName = "aws.demo.test_sched_v1__1";
        CompletableFuture<List<JavaResponse>> firstFetch = new CompletableFuture<>();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(2);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenReturn(
                firstFetch,
                CompletableFuture.completedFuture(List.of(successResponse(joinName))));

        io.vertx.core.Future<Void> warmup = FetcherWarmup.run(vertx, mockFetcher, mockCfgStore);
        verify(mockFetcher, timeout(1000).times(1)).fetchJoinBase64Avro(anyList());
        verify(mockFetcher, times(1)).fetchJoinBase64Avro(anyList());

        warmup
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(2)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
        firstFetch.complete(List.of(successResponse(joinName)));
    }

    @Test
    public void testStoreRecommendationWarmsCacheBypassThenLivePathWithBoundedConcurrency(TestContext ctx)
            throws Exception {
        Async async = ctx.async();
        String joinName = "aws.demo.test_sched_v1__1";
        CountDownLatch bothStarted = new CountDownLatch(2);
        List<CompletableFuture<List<JavaResponse>>> pending = new CopyOnWriteArrayList<>();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.startupWarmupMinimumRequestsPerJoin()).thenReturn(2);
        when(mockFetcher.startupWarmupConcurrency()).thenReturn(2);
        when(mockFetcher.startupWarmupRequestBatchSize()).thenReturn(50);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64AvroWarmup(anyList(), eq(false))).thenAnswer(inv -> {
            CompletableFuture<List<JavaResponse>> response = new CompletableFuture<>();
            pending.add(response);
            bothStarted.countDown();
            return response;
        });
        when(mockFetcher.fetchJoinBase64AvroWarmup(anyList(), eq(true))).thenAnswer(inv -> {
            List<JavaRequest> requests = inv.getArgument(0);
            return CompletableFuture.completedFuture(List.of(successResponse(requests.get(0).name)));
        });

        io.vertx.core.Future<Void> warmup = FetcherWarmup.run(vertx, mockFetcher, mockCfgStore);
        ctx.assertTrue(bothStarted.await(2, TimeUnit.SECONDS));
        ctx.assertFalse(warmup.isComplete());
        verify(mockFetcher, times(2)).fetchJoinBase64AvroWarmup(anyList(), eq(false));
        verify(mockFetcher, never()).fetchJoinBase64AvroWarmup(anyList(), eq(true));
        verify(mockFetcher, never()).fetchJoinBase64Avro(anyList());
        verify(mockFetcher, times(2)).fetchJoinBase64AvroWarmup(argThat(requests ->
                requests.size() == 50
                        && requests.stream().allMatch(request -> request.atMillis == null)
                        && requests.stream().map(request -> request.keys).distinct().count() == 1),
                eq(false));

        warmup.onComplete(ar -> {
            ctx.assertTrue(ar.succeeded());
            verify(mockFetcher, times(2)).fetchJoinBase64AvroWarmup(anyList(), eq(true));
            async.complete();
        });
        pending.forEach(response -> response.complete(List.of(successResponse(joinName))));
    }

    @Test
    public void testDegradedRedisWarmupStopsEarlyButDoesNotBlockStartup(TestContext ctx) {
        Async async = ctx.async();
        String joinName = "aws.demo.test_sched_v1__1";
        AtomicInteger calls = new AtomicInteger();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(2);
        when(mockFetcher.startupWarmupMinimumRequestsPerJoin()).thenReturn(2);
        when(mockFetcher.startupWarmupConcurrency()).thenReturn(1);
        when(mockFetcher.startupWarmupRequestBatchSize()).thenReturn(50);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.startupWarmupDegradedReadCount()).thenAnswer(inv -> calls.get() == 0 ? 0L : 1L);
        when(mockFetcher.fetchJoinBase64AvroWarmup(anyList(), anyBoolean())).thenAnswer(inv -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(List.of(successResponse(joinName)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore).onComplete(ar -> {
            ctx.assertTrue(ar.succeeded());
            ctx.assertEquals(1, calls.get());
            async.complete();
        });
    }

    @Test
    public void testWarmupUsesProvidedWorkerExecutor(TestContext ctx) {
        Async async = ctx.async();
        WorkerExecutor workerExecutor = vertx.createSharedWorkerExecutor("test-fetch-init", 2);
        AtomicReference<String> initiationThread = new AtomicReference<>();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            initiationThread.set(Thread.currentThread().getName());
            return CompletableFuture.completedFuture(
                    List.of(successResponse("aws.demo.test_sched_v1__1")));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, workerExecutor)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    ctx.assertTrue(initiationThread.get().contains("test-fetch-init"));
                    workerExecutor.close();
                    async.complete();
                });
    }

    @Test
    public void testListJoinsRetriesAndSucceeds(TestContext ctx) {
        // Verifies a transient listJoins failure is retried and warmup proceeds on success.
        // The retry delay is 2s so this test will take ~2s to complete.
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("rec\\.model.*", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);

        // Fail once then succeed
        CompletableFuture<List<String>> fail1 = new CompletableFuture<>();
        fail1.completeExceptionally(new RuntimeException("timeout attempt 1"));
        when(mockFetcher.listJoins(true)).thenReturn(fail1, CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(2)).listJoins(true);
                    verify(mockFetcher, times(1)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }

    @Test
    public void testListJoinsExhaustionSkipsWarmup(TestContext ctx) {
        Async async = ctx.async();
        CompletableFuture<List<String>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("metadata unavailable"));
        when(mockFetcher.listJoins(true)).thenReturn(failed);

        FetcherWarmup.listJoinsWithRetry(vertx, mockFetcher, 0, 0L, 0)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    ctx.assertTrue(ar.result().isEmpty());
                    async.complete();
                });
    }

    @Test
    public void testListJoinsTimeoutRetriesAndSucceeds(TestContext ctx) {
        Async async = ctx.async(10_000);

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("rec\\.model.*", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);

        CompletableFuture<List<String>> neverCompletes = new CompletableFuture<>();
        when(mockFetcher.listJoins(true))
                .thenReturn(neverCompletes, CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoinBase64Avro(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(2)).listJoins(true);
                    verify(mockFetcher, times(1)).fetchJoinBase64Avro(anyList());
                    async.complete();
                });
    }
}
