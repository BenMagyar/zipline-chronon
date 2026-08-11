package ai.chronon.service;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
        return new JavaResponse(req, JTry.success(Map.of("feature_1", 1.0)));
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
                    verify(mockFetcher, never()).fetchJoin(anyList());
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
                    verify(mockFetcher, never()).fetchJoin(anyList());
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
        when(mockFetcher.fetchJoin(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // Only aws.demo.test_sched_v1__1 matches — the other two do not
                    verify(mockFetcher, times(1)).fetchJoin(anyList());
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
                    verify(mockFetcher, never()).fetchJoin(anyList());
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
        when(mockFetcher.fetchJoin(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // 1 matching join × 3 times
                    verify(mockFetcher, times(3)).fetchJoin(anyList());
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
        when(mockFetcher.fetchJoin(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // 1 join per group × 1 time = 2 total fetchJoin calls
                    verify(mockFetcher, times(2)).fetchJoin(anyList());
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
        when(mockFetcher.fetchJoin(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // 2 groups × 1 matching join each × 5 times = 10 total
                    verify(mockFetcher, times(10)).fetchJoin(anyList());
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
        when(mockFetcher.fetchJoin(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // Only group 1 runs
                    verify(mockFetcher, times(1)).fetchJoin(anyList());
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
        when(mockFetcher.fetchJoin(anyList())).thenReturn(failedFuture);

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
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
        when(mockFetcher.fetchJoin(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(2)).listJoins(true);
                    verify(mockFetcher, times(1)).fetchJoin(anyList());
                    async.complete();
                });
    }
}
