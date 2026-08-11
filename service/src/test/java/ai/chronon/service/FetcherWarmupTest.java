package ai.chronon.service;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import io.vertx.core.Future;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@RunWith(VertxUnitRunner.class)
public class FetcherWarmupTest {

    @Mock private JavaFetcher mockFetcher;
    @Mock private ConfigStore mockCfgStore;

    private Vertx vertx;

    private static final String PAYLOAD = "[{\"user_id\":\"1\",\"listing_id\":5}]";
    private static final List<String> ALL_JOINS = List.of(
            "aws.demo.test_sched_v1__1",
            "aws.demo.other_join_v1__1",
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
    public void testNoOpWhenRegexAbsent(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupJoinRegex()).thenReturn(Optional.empty());

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, never()).listJoins(anyBoolean());
                    verify(mockFetcher, never()).fetchJoin(anyList());
                    async.complete();
                });
    }

    @Test
    public void testNoOpWhenPayloadAbsent(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupJoinRegex()).thenReturn(Optional.of("aws\\.demo\\.test_sched_v1__1"));
        when(mockCfgStore.getWarmupPayload()).thenReturn(Optional.empty());

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

        when(mockCfgStore.getWarmupJoinRegex()).thenReturn(Optional.of("aws\\.demo\\.test_sched_v1__1"));
        when(mockCfgStore.getWarmupPayload()).thenReturn(Optional.of("{not valid json array"));

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, never()).fetchJoin(anyList());
                    async.complete();
                });
    }

    @Test
    public void testRegexFiltersJoins(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupJoinRegex()).thenReturn(Optional.of("aws\\.demo\\.test_sched_v1__1"));
        when(mockCfgStore.getWarmupPayload()).thenReturn(Optional.of(PAYLOAD));
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

        when(mockCfgStore.getWarmupJoinRegex()).thenReturn(Optional.of("nonexistent\\.prefix.*"));
        when(mockCfgStore.getWarmupPayload()).thenReturn(Optional.of(PAYLOAD));
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
    public void testTilesPerJoin(TestContext ctx) {
        Async async = ctx.async();

        int tiles = 3;
        when(mockCfgStore.getWarmupJoinRegex()).thenReturn(Optional.of("rec\\.model.*"));
        when(mockCfgStore.getWarmupPayload()).thenReturn(Optional.of(PAYLOAD));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(tiles);

        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.fetchJoin(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(tiles)).fetchJoin(anyList());
                    async.complete();
                });
    }

    @Test
    public void testFetchJoinFailureDoesNotBlockStartup(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupJoinRegex()).thenReturn(Optional.of("aws\\.demo\\.test_sched_v1__1"));
        when(mockCfgStore.getWarmupPayload()).thenReturn(Optional.of(PAYLOAD));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);

        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        CompletableFuture<List<JavaResponse>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("KV store unavailable"));
        when(mockFetcher.fetchJoin(anyList())).thenReturn(failedFuture);

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore)
                .onComplete(ar -> {
                    // Must succeed even though fetchJoin failed
                    ctx.assertTrue(ar.succeeded());
                    async.complete();
                });
    }

    @Test
    public void testListJoinsRetriesAndSucceeds(TestContext ctx) {
        // Verifies a transient listJoins failure is retried and warmup proceeds on success.
        // The retry delay is 2s so this test will take ~2s to complete.
        Async async = ctx.async();

        when(mockCfgStore.getWarmupJoinRegex()).thenReturn(Optional.of("rec\\.model.*"));
        when(mockCfgStore.getWarmupPayload()).thenReturn(Optional.of(PAYLOAD));
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
