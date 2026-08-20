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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
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

    private Set<String> newTouchedJoins() {
        return ConcurrentHashMap.newKeySet();
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, never()).listJoins(anyBoolean());
                    verify(mockFetcher, never()).fetchJoin(anyList());
                    async.complete();
                });
    }

    @Test
    public void testNoOpWhenCompileAllDisabledAndNoGroups(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(false);

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, never()).listJoins(anyBoolean());
                    async.complete();
                });
    }

    @Test
    public void testNoOpWhenRegexInvalid(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("[invalid(regex", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    // 2 groups × 1 matching join each × 5 times = 10 total
                    verify(mockFetcher, times(10)).fetchJoin(anyList());
                    async.complete();
                });
    }

    // fireWarmupRequests used to fire every matching join's timesPerJoin requests all at once - an uncapped
    // matchingJoins * timesPerJoin burst against the KV store. It now processes one join at a time (though all
    // timesPerJoin requests within a join still fire concurrently), so concurrency should never exceed
    // timesPerJoin regardless of how many joins match.
    @Test
    public void testWarmupRequestsProcessOneJoinAtATimeNotAllJoinsConcurrently(TestContext ctx) {
        Async async = ctx.async();

        int timesPerJoin = 5;
        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\..*", PAYLOAD_USER) // matches 2 of the 3 ALL_JOINS
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(timesPerJoin);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));

        AtomicInteger inFlight = new AtomicInteger(0);
        AtomicInteger maxObservedConcurrency = new AtomicInteger(0);
        when(mockFetcher.fetchJoin(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            maxObservedConcurrency.updateAndGet(prev -> Math.max(prev, inFlight.incrementAndGet()));
            CompletableFuture<List<JavaResponse>> future = new CompletableFuture<>();
            CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS).execute(() -> {
                inFlight.decrementAndGet();
                future.complete(Collections.singletonList(successResponse(reqs.get(0).name)));
            });
            return future;
        });

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(2 * timesPerJoin)).fetchJoin(anyList());
                    ctx.assertTrue(maxObservedConcurrency.get() <= timesPerJoin,
                            "expected concurrency capped at timesPerJoin=" + timesPerJoin
                                    + " but observed " + maxObservedConcurrency.get());
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
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

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(mockFetcher, times(2)).listJoins(true);
                    verify(mockFetcher, times(1)).fetchJoin(anyList());
                    async.complete();
                });
    }

    // --- Compile-touch (all online joins) ---

    @Test
    public void testCompileAllOnlineJoinsAtStartup(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(true);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.warmUpJoinCodec(anyString(), anyLong())).thenReturn(JTry.success(CompletableFuture.completedFuture(null)));

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    for (String join : ALL_JOINS) {
                        verify(mockFetcher, times(1)).warmUpJoinCodec(eq(join), anyLong());
                    }
                    // compile-all-online-joins is independent of the regex/payload groups — no traffic replay
                    verify(mockFetcher, never()).fetchJoin(anyList());
                    async.complete();
                });
    }

    @Test
    public void testTouchedJoinsPopulatedAfterStartup(TestContext ctx) {
        Async async = ctx.async();
        Set<String> touchedJoins = newTouchedJoins();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(true);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.warmUpJoinCodec(anyString(), anyLong())).thenReturn(JTry.success(CompletableFuture.completedFuture(null)));

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, touchedJoins)
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    ctx.assertEquals(new HashSet<>(ALL_JOINS), touchedJoins);
                    async.complete();
                });
    }

    @Test
    public void testCompileTouchFailureForOneJoinDoesNotBlockOthers(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(true);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.warmUpJoinCodec(anyString(), anyLong())).thenReturn(JTry.success(CompletableFuture.completedFuture(null)));
        // Simulate an unexpected throw (rather than a returned JTry.failure) for one join
        when(mockFetcher.warmUpJoinCodec(eq(ALL_JOINS.get(0)), anyLong())).thenThrow(new RuntimeException("boom"));

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    for (String join : ALL_JOINS) {
                        verify(mockFetcher, times(1)).warmUpJoinCodec(eq(join), anyLong());
                    }
                    async.complete();
                });
    }

    // --- Periodic re-check ---

    @Test
    public void testPeriodicTouchesOnlyNewlyDiscoveredJoins(TestContext ctx) {
        Async async = ctx.async();

        Set<String> touchedJoins = newTouchedJoins();
        touchedJoins.addAll(ALL_JOINS); // pretend startup warmup already saw these

        String newJoin = "new.join.v1";
        List<String> joinsWithNew = new ArrayList<>(ALL_JOINS);
        joinsWithNew.add(newJoin);

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(true);
        when(mockCfgStore.getWarmupPeriodicIntervalSeconds()).thenReturn(1L);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(joinsWithNew));
        when(mockFetcher.warmUpJoinCodec(anyString(), anyLong())).thenReturn(JTry.success(CompletableFuture.completedFuture(null)));

        long timerId = FetcherWarmup.schedulePeriodic(vertx, mockFetcher, mockCfgStore, touchedJoins);

        // Several 1s ticks will fire in this window — the already-touched joins should never be re-touched,
        // and the new join should be touched exactly once (added to touchedJoins after its first tick).
        vertx.setTimer(2500, id -> {
            vertx.cancelTimer(timerId);
            for (String join : ALL_JOINS) {
                verify(mockFetcher, never()).warmUpJoinCodec(eq(join), anyLong());
            }
            verify(mockFetcher, times(1)).warmUpJoinCodec(eq(newJoin), anyLong());
            ctx.assertTrue(touchedJoins.contains(newJoin));
            async.complete();
        });
    }

    @Test
    public void testPeriodicTrafficReplayFiresOnceForNewlyMatchedJoin(TestContext ctx) {
        Async async = ctx.async();

        Set<String> touchedJoins = newTouchedJoins();
        String newJoin = "aws.demo.test_sched_v1__1";

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.getWarmupTimesPerJoin()).thenReturn(1);
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(false);
        when(mockCfgStore.getWarmupPeriodicIntervalSeconds()).thenReturn(1L);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(List.of(newJoin)));
        when(mockFetcher.fetchJoin(anyList())).thenAnswer(inv -> {
            List<JavaRequest> reqs = inv.getArgument(0);
            return CompletableFuture.completedFuture(
                    Collections.singletonList(successResponse(reqs.get(0).name)));
        });

        long timerId = FetcherWarmup.schedulePeriodic(vertx, mockFetcher, mockCfgStore, touchedJoins);

        vertx.setTimer(2500, id -> {
            vertx.cancelTimer(timerId);
            // Even though multiple ticks fire in this window, the join is only replayed once — after the
            // first tick it's in touchedJoins and later ticks' newJoins filter excludes it.
            verify(mockFetcher, times(1)).fetchJoin(anyList());
            async.complete();
        });
    }

    @Test
    public void testPeriodicOverlapGuardSkipsConcurrentTick(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(true);
        when(mockCfgStore.getWarmupPeriodicIntervalSeconds()).thenReturn(1L);

        // Never completes during the test window, so the first tick stays "in progress" the whole time —
        // without the overlap guard, ticks at the 1s and 2s marks would fire a second and third listJoins call.
        CompletableFuture<List<String>> neverCompletes = new CompletableFuture<>();
        when(mockFetcher.listJoins(true)).thenReturn(neverCompletes);

        long timerId = FetcherWarmup.schedulePeriodic(vertx, mockFetcher, mockCfgStore, newTouchedJoins());

        vertx.setTimer(2500, id -> {
            vertx.cancelTimer(timerId);
            verify(mockFetcher, times(1)).listJoins(true);
            async.complete();
        });
    }

    @Test
    public void testSchedulePeriodicReturnsNoTimerWhenNothingToDo(TestContext ctx) {
        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(false);

        long timerId = FetcherWarmup.schedulePeriodic(vertx, mockFetcher, mockCfgStore, newTouchedJoins());

        ctx.assertEquals(FetcherWarmup.NO_TIMER, timerId);
    }

    // getWarmupTimesPerJoin() throws IllegalArgumentException for a misconfigured (non-positive) value, even
    // when the misconfigured value is irrelevant to what's actually enabled (e.g. only compile-all-online-joins,
    // no groups). Unlike run()/doRun(), schedulePeriodic() had no try/catch around this, so the exception used
    // to propagate straight out to the caller instead of degrading gracefully like the rest of warmup does.
    @Test
    public void testSchedulePeriodicReturnsNoTimerWhenConfigThrows(TestContext ctx) {
        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of(
                new ConfigStore.WarmupGroup("aws\\.demo\\.test_sched_v1__1", PAYLOAD_USER)
        ));
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(false);
        when(mockCfgStore.getWarmupTimesPerJoin())
                .thenThrow(new IllegalArgumentException("WARMUP_TIMES_PER_JOIN must be a positive integer, got: 0"));

        long timerId = FetcherWarmup.schedulePeriodic(vertx, mockFetcher, mockCfgStore, newTouchedJoins());

        ctx.assertEquals(FetcherWarmup.NO_TIMER, timerId);
    }

    @Test
    public void testCompileWaitTimeoutIsPassedThroughToWarmUpJoinCodec(TestContext ctx) {
        Async async = ctx.async();

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(true);
        when(mockCfgStore.getWarmupCompileWaitTimeoutMillis()).thenReturn(5000L);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(ALL_JOINS));
        when(mockFetcher.warmUpJoinCodec(anyString(), anyLong())).thenReturn(JTry.success(CompletableFuture.completedFuture(null)));

        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    for (String join : ALL_JOINS) {
                        verify(mockFetcher, times(1)).warmUpJoinCodec(join, 5000L);
                    }
                    async.complete();
                });
    }

    // The actual scalability property motivating compileTouchJoins bridging the wait via
    // Future.fromCompletionStage instead of waiting on it inside executeBlocking: a design that held a worker
    // thread for the whole wait would be bounded by Vert.x's default 20-thread worker pool, needing multiple
    // rounds to get through more joins than that - so this uses 30 joins (intentionally > 20) and asserts the
    // total time stays close to a single wait, not a multiple of it.
    @Test
    public void testManyConcurrentCompileTouchesDoNotSerializeOnWorkerPool(TestContext ctx) {
        Async async = ctx.async();

        List<String> manyJoins = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            manyJoins.add("join." + i);
        }
        long waitMillis = 400L;

        when(mockCfgStore.getWarmupGroups()).thenReturn(List.of());
        when(mockCfgStore.isWarmupCompileAllOnlineJoinsEnabled()).thenReturn(true);
        when(mockCfgStore.getWarmupCompileWaitTimeoutMillis()).thenReturn(waitMillis);
        when(mockFetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(manyJoins));
        // Each call's "wait" is a real, independently-delayed CompletableFuture (not pre-completed), so the
        // only way all 30 finish close together is if none of them are serialized behind a bounded thread pool.
        when(mockFetcher.warmUpJoinCodec(anyString(), anyLong())).thenAnswer(inv -> JTry.success(
                CompletableFuture.supplyAsync(() -> null, CompletableFuture.delayedExecutor(waitMillis, TimeUnit.MILLISECONDS))));

        long start = System.currentTimeMillis();
        FetcherWarmup.run(vertx, mockFetcher, mockCfgStore, newTouchedJoins())
                .onComplete(ar -> {
                    long elapsed = System.currentTimeMillis() - start;
                    ctx.assertTrue(ar.succeeded());
                    ctx.assertTrue(elapsed < waitMillis * 2,
                            "expected roughly one wait's worth of time (~" + waitMillis + "ms), took " + elapsed + "ms — "
                                    + "suggests the wait is serializing on a bounded thread pool");
                    async.complete();
                });
    }
}
