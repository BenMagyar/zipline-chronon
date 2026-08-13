package ai.chronon.service;

import ai.chronon.online.JavaFetcher;
import io.vertx.core.WorkerExecutor;
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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(VertxUnitRunner.class)
public class OnlineJoinWarmupMonitorTest {

    @Mock private JavaFetcher fetcher;

    private Vertx vertx;
    private WorkerExecutor workerExecutor;
    private OnlineJoinWarmupMonitor monitor;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        vertx = Vertx.vertx();
        workerExecutor = vertx.createSharedWorkerExecutor("test-online-join-warmup", 1);
    }

    @After
    public void tearDown(TestContext ctx) {
        if (monitor != null) {
            monitor.close();
        } else {
            workerExecutor.close();
        }
        vertx.close(ctx.asyncAssertSuccess());
    }

    private OnlineJoinWarmupMonitor newMonitor(Set<String> alreadyWarm) {
        monitor = new OnlineJoinWarmupMonitor(
                vertx,
                fetcher,
                List.of(new ConfigStore.WarmupGroup("ranking[.]search[.].*", "[]")),
                alreadyWarm,
                workerExecutor,
                () -> 60_000L);
        return monitor;
    }

    @Test
    public void preparesOnlyNewMatchingJoinsOnce(TestContext ctx) {
        Async async = ctx.async();
        String oldJoin = "ranking.search.attempt__51";
        String newJoin = "ranking.search.attempt__52";
        when(fetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(
                List.of(oldJoin, newJoin, "recs.unconfigured.v1")));

        OnlineJoinWarmupMonitor subject = newMonitor(Set.of(oldJoin));
        subject.pollNow()
                .compose(ignored -> subject.pollNow())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(fetcher, times(1)).prepareJoinFastSerdeBackground(eq(List.of(newJoin)), anyLong());
                    verify(fetcher, never()).prepareJoinFastSerdeBackground(eq(List.of(oldJoin)), anyLong());
                    ctx.assertEquals(Set.of(oldJoin, newJoin), subject.warmedJoinNames());
                    async.complete();
                });
    }

    @Test
    public void incompletePublicationRemainsRetryable(TestContext ctx) {
        Async async = ctx.async();
        String newJoin = "ranking.search.attempt__52";
        when(fetcher.listJoins(true)).thenReturn(CompletableFuture.completedFuture(List.of(newJoin)));
        doThrow(new IllegalStateException("GroupBy metadata not published"))
                .doNothing()
                .when(fetcher).prepareJoinFastSerdeBackground(eq(List.of(newJoin)), anyLong());

        OnlineJoinWarmupMonitor subject = newMonitor(Set.of());
        subject.pollNow()
                .compose(ignored -> subject.pollNow())
                .onComplete(ar -> {
                    ctx.assertTrue(ar.succeeded());
                    verify(fetcher, times(2)).prepareJoinFastSerdeBackground(eq(List.of(newJoin)), anyLong());
                    ctx.assertEquals(Set.of(newJoin), subject.warmedJoinNames());
                    async.complete();
                });
    }

    @Test
    public void overlappingPollsDoNotDuplicateDiscoveryOrPreparation(TestContext ctx) {
        Async async = ctx.async();
        String newJoin = "ranking.search.attempt__52";
        CompletableFuture<List<String>> pendingList = new CompletableFuture<>();
        when(fetcher.listJoins(true)).thenReturn(pendingList);

        OnlineJoinWarmupMonitor subject = newMonitor(Set.of());
        io.vertx.core.Future<Void> first = subject.pollNow();
        io.vertx.core.Future<Void> overlapping = subject.pollNow();
        ctx.assertTrue(overlapping.succeeded());
        verify(fetcher, times(1)).listJoins(true);

        first.onComplete(ar -> {
            ctx.assertTrue(ar.succeeded());
            verify(fetcher, times(1)).prepareJoinFastSerdeBackground(eq(List.of(newJoin)), anyLong());
            async.complete();
        });
        pendingList.complete(List.of(newJoin));
    }
}
