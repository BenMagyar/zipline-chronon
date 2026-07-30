package ai.chronon.service.handlers;

import ai.chronon.online.JavaFetcher;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class FetchHandlerV2Test {

    @Mock
    private JavaFetcher mockFetcher;

    @Mock
    private RoutingContext routingContext;

    @Mock
    private RequestBody requestBody;

    private Vertx vertx;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        vertx = Vertx.vertx();
        when(routingContext.vertx()).thenReturn(vertx);
        when(routingContext.body()).thenReturn(requestBody);
        when(routingContext.pathParam("name")).thenReturn("test_join.v1");
        when(requestBody.asString()).thenReturn("[{\"user_id\":\"123\"}]");
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
        CountDownLatch responsePrepared = new CountDownLatch(2);
        CountDownLatch eventLoopDrained = new CountDownLatch(1);
        AtomicBoolean ranOnEventLoop = new AtomicBoolean(false);
        AtomicInteger activeFetches = new AtomicInteger();
        AtomicInteger maxActiveFetches = new AtomicInteger();
        WorkerExecutor workerExecutor = vertx.createSharedWorkerExecutor("fetch-handler-v2-init-test", 2);

        FetchHandlerV2 handler = new FetchHandlerV2(
                mockFetcher,
                (fetcher, requests) -> {
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
                },
                (results, ctx) -> {
                    responsePrepared.countDown();
                    return null;
                },
                workerExecutor);

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
            context.assertTrue(responsePrepared.await(2, TimeUnit.SECONDS));
            workerExecutor.close();
        }
    }
}
