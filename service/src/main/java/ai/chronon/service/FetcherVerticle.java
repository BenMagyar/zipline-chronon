package ai.chronon.service;

import ai.chronon.online.Api;
import ai.chronon.online.CatalystUtil;
import ai.chronon.online.JavaFetcher;
import ai.chronon.service.handlers.FetchRouter;
import ai.chronon.service.handlers.FetchRouterV2;
import ai.chronon.service.handlers.GroupBySchemaHandler;
import ai.chronon.service.handlers.GroupByStatusHandler;
import ai.chronon.service.handlers.JoinListHandler;
import ai.chronon.service.handlers.JoinSchemaHandler;
import ai.chronon.service.handlers.StatsDriftHandler;
import ai.chronon.service.handlers.StatsHandler;
import ai.chronon.service.handlers.StatsTrailingDriftHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.http.Http2Settings;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Entry point for the Chronon fetcher endpoints. We wire up our API routes and configure and launch our HTTP service here.
 * We choose to use just 1 verticle for now as it allows us to keep things simple and we don't need to scale /
 * independently deploy different endpoint routes.
 */
public class FetcherVerticle extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(FetcherVerticle.class);
    private static final String FETCH_WORKER_POOL_SIZE_PROPERTY = "ai.chronon.fetcher.worker.pool.size";
    private static final String FETCH_WORKER_POOL_SIZE_ENV = "FETCHER_WORKER_POOL_SIZE";
    private static final String FETCHER_WARMUP_JARS_PROPERTY = "ai.chronon.fetcher.warmup.jars";
    private static final String FETCHER_WARMUP_JARS_ENV = "FETCHER_WARMUP_JARS";
    private static final int DEFAULT_FETCH_WORKER_POOL_SIZE = 20;
    private static final long FETCHER_WARMUP_TIMEOUT_MILLIS = 120_000L;

    private HttpServer server;
    private WorkerExecutor fetchWorkerExecutor;

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        ConfigStore cfgStore = new ConfigStore(vertx);

        Api api = ApiProvider.buildApi(cfgStore);

        long joinConfTtl = cfgStore.getJoinConfTtlMillis();
        long joinCodecTtl = cfgStore.getJoinCodecTtlMillis();
        logger.info("Join conf TTL: {}ms, Join codec TTL: {}ms", joinConfTtl, joinCodecTtl);

        // Execute the blocking Bigtable initialization in a separate worker thread
        vertx.executeBlocking(() -> {
            prefetchJars(configuredWarmupJars());
            return api.buildJavaFetcher("feature-service", false, joinConfTtl, joinCodecTtl);
        })
        .onSuccess(fetcher -> {
            WorkerExecutor warmupWorkerExecutor = null;
            try {
                int warmupWorkerPoolSize = fetchWorkerPoolSize();
                warmupWorkerExecutor = vertx.createSharedWorkerExecutor("chronon-fetch-init", warmupWorkerPoolSize);
                logger.info("Fetcher warmup worker pool size: {}", warmupWorkerPoolSize);
                WorkerExecutor executorToClose = warmupWorkerExecutor;
                // Preserve the raw outcome here: a clean pool retains its warm thread-local state, while any failed
                // or timed-out warmup is discarded before fail-open readiness.
                FetcherWarmup.runForWorkerReuse(vertx, fetcher, cfgStore, warmupWorkerExecutor)
                    .onComplete(ar -> {
                        if (ar.succeeded()) {
                            fetchWorkerExecutor = executorToClose;
                        } else {
                            logger.warn("Warmup stopped early; continuing startup with a fresh initiation pool", ar.cause());
                            executorToClose.close();
                        }
                        try {
                            startHttpServer(cfgStore.getServerPort(), cfgStore.encodeConfig(), api, fetcher, startPromise);
                        } catch (Exception e) {
                            closeFetchWorkerExecutor();
                            startPromise.fail(e);
                        }
                    });
            } catch (Exception e) {
                if (warmupWorkerExecutor != null) {
                    warmupWorkerExecutor.close();
                }
                closeFetchWorkerExecutor();
                startPromise.fail(e);
            }
        })
        .onFailure(startPromise::fail);
    }

    /**
     * Best-effort startup localization of immutable derivation jars. A stale optional warmup entry must not stop an
     * otherwise healthy fetcher; normal ADD JAR setup retains its existing error behavior if that jar is required.
     */
    static void prefetchJars(List<String> jarUris) {
        if (jarUris.isEmpty()) {
            return;
        }

        prefetchJars(jarUris, FETCHER_WARMUP_TIMEOUT_MILLIS, CatalystUtil::prefetchJar);
    }

    static boolean prefetchJars(List<String> jarUris, long timeoutMillis, Function<String, File> jarPrefetcher) {
        if (jarUris.isEmpty()) {
            return true;
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("Fetcher jar warmup timeout must be positive");
        }

        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "chronon-fetcher-jar-warmup");
            // A non-interruptible filesystem bug must not prevent JVM shutdown.
            thread.setDaemon(true);
            return thread;
        };
        ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
        AtomicBoolean stopRequested = new AtomicBoolean();
        logger.info("Prefetching {} fetcher derivation jar(s)", jarUris.size());
        long allStartedAtNanos = System.nanoTime();
        Future<?> warmup = executor.submit(() -> {
            for (String jarUri : jarUris) {
                if (stopRequested.get()) {
                    return;
                }
                long startedAtNanos = System.nanoTime();
                try {
                    File localFile = jarPrefetcher.apply(jarUri);
                    logger.info("Prefetched fetcher derivation jar {} ({} bytes) in {}ms",
                            jarUri, localFile.length(), (System.nanoTime() - startedAtNanos) / 1_000_000L);
                } catch (Exception error) {
                    logger.warn("Fetcher derivation jar prefetch failed for {} after {}ms; continuing startup",
                            jarUri, (System.nanoTime() - startedAtNanos) / 1_000_000L, error);
                }
            }
        });
        try {
            warmup.get(timeoutMillis, TimeUnit.MILLISECONDS);
            logger.info("Fetcher derivation jar prefetch completed in {}ms",
                    (System.nanoTime() - allStartedAtNanos) / 1_000_000L);
            return true;
        } catch (TimeoutException error) {
            stopRequested.set(true);
            TimeoutException prefetchTimeout = new TimeoutException(
                    "Fetcher derivation jar prefetch exceeded its " + timeoutMillis + "ms startup budget");
            jarUris.forEach(jarUri -> CatalystUtil.expirePrefetchJar(jarUri, prefetchTimeout));
            warmup.cancel(true);
            logger.warn("Fetcher derivation jar prefetch exceeded its {}ms startup budget; continuing startup",
                    timeoutMillis);
            return false;
        } catch (InterruptedException error) {
            warmup.cancel(true);
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while warming fetcher derivation jars", error);
        } catch (ExecutionException error) {
            Throwable cause = error.getCause();
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException("Unexpected fetcher derivation jar warmup failure", cause);
        } finally {
            executor.shutdownNow();
        }
    }

    static List<String> parseWarmupJars(String configured) {
        if (configured == null || configured.trim().isEmpty()) {
            return List.of();
        }

        LinkedHashSet<String> distinct = new LinkedHashSet<>();
        for (String item : configured.split(",")) {
            String jarUri = item.trim();
            if (!jarUri.isEmpty()) {
                distinct.add(jarUri);
            }
        }
        return new ArrayList<>(distinct);
    }

    private static List<String> configuredWarmupJars() {
        String configured = System.getProperty(FETCHER_WARMUP_JARS_PROPERTY);
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv(FETCHER_WARMUP_JARS_ENV);
        }
        return parseWarmupJars(configured);
    }

    protected void startHttpServer(int port, String configJsonString, Api api, JavaFetcher fetcher, Promise<Void> startPromise) throws Exception {
        Router router = Router.router(vertx);
        createFetchWorkerExecutor();

        // Define routes

        // Set up sub-routes for the various feature retrieval apis
        router.route("/v1/fetch/*").subRouter(FetchRouter.createFetchRoutes(vertx, fetcher, fetchWorkerExecutor));
        router.route("/v2/fetch/*").subRouter(FetchRouterV2.createFetchRoutes(vertx, fetcher, fetchWorkerExecutor));

        // Set up route for list of online joins
        router.get("/v1/joins").handler(new JoinListHandler(fetcher));

        // Set up route for retrieval of Join schema
        router.get("/v1/join/:name/schema").handler(new JoinSchemaHandler(fetcher));

        // Set up route for retrieval of GroupBy schema
        router.get("/v1/groupby/:name/schema").handler(new GroupBySchemaHandler(fetcher));

        // Set up route for retrieval of GroupBy status
        router.get("/v1/groupby/:name/status").handler(new GroupByStatusHandler(fetcher));

        // Set up route for fetching enhanced statistics
        router.get("/v1/stats/:tableName/trailing-drift").handler(new StatsTrailingDriftHandler(api));
        router.get("/v1/stats/:tableName/drift").handler(new StatsDriftHandler(api));
        router.get("/v1/stats/:tableName").handler(new StatsHandler(api));

        // Health check route
        router.get("/ping").handler(ctx -> {
            ctx.json("Pong!");
        });

        // Add route to show current configuration
        router.get("/config").handler(ctx -> {
            ctx.response()
               .putHeader("content-type", "application/json")
               .end(configJsonString);
        });

        Http2Settings http2Settings = new Http2Settings()
                .setMaxConcurrentStreams(200)
                .setInitialWindowSize(1024 * 1024);

        // Start HTTP server
        HttpServerOptions httpOptions =
                new HttpServerOptions()
                        .setCompressionSupported(true)
                        .setTcpKeepAlive(true)
                        .setIdleTimeout(60)
                        // HTTP/2 specific settings - these are currently the default in our Vert.x version
                        // but we make them explicit for clarity and to guard against future changes
                        .setUseAlpn(false)           // No need for ALPN in cleartext mode
                        .setSsl(false)               // No SSL for cleartext
                        .setHttp2ClearTextEnabled(true)
                        .setInitialSettings(http2Settings);
        server = vertx.createHttpServer(httpOptions);
        server.requestHandler(router)
                .listen(port)
                .onSuccess(server -> {
                    logger.info("HTTP server started on port {}", server.actualPort());
                    startPromise.complete();
                })
                .onFailure(err -> {
                    logger.error("Failed to start HTTP server", err);
                    closeFetchWorkerExecutor();
                    startPromise.fail(err);
                });
    }

    private static int fetchWorkerPoolSize() {
        String configured = System.getProperty(FETCH_WORKER_POOL_SIZE_PROPERTY);
        if (configured == null || configured.trim().isEmpty()) {
            configured = System.getenv(FETCH_WORKER_POOL_SIZE_ENV);
        }
        if (configured == null || configured.trim().isEmpty()) {
            return DEFAULT_FETCH_WORKER_POOL_SIZE;
        }

        try {
            int poolSize = Integer.parseInt(configured.trim());
            if (poolSize <= 0) {
                throw new IllegalArgumentException("Fetcher worker pool size must be positive");
            }
            return poolSize;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("Invalid fetcher worker pool size: " + configured, exception);
        }
    }

    private void createFetchWorkerExecutor() {
        if (fetchWorkerExecutor == null) {
            int fetchWorkerPoolSize = fetchWorkerPoolSize();
            fetchWorkerExecutor = vertx.createSharedWorkerExecutor("chronon-fetch-init", fetchWorkerPoolSize);
            logger.info("Fetcher initiation worker pool size: {}", fetchWorkerPoolSize);
        }
    }

    private void closeFetchWorkerExecutor() {
        if (fetchWorkerExecutor != null) {
            fetchWorkerExecutor.close();
            fetchWorkerExecutor = null;
        }
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        logger.info("Stopping HTTP server...");
        if (server != null) {
            server.close()
                    .onSuccess(v -> {
                        logger.info("HTTP server stopped successfully");
                        closeFetchWorkerExecutor();
                        stopPromise.complete();
                    })
                    .onFailure(err -> {
                        logger.error("Failed to stop HTTP server", err);
                        closeFetchWorkerExecutor();
                        stopPromise.fail(err);
                    });
        } else {
            closeFetchWorkerExecutor();
            stopPromise.complete();
        }
    }
}
