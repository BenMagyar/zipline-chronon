package ai.chronon.service;

import ai.chronon.online.JavaFetcher;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Best-effort preparation of configured Joins that become online after this fetcher has started. */
final class OnlineJoinWarmupMonitor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(OnlineJoinWarmupMonitor.class);
    private static final long MIN_DISCOVERY_DELAY_MILLIS = TimeUnit.MINUTES.toMillis(4);
    private static final long MAX_DISCOVERY_DELAY_MILLIS = TimeUnit.MINUTES.toMillis(6);
    private static final long LIST_TIMEOUT_SECONDS = 5L;
    private static final long PREPARATION_TIMEOUT_MILLIS = 60_000L;

    private final Vertx vertx;
    private final JavaFetcher fetcher;
    private final WorkerExecutor workerExecutor;
    private final List<Pattern> patterns;
    private final LongSupplier delayMillis;
    private final Set<String> warmedJoinNames = ConcurrentHashMap.newKeySet();
    private final Set<String> preparingJoinNames = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean polling = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private long timerId = -1L;

    static OnlineJoinWarmupMonitor startIfEnabled(
            Vertx vertx,
            JavaFetcher fetcher,
            List<ConfigStore.WarmupGroup> groups,
            Collection<String> startupWarmedJoinNames) {
        List<ConfigStore.WarmupGroup> validGroups = FetcherWarmup.validateGroups(groups);
        if (validGroups.isEmpty() || fetcher.startupWarmupMinimumRequestsPerJoin() <= 0) {
            return null;
        }

        WorkerExecutor workerExecutor = vertx.createSharedWorkerExecutor("chronon-online-join-warmup", 1);
        OnlineJoinWarmupMonitor monitor = new OnlineJoinWarmupMonitor(
                vertx,
                fetcher,
                validGroups,
                startupWarmedJoinNames,
                workerExecutor,
                () -> ThreadLocalRandom.current().nextLong(
                        MIN_DISCOVERY_DELAY_MILLIS, MAX_DISCOVERY_DELAY_MILLIS + 1L));
        monitor.start();
        logger.info(
                "Started online Join warmup discovery for {} configured group(s); {} Join(s) already warm",
                validGroups.size(), monitor.warmedJoinNames.size());
        return monitor;
    }

    OnlineJoinWarmupMonitor(
            Vertx vertx,
            JavaFetcher fetcher,
            List<ConfigStore.WarmupGroup> groups,
            Collection<String> startupWarmedJoinNames,
            WorkerExecutor workerExecutor,
            LongSupplier delayMillis) {
        this.vertx = vertx;
        this.fetcher = fetcher;
        this.workerExecutor = workerExecutor;
        this.patterns = groups.stream().map(group -> Pattern.compile(group.regex)).collect(Collectors.toList());
        this.delayMillis = delayMillis;
        this.warmedJoinNames.addAll(startupWarmedJoinNames);
    }

    void start() {
        scheduleNext();
    }

    /** Exposed package-locally for a deterministic one-pass test without waiting for the production timer. */
    Future<Void> pollNow() {
        if (closed.get() || !polling.compareAndSet(false, true)) {
            return Future.succeededFuture();
        }

        Promise<Void> result = Promise.promise();
        Future<List<String>> listFuture;
        try {
            listFuture = Future.fromCompletionStage(fetcher.listJoins(true));
        } catch (Throwable error) {
            listFuture = Future.failedFuture(error);
        }

        listFuture
                .timeout(LIST_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .compose(this::prepareNewJoins)
                .onComplete(ar -> {
                    polling.set(false);
                    if (ar.failed()) {
                        logger.warn("Online Join warmup discovery failed; it will retry on a later pass", ar.cause());
                    }
                    result.complete();
                });
        return result.future();
    }

    Set<String> warmedJoinNames() {
        return Set.copyOf(warmedJoinNames);
    }

    private Future<Void> prepareNewJoins(List<String> onlineJoinNames) {
        if (onlineJoinNames == null || onlineJoinNames.isEmpty() || closed.get()) {
            return Future.succeededFuture();
        }

        List<String> newJoinNames = new LinkedHashSet<>(onlineJoinNames).stream()
                .filter(this::matchesConfiguredGroup)
                .filter(name -> !warmedJoinNames.contains(name))
                .filter(preparingJoinNames::add)
                .collect(Collectors.toList());

        Future<Void> chain = Future.succeededFuture();
        for (String joinName : newJoinNames) {
            chain = chain.compose(ignored -> prepareJoin(joinName));
        }
        return chain;
    }

    private Future<Void> prepareJoin(String joinName) {
        if (closed.get()) {
            preparingJoinNames.remove(joinName);
            return Future.succeededFuture();
        }

        long startedAtNanos = System.nanoTime();
        Future<Void> preparation = workerExecutor
                .<Void>executeBlocking(() -> {
                    fetcher.prepareJoinFastSerdeBackground(List.of(joinName), PREPARATION_TIMEOUT_MILLIS);
                    return null;
                }, false);
        return preparation
                .onSuccess(ignored -> {
                    warmedJoinNames.add(joinName);
                    logger.info(
                            "Prepared newly-online Join '{}' in {}ms",
                            joinName,
                            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAtNanos));
                })
                .recover(error -> {
                    // A Join can appear in the online-name index before all GroupBy metadata is published. Keep it
                    // retryable and continue with other new Joins instead of affecting readiness or live requests.
                    logger.warn("Newly-online Join '{}' is not ready for background preparation; will retry", joinName,
                            error);
                    return Future.<Void>succeededFuture();
                })
                .onComplete(ignored -> preparingJoinNames.remove(joinName));
    }

    private boolean matchesConfiguredGroup(String joinName) {
        return patterns.stream().anyMatch(pattern -> pattern.matcher(joinName).matches());
    }

    private void pollAndReschedule() {
        pollNow().onComplete(ignored -> scheduleNext());
    }

    private synchronized void scheduleNext() {
        if (closed.get()) {
            return;
        }
        long delay = Math.max(1L, delayMillis.getAsLong());
        timerId = vertx.setTimer(delay, ignored -> pollAndReschedule());
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (timerId >= 0L) {
            vertx.cancelTimer(timerId);
            timerId = -1L;
        }
        workerExecutor.close();
    }
}
