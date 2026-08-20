package ai.chronon.service;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.service.handlers.FetchHandler;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FetcherWarmup {

    private static final Logger logger = LoggerFactory.getLogger(FetcherWarmup.class);
    private static final int LIST_JOINS_MAX_RETRIES = 5;
    private static final long LIST_JOINS_RETRY_DELAY_MS = 2000L;
    // Safety net timeout for individual KV-store-backed calls (fetchJoin, listJoins) — defense-in-depth
    // on top of any KV store SDK timeout, so a hung call can't wedge warmup or the periodic tick forever.
    private static final long FETCH_TIMEOUT_SECONDS = 30L;
    // Sentinel returned by schedulePeriodic when there's nothing to schedule.
    public static final long NO_TIMER = -1L;

    // Called once at startup. touchedJoins is populated with every online join seen here, so a later
    // schedulePeriodic() call only acts on joins that come online afterward.
    public static Future<Void> run(Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore, Set<String> touchedJoins) {
        try {
            return doRun(vertx, fetcher, cfgStore, touchedJoins);
        } catch (Exception e) {
            logger.warn("Warmup encountered an unexpected error, continuing startup", e);
            return Future.succeededFuture();
        }
    }

    private static Future<Void> doRun(Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore, Set<String> touchedJoins) {
        List<ConfigStore.WarmupGroup> groups = validateGroups(cfgStore.getWarmupGroups());
        boolean compileAllOnlineJoins = cfgStore.isWarmupCompileAllOnlineJoinsEnabled();

        if (groups.isEmpty() && !compileAllOnlineJoins) {
            logger.info("No warmup groups configured and compile-all-online-joins disabled, skipping warmup");
            return Future.succeededFuture();
        }

        int timesPerJoin = cfgStore.getWarmupTimesPerJoin();
        long compileWaitMillis = cfgStore.getWarmupCompileWaitTimeoutMillis();
        long warmupStart = System.currentTimeMillis();

        logger.info("Starting warmup: {} group(s), timesPerJoin={}, compileAllOnlineJoins={}, compileWaitMillis={}",
                groups.size(), timesPerJoin, compileAllOnlineJoins, compileWaitMillis);

        return listJoinsWithRetry(vertx, fetcher, LIST_JOINS_MAX_RETRIES, LIST_JOINS_RETRY_DELAY_MS, 0)
                .compose(allJoins -> {
                    touchedJoins.addAll(allJoins);

                    List<Future<Void>> work = new ArrayList<>();
                    if (compileAllOnlineJoins) {
                        work.add(compileTouchJoins(vertx, fetcher, allJoins, compileWaitMillis));
                    }
                    if (!groups.isEmpty()) {
                        work.add(fireAllGroups(fetcher, allJoins, groups, timesPerJoin, warmupStart));
                    }
                    return Future.all(work).mapEmpty();
                });
    }

    // Called once at startup, after run() completes, if periodic re-checking is enabled. Returns the Vert.x
    // timer id (or NO_TIMER if there's nothing to schedule) so the caller can cancel it on shutdown.
    public static long schedulePeriodic(Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore, Set<String> touchedJoins) {
        try {
            return doSchedulePeriodic(vertx, fetcher, cfgStore, touchedJoins);
        } catch (Exception e) {
            logger.warn("Warmup: couldn't schedule periodic re-check, continuing without it", e);
            return NO_TIMER;
        }
    }

    private static long doSchedulePeriodic(Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore, Set<String> touchedJoins) {
        List<ConfigStore.WarmupGroup> groups = validateGroups(cfgStore.getWarmupGroups());
        boolean compileAllOnlineJoins = cfgStore.isWarmupCompileAllOnlineJoinsEnabled();

        if (groups.isEmpty() && !compileAllOnlineJoins) {
            logger.info("Warmup: nothing to do periodically (no groups configured, compile-all-online-joins disabled)");
            return NO_TIMER;
        }

        int timesPerJoin = cfgStore.getWarmupTimesPerJoin();
        long compileWaitMillis = cfgStore.getWarmupCompileWaitTimeoutMillis();
        long intervalMs = cfgStore.getWarmupPeriodicIntervalSeconds() * 1000L;
        AtomicBoolean tickInProgress = new AtomicBoolean(false);

        logger.info("Warmup: periodic re-check enabled, interval={}ms", intervalMs);

        return vertx.setPeriodic(intervalMs, id -> {
            if (!tickInProgress.compareAndSet(false, true)) {
                logger.warn("Warmup: periodic tick fired while the previous tick was still running, skipping this cycle");
                return;
            }
            runPeriodicTick(vertx, fetcher, touchedJoins, groups, timesPerJoin, compileAllOnlineJoins, compileWaitMillis)
                    .onComplete(ar -> tickInProgress.set(false));
        });
    }

    private static Future<Void> runPeriodicTick(
            Vertx vertx,
            JavaFetcher fetcher,
            Set<String> touchedJoins,
            List<ConfigStore.WarmupGroup> groups,
            int timesPerJoin,
            boolean compileAllOnlineJoins,
            long compileWaitMillis) {

        try {
            return listJoinsWithRetry(vertx, fetcher, LIST_JOINS_MAX_RETRIES, LIST_JOINS_RETRY_DELAY_MS, 0)
                    .compose(allJoins -> {
                        List<String> newJoins = allJoins.stream()
                                .filter(name -> !touchedJoins.contains(name))
                                .collect(Collectors.toList());

                        if (newJoins.isEmpty()) {
                            return Future.succeededFuture();
                        }

                        logger.info("Warmup: periodic check found {} newly-online join(s): {}", newJoins.size(), newJoins);
                        touchedJoins.addAll(newJoins);

                        List<Future<Void>> work = new ArrayList<>();
                        if (compileAllOnlineJoins) {
                            work.add(compileTouchJoins(vertx, fetcher, newJoins, compileWaitMillis));
                        }
                        if (!groups.isEmpty()) {
                            // Traffic replay fires once per join, on the tick it's first seen — newJoins never
                            // contains an already-touched join again, so this doesn't repeat on later ticks.
                            work.add(fireAllGroups(fetcher, newJoins, groups, timesPerJoin, System.currentTimeMillis()));
                        }
                        return Future.all(work).mapEmpty();
                    });
        } catch (Exception e) {
            logger.warn("Warmup: periodic tick encountered an unexpected error, will retry next cycle", e);
            return Future.succeededFuture();
        }
    }

    // Best-effort triggers FastSerde class generation for every given join's (and its GroupBys') schemas —
    // see Fetcher.warmUpJoinCodec. Each join resolves its conf and GroupBy serving info synchronously (blocking
    // KV/metadata calls on cache misses), and — when compileWaitMillis > 0 — additionally blocks (bounded) for
    // the compiles to finish, so each is dispatched onto Vert.x's worker pool rather than run in-line on the
    // event loop; "false" (unordered) lets the joins compile-touch concurrently.
    private static Future<Void> compileTouchJoins(Vertx vertx, JavaFetcher fetcher, List<String> joinNames, long compileWaitMillis) {
        if (joinNames.isEmpty()) {
            return Future.succeededFuture();
        }

        logger.info("Warmup: compile-touching schemas for {} online join(s), compileWaitMillis={}",
                joinNames.size(), compileWaitMillis);

        List<Future<Void>> touchFutures = new ArrayList<>();
        for (String joinName : joinNames) {
            // Resolving the join/GroupBys and firing the FastSerde triggers does real KV/schema resolution
            // (blocking), so that part still needs executeBlocking - but it's fast. The wait for those
            // triggered compiles to settle is non-blocking under the hood (see AvroCodec.warmUp), so bridging
            // it via Future.fromCompletionStage - rather than waiting on it inside executeBlocking - releases
            // the worker thread immediately instead of holding it for up to compileWaitMillis.
            Future<Void> touchFuture = vertx.<CompletableFuture<Void>>executeBlocking(() -> {
                JTry<CompletableFuture<Void>> result = fetcher.warmUpJoinCodec(joinName, compileWaitMillis);
                if (!result.isSuccess()) {
                    logger.warn("Warmup: couldn't compile-touch schemas for join '{}': {}",
                            joinName, result.getException().getMessage());
                    return CompletableFuture.completedFuture(null);
                }
                return result.getValue();
            }, false)
            .compose(Future::fromCompletionStage)
            // The join/GroupBy resolution above is real KV/metadata I/O with no timeout of its own - bound the
            // whole thing (resolution + the compile wait, which is separately bounded by compileWaitMillis) so
            // one join's KV lookup hanging can't stall Future.all(touchFutures) - and therefore the whole
            // startup warmup pass - forever.
            .timeout(FETCH_TIMEOUT_SECONDS * 1000L + compileWaitMillis, TimeUnit.MILLISECONDS)
            .recover(err -> {
                logger.warn("Warmup: compile-touch failed unexpectedly for join '{}': {}", joinName, err.getMessage());
                return Future.succeededFuture();
            });
            touchFutures.add(touchFuture);
        }

        return Future.all(touchFutures).mapEmpty();
    }

    // Validates and filters groups — skips any with an invalid regex or payload with a warning.
    private static List<ConfigStore.WarmupGroup> validateGroups(List<ConfigStore.WarmupGroup> groups) {
        List<ConfigStore.WarmupGroup> valid = new ArrayList<>();
        for (ConfigStore.WarmupGroup group : groups) {
            try {
                Pattern.compile(group.regex);
            } catch (Exception e) {
                logger.warn("Warmup group has invalid regex '{}', skipping: {}", group.regex, e.getMessage());
                continue;
            }
            JTry<List<JavaRequest>> check = FetchHandler.parseJavaRequest("__warmup_check__", group.payload);
            if (!check.isSuccess()) {
                logger.warn("Warmup group regex='{}' has invalid payload JSON, skipping: {}",
                        group.regex, check.getException().getMessage());
            } else {
                valid.add(group);
            }
        }
        return valid;
    }

    // Retries fetcher.listJoins() up to maxRetries times with a fixed delay between attempts,
    // using Vert.x timers so we don't block any thread during the wait.
    private static Future<List<String>> listJoinsWithRetry(
            Vertx vertx, JavaFetcher fetcher, int maxRetries, long retryDelayMs, int attempt) {

        return Future.<List<String>>fromCompletionStage(fetcher.listJoins(true))
                .timeout(FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .recover(err -> {
                    if (attempt >= maxRetries) {
                        logger.warn("Warmup: listJoins failed after {} attempt(s), skipping warmup: {}",
                                attempt + 1, err.getMessage());
                        return Future.succeededFuture(new ArrayList<>());
                    }
                    logger.warn("Warmup: listJoins attempt {}/{} failed ({}), retrying in {}ms",
                            attempt + 1, maxRetries + 1, err.getMessage(), retryDelayMs);
                    Promise<List<String>> retryPromise = Promise.promise();
                    vertx.setTimer(retryDelayMs, id ->
                            listJoinsWithRetry(vertx, fetcher, maxRetries, retryDelayMs, attempt + 1)
                                    .onComplete(retryPromise));
                    return retryPromise.future();
                });
    }

    private static Future<Void> fireAllGroups(
            JavaFetcher fetcher,
            List<String> allJoins,
            List<ConfigStore.WarmupGroup> groups,
            int timesPerJoin,
            long warmupStart) {

        List<Future<Void>> groupFutures = new ArrayList<>();
        for (ConfigStore.WarmupGroup group : groups) {
            groupFutures.add(fireWarmupRequests(fetcher, allJoins, group, timesPerJoin));
        }

        return Future.all(groupFutures)
                .onComplete(ar -> {
                    long elapsedMs = System.currentTimeMillis() - warmupStart;
                    logger.info("Warmup complete: {} group(s), {}ms elapsed", groups.size(), elapsedMs);
                })
                .mapEmpty();
    }

    private static Future<Void> fireWarmupRequests(
            JavaFetcher fetcher,
            List<String> allJoins,
            ConfigStore.WarmupGroup group,
            int timesPerJoin) {

        Pattern pattern = Pattern.compile(group.regex);
        List<String> matchingJoins = allJoins.stream()
                .filter(name -> pattern.matcher(name).matches())
                .collect(Collectors.toList());

        if (matchingJoins.isEmpty()) {
            logger.info("Warmup: no online joins matched regex '{}', skipping. All online joins: {}",
                    group.regex, allJoins);
            return Future.succeededFuture();
        }

        logger.info("Warmup: {} joins matched regex '{}': {}", matchingJoins.size(), group.regex, matchingJoins);

        int totalRequests = matchingJoins.size() * timesPerJoin;
        logger.info("Warmup: firing {} requests for regex '{}' ({} per join, one join at a time)",
                totalRequests, group.regex, timesPerJoin);

        // One join at a time (though all timesPerJoin requests within a join still fire concurrently) - firing
        // every matched join's requests all at once would be an uncapped matchingJoins * timesPerJoin burst
        // against the KV store, which is the opposite of what a warmup is for.
        Future<Void> chain = Future.succeededFuture();
        for (String joinName : matchingJoins) {
            chain = chain.compose(v -> fireJoinWarmupRequests(fetcher, joinName, group, timesPerJoin));
        }
        return chain;
    }

    private static Future<Void> fireJoinWarmupRequests(
            JavaFetcher fetcher,
            String joinName,
            ConfigStore.WarmupGroup group,
            int timesPerJoin) {

        List<JavaRequest> requests = FetchHandler.parseJavaRequest(joinName, group.payload).getValue();
        List<Future<Void>> warmupFutures = new ArrayList<>();
        for (int time = 0; time < timesPerJoin; time++) {
            final int timeNum = time;
            Future<Void> timeFuture = Future.fromCompletionStage(fetcher.fetchJoin(requests))
                    .timeout(FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .onSuccess(responses ->
                            logger.debug("Warmup request {}/{} complete for join '{}'", timeNum + 1, timesPerJoin, joinName))
                    .recover(err -> {
                        logger.warn("Warmup request {}/{} failed for join '{}': {}",
                                timeNum + 1, timesPerJoin, joinName, err.getMessage());
                        return Future.succeededFuture(null);
                    })
                    .mapEmpty();
            warmupFutures.add(timeFuture);
        }
        return Future.all(warmupFutures).mapEmpty();
    }
}
