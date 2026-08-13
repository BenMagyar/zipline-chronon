package ai.chronon.service;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.service.handlers.FetchHandler;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FetcherWarmup {

    private static final Logger logger = LoggerFactory.getLogger(FetcherWarmup.class);
    private static final int LIST_JOINS_MAX_RETRIES = 5;
    private static final long LIST_JOINS_RETRY_DELAY_MS = 2000L;
    private static final long LIST_JOINS_TIMEOUT_SECONDS = 5L;
    private static final int FETCH_MAX_RETRIES = 5;
    private static final long FETCH_RETRY_DELAY_MS = 2000L;
    // Bounds both synchronous fetch initiation and the resulting asynchronous fetch.
    private static final long FETCH_TIMEOUT_SECONDS = 30L;
    private static final long FAST_SERDE_PREPARATION_TIMEOUT_SECONDS = 60L;

    static final class Result {
        private final Set<String> warmedJoinNames;

        Result(Collection<String> warmedJoinNames) {
            this.warmedJoinNames = Set.copyOf(new LinkedHashSet<>(warmedJoinNames));
        }

        Set<String> warmedJoinNames() {
            return warmedJoinNames;
        }
    }

    public static Future<Void> run(Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore) {
        return run(vertx, fetcher, cfgStore, null);
    }

    public static Future<Void> run(
            Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore, WorkerExecutor fetchWorkerExecutor) {
        try {
            Future<Void> completed = doRun(vertx, fetcher, cfgStore, fetchWorkerExecutor)
                    .map(ignored -> (Void) null);
            return completed
                    .recover(error -> {
                        logger.warn("Warmup stopped early; continuing startup", error);
                        return Future.<Void>succeededFuture();
                    });
        } catch (Exception e) {
            logger.warn("Warmup encountered an unexpected error, continuing startup", e);
            return Future.succeededFuture();
        }
    }

    /** Raw warmup outcome for lifecycle owners that must decide whether a worker pool is safe to reuse. */
    static Future<Result> runForWorkerReuse(
            Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore, WorkerExecutor fetchWorkerExecutor) {
        try {
            return doRun(vertx, fetcher, cfgStore, fetchWorkerExecutor);
        } catch (Exception error) {
            return Future.failedFuture(error);
        }
    }

    private static Future<Result> doRun(
            Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore, WorkerExecutor fetchWorkerExecutor) {
        List<ConfigStore.WarmupGroup> groups = validateGroups(cfgStore.getWarmupGroups());
        if (groups.isEmpty()) {
            logger.info("No warmup groups configured, skipping warmup");
            return Future.succeededFuture(new Result(List.of()));
        }

        int configuredTimesPerJoin = cfgStore.getWarmupTimesPerJoin();
        int minimumTimesPerJoin = Math.max(0, fetcher.startupWarmupMinimumRequestsPerJoin());
        int timesPerJoin = Math.max(configuredTimesPerJoin, minimumTimesPerJoin);
        int concurrency = Math.max(1, Math.min(timesPerJoin, fetcher.startupWarmupConcurrency()));
        int requestBatchSize = Math.max(1, fetcher.startupWarmupRequestBatchSize());
        boolean runRedisWarmupPhases = minimumTimesPerJoin > 0;
        long warmupStart = System.currentTimeMillis();

        logger.info(
                "Starting warmup: {} group(s), configuredTimesPerJoin={}, effectiveTimesPerJoin={}, concurrency={}, " +
                        "requestBatchSize={}",
                groups.size(), configuredTimesPerJoin, timesPerJoin, concurrency, requestBatchSize);

        return listJoinsWithRetry(vertx, fetcher, LIST_JOINS_MAX_RETRIES, LIST_JOINS_RETRY_DELAY_MS, 0)
                .compose(allJoins -> {
                    List<String> matchedJoins = matchingJoins(allJoins, groups);
                    return prepareFastSerde(vertx, fetchWorkerExecutor, fetcher, matchedJoins)
                            .compose(ignored -> fireAllGroups(
                                    vertx,
                                    fetchWorkerExecutor,
                                    fetcher,
                                    allJoins,
                                    groups,
                                    timesPerJoin,
                                    concurrency,
                                    requestBatchSize,
                                    runRedisWarmupPhases,
                                    warmupStart))
                            .map(new Result(matchedJoins));
                });
    }

    private static List<String> matchingJoins(List<String> allJoins, List<ConfigStore.WarmupGroup> groups) {
        return allJoins.stream()
                .filter(join -> groups.stream().anyMatch(group -> Pattern.matches(group.regex, join)))
                .distinct()
                .collect(Collectors.toList());
    }

    private static Future<Void> prepareFastSerde(
            Vertx vertx,
            WorkerExecutor fetchWorkerExecutor,
            JavaFetcher fetcher,
            List<String> matchingJoins) {
        if (matchingJoins.isEmpty()) {
            return Future.succeededFuture();
        }

        logger.info("Warmup: preparing FastSerde for {} join(s): {}", matchingJoins.size(), matchingJoins);
        return executeBlocking(vertx, fetchWorkerExecutor, () -> {
                    fetcher.prepareJoinFastSerde(
                            matchingJoins, TimeUnit.SECONDS.toMillis(FAST_SERDE_PREPARATION_TIMEOUT_SECONDS));
                    return null;
                }, false)
                // The inner deadline makes schema generation cooperative; this outer bound also covers synchronous
                // metadata/codec construction that cannot observe it. Warmup is best-effort, so readiness continues.
                .timeout(FAST_SERDE_PREPARATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .mapEmpty();
    }

    // Validates and filters groups — skips any with an invalid payload with a warning.
    static List<ConfigStore.WarmupGroup> validateGroups(List<ConfigStore.WarmupGroup> groups) {
        List<ConfigStore.WarmupGroup> valid = new ArrayList<>();
        for (ConfigStore.WarmupGroup group : groups) {
            try {
                Pattern.compile(group.regex);
            } catch (RuntimeException error) {
                logger.warn("Warmup group has invalid regex='{}', skipping: {}", group.regex, error.getMessage());
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
    static Future<List<String>> listJoinsWithRetry(
            Vertx vertx, JavaFetcher fetcher, int maxRetries, long retryDelayMs, int attempt) {

        Future<List<String>> listAttempt;
        try {
            listAttempt = Future.fromCompletionStage(fetcher.listJoins(true));
        } catch (Exception error) {
            listAttempt = Future.failedFuture(error);
        }

        return listAttempt
                .timeout(LIST_JOINS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
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
            Vertx vertx,
            WorkerExecutor fetchWorkerExecutor,
            JavaFetcher fetcher,
            List<String> allJoins,
            List<ConfigStore.WarmupGroup> groups,
            int timesPerJoin,
            int concurrency,
            int requestBatchSize,
            boolean runRedisWarmupPhases,
            long warmupStart) {

        Future<Void> groupChain = Future.succeededFuture();
        for (ConfigStore.WarmupGroup group : groups) {
            groupChain = groupChain.compose(
                    ignored -> fireWarmupRequests(
                            vertx,
                            fetchWorkerExecutor,
                            fetcher,
                            allJoins,
                            group,
                            timesPerJoin,
                            concurrency,
                            requestBatchSize,
                            runRedisWarmupPhases));
        }

        return groupChain
                .onComplete(ar -> {
                    long elapsedMs = System.currentTimeMillis() - warmupStart;
                    if (ar.succeeded()) {
                        logger.info("Warmup complete: {} group(s), {}ms elapsed", groups.size(), elapsedMs);
                    } else {
                        logger.warn("Warmup aborted after {}ms: {}", elapsedMs, ar.cause().getMessage());
                    }
                });
    }

    private static Future<Void> fireWarmupRequests(
            Vertx vertx,
            WorkerExecutor fetchWorkerExecutor,
            JavaFetcher fetcher,
            List<String> allJoins,
            ConfigStore.WarmupGroup group,
            int timesPerJoin,
            int concurrency,
            int requestBatchSize,
            boolean runRedisWarmupPhases) {

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

        int phasesPerJoin = runRedisWarmupPhases ? 2 : 1;
        int totalRequests = matchingJoins.size() * timesPerJoin * phasesPerJoin;
        logger.info(
                "Warmup: firing {} requests across {} phase(s) for regex '{}' at concurrency {}",
                totalRequests, phasesPerJoin, group.regex, concurrency);

        long degradedReadsBefore = fetcher.startupWarmupDegradedReadCount();

        Future<Void> warmupChain = Future.succeededFuture();
        for (String joinName : matchingJoins) {
            List<JavaRequest> requests = ensureMinimumBatchSize(
                    FetchHandler.parseJavaRequest(joinName, group.payload).getValue(), requestBatchSize);
            List<Boolean> cacheModes = runRedisWarmupPhases ? List.of(false, true) : List.of(true);
            for (boolean allowBatchIrCache : cacheModes) {
                for (int windowStart = 0; windowStart < timesPerJoin; windowStart += concurrency) {
                    int firstRequest = windowStart;
                    int windowSize = Math.min(concurrency, timesPerJoin - windowStart);
                    warmupChain = warmupChain.compose(ignored -> {
                        List<Future<?>> window = new ArrayList<>(windowSize);
                        for (int offset = 0; offset < windowSize; offset++) {
                            int timeNum = firstRequest + offset;
                            window.add(fetchWithRetry(
                                    vertx,
                                    fetchWorkerExecutor,
                                    fetcher,
                                    requests,
                                    joinName,
                                    timeNum,
                                    timesPerJoin,
                                    runRedisWarmupPhases,
                                    allowBatchIrCache,
                                    0));
                        }
                        // Wait for every started request before failing the window. Future.all fails fast, which could
                        // let readiness open while sibling GLIDE/decode work from the warmup was still running.
                        return Future.join(window).compose(results -> {
                            long degradedReads = fetcher.startupWarmupDegradedReadCount() - degradedReadsBefore;
                            return degradedReads > 0
                                    ? Future.failedFuture(
                                            "Warmup was degraded by " + degradedReads +
                                                    " Redis failed/admission/timeout read(s)")
                                    : Future.succeededFuture();
                        });
                    });
                }
            }
        }

        return warmupChain;
    }

    private static List<JavaRequest> ensureMinimumBatchSize(List<JavaRequest> requests, int minimumSize) {
        if (requests.isEmpty() || requests.size() >= minimumSize) {
            return requests;
        }
        List<JavaRequest> expanded = new ArrayList<>(minimumSize);
        for (int index = 0; index < minimumSize; index++) {
            // Keep every authored key and timestamp exact. Repeating the outer rows exercises 50-row response fan-out
            // and V2 encoding, while normal JoinPart deduplication prevents one representative entity from becoming
            // 50 independent GroupBy/Redis reads. Multiple authored rows remain independently represented.
            JavaRequest template = requests.get(index % requests.size());
            expanded.add(new JavaRequest(template.name, template.keys, template.atMillis));
        }
        return expanded;
    }

    private static Future<Void> fetchWithRetry(
            Vertx vertx,
            WorkerExecutor fetchWorkerExecutor,
            JavaFetcher fetcher,
            List<JavaRequest> requests,
            String joinName,
            int timeNum,
            int timesPerJoin,
            boolean redisWarmupPath,
            boolean allowBatchIrCache,
            int attempt) {

        // fetchJoin performs synchronous metadata/codec resolution before returning its
        // CompletableFuture. Keep that phase off the event loop so this timeout can fire.
        Future<CompletableFuture<List<JavaResponse>>> fetchInitiation = executeBlocking(
                vertx,
                fetchWorkerExecutor,
                () -> redisWarmupPath
                        ? fetcher.fetchJoinBase64AvroWarmup(requests, allowBatchIrCache)
                        : fetcher.fetchJoinBase64Avro(requests),
                false);

        return fetchInitiation
                .compose(Future::fromCompletionStage)
                .timeout(FETCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .compose(responses -> {
                    List<String> failures = responseFailures(responses);
                    return failures.isEmpty()
                            ? Future.<Void>succeededFuture()
                            : Future.<Void>failedFuture(String.join("; ", failures));
                })
                .onSuccess(ignored -> logger.debug(
                        "Warmup request {}/{} complete for join '{}'",
                        timeNum + 1, timesPerJoin, joinName))
                .recover(error -> {
                    // A timed-out initiation may still occupy its worker thread. Stop the sequential
                    // warmup chain so no queued or duplicate cold builds continue after readiness.
                    if (error instanceof TimeoutException) {
                        logger.warn(
                                "Warmup request {}/{} timed out for join '{}'; stopping remaining warmup",
                                timeNum + 1, timesPerJoin, joinName);
                        return Future.failedFuture(error);
                    }
                    if (attempt >= FETCH_MAX_RETRIES) {
                        logger.warn(
                                "Warmup request {}/{} failed for join '{}' after {} attempt(s); " +
                                        "stopping remaining warmup: {}",
                                timeNum + 1, timesPerJoin, joinName, attempt + 1, error.getMessage());
                        return Future.failedFuture(error);
                    }

                    logger.warn(
                            "Warmup request {}/{} failed for join '{}' ({}), retrying in {}ms",
                            timeNum + 1, timesPerJoin, joinName, error.getMessage(), FETCH_RETRY_DELAY_MS);
                    Promise<Void> retryPromise = Promise.promise();
                    vertx.setTimer(FETCH_RETRY_DELAY_MS, id ->
                            fetchWithRetry(
                                    vertx,
                                    fetchWorkerExecutor,
                                    fetcher,
                                    requests,
                                    joinName,
                                    timeNum,
                                    timesPerJoin,
                                    redisWarmupPath,
                                    allowBatchIrCache,
                                    attempt + 1)
                                    .onComplete(retryPromise));
                    return retryPromise.future();
                });
    }

    private static <T> Future<T> executeBlocking(
            Vertx vertx,
            WorkerExecutor fetchWorkerExecutor,
            java.util.concurrent.Callable<T> task,
            boolean ordered) {
        return fetchWorkerExecutor == null
                ? vertx.executeBlocking(task, ordered)
                : fetchWorkerExecutor.executeBlocking(task, ordered);
    }

    private static List<String> responseFailures(List<JavaResponse> responses) {
        List<String> failures = new ArrayList<>();
        if (responses == null) {
            failures.add("fetchJoin returned null responses");
            return failures;
        }
        if (responses.isEmpty()) {
            failures.add("fetchJoin returned no responses");
            return failures;
        }
        for (JavaResponse response : responses) {
            if (response == null || response.valuesAvroString == null || response.errorsV2 == null) {
                failures.add("fetchJoin returned an invalid response");
            } else if (!response.valuesAvroString.isSuccess()) {
                Throwable error = response.valuesAvroString.getException();
                String message = error == null ? null : error.getMessage();
                failures.add(message == null ? "join response failed" : message);
            } else if (response.valuesAvroString.getValue() == null) {
                failures.add("fetchJoin returned no Avro response");
            } else if (!response.errorsV2.isSuccess()) {
                Throwable error = response.errorsV2.getException();
                String message = error == null ? null : error.getMessage();
                failures.add(message == null ? "join response errors failed" : message);
            } else if (response.errorsV2.getValue() == null) {
                failures.add("fetchJoin returned no error map");
            } else {
                response.errorsV2.getValue().forEach(
                        (feature, error) -> failures.add(feature + ": " + error));
            }
        }
        return failures;
    }
}
