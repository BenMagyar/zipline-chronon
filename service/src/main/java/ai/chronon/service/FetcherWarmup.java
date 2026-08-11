package ai.chronon.service;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.service.handlers.FetchHandler;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
    private static final String FEATURE_EXCEPTION_SUFFIX = "_exception";
    // Bounds both synchronous fetch initiation and the resulting asynchronous fetch.
    private static final long FETCH_TIMEOUT_SECONDS = 30L;

    public static Future<Void> run(Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore) {
        try {
            return doRun(vertx, fetcher, cfgStore)
                    .recover(error -> {
                        logger.warn("Warmup stopped early; continuing startup", error);
                        return Future.succeededFuture();
                    });
        } catch (Exception e) {
            logger.warn("Warmup encountered an unexpected error, continuing startup", e);
            return Future.succeededFuture();
        }
    }

    private static Future<Void> doRun(Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore) {
        List<ConfigStore.WarmupGroup> groups = validateGroups(cfgStore.getWarmupGroups());
        if (groups.isEmpty()) {
            logger.info("No warmup groups configured, skipping warmup");
            return Future.succeededFuture();
        }

        int timesPerJoin = cfgStore.getWarmupTimesPerJoin();
        long warmupStart = System.currentTimeMillis();

        logger.info("Starting warmup: {} group(s), timesPerJoin={}", groups.size(), timesPerJoin);

        return listJoinsWithRetry(vertx, fetcher, LIST_JOINS_MAX_RETRIES, LIST_JOINS_RETRY_DELAY_MS, 0)
                .compose(allJoins -> fireAllGroups(vertx, fetcher, allJoins, groups, timesPerJoin, warmupStart));
    }

    // Validates and filters groups — skips any with an invalid payload with a warning.
    private static List<ConfigStore.WarmupGroup> validateGroups(List<ConfigStore.WarmupGroup> groups) {
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
    private static Future<List<String>> listJoinsWithRetry(
            Vertx vertx, JavaFetcher fetcher, int maxRetries, long retryDelayMs, int attempt) {

        return Future.<List<String>>fromCompletionStage(fetcher.listJoins(true))
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
            JavaFetcher fetcher,
            List<String> allJoins,
            List<ConfigStore.WarmupGroup> groups,
            int timesPerJoin,
            long warmupStart) {

        Future<Void> groupChain = Future.succeededFuture();
        for (ConfigStore.WarmupGroup group : groups) {
            groupChain = groupChain.compose(
                    ignored -> fireWarmupRequests(vertx, fetcher, allJoins, group, timesPerJoin));
        }

        return groupChain
                .onComplete(ar -> {
                    long elapsedMs = System.currentTimeMillis() - warmupStart;
                    if (ar.succeeded()) {
                        logger.info("Warmup complete: {} group(s), {}ms elapsed", groups.size(), elapsedMs);
                    } else {
                        logger.warn("Warmup aborted after {}ms: {}", elapsedMs, ar.cause().getMessage());
                    }
                })
                .mapEmpty();
    }

    private static Future<Void> fireWarmupRequests(
            Vertx vertx,
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
        logger.info("Warmup: firing {} sequential requests for regex '{}'", totalRequests, group.regex);

        Future<Void> warmupChain = Future.succeededFuture();
        for (String joinName : matchingJoins) {
            List<JavaRequest> requests = FetchHandler.parseJavaRequest(joinName, group.payload).getValue();
            for (int time = 0; time < timesPerJoin; time++) {
                final int timeNum = time;
                warmupChain = warmupChain.compose(ignored ->
                        fetchWithRetry(vertx, fetcher, requests, joinName, timeNum, timesPerJoin, 0));
            }
        }

        return warmupChain;
    }

    private static Future<Void> fetchWithRetry(
            Vertx vertx,
            JavaFetcher fetcher,
            List<JavaRequest> requests,
            String joinName,
            int timeNum,
            int timesPerJoin,
            int attempt) {

        // fetchJoin performs synchronous metadata/codec resolution before returning its
        // CompletableFuture. Keep that phase off the event loop so this timeout can fire.
        Future<CompletableFuture<List<JavaResponse>>> fetchInitiation =
                vertx.executeBlocking(() -> fetcher.fetchJoin(requests), false);

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
                                "Warmup request {}/{} failed for join '{}' after {} attempt(s): {}",
                                timeNum + 1, timesPerJoin, joinName, attempt + 1, error.getMessage());
                        return Future.succeededFuture();
                    }

                    logger.warn(
                            "Warmup request {}/{} failed for join '{}' ({}), retrying in {}ms",
                            timeNum + 1, timesPerJoin, joinName, error.getMessage(), FETCH_RETRY_DELAY_MS);
                    Promise<Void> retryPromise = Promise.promise();
                    vertx.setTimer(FETCH_RETRY_DELAY_MS, id ->
                            fetchWithRetry(
                                    vertx,
                                    fetcher,
                                    requests,
                                    joinName,
                                    timeNum,
                                    timesPerJoin,
                                    attempt + 1)
                                    .onComplete(retryPromise));
                    return retryPromise.future();
                });
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
            if (response == null || response.values == null) {
                failures.add("fetchJoin returned an invalid response");
            } else if (!response.values.isSuccess()) {
                Throwable error = response.values.getException();
                String message = error == null ? null : error.getMessage();
                failures.add(message == null ? "join response failed" : message);
            } else if (response.values.getValue() != null) {
                response.values.getValue().forEach((feature, value) -> {
                    if (feature.endsWith(FEATURE_EXCEPTION_SUFFIX)) {
                        failures.add(feature + ": " + String.valueOf(value));
                    }
                });
            }
        }
        return failures;
    }
}
