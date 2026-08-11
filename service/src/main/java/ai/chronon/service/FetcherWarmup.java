package ai.chronon.service;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.service.handlers.FetchHandler;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FetcherWarmup {

    private static final Logger logger = LoggerFactory.getLogger(FetcherWarmup.class);
    private static final int LIST_JOINS_MAX_RETRIES = 5;
    private static final long LIST_JOINS_RETRY_DELAY_MS = 2000L;
    // Per-fetch safety net timeout — defense-in-depth on top of any KV store SDK timeout
    private static final long FETCH_TIMEOUT_SECONDS = 30L;

    public static Future<Void> run(Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore) {
        try {
            return doRun(vertx, fetcher, cfgStore);
        } catch (Exception e) {
            logger.warn("Warmup encountered an unexpected error, continuing startup", e);
            return Future.succeededFuture();
        }
    }

    private static Future<Void> doRun(Vertx vertx, JavaFetcher fetcher, ConfigStore cfgStore) {
        Optional<String> maybeRegex = cfgStore.getWarmupJoinRegex();
        if (!maybeRegex.isPresent()) {
            logger.info("No WARMUP_JOIN_REGEX configured, skipping warmup");
            return Future.succeededFuture();
        }

        String regex = maybeRegex.get();
        Optional<String> maybePayload = cfgStore.getWarmupPayload();
        if (!maybePayload.isPresent()) {
            logger.warn("WARMUP_JOIN_REGEX is set but WARMUP_PAYLOAD is missing, skipping warmup");
            return Future.succeededFuture();
        }

        // Validate the payload parses before doing any work
        String rawPayload = maybePayload.get();
        JTry<List<JavaRequest>> payloadCheck = FetchHandler.parseJavaRequest("__warmup_check__", rawPayload);
        if (!payloadCheck.isSuccess()) {
            logger.warn("WARMUP_PAYLOAD is not valid JSON, skipping warmup: {}",
                    payloadCheck.getException().getMessage());
            return Future.succeededFuture();
        }

        int timesPerJoin = cfgStore.getWarmupTimesPerJoin();
        Pattern pattern = Pattern.compile(regex);
        long warmupStart = System.currentTimeMillis();

        logger.info("Starting warmup: regex='{}', timesPerJoin={}", regex, timesPerJoin);

        return listJoinsWithRetry(vertx, fetcher, LIST_JOINS_MAX_RETRIES, LIST_JOINS_RETRY_DELAY_MS, 0)
                .compose(allJoins -> fireWarmupRequests(fetcher, allJoins, pattern, regex, rawPayload, timesPerJoin, warmupStart));
    }

    // Retries fetcher.listJoins() up to maxRetries times with a fixed delay between attempts,
    // using Vert.x timers so we don't block any thread during the wait.
    private static Future<List<String>> listJoinsWithRetry(
            Vertx vertx, JavaFetcher fetcher, int maxRetries, long retryDelayMs, int attempt) {

        return Future.<List<String>>fromCompletionStage(fetcher.listJoins(true))
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

    private static Future<Void> fireWarmupRequests(
            JavaFetcher fetcher,
            List<String> allJoins,
            Pattern pattern,
            String regex,
            String rawPayload,
            int timesPerJoin,
            long warmupStart) {

        List<String> matchingJoins = allJoins.stream()
                .filter(name -> pattern.matcher(name).matches())
                .collect(Collectors.toList());

        if (matchingJoins.isEmpty()) {
            logger.info("Warmup: no online joins matched regex '{}', skipping. All online joins: {}", regex, allJoins);
            return Future.succeededFuture();
        }

        logger.info("Warmup: {} joins matched regex '{}': {}", matchingJoins.size(), regex, matchingJoins);

        List<Future<Void>> warmupFutures = new ArrayList<>();
        for (String joinName : matchingJoins) {
            List<JavaRequest> requests = FetchHandler.parseJavaRequest(joinName, rawPayload).getValue();
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
        }

        return Future.all(warmupFutures)
                .onComplete(ar -> {
                    long elapsedMs = System.currentTimeMillis() - warmupStart;
                    int totalRequests = matchingJoins.size() * timesPerJoin;
                    logger.info("Warmup complete: {} joins, {} total requests, {}ms elapsed",
                            matchingJoins.size(), totalRequests, elapsedMs);
                })
                .mapEmpty();
    }
}
