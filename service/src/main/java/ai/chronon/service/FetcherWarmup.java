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
        List<ConfigStore.WarmupGroup> groups = validateGroups(cfgStore.getWarmupGroups());
        if (groups.isEmpty()) {
            logger.info("No warmup groups configured, skipping warmup");
            return Future.succeededFuture();
        }

        int timesPerJoin = cfgStore.getWarmupTimesPerJoin();
        long warmupStart = System.currentTimeMillis();

        logger.info("Starting warmup: {} group(s), timesPerJoin={}", groups.size(), timesPerJoin);

        return listJoinsWithRetry(vertx, fetcher, LIST_JOINS_MAX_RETRIES, LIST_JOINS_RETRY_DELAY_MS, 0)
                .compose(allJoins -> fireAllGroups(fetcher, allJoins, groups, timesPerJoin, warmupStart));
    }

    // Validates and filters groups — skips any with an invalid payload with a warning.
    private static List<ConfigStore.WarmupGroup> validateGroups(List<ConfigStore.WarmupGroup> groups) {
        List<ConfigStore.WarmupGroup> valid = new ArrayList<>();
        for (ConfigStore.WarmupGroup group : groups) {
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

        List<Future<Void>> warmupFutures = new ArrayList<>();
        for (String joinName : matchingJoins) {
            List<JavaRequest> requests = FetchHandler.parseJavaRequest(joinName, group.payload).getValue();
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

        int totalRequests = matchingJoins.size() * timesPerJoin;
        logger.info("Warmup: firing {} requests for regex '{}'", totalRequests, group.regex);

        return Future.all(warmupFutures).mapEmpty();
    }
}
