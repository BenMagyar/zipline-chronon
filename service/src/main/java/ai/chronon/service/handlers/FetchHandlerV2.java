package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaRequest;
import ai.chronon.online.JavaResponse;
import ai.chronon.online.fetcher.FeaturesResponseType;
import ai.chronon.service.model.GetFeaturesResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Failure;
import static ai.chronon.service.model.GetFeaturesResponse.Result.Status.Success;

public class FetchHandlerV2 implements Handler<RoutingContext> {
    private static final Logger logger = LoggerFactory.getLogger(FetchHandlerV2.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final JavaFetcher fetcher;
    private final BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> fetchFunction;
    private final BiFunction<List<GetFeaturesResponse.Result>, RoutingContext, Void> onSuccessFunction;
    private final WorkerExecutor workerExecutor;

    public FetchHandlerV2(JavaFetcher fetcher,
                          BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> fetchFunction,
                          BiFunction<List<GetFeaturesResponse.Result>, RoutingContext, Void> onSuccessFunction
    ) {
        this(fetcher, fetchFunction, onSuccessFunction, null);
    }

    public FetchHandlerV2(
            JavaFetcher fetcher,
            BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> fetchFunction,
            BiFunction<List<GetFeaturesResponse.Result>, RoutingContext, Void> onSuccessFunction,
            WorkerExecutor workerExecutor) {
        this.fetcher = fetcher;
        this.fetchFunction = fetchFunction;
        this.onSuccessFunction = onSuccessFunction;
        this.workerExecutor = workerExecutor;
    }

    @Override
    public void handle(RoutingContext ctx) {

        String entityName = ctx.pathParam("name");

        logger.debug("Retrieving {}", entityName);

        JTry<List<JavaRequest>> maybeRequest = parseJavaRequest(entityName, ctx.body());

        if (! maybeRequest.isSuccess()) {

            logger.error("Unable to parse request body", maybeRequest.getException());

            List<String> errorMessages = Collections.singletonList(maybeRequest.getException().getMessage());

            ctx.response()
                    .setStatusCode(400)
                    .putHeader("content-type", "application/json")
                    .end(new JsonObject().put("errors", errorMessages).encode());

            return;
        }

        List<JavaRequest> requests = maybeRequest.getValue();

        // Fetch initiation performs synchronous metadata, codec, and Future-graph construction. Keep it off the
        // event loop, but release the worker as soon as the asynchronous fetch has been started.
        Future<CompletableFuture<List<JavaResponse>>> fetchInitiation = workerExecutor == null
                ? ctx.vertx().executeBlocking(() -> fetchFunction.apply(fetcher, requests), false)
                : workerExecutor.executeBlocking(() -> fetchFunction.apply(fetcher, requests), false);
        Future<List<GetFeaturesResponse.Result>> maybeFeatureResponses =
                fetchInitiation
                        .compose(Future::fromCompletionStage)
                        .map(result ->
                                result.stream()
                                        .map(FetchHandlerV2::responseToPoJo)
                                        .collect(Collectors.toList()));

        maybeFeatureResponses.onSuccess(resultList -> onSuccessFunction.apply(resultList, ctx));

        maybeFeatureResponses.onFailure(
                err -> {

                    List<String> failureMessages = Collections.singletonList(err.getMessage());

                    ctx.response()
                            .setStatusCode(500)
                            .putHeader("content-type", "application/json")
                            .end(new JsonObject().put("errors", failureMessages).encode());
                });
    }

    public static GetFeaturesResponse.Result responseToPoJo(JavaResponse response) {
        var valueType = response.valueType;
        var builder = GetFeaturesResponse.Result.builder()
                .entityKeys(response.request.keys);

        builder.featuresErrors(response.errorsV2.getValue());

        // Get the appropriate JTry and set up success builder based on response type
        JTry featureValues;
        if (valueType == FeaturesResponseType.Map()) {
            featureValues = response.values;
            if (featureValues.isSuccess()) {
                return builder.status(Success).features((Map<String, Object>) featureValues.getValue()).build();
            }
        } else if (valueType == FeaturesResponseType.AvroString()) {
            featureValues = response.valuesAvroString;
            if (featureValues.isSuccess()) {
                return builder.status(Success).featureAvroString((String) featureValues.getValue()).build();
            }
        } else {
            return builder.status(Failure).error("Unknown response type: " + valueType).build();
        }

        // Handle failure case (featureValues.isSuccess() was false)
        return builder.status(Failure).error(featureValues.getException().getMessage()).build();
    }

    public static JTry<List<JavaRequest>> parseJavaRequest(String name, RequestBody body) {

        TypeReference<List<Map<String, Object>>> ref = new TypeReference<List<Map<String, Object>>>() { };

        try {

            List<Map<String, Object>> entityKeysList = objectMapper.readValue(body.asString(), ref);

            List<JavaRequest> requests = entityKeysList
                    .stream()
                    .map(m -> new JavaRequest(name, m))
                    .collect(Collectors.toList());

            return JTry.success(requests);

        } catch (Exception e) {
            return JTry.failure(e);
        }
    }
}
