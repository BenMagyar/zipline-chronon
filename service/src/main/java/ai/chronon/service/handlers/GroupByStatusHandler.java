package ai.chronon.service.handlers;

import ai.chronon.online.JTry;
import ai.chronon.online.JavaFetcher;
import ai.chronon.online.JavaGroupByStatusResponse;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.Collections;
import java.util.List;

public class GroupByStatusHandler implements Handler<RoutingContext> {

    private final JavaFetcher fetcher;

    public GroupByStatusHandler(JavaFetcher fetcher) {
        this.fetcher = fetcher;
    }

    @Override
    public void handle(RoutingContext ctx) {
        String entityName = ctx.pathParam("name");

        ctx.vertx()
                .<JTry<JavaGroupByStatusResponse>>executeBlocking(() -> fetcher.fetchGroupByStatus(entityName))
                .onSuccess(groupByStatusResponseTry -> handleResult(ctx, groupByStatusResponseTry))
                .onFailure(exception -> writeError(ctx, exception));
    }

    private void handleResult(RoutingContext ctx, JTry<JavaGroupByStatusResponse> groupByStatusResponseTry) {
        if (!groupByStatusResponseTry.isSuccess()) {
            writeError(ctx, groupByStatusResponseTry.getException());
            return;
        }

        JavaGroupByStatusResponse groupByStatusResponse = groupByStatusResponseTry.getValue();

        ctx.response()
                .setStatusCode(200)
                .putHeader("content-type", "application/json")
                .end(JsonObject.mapFrom(groupByStatusResponse).encode());
    }

    private void writeError(RoutingContext ctx, Throwable exception) {
        String errorMessage =
                exception == null || exception.getMessage() == null ? "Unknown error" : exception.getMessage();
        List<String> errorMessages = Collections.singletonList(errorMessage);
        int statusCode = exception instanceof IllegalArgumentException ? 400 : 500;

        ctx.response()
                .setStatusCode(statusCode)
                .putHeader("content-type", "application/json")
                .end(new JsonObject().put("errors", errorMessages).encode());
    }
}
