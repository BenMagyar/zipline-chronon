package ai.chronon.service.handlers;

import ai.chronon.online.*;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

// Configures the routes for our get features endpoints
// We support bulkGets of groupBys and bulkGets of joins
public class FetchRouter {

    public static class GroupByFetcherFunction implements BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> {
        @Override
        public CompletableFuture<List<JavaResponse>> apply(JavaFetcher fetcher, List<JavaRequest> requests) {
            return fetcher.fetchGroupBys(requests);
        }
    }

    public static class JoinFetcherFunction implements BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> {
        @Override
        public CompletableFuture<List<JavaResponse>> apply(JavaFetcher fetcher, List<JavaRequest> requests) {
            return fetcher.fetchJoin(requests);
        }
    }

    public static class ModelTransformsFetcherFunction implements BiFunction<JavaFetcher, List<JavaRequest>, CompletableFuture<List<JavaResponse>>> {
        @Override
        public CompletableFuture<List<JavaResponse>> apply(JavaFetcher fetcher, List<JavaRequest> requests) {
            return fetcher.fetchModelTransforms(requests);
        }
    }

    public static Router createFetchRoutes(Vertx vertx, JavaFetcher fetcher) {
        return createFetchRoutes(vertx, fetcher, null);
    }

    public static Router createFetchRoutes(Vertx vertx, JavaFetcher fetcher, WorkerExecutor workerExecutor) {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        router.post("/groupby/:name")
                .handler(new FetchHandler(fetcher, new GroupByFetcherFunction(), workerExecutor));
        router.post("/join/:name")
                .handler(new FetchHandler(fetcher, new JoinFetcherFunction(), workerExecutor));
        router.post("/modeltransforms/:name")
                .handler(new FetchHandler(fetcher, new ModelTransformsFetcherFunction(), workerExecutor));

        return router;
    }
}
