/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online;

import ai.chronon.api.ScalaJavaConversions;
import ai.chronon.online.fetcher.Fetcher;
import ai.chronon.online.fetcher.FeaturesResponseType;
import com.linkedin.avro.fastserde.FastSerdeCache;
import scala.compat.java8.FutureConverters;
import scala.concurrent.ExecutionContext;
import scala.util.Try;
import ai.chronon.online.metrics.Metrics;
import ai.chronon.online.metrics.TTLCache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class JavaFetcher {
  Fetcher fetcher;
  private final MetricsInstrumenter metricsInstrumenter = new MetricsInstrumenter();

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis, Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry, String callerName, Boolean disableErrorThrows) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis, logFunc, false, registry, null, callerName, null, disableErrorThrows, null, TTLCache.DefaultTtlMillis(), TTLCache.DefaultTtlMillis());
  }

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis, Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis, logFunc, false, registry, null, null, null, false, null, TTLCache.DefaultTtlMillis(), TTLCache.DefaultTtlMillis());
  }

  public JavaFetcher(KVStore kvStore, String metaDataSet, Long timeoutMillis, Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry, ModelPlatformProvider modelPlatformProvider, String callerName, FlagStore flagStore, Boolean disableErrorThrows) {
    this.fetcher = new Fetcher(kvStore, metaDataSet, timeoutMillis, logFunc, false, registry, modelPlatformProvider, callerName, flagStore, disableErrorThrows, null, TTLCache.DefaultTtlMillis(), TTLCache.DefaultTtlMillis());
  }

    /* user builder pattern to create JavaFetcher
    example way to create the java fetcher
    JavaFetcher fetcher = new JavaFetcher.Builder(kvStore, metaDataSet, timeoutMillis, logFunc, registry)
                                        .callerName(callerName)
                                        .flagStore(flagStore)
                                        .disableErrorThrows(disableErrorThrows)
                                        .build();
     */
  private JavaFetcher(Builder builder) {
    this.fetcher = new Fetcher(builder.kvStore,
            builder.metaDataSet,
            builder.timeoutMillis,
            builder.logFunc,
            builder.debug,
            builder.registry,
            builder.modelPlatformProvider,
            builder.callerName,
            builder.flagStore,
            builder.disableErrorThrows,
            builder.executionContextOverride,
            builder.joinConfTtlMillis,
            builder.joinCodecTtlMillis);
  }

  public static class Builder {
    private KVStore kvStore;
    private String metaDataSet;
    private Long timeoutMillis;
    private Consumer<LoggableResponse> logFunc;
    private ExternalSourceRegistry registry;
    private String callerName;
    private boolean debug = false;
    private FlagStore flagStore;
    private boolean disableErrorThrows = false;
    private ExecutionContext executionContextOverride;
    private ModelPlatformProvider modelPlatformProvider;
    private long joinConfTtlMillis = TTLCache.DefaultTtlMillis();
    private long joinCodecTtlMillis = TTLCache.DefaultTtlMillis();

    public Builder(KVStore kvStore, String metaDataSet, Long timeoutMillis,
                   Consumer<LoggableResponse> logFunc, ExternalSourceRegistry registry) {
      this.kvStore = kvStore;
      this.metaDataSet = metaDataSet;
      this.timeoutMillis = timeoutMillis;
      this.logFunc = logFunc;
      this.registry = registry;
    }

    public Builder callerName(String callerName) {
      this.callerName = callerName;
      return this;
    }

    public Builder flagStore(FlagStore flagStore) {
      this.flagStore = flagStore;
      return this;
    }

    public Builder disableErrorThrows(boolean disableErrorThrows) {
      this.disableErrorThrows = disableErrorThrows;
      return this;
    }

    public Builder debug(boolean debug) {
      this.debug = debug;
      return this;
    }

    public Builder executionContextOverride(ExecutionContext executionContextOverride) {
      this.executionContextOverride = executionContextOverride;
      return this;
    }

    public Builder modelPlatformProvider(ModelPlatformProvider modelPlatformProvider) {
      this.modelPlatformProvider = modelPlatformProvider;
      return this;
    }

    public Builder joinConfTtlMillis(long joinConfTtlMillis) {
      this.joinConfTtlMillis = joinConfTtlMillis;
      return this;
    }

    public Builder joinCodecTtlMillis(long joinCodecTtlMillis) {
      this.joinCodecTtlMillis = joinCodecTtlMillis;
      return this;
    }

    public JavaFetcher build() {
      return new JavaFetcher(this);
    }
  }

  private List<Fetcher.Request> toScalaRequests(List<JavaRequest> requests, boolean isGroupBy, long startTs) {
    List<Fetcher.Request> scalaRequests = new ArrayList<>();
    Set<String> requestNames = new LinkedHashSet<>();
    for (JavaRequest request : requests) {
      scalaRequests.add(request.toScalaRequest());
      requestNames.add(request.name);
    }
    metricsInstrumenter.instrument(requestNames, isGroupBy, "java.request_conversion.latency.millis", startTs);
    return scalaRequests;
  }

  private <T extends Fetcher.BaseResponse> CompletableFuture<List<JavaResponse>> wrapResponses(
          CompletableFuture<java.util.List<T>> cf,
          boolean isGroupBy,
          long startTs) {
    return cf.thenApply(responses -> {
        long conversionStartTs = System.currentTimeMillis();
        List<JavaResponse> jResps = responses.stream()
            .map(JavaResponse::new)
            .collect(Collectors.toList());
        Set<String> names = jResps.stream()
            .map(r -> r.request.name)
            .collect(Collectors.toCollection(LinkedHashSet::new));
        metricsInstrumenter.instrument(names, isGroupBy, "java.response_conversion.latency.millis", conversionStartTs);
        metricsInstrumenter.instrument(names, isGroupBy, "java.overall.latency.millis", startTs);
        return jResps;
    });
  }

  public CompletableFuture<List<JavaResponse>> fetchGroupBys(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    return wrapResponses(this.fetcher.fetchGroupBys(toScalaRequests(requests, true, startTs)), true, startTs);
  }

  public CompletableFuture<List<JavaResponse>> fetchJoin(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    return wrapResponses(this.fetcher.fetchJoin(toScalaRequests(requests, false, startTs)), false, startTs);
  }

  public CompletableFuture<List<JavaResponse>> fetchJoinBase64Avro(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    return wrapResponses(
        this.fetcher.fetchJoinV2(toScalaRequests(requests, false, startTs), FeaturesResponseType.AvroString()),
        false, startTs);
  }

  public CompletableFuture<List<JavaResponse>> fetchModelTransforms(List<JavaRequest> requests) {
    long startTs = System.currentTimeMillis();
    return wrapResponses(this.fetcher.fetchModelTransforms(toScalaRequests(requests, false, startTs)), false, startTs);
  }

  public CompletableFuture<List<String>> listJoins(boolean isOnline) {
    // Get responses from the fetcher
    // convert to Java friendly types
    return FutureConverters.toJava(this.fetcher.metadataStore().listJoins(isOnline)).toCompletableFuture().thenApply(ScalaJavaConversions::toJava);
  }

  public JTry<JavaJoinSchemaResponse> fetchJoinSchema(String joinName) {
    Try<Fetcher.JoinSchemaResponse> scalaResponse = this.fetcher.fetchJoinSchema(joinName);
    return JTry.fromScala(scalaResponse).map(JavaJoinSchemaResponse::new);
  }

  public JTry<JavaGroupBySchemaResponse> fetchGroupBySchema(String groupByName) {
    Try<Fetcher.GroupBySchemaResponse> scalaResponse = this.fetcher.fetchGroupBySchema(groupByName);
    return JTry.fromScala(scalaResponse).map(JavaGroupBySchemaResponse::new);
  }

  public JTry<JavaGroupByStatusResponse> fetchGroupByStatus(String groupByName) {
    Try<Fetcher.GroupByStatusResponse> scalaResponse = this.fetcher.fetchGroupByStatus(groupByName);
    return JTry.fromScala(scalaResponse).map(JavaGroupByStatusResponse::new);
  }

  // Best-effort triggers FastSerde class generation for the join's and its GroupBys' schemas ahead of live
  // traffic - see Fetcher.warmUpJoinCodec for the mechanism. No request payload needed.
  public JTry<CompletableFuture<Void>> warmUpJoinCodec(String joinName) {
    return warmUpJoinCodec(joinName, 0L);
  }

  // The returned JTry's success/failure reflects whether resolving the join/GroupBys and firing the
  // (synchronous, cheap) FastSerde triggers succeeded. The wrapped CompletableFuture completes once the
  // (optionally asynchronous, non-blocking) wait for those triggered compiles to settle finishes or times
  // out at waitForCompileMillis - awaiting it does not block any thread. See Fetcher.warmUpJoinCodec /
  // AvroCodec.warmUp for the mechanism.
  public JTry<CompletableFuture<Void>> warmUpJoinCodec(String joinName, long waitForCompileMillis) {
    Try<scala.concurrent.Future<scala.runtime.BoxedUnit>> scalaResponse =
        this.fetcher.warmUpJoinCodec(joinName, waitForCompileMillis, FastSerdeCache.getDefaultInstance());
    return JTry.fromScala(scalaResponse)
        .map(future -> FutureConverters.toJava(future).toCompletableFuture().thenApply(ignored -> (Void) null));
  }

  @FunctionalInterface
  interface DistributionRecorder {
    void record(Metrics.Context context, String metricName, long value);
  }

  static final class MetricsInstrumenter {
    static final int MAX_CONTEXT_CACHE_ENTRIES = 4096;

    private final ConcurrentMap<String, Metrics.Context> joinContexts = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Metrics.Context> groupByContexts = new ConcurrentHashMap<>();
    private final DistributionRecorder distributionRecorder;

    MetricsInstrumenter() {
      this((context, metricName, value) -> context.distribution(metricName, value));
    }

    MetricsInstrumenter(DistributionRecorder distributionRecorder) {
      this.distributionRecorder = distributionRecorder;
    }

    void instrument(Collection<String> requestNames, boolean isGroupBy, String metricName, long startTs) {
      long value = System.currentTimeMillis() - startTs;
      Collection<String> distinctRequestNames = requestNames instanceof Set<?>
          ? requestNames
          : new LinkedHashSet<>(requestNames);
      for (String requestName : distinctRequestNames) {
        Metrics.Context context = isGroupBy ? getGroupByContext(requestName) : getJoinContext(requestName);
        distributionRecorder.record(context, metricName, value);
      }
    }

    private Metrics.Context getJoinContext(String joinName) {
      if (joinName == null) {
        return newJoinContext(null);
      }
      return getOrCreateContext(joinContexts, joinName, false);
    }

    private Metrics.Context getGroupByContext(String groupByName) {
      if (groupByName == null) {
        return newGroupByContext(null);
      }
      return getOrCreateContext(groupByContexts, groupByName, true);
    }

    private Metrics.Context getOrCreateContext(ConcurrentMap<String, Metrics.Context> contexts,
                                               String requestName,
                                               boolean isGroupBy) {
      Metrics.Context existing = contexts.get(requestName);
      if (existing != null) {
        return existing;
      }

      synchronized (contexts) {
        Metrics.Context raced = contexts.get(requestName);
        if (raced != null) {
          return raced;
        }
        Metrics.Context created = isGroupBy ? newGroupByContext(requestName) : newJoinContext(requestName);
        if (contexts.size() < MAX_CONTEXT_CACHE_ENTRIES) {
          contexts.put(requestName, created);
        }
        return created;
      }
    }

    int cachedJoinContextCount() {
      return joinContexts.size();
    }

    int cachedGroupByContextCount() {
      return groupByContexts.size();
    }

    private Metrics.Context newJoinContext(String joinName) {
      return new Metrics.Context("join.fetch", joinName, null, null, false, null, null, null, null, null, null, null);
    }

    private Metrics.Context newGroupByContext(String groupByName) {
      return new Metrics.Context("group_by.fetch", null, groupByName, null, false, null, null, null, null, null, null, null);
    }
  }
}
