# Valkey and GLIDE staged read benchmark

Date: 2026-07-29

This is a local rollout-direction benchmark, not a production capacity result. It measures Chronon's real
`KVStore.multiGet` path against a native three-primary/three-replica, six-process cluster in one Colima container. Every
stage used a fresh JVM and fresh cluster.

## Workload and controls

- 50 candidates and 50 GroupBys
- `PERF_REDIS_PROFILES=deduplicated`; every result table in this report is for that one profile
- 1,530 unique Redis read commands per `multiGet`
- 5,026 returned timed values and 224,504 payload bytes per batch
- 30 warm-up iterations and 200 measured iterations
- 32 Chronon worker threads, a 10,000-task queue, and batch concurrency 1, 16, and 32
- Colima server VM: 4 vCPUs and 7.9 GiB memory; the benchmark JVM ran on the macOS host
- Jedis used 12 shared pipeline-sync threads
- GLIDE used the shared 10-second Redis socket/request timeout; its native channel safeguards remained internal
- The direct dispatch stage is a benchmark-only client-migration control; production GLIDE reads always use cluster batching
- No Redis/Valkey container CPU limit in the main matrix
- All reported stages completed with zero failed batches

The longer-running helper in `redis/README.md` is a rollout template, not the command that produced this smoke matrix.
With Java 17 and the Testcontainers Docker environment already configured, the exact main-matrix shape was:

```bash
run_smoke() {
  env \
    CHRONON_PERF_TEST_ENABLED=true \
    PERF_WARMUP_ITERATIONS=30 \
    PERF_MEASUREMENT_ITERATIONS=200 \
    PERF_BATCH_CONCURRENCIES=1,16,32 \
    PERF_REDIS_THREAD_COUNTS=32 \
    PERF_REDIS_QUEUE_CAPACITIES=10000 \
    PERF_REDIS_PIPELINE_SYNC_THREAD_COUNTS=12 \
    PERF_REDIS_PROFILES=deduplicated \
    PERF_REDIS_SERVER="$1" \
        PERF_REDIS_CLIENT="$2" \
        PERF_REDIS_GLIDE_EXECUTION_MODE="$3" \
        PERF_REDIS_READ_FROM="$4" \
        PERF_REDIS_CLIENT_AZ="$5" \
    PERF_REDIS_SO_TIMEOUT_MS=10000 \
    ./mill --no-server redis.test.testOnly ai.chronon.integrations.redis.RedisFetcherReadPerfTestHarness
}

run_smoke redis jedis direct primary
run_smoke valkey jedis direct primary
run_smoke valkey glide direct primary
run_smoke valkey glide cluster-batch primary
run_smoke valkey glide cluster-batch all-nodes
run_smoke valkey glide cluster-batch az-affinity-replicas-and-primary zone-a
```

The AZ-local policy uses both replicas and the primary in the client's availability zone before falling back to other
nodes. Set `PERF_REDIS_CLIENT_AZ` explicitly for external-cluster benchmarks; the local Valkey fixture uses `zone-a`.

## Staged matrix

Throughput is complete Chronon `multiGet` batches per second.

| Stage | Concurrency 1 | Concurrency 16 | Concurrency 32 |
| --- | ---: | ---: | ---: |
| Redis 7.2.5 + Jedis | 195.41 | 684.35 | 728.63 |
| Valkey 9.0.5 + Jedis | 202.05 | 849.60 | 823.42 |
| Valkey + GLIDE direct + primary | 77.02 | 96.46 | 95.32 |
| Valkey + GLIDE cluster batch + primary | 97.55 | 512.68 | 570.71 |
| Valkey + GLIDE cluster batch + `ALL_NODES` | 93.32 | 661.01 | 635.51 |
| Valkey + GLIDE cluster batch + AZ-local replicas and primary | 91.94 | 580.98 | 549.71 |

P99 latency is milliseconds per complete `multiGet`.

| Stage | Concurrency 1 | Concurrency 16 | Concurrency 32 |
| --- | ---: | ---: | ---: |
| Redis 7.2.5 + Jedis | 9.299 | 43.131 | 93.105 |
| Valkey 9.0.5 + Jedis | 8.734 | 42.231 | 81.450 |
| Valkey + GLIDE direct + primary | 18.222 | 172.061 | 349.621 |
| Valkey + GLIDE cluster batch + primary | 16.477 | 57.669 | 84.554 |
| Valkey + GLIDE cluster batch + `ALL_NODES` | 16.606 | 39.909 | 84.419 |
| Valkey + GLIDE cluster batch + AZ-local replicas and primary | 22.491 | 46.141 | 99.338 |

Main observations:

- Holding Jedis fixed, Valkey was 3.40%, 24.15%, and 13.01% faster than Redis at matched concurrency. Its best point
  was 16.60% above Redis's best point in this sweep.
- GLIDE direct fan-out is not viable for this large request shape. Cluster batching reached 5.31x its throughput at
  concurrency 16 and 5.99x at concurrency 32, but GLIDE's best result was still 22.20% below Valkey/Jedis's best result.
- `ALL_NODES` improved GLIDE cluster batching by 28.93% at concurrency 16 and 11.35% at concurrency 32. Comparing the
  best point for each GLIDE batch policy, it improved throughput by 15.82%. It did not double end-to-end throughput.
- The local server has only four vCPUs. Three single-threaded primaries can already occupy roughly three server cores,
  while activating all six primary/replica processes can use at most four. A server-CPU-bound local speedup is therefore
  capped near 1.33x, and the observed 1.29x at concurrency 16 is close to that machine limit.

## Routing evidence

The harness clears per-node command counters, performs one routing probe, and asserts the expected policy before timing.

- `primary`: 1,530 primary calls and 0 replica calls.
- `ALL_NODES`: 766 primary calls and 764 replica calls, or 50.07%/49.93%.
- The GLIDE AZ-local replicas-and-primary policy with synthetic `zone-a` made 521 primary calls and 1,009 replica
  calls. The two eligible local nodes served 1,038 of 1,530 calls (67.84%); the remaining commands fell back outside
  the zone.

This proves policy selection and fallback, but synthetic zones do not reproduce cross-AZ latency or transfer cost.

## Fixed-total-CPU stress check

To make server capacity the dominant constraint, a second primary-versus-`ALL_NODES` comparison limited the entire
six-process Valkey cluster container to 0.25 vCPU and used 64 Chronon worker threads. This holds total cluster CPU fixed;
it does not model six independently provisioned managed-service nodes.

It used the same `run_smoke` settings except for
`PERF_REDIS_CPU_LIMIT=0.25`, `PERF_BATCH_CONCURRENCIES=16,32,64`, and `PERF_REDIS_THREAD_COUNTS=64`.

| Read policy | Concurrency 16 | Concurrency 32 | Concurrency 64 |
| --- | ---: | ---: | ---: |
| Primary batches/s | 45.40 | 41.62 | 40.79 |
| `ALL_NODES` batches/s | 37.15 | 43.43 | 62.74 |
| Matched change | -18.17% | +4.35% | +53.81% |

At the highest tested concurrency, routing across replicas added 53.81% throughput despite the fixed total server CPU.
Comparing each policy's best point, the gain was 38.19%. This stress result still was not 2x, but it also cannot estimate
the scale-out efficiency of separately provisioned replica nodes: a cluster batch must fan out and wait for multiple
node-local responses while every server process here competes for the same 0.25-vCPU quota.

## Decision

- The Valkey server stage is compatible and faster in this smoke test.
- Keep GLIDE opt-in. Production GLIDE reads always use `cluster-batch`; direct mode is benchmark-only.
- `ALL_NODES` is the correct no-preference treatment and demonstrably consumes primary and replica capacity, but the
  current local result does not justify a 2x throughput claim.
- `az-affinity-replicas-and-primary` is the zonal treatment. Kubernetes projects the scheduled pod's topology label;
  the fetcher does not need an operator-provided AZ value.
- Run the same `primary` versus `all-nodes` harness against the target cluster with production client CPU/memory limits,
  replica-lag metrics, and enough load generators to saturate the service before choosing rollout capacity. A local
  capacity test of the 2x hypothesis needs at least six server vCPUs or six independently limited node containers.

## 2026-07-30 regression gate

The current branch was rerun after adding zonal routing, projected-AZ discovery, and GLIDE phase metrics. Each cell is
1,500 measured 50-candidate batches after 200 warmups, using 16 Chronon workers, the full 1,530-read deduplicated
profile, and read metrics disabled. The local six-node Valkey fixture was otherwise unconstrained.

Complete `KVStore.multiGet` batches per second:

| Client and policy | Concurrency 16 | Concurrency 32 | Concurrency 48 | Concurrency 64 |
| --- | ---: | ---: | ---: | ---: |
| Jedis primary | 896.78 | 919.60 | 929.93 | 922.29 |
| GLIDE primary | 697.57 | 667.65 | 681.81 | 694.38 |
| GLIDE all nodes | 677.58 | 692.73 | 688.30 | 687.86 |
| GLIDE AZ-local replicas and primary | 559.20 | 577.45 | 574.05 | 596.00 |

P95 milliseconds:

| Client and policy | Concurrency 16 | Concurrency 32 | Concurrency 48 | Concurrency 64 |
| --- | ---: | ---: | ---: | ---: |
| Jedis primary | 22.876 | 41.140 | 56.992 | 75.974 |
| GLIDE primary | 28.634 | 65.046 | 102.365 | 122.929 |
| GLIDE all nodes | 31.409 | 57.244 | 89.404 | 124.808 |
| GLIDE AZ-local replicas and primary | 38.325 | 70.926 | 107.254 | 135.835 |

P99 milliseconds:

| Client and policy | Concurrency 16 | Concurrency 32 | Concurrency 48 | Concurrency 64 |
| --- | ---: | ---: | ---: | ---: |
| Jedis primary | 27.422 | 43.835 | 63.043 | 79.202 |
| GLIDE primary | 34.592 | 88.988 | 116.850 | 133.227 |
| GLIDE all nodes | 38.771 | 80.556 | 102.507 | 148.197 |
| GLIDE AZ-local replicas and primary | 53.683 | 94.584 | 141.370 | 179.308 |

This isolated KV-only test still favors Jedis raw throughput: GLIDE all-nodes peaked 25.51% below Jedis, consistent with
the earlier staged result. The executor behavior is the reason to continue the end-to-end canary: at concurrency 64,
Jedis pinned all 16 Chronon workers and queued 48 tasks, while GLIDE all-nodes peaked at 9 active workers and a queue of
2 because native network waits do not occupy the fetcher pool. The test intentionally excludes the downstream join
callbacks that compete with blocked Jedis reads in production.

Enabling the new Chronon GLIDE phase metrics was within run-to-run noise. At concurrency 32, throughput moved from
692.73 to 694.83 batches/s and p95 from 57.244 to 57.878 ms. At concurrency 64, throughput moved from 687.86 to 681.43
batches/s (-0.94%) and p95 from 124.808 to 123.712 ms. There were zero failures in every cell.

## Full Fetcher online-shaped gate

The harness now has a second, opt-in scope that enters the real `Fetcher.fetchJoin` API with `RedisKVStoreImpl` as its
KV backend. It keeps the original KV-only scope and output unchanged. The new scope is disabled by default because
`PERF_ONLINE_SHAPED_ITERATIONS=0`; normal tests and existing benchmark commands therefore do not load its fixture or
run it.

The online-shaped fixture copies the established `FetcherFetchJoinPerfTest` topology:

- 50 candidate requests per call, 25 context GroupBys, and 25 candidate GroupBys
- 1,275 deduplicated batch reads plus 255 streaming reads, or 1,530 Redis reads per call
- 80 rotating request batches so 4,000 candidate IDs are represented
- real Chronon key encoding, tiled streaming-read planning, Redis I/O, Avro batch decoding, response indexing and
  fanout, JoinCodec identity derivation, and final response construction
- one fixed 16-thread execution context shared by Redis client work and every Fetcher continuation; ingress drivers
  remain on a separate executor
- Caffeine batch-IR caching disabled so each measured Fetcher call reaches Redis with the stated request shape

The feature names, IDs, and one-field Avro values are deterministic benchmark data. This scope does not claim to model
search-specific external calls, non-identity UDF/SQL derivation cost, response Avro encoding, or the latest production
join-part count. It isolates the client/executor interaction that the KV-only result cannot show.

Run one client per fresh JVM. `PERF_REDIS_KV_ONLY_ENABLED=false` skips the existing KV-only scope when only the new gate
is needed:

```bash
run_online_shape() {
  env \
    CHRONON_PERF_TEST_ENABLED=true \
    PERF_REDIS_KV_ONLY_ENABLED=false \
    PERF_ONLINE_SHAPED_WARMUP_ITERATIONS=200 \
    PERF_ONLINE_SHAPED_ITERATIONS=1500 \
    PERF_ONLINE_SHAPED_BATCH_CONCURRENCIES=16 \
    PERF_ONLINE_SHAPED_QUEUE_CAPACITY=10000 \
    PERF_ONLINE_SHAPED_PIPELINE_SYNC_THREADS=12 \
    PERF_REDIS_SERVER=valkey \
    PERF_REDIS_CLIENT="$1" \
    PERF_REDIS_GLIDE_EXECUTION_MODE=cluster-batch \
    PERF_REDIS_READ_FROM=primary \
    PERF_REDIS_THREAD_COUNTS=16 \
    PERF_REDIS_QUEUE_CAPACITIES=10000 \
    PERF_REDIS_PIPELINE_SYNC_THREAD_COUNTS=12 \
    ./mill --no-server redis.test.testOnly ai.chronon.integrations.redis.RedisFetcherReadPerfTestHarness
}

run_online_shape jedis
run_online_shape glide
```

`REDIS_FETCHER_ONLINE_SHAPED_RESULT` reports end-to-end throughput and p50/p95/p99, Fetcher completion p95/p99,
the final shared-executor callback's queue p50/p95/p99 and CPU p95, shared executor peak active/queued tasks, task
counts, failures, and a response checksum. The final callback queue timer is an explicit handoff after
`Fetcher.fetchJoin`; it does not pretend to be a per-stage timer for Fetcher's internal continuations. Compare it with
the shared executor queue depth to distinguish client latency from callback starvation.

The first matched full-Fetcher run used 100 warmups and 500 measured batches at each concurrency. Both clients used
primary reads so the result isolates client/executor behavior from replica capacity. Read metrics were disabled.

| Client | Metric | Concurrency 16 | Concurrency 32 | Concurrency 64 |
| --- | --- | ---: | ---: | ---: |
| Jedis | batches/s | 255.40 | 435.58 | 452.04 |
| GLIDE | batches/s | 270.04 | 479.88 | 396.75 |
| Jedis | p95 ms | 127.719 | 134.102 | 304.401 |
| GLIDE | p95 ms | 137.305 | 113.071 | 329.004 |
| Jedis | p99 ms | 175.040 | 171.076 | 363.128 |
| GLIDE | p99 ms | 176.993 | 139.627 | 358.811 |
| Jedis | final callback queue p95 ms | 0.596 | 5.067 | 17.586 |
| GLIDE | final callback queue p95 ms | 0.262 | 0.746 | 13.560 |
| Jedis | peak shared-executor queue | 7 | 24 | 48 |
| GLIDE | peak shared-executor queue | 7 | 16 | 55 |

At concurrency 32, GLIDE improved throughput by 10.17%, p95 by 15.68%, and p99 by 18.39%; its final callback waited
85.28% less at p95. At concurrency 16 the clients were close and GLIDE's p95 was 7.51% worse. At concurrency 64,
GLIDE's additional continuation tasks saturated the same 16 workers: throughput was 12.23% lower and p95 was 8.08%
higher, even though final callback queue time remained lower. This result supports async I/O as a moderate queueing
improvement, not a claim that changing clients alone removes the full search-join tail.

A five-warmup smoke initially produced roughly 300 ms p95 for both clients and was discarded as an under-warmed JVM
result. The first fixture version also inherited a 2024 batch boundary, causing every streaming request to fan out over
two years of daily Redis keys; the recorded matrix uses a current boundary and does not contain that artifact.
