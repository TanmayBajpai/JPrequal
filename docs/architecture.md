# Architecture

## Overview

```
                 ┌────────────────────────────────────────┐
                 │            jprequal-proxy              │
                 │                                        │
 client ────────►│  HttpServer ──► PrequalSelector.select │────► backend /work
   (loadgen)     │        │               │               │
                 │        │         ProbePool ────────────│────► backend /probe
                 │        └── reportQueryOutcome ─┘       │      (async, after
                 └────────────────────────────────────────┘       each request)
```

Requests enter the proxy, `jprequal-core` picks a replica from its probe pool,
the proxy forwards the request, and after every request it asynchronously fires
fresh probes to keep the pool current.

## Stack

- Java 21, virtual threads throughout — every request handler, probe, and
  background worker is a virtual thread.
- `com.sun.net.httpserver.HttpServer` for HTTP serving, `java.net.http.HttpClient`
  for outbound requests. No Spring, no Netty, no third-party dependencies.
- Maven multi-module build (`io.jprequal:jprequal`, version 0.1.0).

## Modules

### jprequal-core

The algorithm, isolated from any serving concerns. Five types:

| Type | Role |
|------|------|
| `Probe` | Immutable record `(replica, timestamp, rif, latencyEstimate)`. Timestamp is `System.nanoTime()` at probe receipt, used for staleness eviction. |
| `ProbePool` | Owns the probe pool and its full lifecycle: firing probes, adding/evicting/reusing them, the RIF history for the hot/cold threshold, and per-replica health tracking. `AutoCloseable`. |
| `PrequalSelector` | Implements `ReplicaSelector`; performs HCL selection over the pool snapshot. `AutoCloseable` (closes its pool). |
| `PrequalConfig` | Validated record of all tunables. Validation runs in the compact constructor and throws `IllegalArgumentException` on bad input. |
| `ReplicaSelector` | Single-method interface: `String select()`. |

`ProbePool` internals worth knowing:

- The pool is a bounded `ArrayList<PoolEntry>` (a `Probe` plus a use count),
  guarded by `synchronized` methods — the pool is small (≤ `max_size`) so a
  single lock is fine.
- A **separate 128-sample RIF history** feeds the hot/cold quantile threshold.
  It is kept independent of the pool contents so that the "all probes are hot"
  branch of HCL is genuinely reachable (if the threshold were computed from the
  pool itself, some probe would always be at or below its own quantile).
- Health tracking is a per-replica consecutive-failure counter
  (`ConcurrentHashMap`). A replica at `max_failures` is excluded from both
  selection and regular probing, and re-probed every `recovery_interval_ms`;
  any successful probe or query resets its counter.
- A background **idle prober** virtual thread wakes every 500 ms and fires
  probes if the pool has fewer than 2 entries, so the pool refills even when no
  client traffic is arriving.

See [algorithm.md](algorithm.md) for the selection and pool-management rules.

### jprequal-proxy

`ProxyServer` — a single-file HTTP reverse proxy:

- Loads `proxy.conf` via `java.util.Properties`, builds a `PrequalConfig`, and
  exits with a clear log message on invalid configuration.
- Serves on a virtual-thread-per-task executor.
- Forwards the original method, headers, and body to the selected replica,
  stripping hop-by-hop headers in both directions (`Connection`,
  `Transfer-Encoding`, `Content-Length`, etc.).
- Retries failed attempts (exception or 5xx) against a **freshly selected**
  replica, up to `max_retries` extra attempts — but only for methods it treats
  as idempotent. `POST` and `PATCH` get exactly one attempt.
- Reports every attempt's outcome to the selector (`reportQueryOutcome`), which
  drives health tracking. A response below 500 counts as success.
- After every request (success or failure) it fires probes asynchronously on a
  fresh virtual thread, so probing never adds latency to the client path.
- Returns `503 Service unavailable` if all attempts fail.
- Shutdown hook stops the server with a 5-second drain, then closes the selector.

### backend-sim

`BackendServer` — a simulated backend designed to produce realistic load
signals:

- Each process draws a random seed at startup that fixes its personality:
  base latency 100–200 ms, a throttle cycle (every 15–30 s it becomes 2–4×
  slower for 2–8 s). Higher seed ⇒ slower and more frequently throttled.
- `/work` sleeps for the (possibly throttled) base latency under a semaphore
  capping concurrency at 20. RIF is incremented **on arrival, before the
  semaphore**, so queue time is visible in the probe signal — a backend blind
  to its own queue would report a healthy RIF while drowning.
- Latency samples are bucketed by **arrival RIF** (`ConcurrentHashMap<Integer,
  Bucket>`, a 32-sample ring buffer per bucket) and the median is computed over
  the filled portion.
- `/probe` returns `{"rif": N, "latencyEstimateMillis": M}` where `M` is the
  median of the bucket nearest the current RIF (0 if no samples exist yet).

### loadgen

`LoadGenerator` — an **open-loop** load generator: arrivals follow the
configured schedule regardless of how fast responses come back, so a struggling
system under test cannot slow the generator down and mask its own queueing
(coordinated omission). Key mechanics:

- `LoadProfile` maps elapsed time to a target QPS for five patterns
  (constant, ramp, sine, step, spike); arrivals within the schedule are Poisson
  (exponential gaps) or uniform.
- Each request runs on its own virtual thread; a semaphore caps in-flight
  requests at `max_in_flight`, and arrivals beyond the cap are **dropped and
  counted**, never queued, to preserve the open-loop property.
- Stats are lock-free (`LongAdder`s plus a 1 ms-resolution `AtomicLongArray`
  histogram capped at 60 s). Warmup traffic is reported in progress lines but
  excluded from the cumulative summary.
- During idle stretches of a profile (rate ≈ 0) the schedule is re-anchored so
  the pause is not "repaid" as a burst when the rate returns.

## Threading model summary

| Thread | Module | Purpose |
|--------|--------|---------|
| Virtual thread per request | proxy, backend-sim | Request handling |
| Virtual thread per probe | core | `/probe` calls, fire-and-forget |
| Idle prober (1 virtual thread) | core | Refill pool when occupancy < 2 |
| Throttle cycler (1 virtual thread) | backend-sim | Toggle slow mode on schedule |
| Scheduler loop (main) + reporter | loadgen | Arrival scheduling, periodic reporting |

All shared pool state in `ProbePool` is confined behind its monitor; health
counters and the backend's latency buckets use concurrent maps with per-bucket
synchronization.
