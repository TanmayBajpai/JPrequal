# Probe protocol & embedding jprequal-core

## The `/probe` contract

Any backend fronted by JPrequal must expose a `GET /probe` endpoint returning
HTTP 200 with a JSON body containing two integer fields:

```json
{"rif": 3, "latencyEstimateMillis": 102}
```

| Field | Meaning |
|-------|---------|
| `rif` | Current number of requests in flight, counted from **arrival** (including any time spent queued before processing) to response completion. |
| `latencyEstimateMillis` | Median latency of recent requests at or near the current RIF level, measured from arrival to response completion. |

Notes on the contract:

- The parser is a lenient regex match for `"rif":<digits>` and
  `"latencyEstimateMillis":<digits>` — extra fields are fine, but the two keys
  must appear exactly (no whitespace between key, colon, and value).
- Non-200 responses, timeouts (`probe_timeout_ms`), and unparseable bodies all
  count as probe failures against the replica's health.
- `/probe` should be served out-of-band of the work queue and answer in
  microseconds-to-milliseconds; a probe endpoint that queues behind work
  defeats the purpose and will time out exactly when its signal matters most.

### Getting the signals right

**RIF must include queue time.** Increment the counter the moment a request
arrives, before any admission control or queueing, and decrement when the
response completes. A backend that counts only actively-processing requests
reports a healthy RIF while its queue explodes — the exact failure mode Prequal
exists to route around.

**Latency should be RIF-conditioned.** The paper's estimator (and
`backend-sim`'s reference implementation) buckets latency samples by the RIF at
request arrival and reports the median of the bucket nearest the current RIF.
This makes the estimate answer the question the balancer is actually asking:
"how long would a request sent *now* take?" A global sliding-window median is a
workable approximation but reacts more slowly to load changes.

See `backend-sim/src/main/java/io/jprequal/sim/BackendServer.java` for the
reference implementation (per-RIF 32-sample ring buffers, median over the
filled portion, nearest-bucket lookup).

## Embedding jprequal-core

To use Prequal inside your own RPC stack (gRPC, Netty, a custom client)
instead of routing through the proxy, depend on `io.jprequal:jprequal-core`
and drive a `PrequalSelector` directly:

```java
PrequalConfig config = new PrequalConfig(
    List.of("host1:8080", "host2:8080", "host3:8080", "host4:8080"),
    /* maxSize */            3,     // must be < number of replicas
    /* probingRate */        2.0,
    /* rremove */            1.0,
    /* delta */              1.0,
    /* probeTimeoutMs */     1000,
    /* maxFailures */        5,
    /* qRif */               0.84,
    /* probeStalenessMs */   1000L,
    /* recoveryIntervalMs */ 5000L);

try (PrequalSelector selector = new PrequalSelector(config)) {
    String replica = selector.select();
    boolean success = sendRequest(replica);        // your transport
    selector.reportQueryOutcome(replica, success); // feeds health tracking
    selector.fireProbes();                         // call after each request
}
```

Integration responsibilities:

- **Call `select()` per request.** It is cheap: a snapshot of a ≤16-entry pool
  plus a quantile over a 128-int history, behind one uncontended lock.
- **Call `fireProbes()` after each request** (fire-and-forget; it spawns
  virtual threads and returns immediately). This is what keeps the pool fresh —
  the probing rate scales with your query rate, as the paper intends. An
  internal idle prober keeps a minimal pool alive during quiet periods.
- **Report outcomes honestly.** `reportQueryOutcome(replica, false)` for
  transport errors and server-side failures (the proxy treats 5xx as failure,
  4xx as success — client errors say nothing about replica health).
- **Close the selector** when done; it stops the idle prober and closes the
  probe HTTP client. `PrequalSelector` is safe to share across threads.

Probing is HTTP-based (`http://<replica>/probe`) even when embedded — replicas
are addressed as `host:port` strings and must serve the probe endpoint over
HTTP regardless of what protocol your actual queries use.
