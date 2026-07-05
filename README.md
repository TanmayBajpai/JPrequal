# JPrequal

JPrequal is a Java 21 implementation of Prequal, the load balancing algorithm deployed at YouTube and described in:

> **Load is not what you should balance: Introducing Prequal**  
> Wydrowski, Kleinberg, Rumble, Archer — NSDI 2024  
> https://www.usenix.org/conference/nsdi24/presentation/wydrowski

---

## What is Prequal

Prequal is a load balancer for distributed multi-tenant systems. Unlike traditional approaches that balance CPU utilization across replicas, Prequal selects backends using two real-time signals: **requests-in-flight (RIF)** and **latency**. It extends the power-of-d-choices paradigm with asynchronous, reusable probes and a hot-cold lexicographic (HCL) selection rule.

The core insight is that CPU utilization is a trailing signal that becomes unreliable when antagonist processes compete for CPU on shared machines. RIF and latency are instantaneous and accurately reflect a replica's current capacity to serve requests.

At YouTube, Prequal reduced tail latency by 2x, tail RIF by 5-10x, and tail memory usage by 10-20%, while enabling significantly higher utilization targets.

---

## Modules

| Module | Description |
|--------|-------------|
| `jprequal-core` | The Prequal algorithm. No I/O dependencies beyond the JDK. |
| `jprequal-proxy` | HTTP reverse proxy using `jprequal-core` for backend selection. |
| `backend-sim` | Simulated backends with RIF tracking, latency bucketing, and antagonist load for testing. |
| `loadgen` | Open-loop HTTP load generator with dynamic traffic patterns for exercising the proxy. |

---

## Requirements

- Java 21
- Maven 3.9+

---

## Quick start

### 1. Build

```bash
mvn clean install
```

### 2. Start backends

```bash
./start-backends.sh 8
```

Starts 8 backend servers on ports 9001–9008.

### 3. Configure

Edit `JPrequal.conf`:

```properties
port=8080
replicas=localhost:9001,localhost:9002,localhost:9003,localhost:9004,localhost:9005,localhost:9006,localhost:9007,localhost:9008
max_size=4
probing_rate=3
rremove=1
delta=1.0
q_rif=0.84
probe_timeout_ms=1000
probe_staleness_ms=1000
recovery_interval_ms=5000
max_failures=5
max_retries=2
request_timeout_ms=5000
```

### 4. Start the proxy

```bash
cd jprequal-proxy
mvn exec:java -Dexec.mainClass="io.jprequal.proxy.ProxyServer"
```

### 5. Send requests

```bash
curl http://localhost:8080/your/endpoint
```

### 6. Generate load

```bash
cd loadgen
mvn exec:java -Dexec.mainClass="io.jprequal.loadgen.LoadGenerator" \
    -Dexec.args="../loadgen.conf pattern=sine min_qps=20 max_qps=200"
```

---

## Load generation

The `loadgen` module drives configurable traffic at a target URL and reports throughput and latency percentiles. It is **open-loop**: arrivals follow the configured schedule regardless of how fast responses come back, so a struggling proxy or backend cannot slow the generator down and mask its own queueing (coordinated omission).

Configuration comes from `loadgen.conf` (or a config file passed as the first argument), and any key can be overridden with `key=value` arguments:

```bash
java -jar loadgen/target/loadgen-0.1.0.jar loadgen.conf qps=500 duration_s=120
```

### Traffic patterns

| Pattern | Shape |
|---------|-------|
| `constant` | `qps` for the whole run |
| `ramp` | `min_qps` → `max_qps` linearly over `duration_s` |
| `sine` | Oscillates between `min_qps` and `max_qps` with period `period_s`, starting at `min_qps` |
| `step` | `min_qps` → `max_qps` in `steps` equal levels over `duration_s` |
| `spike` | `qps` baseline, bursting to `max_qps` for `burst_s` at the start of every `period_s` |

Arrivals within the schedule are Poisson by default (realistic, bursty) or evenly spaced with `arrival=uniform`.

### Load generator configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target` | `http://localhost:8080/work` | URL to send requests to |
| `method` | `GET` | HTTP method |
| `body` | — | Optional request body |
| `headers` | — | Optional headers, semicolon-separated (`Name: Value; Name2: Value2`) |
| `pattern` | `constant` | Traffic pattern (see above) |
| `qps` | `100` | Base rate: the constant rate, or the spike baseline |
| `min_qps` | `0` | Lower rate bound for ramp/sine/step |
| `max_qps` | `qps` | Upper rate bound for ramp/sine/step and the spike burst rate |
| `period_s` | `30` | Cycle length for sine and spike patterns |
| `burst_s` | `5` | Burst length for the spike pattern |
| `steps` | `5` | Number of levels for the step pattern |
| `duration_s` | `60` | Measured run length in seconds |
| `warmup_s` | `0` | Warmup before measurement starts, excluded from the final summary |
| `arrival` | `poisson` | Arrival process: `poisson` or `uniform` |
| `request_timeout_ms` | `5000` | Per-request timeout; timeouts count as errors |
| `report_interval_s` | `5` | Progress report interval |
| `max_in_flight` | `10000` | Cap on concurrent requests; arrivals beyond it are dropped, not queued |

Progress is reported per interval, and a final summary gives throughput, error counts, and mean/p50/p90/p99/p99.9/max latency over the measured window.

---

## Backend requirements

Your backends must expose a `/probe` endpoint returning:

```json
{"rif": 3, "latencyEstimateMillis": 102}
```

Where `rif` is the current number of requests in flight and `latencyEstimateMillis` is the median latency of recent requests at or near the current RIF level, measured from request arrival (before any queueing) to response completion. The `backend-sim` module provides a reference implementation.

---

## Embedding jprequal-core

If you are running an existing RPC framework (gRPC, Netty, etc.) and want to integrate Prequal directly rather than routing through the proxy:

```java
PrequalConfig config = new PrequalConfig(
    List.of("host1:8080", "host2:8080", "host3:8080"),
    /* maxSize */            4,
    /* probingRate */        2.0,
    /* rremove */            1.0,
    /* delta */              1.0,
    /* probeTimeoutMs */     1000,
    /* maxFailures */        5,
    /* qRif */               0.84,
    /* probeStalenessMs */   1000L,
    /* recoveryIntervalMs */ 5000L
);

try (PrequalSelector selector = new PrequalSelector(config)) {
    String replica = selector.select();
    // send request to replica
    selector.reportQueryOutcome(replica, success);
    selector.fireProbes(); // call after each request
}
```

---

## Configuration reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `port` | `8080` | Port the proxy listens on |
| `replicas` | — | Comma-separated list of `host:port` backend addresses |
| `max_size` | `4` | Maximum probe pool size. Must be less than the number of replicas. |
| `probing_rate` | `2` | Probes fired per request. Fractional values supported. |
| `rremove` | `1` | Probes removed per request for degradation control. Must be less than `probing_rate`. |
| `delta` | `1.0` | Controls net probe accumulation rate in the pool (δ > 0). |
| `q_rif` | `0.84` | RIF quantile threshold separating hot from cold replicas (paper recommends 0.6–0.9). |
| `probe_timeout_ms` | `100` | Timeout for probe requests in milliseconds. |
| `probe_staleness_ms` | `1000` | Maximum probe age before eviction. |
| `recovery_interval_ms` | `5000` | Interval at which unhealthy replicas are re-probed for recovery. |
| `max_failures` | `5` | Consecutive failures before a replica is marked unhealthy. |
| `max_retries` | `2` | Maximum retry attempts for idempotent requests (GET, PUT, DELETE, HEAD). POST and PATCH are not retried. |
| `request_timeout_ms` | `5000` | Timeout for upstream requests in milliseconds. |

---

## How it works

**Probing:** After each request, the proxy fires `probing_rate` probe requests asynchronously to randomly selected healthy backends (sampled without replacement). Each probe hits the backend's `/probe` endpoint and returns the current RIF and a latency estimate. Probe results are stored in a bounded pool of size `max_size`.

**HCL selection:** On each incoming request, the proxy reads the probe pool, computes the `q_rif` quantile of a 128-sample RIF history, and partitions probes into hot (RIF above threshold) and cold (RIF at or below threshold). If any cold probes exist, the one with the lowest latency is selected. If all probes are hot, the one with the lowest RIF is selected. If the pool has fewer than 2 probes, the proxy falls back to a random healthy replica.

**Pool management:** Probes are evicted when they exceed `probe_staleness_ms`, when the pool exceeds `max_size` (oldest evicted), or when their reuse budget is exhausted. The reuse budget `breuse` is computed per paper Eq. (1). On each query, `rremove` additional probes are removed, alternating between the oldest and the worst-loaded, to prevent the pool from accumulating only heavily loaded replicas.

**Health tracking:** Each probe failure and each upstream 5xx increments a per-replica failure counter. Once a replica reaches `max_failures`, it is excluded from selection and probing. Unhealthy replicas are re-probed every `recovery_interval_ms`; a successful probe or query resets the counter.

---

## License

MIT
