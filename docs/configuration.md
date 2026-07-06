# Configuration reference

## Proxy (`proxy.conf`)

The proxy reads a Java properties file — `proxy.conf` in the working directory
by default, or a path passed as the first argument. Defaults below are the
code's defaults (`ProxyServer` / `PrequalConfig`); invalid values make the
proxy log the problem and exit.

### Server

| Parameter | Default | Constraints | Description |
|-----------|---------|-------------|-------------|
| `port` | `8080` | 1–65535 | Port the proxy listens on. |
| `replicas` | — | non-empty | Comma-separated `host:port` backend addresses. Required. |
| `max_retries` | `2` | ≥ 0 | Extra attempts for failed requests, each against a freshly selected replica. Applies only to methods treated as idempotent (`GET`, `PUT`, `DELETE`, `HEAD`, …); `POST` and `PATCH` are never retried. |
| `request_timeout_ms` | `5000` | > 0 | Connect + response timeout for proxied upstream requests. |

### Prequal algorithm

| Parameter | Default | Constraints | Description |
|-----------|---------|-------------|-------------|
| `max_size` | `16` | 0 < m **< number of replicas** | Maximum probe pool size. The strict `m < n` bound is required by the reuse-budget formula (paper Eq. 1), which divides by `1 − m/n`. |
| `probing_rate` | `2` | > 0, > `rremove` | Probes fired per request. Fractional values supported (randomized rounding). |
| `rremove` | `1` | ≥ 0, < `probing_rate` | Probes removed per request for degradation control (alternating oldest / worst-loaded). Fractional values supported. |
| `delta` | `1.0` | > 0 | δ in the reuse-budget formula; controls net probe accumulation. Higher ⇒ probes reused more before removal. |
| `q_rif` | `0.84` | 0–1 | RIF-history quantile separating hot from cold probes. `0` ⇒ pure lowest-RIF selection, `1` ⇒ pure lowest-latency. Paper recommends 0.6–0.9. |
| `probe_timeout_ms` | `1000` | > 0 | HTTP timeout for `/probe` requests. Keep low — probes should be fast. |
| `probe_staleness_ms` | `1000` | > 0 | Probes older than this are evicted from the pool. |
| `max_failures` | `5` | > 0 | Consecutive failures (probe or query) before a replica is marked unhealthy. |
| `recovery_interval_ms` | `5000` | > 0 | How often an unhealthy replica is re-probed so it can recover. |

> **Note:** the default of `16` is the paper's value and assumes a fleet
> larger than 16 replicas. `max_size` must be strictly less than the number of
> replicas you configure — if you point the proxy at a smaller fleet (e.g. the
> 8-backend demo), lower it accordingly (`max_size=7` or less for 8 replicas).

## Load generator (`loadgen.conf`)

Configuration comes from a properties file plus `key=value` command-line
overrides; a bare argument is the config file path. Without one,
`loadgen.conf` is used if present in the working directory.

```bash
java -cp loadgen/target/loadgen-0.1.0.jar io.jprequal.loadgen.LoadGenerator \
    loadgen.conf qps=500 duration_s=120
```

### Target

| Parameter | Default | Description |
|-----------|---------|-------------|
| `target` | `http://localhost:8080/work` | URL to send requests to. |
| `method` | `GET` | HTTP method. |
| `body` | — | Optional request body. |
| `headers` | — | Optional headers, semicolon-separated (`Name: Value; Name2: Value2`). |

### Traffic shape

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pattern` | `constant` | `constant`, `ramp`, `sine`, `step`, or `spike` (see below). |
| `qps` | `100` | Base rate: the constant rate, or the spike baseline. |
| `min_qps` | `0` | Lower bound for ramp/sine/step. |
| `max_qps` | = `qps` | Upper bound for ramp/sine/step, and the spike burst rate. |
| `period_s` | `30` | Cycle length for sine and spike. |
| `burst_s` | `5` | Burst length for spike (must be ≤ `period_s`). |
| `steps` | `5` | Number of levels for step. |
| `duration_s` | `60` | Measured run length, seconds. |
| `warmup_s` | `0` | Warmup before measurement; reported live but excluded from the summary. |
| `arrival` | `poisson` | Arrival process within the schedule: `poisson` (bursty, realistic) or `uniform` (evenly spaced). |

Patterns:

| Pattern | Shape |
|---------|-------|
| `constant` | `qps` for the whole run |
| `ramp` | `min_qps` → `max_qps` linearly over `duration_s` |
| `sine` | Oscillates between `min_qps` and `max_qps` with period `period_s`, starting at `min_qps` |
| `step` | `min_qps` → `max_qps` in `steps` equal levels over `duration_s` |
| `spike` | `qps` baseline, bursting to `max_qps` for `burst_s` at the start of every `period_s` |

### Client behavior

| Parameter | Default | Description |
|-----------|---------|-------------|
| `request_timeout_ms` | `5000` | Per-request timeout; timeouts count as errors. |
| `report_interval_s` | `5` | Progress report interval. |
| `max_in_flight` | `10000` | Cap on concurrent requests. Arrivals beyond the cap are **dropped and counted**, never queued — this preserves the open-loop property. |

## Backend simulator

`backend-sim` takes a single argument, the port (default `8080`):

```bash
mvn exec:java -pl backend-sim -Dexec.mainClass="io.jprequal.sim.BackendServer" -Dexec.args="9001"
```

It has no config file. Each process draws a random seed at startup that fixes
its base latency (100–200 ms), throttle interval (15–30 s), throttle duration
(2–8 s), and slowdown multiplier (2–4×). Concurrency is capped at 20 in-flight
requests. `start-backends.sh N` launches N of them on ports 9001…9000+N.
