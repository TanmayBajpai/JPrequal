# Getting started

## Requirements

- Java 21 (the code uses virtual threads and unnamed-class `main` conventions)
- Maven 3.9+

## Build

From the repository root:

```bash
mvn clean install
```

This builds all four modules. `jprequal-proxy` additionally produces a shaded
(fat) JAR with `io.jprequal.proxy.ProxyServer` as its main class.

## Run the demo

The demo consists of three pieces: simulated backends, the proxy, and the load
generator. Each runs in its own terminal.

### 1. Start simulated backends

```bash
./start-backends.sh 8
```

Starts 8 `backend-sim` servers on ports 9001–9008. Each backend draws a random
seed at startup that determines its base latency (100–200 ms) and how often and
how badly it throttles itself, so the fleet is deliberately heterogeneous —
exactly the environment Prequal is designed for. The seed is logged at startup
(`Backend started on port 9001 with seed: 42`).

### 2. Configure and start the proxy

The proxy reads `proxy.conf` from the working directory (or a path passed as
the first argument). The checked-in `proxy.conf` ships the paper's default
parameters and an example replica list pointing at `localhost:9001`–`9008` —
set `replicas` to your own fleet.

> **Important:** `max_size` must be **strictly less than** the number of
> replicas (`PrequalConfig` rejects the config otherwise — see the
> [configuration reference](configuration.md)). The paper default of 16
> assumes a larger fleet; for this 8-backend demo, set `max_size=7` or lower
> before starting the proxy.

```bash
cd jprequal-proxy
mvn exec:java -Dexec.mainClass="io.jprequal.proxy.ProxyServer"
```

Or with the shaded JAR:

```bash
java -jar jprequal-proxy/target/jprequal-proxy-0.1.0.jar proxy.conf
```

### 3. Send traffic

A single request:

```bash
curl http://localhost:8080/work
```

Or sustained load with the load generator:

```bash
cd loadgen
mvn exec:java -Dexec.mainClass="io.jprequal.loadgen.LoadGenerator" \
    -Dexec.args="../loadgen.conf pattern=sine min_qps=20 max_qps=200"
```

The load generator prints per-interval progress (target vs. actual QPS, error
and drop counts, latency percentiles) and a final summary over the measured
window. Any key in `loadgen.conf` can be overridden with `key=value` arguments;
a bare argument is treated as the config file path.

## Stopping things

- The proxy installs a shutdown hook: on Ctrl-C it stops accepting connections,
  drains in-flight requests for up to 5 seconds, and closes the selector.
- The load generator also prints its summary from a shutdown hook, so Ctrl-C
  mid-run still reports results for the traffic measured so far.
- `start-backends.sh` runs the backends as foreground children of the script;
  Ctrl-C in that terminal (or killing the script's process group) stops them.

## What to look for

Under load, watch the backend logs: each backend periodically logs
`Port 900X slowing down` when its throttle kicks in. The proxy's probe pool
sees the throttled backend's RIF and latency rise and routes around it. Compare
tail latency (p90/p99 in the load generator output) against a round-robin
baseline to see the effect — the difference is most dramatic at p90+, where
round robin keeps feeding queues on throttled replicas.
