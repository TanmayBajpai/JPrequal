# JPrequal Documentation

JPrequal is a Java 21 implementation of **Prequal**, the load balancing algorithm
described in *"Load is not what you should balance: Introducing Prequal"*
(Wydrowski, Kleinberg, Rumble, Archer — NSDI 2024) and deployed at YouTube.

Instead of balancing CPU utilization, Prequal selects backends using two
real-time signals — **requests-in-flight (RIF)** and **latency** — gathered via
asynchronous, reusable probes and combined with a hot-cold lexicographic (HCL)
selection rule.

## Contents

| Document | What it covers |
|----------|----------------|
| [Getting started](getting-started.md) | Build, run the demo (backends + proxy + load generator) |
| [Architecture](architecture.md) | Modules, data flow, threading model |
| [The Prequal algorithm](algorithm.md) | Probing, HCL selection, probe reuse and pool management, as implemented in `jprequal-core` |
| [Configuration reference](configuration.md) | Every parameter of `proxy.conf` and `loadgen.conf`, with defaults and constraints |
| [Probe protocol & embedding](probe-protocol.md) | The `/probe` contract your backends must implement, and how to embed `jprequal-core` in your own stack |

## Module map

```
JPrequal/
├── jprequal-core/    # The Prequal algorithm — pure JDK, no framework dependencies
├── jprequal-proxy/   # HTTP reverse proxy that uses jprequal-core to pick backends
├── backend-sim/      # Simulated heterogeneous backends for testing
├── loadgen/          # Open-loop HTTP load generator
├── proxy.conf        # Proxy configuration
├── loadgen.conf      # Load generator configuration
└── start-backends.sh # Starts N simulated backends on ports 9001+
```

The only module that matters for correctness of the algorithm is
`jprequal-core`; everything else is demo and test infrastructure around it.
