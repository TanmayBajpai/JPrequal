# The Prequal algorithm as implemented

This describes what `jprequal-core` actually does, with pointers into the code.
Paper references are to *Wydrowski et al., NSDI 2024*.

## The idea in one paragraph

Traditional load balancers spread load evenly or balance CPU utilization — a
trailing signal that lies when antagonist processes share the machine. Prequal
instead asks replicas directly, and often: lightweight asynchronous **probes**
return each replica's current **requests-in-flight (RIF)** and a **latency
estimate**. Probe results live briefly in a small pool, and each query picks
from that pool with a **hot-cold lexicographic (HCL)** rule: avoid replicas
whose RIF is high (hot), and among the rest (cold) pick the fastest.

## Probing

`ProbePool.fireProbes()` — called by the proxy after every request, and by the
idle prober when the pool runs low:

1. Shuffle the healthy replicas and take the first `probing_rate` of them
   (sampling **without replacement**, so one request never probes the same
   replica twice). Fractional rates are handled by randomized rounding — e.g.
   `probing_rate=1.5` fires 1 or 2 probes with equal probability.
2. Each probe is a `GET /probe` on its own virtual thread with timeout
   `probe_timeout_ms`. The response `{"rif": N, "latencyEstimateMillis": M}`
   becomes a `Probe(replica, nanoTime, rif, latency)` in the pool.
3. Independently, every unhealthy replica whose last recovery probe is older
   than `recovery_interval_ms` gets one recovery probe, so dead replicas can
   come back.

`addProbe()` records the probe's RIF into a 128-sample ring-buffer **RIF
history** (used for the hot/cold threshold) and appends the probe to the pool,
evicting the oldest entry if the pool is at `max_size`.

## HCL selection

`PrequalSelector.select()`:

1. Take a snapshot of the pool (stale probes older than `probe_staleness_ms`
   are evicted first; probes of unhealthy replicas are filtered out).
2. **Fallback:** if the snapshot has fewer than 2 probes, return a uniformly
   random healthy replica (or any replica if none are healthy).
3. Compute the hot/cold threshold: the `q_rif` quantile of the RIF history.
4. Partition: a probe is **cold** if `rif <= threshold`, otherwise **hot**.
5. Choose:
   - any cold probes exist → the cold probe with the **lowest latency**;
   - all hot → the probe with the **lowest RIF**.
6. Report the chosen probe to the pool (`onQueryServed`) and return its replica.

The lexicographic order is the point: RIF acts as a guardrail against
overloaded replicas, and only among safely-loaded replicas does latency drive
the choice. `q_rif=0` degenerates to pure lowest-RIF; `q_rif=1` to pure
lowest-latency. The paper recommends 0.6–0.9; the default is 0.84.

The RIF history is deliberately **separate** from the pool contents. If the
threshold were the quantile of the pool's own probes, some probe would always
sit at or below it and the all-hot branch could never fire. Against a longer
history, the entire current pool genuinely can be hot.

## Probe reuse and removal

Probes are reused across queries rather than fired per query — that is what
makes probing cheap. But a reused probe is increasingly wrong, so its lifetime
is tightly bounded (`ProbePool.onQueryServed`):

**Reuse budget (paper Eq. 1).** Each selection increments the probe's use
count. The budget is

```
breuse = max(1, round⁎( (1 + δ) / ((1 − m/n) · (r_probe − r_remove)) ))
```

where `m` = `max_size`, `n` = number of replicas, `r_probe` = `probing_rate`,
`r_remove` = `rremove`, and `round⁎` is randomized rounding. A probe that
reaches its budget is removed. Note the `m < n` requirement: at `m = n` the
formula divides by zero, which is why `PrequalConfig` rejects
`max_size >= replicas.size()`.

**Client-side RIF compensation.** When a probe survives reuse, its recorded RIF
is incremented by 1, accounting for the query just sent to that replica — the
pool's view stays roughly consistent between real probes.

**Degradation control.** After each query, `rremove` additional probes are
removed (randomized rounding again), alternating between two strategies:

- the **oldest** probe — fights staleness;
- the **worst-loaded** probe — the highest-RIF probe above the hot threshold,
  or the highest-latency probe if none are hot — fights the selection bias that
  would otherwise leave the pool full of only bad options (good probes get
  selected and consumed; bad ones would linger).

**Staleness.** Independent of all the above, any probe older than
`probe_staleness_ms` is evicted whenever the pool is read.

## Health tracking

Per-replica consecutive-failure counters (`recordFailure` / `recordSuccess`):

- Every failed probe (timeout, non-200, unparseable body) and every failed
  query (exception or 5xx, as reported by the proxy) increments the replica's
  counter. Any success resets it to zero.
- A replica at `max_failures` is **unhealthy**: excluded from selection, from
  regular probing, and its existing probes are filtered out of pool snapshots.
- Unhealthy replicas receive one recovery probe per `recovery_interval_ms`; a
  successful recovery probe resets the counter and the replica rejoins.
- If *every* replica is unhealthy, `select()` still returns a random replica
  rather than failing — degraded service beats no service.

## Idle probing

Probing is driven by request traffic, so with no traffic the pool would decay
to empty (staleness) and the first requests after a lull would all hit the
random-fallback path. A background virtual thread checks every 500 ms and fires
a round of probes whenever the pool has fewer than 2 entries.

## Parameter interactions worth knowing

- `probing_rate` must exceed `rremove`, or the pool drains (validated).
- Larger `delta` (δ) ⇒ larger reuse budgets ⇒ probes work harder before being
  discarded; the pool grows toward `max_size` faster but individual entries are
  staler on average.
- `probe_staleness_ms` is a hard freshness backstop; in a busy system reuse
  budgets and `rremove` usually recycle probes well before staleness does.
