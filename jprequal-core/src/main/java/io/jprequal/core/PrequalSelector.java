package io.jprequal.core;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PrequalSelector implements ReplicaSelector, AutoCloseable {
    private final PrequalConfig config;
    private final ProbePool pool;

    public PrequalSelector(PrequalConfig config) {
        this.config = config;
        this.pool = new ProbePool(config);
    }

    public void fireProbes() {
        pool.fireProbes();
    }

    public void reportQueryOutcome(String replica, boolean success) {
        if (success) {
            pool.recordSuccess(replica);
        } else {
            pool.recordFailure(replica, "query failed", null);
        }
    }

    @Override
    public String select() {
        List<Probe> probePool = pool.getProbePool();

        if (probePool.size() < 2) {
            List<String> candidates = pool.healthyReplicas();
            if (candidates.isEmpty()) {
                candidates = config.replicas();
            }
            return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
        }

        // HCL rule: hot if RIF exceeds the qRif quantile of the estimated RIF
        // distribution. Cold probes compete on latency; if all are hot, pick
        // the lowest RIF.
        int threshold = pool.rifQuantileThreshold();
        List<Probe> coldProbes = probePool.stream()
                .filter(p -> p.rif() <= threshold)
                .toList();

        Probe selected = coldProbes.isEmpty()
                ? probePool.stream().min(Comparator.comparingInt(Probe::rif)).orElseThrow()
                : coldProbes.stream().min(Comparator.comparingInt(Probe::latencyEstimate)).orElseThrow();

        pool.onQueryServed(selected);
        return selected.replica();
    }

    @Override
    public void close() {
        pool.close();
    }
}
