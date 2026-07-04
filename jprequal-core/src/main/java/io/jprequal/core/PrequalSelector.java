package io.jprequal.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PrequalSelector implements ReplicaSelector {
    private final List<String> replicas;
    private final ProbePool pool;

    public PrequalSelector(List<String> replicas, int maxSize, int probingRate, int rremove, double delta, int probeTimeout, int maxFailures) {
        this.replicas = replicas;
        pool = new ProbePool(maxSize, probingRate, rremove, delta, replicas, probeTimeout, maxFailures);
    }

    public void fireProbes() {
        pool.fireProbes();
    }

    @Override
    public String select() {
        List<Probe> probePool = pool.getProbePool();

        if (probePool.size() < 2) {
            return replicas.get(ThreadLocalRandom.current().nextInt(replicas.size()));
        }

        int[] rifs = probePool.stream().mapToInt(Probe::rif).sorted().toArray();
        int threshold = rifs[(int)(0.84 * rifs.length)];

        List<Probe> coldProbes = new ArrayList<>();
        for (Probe probe : probePool) {
            if (probe.rif() <= threshold) coldProbes.add(probe);
        }

        if (coldProbes.isEmpty()) {
            probePool.sort(Comparator.comparingInt(Probe::rif));
            Probe selected = probePool.getFirst();
            pool.onQueryServed(selected);
            return selected.replica();
        }

        coldProbes.sort(Comparator.comparingInt(Probe::latencyEstimate));
        Probe selected = coldProbes.getFirst();
        pool.onQueryServed(selected);
        return selected.replica();
    }
}