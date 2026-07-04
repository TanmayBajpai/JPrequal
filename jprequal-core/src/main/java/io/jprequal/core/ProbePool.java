package io.jprequal.core;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProbePool implements AutoCloseable {

    private static final Pattern RIF_PATTERN = Pattern.compile("\"rif\":(\\d+)");
    private static final Pattern LATENCY_PATTERN = Pattern.compile("\"latencyEstimateMillis\":(\\d+)");
    // Recent probe RIF values used to estimate the RIF distribution across
    // replicas. Kept separate from the pool so "all pool probes are hot" is
    // possible, as the HCL rule requires.
    private static final int RIF_HISTORY_SIZE = 128;

    private static final class PoolEntry {
        Probe probe;
        int uses;

        PoolEntry(Probe probe) {
            this.probe = probe;
        }
    }

    private final PrequalConfig config;
    private final List<PoolEntry> entries = new ArrayList<>();
    private final int[] rifHistory = new int[RIF_HISTORY_SIZE];
    private long rifHistoryCount = 0;
    private boolean removeOldestNext = true;
    private final HttpClient httpClient;
    private final Map<String, Integer> failureCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> lastRecoveryProbeNanos = new ConcurrentHashMap<>();
    private final Thread idleProber;

    private static final Logger logger = Logger.getLogger(ProbePool.class.getName());

    public ProbePool(PrequalConfig config) {
        this.config = config;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(config.probeTimeoutMs()))
                .build();

        this.idleProber = Thread.ofVirtual().start(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(500);
                    if (poolSize() < 2) {
                        fireProbes();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
    }

    private synchronized int poolSize() {
        return entries.size();
    }

    /**
     * breuse = max{1, (1+delta) / ((1 - m/n) * (rprobe - rremove))}, paper eq. (1).
     * Config validation guarantees m < n and rprobe > rremove, so the rate is positive.
     */
    private int computeBreuse() {
        int n = config.replicas().size();
        int m = config.maxSize();
        double rate = (1.0 - (double) m / n) * (config.probingRate() - config.rremove());
        double breuse = (1.0 + config.delta()) / rate;
        return Math.max(1, randomizedRound(breuse));
    }

    /** Rounds to floor or ceiling with probability preserving the expectation. */
    private static int randomizedRound(double value) {
        int floor = (int) Math.floor(value);
        double frac = value - floor;
        return floor + (ThreadLocalRandom.current().nextDouble() < frac ? 1 : 0);
    }

    /**
     * The RIF value at the qRif quantile of recently observed probe RIFs.
     * Probes with RIF strictly above this are hot. With no history yet,
     * everything is cold.
     */
    public synchronized int rifQuantileThreshold() {
        int n = (int) Math.min(rifHistoryCount, RIF_HISTORY_SIZE);
        if (n == 0) {
            return Integer.MAX_VALUE;
        }
        int[] sorted = Arrays.copyOf(rifHistory, n);
        Arrays.sort(sorted);
        int index = Math.min((int) (config.qRif() * n), n - 1);
        return sorted[index];
    }

    public synchronized void addProbe(Probe probe) {
        rifHistory[(int) (rifHistoryCount % RIF_HISTORY_SIZE)] = probe.rif();
        rifHistoryCount++;
        if (entries.size() >= config.maxSize()) {
            entries.removeFirst();
        }
        entries.add(new PoolEntry(probe));
    }

    private synchronized void evictStale() {
        long now = System.nanoTime();
        long stalenessNanos = config.probeStalenessMs() * 1_000_000L;
        entries.removeIf(e -> now - e.probe.timestamp() > stalenessNanos);
    }

    private synchronized void removeWorst() {
        if (entries.isEmpty()) return;

        if (removeOldestNext) {
            entries.removeFirst();
        } else {
            int threshold = rifQuantileThreshold();
            // If at least one probe is hot, remove the hot probe with the
            // highest RIF; otherwise the cold probe with the highest latency.
            PoolEntry worst = entries.stream()
                    .filter(e -> e.probe.rif() > threshold)
                    .max(Comparator.comparingInt(e -> e.probe.rif()))
                    .orElseGet(() -> entries.stream()
                            .max(Comparator.comparingInt(e -> e.probe.latencyEstimate()))
                            .orElseThrow());
            entries.remove(worst);
        }

        removeOldestNext = !removeOldestNext;
    }

    public synchronized List<Probe> getProbePool() {
        evictStale();
        return entries.stream().map(e -> e.probe).filter(p -> isHealthy(p.replica())).toList();
    }

    public synchronized void onQueryServed(Probe usedProbe) {
        int breuse = computeBreuse();
        for (Iterator<PoolEntry> it = entries.iterator(); it.hasNext(); ) {
            PoolEntry entry = it.next();
            if (entry.probe.equals(usedProbe)) {
                entry.uses++;
                if (entry.uses >= breuse) {
                    it.remove();
                } else {
                    // Client-side staleness compensation: we just sent this
                    // replica a query, so bump the RIF on its probe.
                    Probe p = entry.probe;
                    entry.probe = new Probe(p.replica(), p.timestamp(), p.rif() + 1, p.latencyEstimate());
                }
                break;
            }
        }

        int removals = randomizedRound(config.rremove());
        for (int i = 0; i < removals; i++) {
            removeWorst();
        }
    }

    public void fireProbes() {
        List<String> healthy = healthyReplicas();
        if (!healthy.isEmpty()) {
            // Sample probe targets uniformly at random without replacement.
            List<String> targets = new ArrayList<>(healthy);
            Collections.shuffle(targets, ThreadLocalRandom.current());
            int count = Math.min(randomizedRound(config.probingRate()), targets.size());
            for (int i = 0; i < count; i++) {
                probeAsync(targets.get(i));
            }
        }

        // Periodically re-probe unhealthy replicas so they can recover.
        long now = System.nanoTime();
        long recoveryNanos = config.recoveryIntervalMs() * 1_000_000L;
        for (String replica : config.replicas()) {
            if (!isHealthy(replica)) {
                Long last = lastRecoveryProbeNanos.get(replica);
                if (last == null || now - last >= recoveryNanos) {
                    lastRecoveryProbeNanos.put(replica, now);
                    probeAsync(replica);
                }
            }
        }
    }

    private void probeAsync(String replica) {
        Thread.ofVirtual().start(() -> {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + replica + "/probe"))
                        .timeout(Duration.ofMillis(config.probeTimeoutMs()))
                        .GET()
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    recordFailure(replica, "probe returned status " + response.statusCode(), null);
                    return;
                }

                Matcher rifMatcher = RIF_PATTERN.matcher(response.body());
                Matcher latencyMatcher = LATENCY_PATTERN.matcher(response.body());
                if (!rifMatcher.find() || !latencyMatcher.find()) {
                    recordFailure(replica, "unparseable probe response: " + response.body(), null);
                    return;
                }
                int rif = Integer.parseInt(rifMatcher.group(1));
                int latency = Integer.parseInt(latencyMatcher.group(1));

                recordSuccess(replica);
                addProbe(new Probe(replica, System.nanoTime(), rif, latency));
            } catch (Exception e) {
                recordFailure(replica, "probe failed", e);
            }
        });
    }

    public void recordSuccess(String replica) {
        Integer previous = failureCounts.put(replica, 0);
        if (previous != null && previous >= config.maxFailures()) {
            logger.info("Replica " + replica + " recovered");
            lastRecoveryProbeNanos.remove(replica);
        }
    }

    public void recordFailure(String replica, String reason, Exception e) {
        int failures = failureCounts.merge(replica, 1, Integer::sum);
        if (failures == config.maxFailures()) {
            logger.warning("Replica " + replica + " marked unhealthy after " + failures
                    + " consecutive failures (" + reason + ")");
        } else {
            logger.log(Level.FINE, "Failure for replica " + replica + ": " + reason, e);
        }
    }

    public boolean isHealthy(String replica) {
        return failureCounts.getOrDefault(replica, 0) < config.maxFailures();
    }

    public List<String> healthyReplicas() {
        return config.replicas().stream().filter(this::isHealthy).toList();
    }

    @Override
    public void close() {
        idleProber.interrupt();
        httpClient.close();
    }
}
