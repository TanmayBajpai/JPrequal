package io.jprequal.core;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class ProbePool {
    private final int maxSize;
    private final int probingRate;
    private final int rremove;
    private final double delta;
    private final List<String> replicas;
    private final List<Probe> probes;
    private final Map<Probe, Integer> useCounts;
    private final HttpClient httpClient;
    private boolean removeOldestNext = true;

    public ProbePool(int maxSize, int probingRate, int rremove, double delta, List<String> replicas) {
        this.maxSize = maxSize;
        this.probingRate = probingRate;
        this.rremove = rremove;
        this.delta = delta;
        this.replicas = replicas;
        this.probes = Collections.synchronizedList(new ArrayList<>());
        this.useCounts = new ConcurrentHashMap<>();
        this.httpClient = HttpClient.newHttpClient();
    }

    public ProbePool(List<String> replicas) {
        this(16, 2, 1, 1.0, replicas);
    }

    private int computeBreuse() {
        int n = replicas.size();
        int m = maxSize;
        double rate = (1.0 - (double) m / n) * probingRate - rremove;
        if (rate <= 0) return 1;
        double breuse = (1.0 + delta) / rate;
        return (int) Math.max(1, breuse);
    }

    public synchronized void addProbe(Probe probe) {
        if (probes.size() >= maxSize) {
            Probe oldest = probes.getFirst();
            useCounts.remove(oldest);
            probes.removeFirst();
        }
        probes.add(probe);
        useCounts.put(probe, 0);
    }

    private synchronized void evictStale() {
        long now = System.nanoTime();
        probes.removeIf(p -> {
            if (now - p.timestamp() > 1_000_000_000L) {
                useCounts.remove(p);
                return true;
            }
            return false;
        });
    }

    private synchronized void removeWorst() {
        if (probes.isEmpty()) return;

        if (removeOldestNext) {
            Probe oldest = probes.getFirst();
            useCounts.remove(oldest);
            probes.removeFirst();
        } else {
            int[] rifs = probes.stream().mapToInt(Probe::rif).sorted().toArray();
            int threshold = rifs[(int) (0.84 * rifs.length)];

            boolean anyHot = probes.stream().anyMatch(p -> p.rif() > threshold);

            Probe worst;
            if (anyHot) {
                worst = probes.stream()
                        .filter(p -> p.rif() > threshold)
                        .max((a, b) -> a.rif() - b.rif())
                        .orElseThrow();
            } else {
                worst = probes.stream()
                        .max((a, b) -> a.latencyEstimate() - b.latencyEstimate())
                        .orElseThrow();
            }

            useCounts.remove(worst);
            probes.remove(worst);
        }

        removeOldestNext = !removeOldestNext;
    }

    public synchronized List<Probe> getProbePool() {
        evictStale();
        return new ArrayList<>(probes);
    }

    public synchronized void onQueryServed(Probe usedProbe) {
        int breuse = computeBreuse();
        int uses = useCounts.getOrDefault(usedProbe, 0) + 1;

        if (uses >= breuse) {
            probes.remove(usedProbe);
            useCounts.remove(usedProbe);
        } else {
            useCounts.put(usedProbe, uses);
        }

        for (int i = 0; i < rremove; i++) {
            removeWorst();
        }
    }

    public void fireProbes() {
        for (int i = 0; i < probingRate; i++) {
            String replica = replicas.get(ThreadLocalRandom.current().nextInt(replicas.size()));
            Thread.ofVirtual().start(() -> {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://" + replica + "/probe"))
                            .GET()
                            .build();

                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    String body = response.body();

                    int rif = Integer.parseInt(body.replaceAll(".*\"rif\":(\\d+).*", "$1"));
                    int latency = Integer.parseInt(body.replaceAll(".*\"latencyEstimateMillis\":(\\d+).*", "$1"));

                    Probe probe = new Probe(replica, System.nanoTime(), rif, latency);
                    addProbe(probe);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
    }
}