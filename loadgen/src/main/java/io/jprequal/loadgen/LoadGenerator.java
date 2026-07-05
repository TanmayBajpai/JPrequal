package io.jprequal.loadgen;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.Logger;

public class LoadGenerator {

    private static final Logger logger = Logger.getLogger(LoadGenerator.class.getName());

    private static final int MAX_TRACKED_MS = 60_000;

    enum Pattern { CONSTANT, RAMP, SINE, STEP, SPIKE }

    enum Arrival { POISSON, UNIFORM }

    /**
     * Describes the offered load as a function of time. The generator is
     * open-loop: arrivals are scheduled from this profile regardless of how
     * fast responses come back, so a slow system under test cannot slow the
     * generator down and hide its own queueing (coordinated omission).
     */
    record LoadProfile(Pattern pattern, double qps, double minQps, double maxQps,
                       double periodS, double burstS, int steps, double durationS) {

        /** Target rate at {@code t} seconds into the measured window. */
        double qpsAt(double t) {
            return switch (pattern) {
                case CONSTANT -> qps;
                case RAMP -> minQps + (maxQps - minQps) * Math.min(t / durationS, 1.0);
                case SINE -> {
                    double mid = (minQps + maxQps) / 2;
                    double amp = (maxQps - minQps) / 2;
                    // Starts at minQps and rises, so a run never opens at peak load.
                    yield mid - amp * Math.cos(2 * Math.PI * t / periodS);
                }
                case STEP -> {
                    if (steps == 1) yield maxQps;
                    int level = Math.min((int) (t / durationS * steps), steps - 1);
                    yield minQps + (maxQps - minQps) * level / (steps - 1);
                }
                case SPIKE -> (t % periodS) < burstS ? maxQps : qps;
            };
        }
    }

    /** Lock-free counters plus a 1ms-resolution latency histogram. */
    static final class Stats {
        final LongAdder ok = new LongAdder();
        final LongAdder errors = new LongAdder();
        final LongAdder dropped = new LongAdder();
        final LongAdder totalLatencyMs = new LongAdder();
        final AtomicLong maxLatencyMs = new AtomicLong();
        final AtomicLongArray histogram = new AtomicLongArray(MAX_TRACKED_MS + 1);

        void record(long latencyMs, boolean success) {
            (success ? ok : errors).increment();
            histogram.incrementAndGet((int) Math.min(Math.max(latencyMs, 0), MAX_TRACKED_MS));
            totalLatencyMs.add(latencyMs);
            maxLatencyMs.accumulateAndGet(latencyMs, Math::max);
        }

        long completed() {
            return ok.sum() + errors.sum();
        }

        double meanMs() {
            long count = completed();
            return count == 0 ? 0 : (double) totalLatencyMs.sum() / count;
        }

        long percentileMs(double p) {
            long count = completed();
            if (count == 0) return 0;
            long rank = Math.max(1, (long) Math.ceil(p * count));
            long seen = 0;
            for (int i = 0; i <= MAX_TRACKED_MS; i++) {
                seen += histogram.get(i);
                if (seen >= rank) return i;
            }
            return MAX_TRACKED_MS;
        }
    }

    static void main(String[] args) throws IOException, InterruptedException {
        Properties config = loadConfig(args);

        String target;
        String method;
        String body;
        String headers;
        Arrival arrival;
        LoadProfile profile;
        double warmupS;
        double reportIntervalS;
        int requestTimeoutMs;
        int maxInFlight;
        try {
            target = config.getProperty("target", "http://localhost:8080/work");
            method = config.getProperty("method", "GET").toUpperCase(Locale.ROOT);
            body = config.getProperty("body", "");
            headers = config.getProperty("headers", "");

            double qps = Double.parseDouble(config.getProperty("qps", "100"));
            double minQps = Double.parseDouble(config.getProperty("min_qps", "0"));
            double maxQps = Double.parseDouble(config.getProperty("max_qps", String.valueOf(qps)));
            double periodS = Double.parseDouble(config.getProperty("period_s", "30"));
            double burstS = Double.parseDouble(config.getProperty("burst_s", "5"));
            int steps = Integer.parseInt(config.getProperty("steps", "5"));
            double durationS = Double.parseDouble(config.getProperty("duration_s", "60"));

            Pattern pattern;
            try {
                pattern = Pattern.valueOf(config.getProperty("pattern", "constant").toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("pattern must be one of constant, ramp, sine, step, spike");
            }
            try {
                arrival = Arrival.valueOf(config.getProperty("arrival", "poisson").toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("arrival must be poisson or uniform");
            }

            warmupS = Double.parseDouble(config.getProperty("warmup_s", "0"));
            reportIntervalS = Double.parseDouble(config.getProperty("report_interval_s", "5"));
            requestTimeoutMs = Integer.parseInt(config.getProperty("request_timeout_ms", "5000"));
            maxInFlight = Integer.parseInt(config.getProperty("max_in_flight", "10000"));

            if (qps < 0 || minQps < 0 || maxQps < 0)
                throw new IllegalArgumentException("qps values cannot be negative");
            if (minQps > maxQps)
                throw new IllegalArgumentException("min_qps cannot exceed max_qps");
            if (durationS <= 0)
                throw new IllegalArgumentException("duration_s must be greater than 0");
            if (warmupS < 0)
                throw new IllegalArgumentException("warmup_s cannot be negative");
            if (periodS <= 0)
                throw new IllegalArgumentException("period_s must be greater than 0");
            if (burstS < 0 || burstS > periodS)
                throw new IllegalArgumentException("burst_s must be in [0, period_s]");
            if (steps < 1)
                throw new IllegalArgumentException("steps must be at least 1");
            if (reportIntervalS <= 0)
                throw new IllegalArgumentException("report_interval_s must be greater than 0");
            if (requestTimeoutMs <= 0)
                throw new IllegalArgumentException("request_timeout_ms must be greater than 0");
            if (maxInFlight <= 0)
                throw new IllegalArgumentException("max_in_flight must be greater than 0");

            profile = new LoadProfile(pattern, qps, minQps, maxQps, periodS, burstS, steps, durationS);
        } catch (IllegalArgumentException e) {
            logger.severe("Invalid configuration: " + e.getMessage());
            System.exit(1);
            return;
        }

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(requestTimeoutMs))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(target))
                .timeout(Duration.ofMillis(requestTimeoutMs))
                .method(method, body.isEmpty()
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body));
        for (String header : headers.split(";")) {
            int colon = header.indexOf(':');
            if (colon > 0)
                builder.header(header.substring(0, colon).trim(), header.substring(colon + 1).trim());
        }
        HttpRequest request = builder.build();

        Stats cumulative = new Stats();
        AtomicReference<Stats> window = new AtomicReference<>(new Stats());
        Semaphore inFlight = new Semaphore(maxInFlight);

        logger.info(String.format(Locale.ROOT,
                "Load test: %s %s | pattern %s | %.1fs warmup + %.1fs measured | %s arrivals",
                method, target, profile.pattern().name().toLowerCase(Locale.ROOT),
                warmupS, profile.durationS(), arrival.name().toLowerCase(Locale.ROOT)));

        long startNanos = System.nanoTime();
        long warmupNanos = (long) (warmupS * 1e9);
        long endNanos = (long) ((warmupS + profile.durationS()) * 1e9);

        AtomicBoolean summaryPrinted = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> printSummary(summaryPrinted, cumulative, profile, warmupS, startNanos, warmupNanos)));

        Thread reporter = Thread.ofVirtual().start(() -> {
            try {
                while (true) {
                    Thread.sleep((long) (reportIntervalS * 1000));
                    Stats w = window.getAndSet(new Stats());
                    long elapsed = System.nanoTime() - startNanos;
                    double t = Math.max(0, elapsed / 1e9 - warmupS);
                    logger.info(String.format(Locale.ROOT,
                            "%5.0fs%s | target %7.1f qps | actual %7.1f qps | ok %d err %d dropped %d"
                                    + " | p50 %dms p90 %dms p99 %dms max %dms",
                            elapsed / 1e9, elapsed < warmupNanos ? " (warmup)" : "",
                            profile.qpsAt(t), w.completed() / reportIntervalS,
                            w.ok.sum(), w.errors.sum(), w.dropped.sum(),
                            w.percentileMs(0.50), w.percentileMs(0.90), w.percentileMs(0.99),
                            w.maxLatencyMs.get()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        ThreadLocalRandom random = ThreadLocalRandom.current();
        long nextFire = 0;
        while (true) {
            long now = System.nanoTime() - startNanos;
            if (now >= endNanos) break;

            double t = Math.max(0, now / 1e9 - warmupS);
            double rate = Math.max(profile.qpsAt(t), 0.0);
            if (rate < 0.001) {
                // Idle stretch of the profile: re-anchor the schedule so the
                // pause is not "repaid" as a burst when the rate comes back.
                Thread.sleep(10);
                nextFire = now;
                continue;
            }

            double gapS = arrival == Arrival.POISSON
                    ? -Math.log(1 - random.nextDouble()) / rate
                    : 1.0 / rate;
            nextFire += (long) (gapS * 1e9);

            long sleepNanos = nextFire - (System.nanoTime() - startNanos);
            if (sleepNanos > 0) Thread.sleep(Duration.ofNanos(sleepNanos));

            boolean measured = nextFire >= warmupNanos;
            if (!inFlight.tryAcquire()) {
                // Keeping the schedule open-loop means shedding, not queueing,
                // when the system under test cannot absorb the offered rate.
                window.get().dropped.increment();
                if (measured) cumulative.dropped.increment();
                continue;
            }

            Thread.ofVirtual().start(() -> {
                long reqStart = System.nanoTime();
                boolean success = false;
                try {
                    HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                    success = response.statusCode() < 500;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    // Timeouts and connection failures count as errors below.
                } finally {
                    long latencyMs = (System.nanoTime() - reqStart) / 1_000_000;
                    window.get().record(latencyMs, success);
                    if (measured) cumulative.record(latencyMs, success);
                    inFlight.release();
                }
            });
        }

        long drainDeadline = System.nanoTime() + (requestTimeoutMs + 1000) * 1_000_000L;
        while (inFlight.availablePermits() < maxInFlight && System.nanoTime() < drainDeadline)
            Thread.sleep(50);
        reporter.interrupt();

        printSummary(summaryPrinted, cumulative, profile, warmupS, startNanos, warmupNanos);
    }

    private static void printSummary(AtomicBoolean summaryPrinted, Stats stats, LoadProfile profile,
                                     double warmupS, long startNanos, long warmupNanos) {
        if (!summaryPrinted.compareAndSet(false, true)) return;
        double measuredS = Math.min((System.nanoTime() - startNanos - warmupNanos) / 1e9, profile.durationS());
        if (measuredS <= 0) {
            logger.info("Load test stopped during warmup; no measured results");
            return;
        }
        logger.info(String.format(Locale.ROOT, """
                        Load test complete (%s pattern, %.1fs measured, %.1fs warmup excluded)
                          requests:   %d ok, %d errors, %d dropped (client saturated)
                          throughput: %.1f qps
                          latency:    mean %.1fms | p50 %dms | p90 %dms | p99 %dms | p99.9 %dms | max %dms""",
                profile.pattern().name().toLowerCase(Locale.ROOT), measuredS, warmupS,
                stats.ok.sum(), stats.errors.sum(), stats.dropped.sum(),
                stats.completed() / measuredS, stats.meanMs(),
                stats.percentileMs(0.50), stats.percentileMs(0.90), stats.percentileMs(0.99),
                stats.percentileMs(0.999), stats.maxLatencyMs.get()));
    }

    /**
     * Configuration comes from a properties file plus {@code key=value}
     * command-line overrides, e.g. {@code loadgen.conf qps=500 pattern=sine}.
     * A bare argument is the config file path; without one, {@code loadgen.conf}
     * is used if present in the working directory.
     */
    private static Properties loadConfig(String[] args) throws IOException {
        String configPath = null;
        Properties overrides = new Properties();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (eq > 0) {
                overrides.setProperty(arg.substring(0, eq), arg.substring(eq + 1));
            } else {
                configPath = arg;
            }
        }
        if (configPath == null && new File("loadgen.conf").isFile())
            configPath = "loadgen.conf";

        Properties config = new Properties();
        if (configPath != null) {
            try (InputStream in = new FileInputStream(configPath)) {
                config.load(in);
            }
        }
        config.putAll(overrides);
        return config;
    }
}
