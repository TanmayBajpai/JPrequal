package io.jprequal.sim;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BackendServer {

    private static final Logger logger = Logger.getLogger(BackendServer.class.getName());

    private static final int MAX_CONCURRENCY = 20;
    private static final int SAMPLES_PER_BUCKET = 32;

    /** Ring buffer of recent latency samples for one arrival-RIF value. */
    static final class Bucket {
        private final long[] samples = new long[SAMPLES_PER_BUCKET];
        private long count = 0;

        synchronized void record(long latencyMillis) {
            samples[(int) (count % SAMPLES_PER_BUCKET)] = latencyMillis;
            count++;
        }

        synchronized boolean hasSamples() {
            return count > 0;
        }

        synchronized long median() {
            int n = (int) Math.min(count, SAMPLES_PER_BUCKET);
            long[] copy = Arrays.copyOf(samples, n);
            Arrays.sort(copy);
            return n % 2 == 1 ? copy[n / 2] : (copy[n / 2 - 1] + copy[n / 2]) / 2;
        }
    }

    static Bucket findNearestBucket(Map<Integer, Bucket> latencyByRif, int rif) {
        return latencyByRif.entrySet().stream()
                .filter(e -> e.getValue().hasSamples())
                .min(Comparator.comparingInt(e -> Math.abs(e.getKey() - rif)))
                .map(Map.Entry::getValue)
                .orElse(null);
    }

    static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;

        Semaphore capacity = new Semaphore(MAX_CONCURRENCY);

        AtomicInteger rif = new AtomicInteger(0);

        Map<Integer, Bucket> latencyByRif = new ConcurrentHashMap<>();

        AtomicBoolean slow = new AtomicBoolean(false);

        Thread.ofVirtual().start(() -> {
            while (true) {
                try {
                    Thread.sleep(20_000);
                    slow.set(true);
                    logger.info("Port " + port + " slowing down");
                    Thread.sleep(3_000);
                    slow.set(false);
                    logger.info("Port " + port + " back to normal");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });

        server(port, capacity, rif, latencyByRif, slow);
    }

    private static void server(int port, Semaphore capacity, AtomicInteger rif,
                               Map<Integer, Bucket> latencyByRif, AtomicBoolean slow) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/work", exchange -> {
            // The query arrives now: RIF and latency must include any time
            // spent queued on the capacity semaphore, or both signals go
            // blind exactly when the replica is overloaded.
            int arrivalRif = rif.incrementAndGet();
            long start = System.currentTimeMillis();
            try {
                capacity.acquire();
                try {
                    Thread.sleep(slow.get() ? 500 : 100);
                    byte[] response = "OK".getBytes();
                    exchange.sendResponseHeaders(200, response.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response);
                    }
                } finally {
                    capacity.release();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.log(Level.WARNING, "Error handling /work request", e);
            } finally {
                long latency = System.currentTimeMillis() - start;
                latencyByRif.computeIfAbsent(arrivalRif, k -> new Bucket()).record(latency);
                rif.decrementAndGet();
                exchange.close();
            }
        });

        server.createContext("/probe", exchange -> {
            try {
                int currentRif = rif.get();
                Bucket bucket = latencyByRif.get(currentRif);
                if (bucket == null || !bucket.hasSamples()) {
                    bucket = findNearestBucket(latencyByRif, currentRif);
                }
                // A replica that has never served a query reports 0: it is
                // genuinely idle, so it should look attractive.
                long medianLatency = bucket == null ? 0 : bucket.median();

                String body = "{\"rif\":" + currentRif + ",\"latencyEstimateMillis\":" + medianLatency + "}";
                byte[] bytes = body.getBytes();
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "Error handling /probe request", e);
            } finally {
                exchange.close();
            }
        });

        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        logger.info("Backend started on port " + port);
    }
}
