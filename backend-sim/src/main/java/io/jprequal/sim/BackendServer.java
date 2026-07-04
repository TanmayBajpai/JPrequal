package io.jprequal.sim;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
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

    static long[] findNearestBucket(Map<Integer, long[]> latencyByRif, int rif) {
        int l = rif - 1;
        int r = rif + 1;

        while (l >= 0 || r <= 20) {
            if (l >= 0 && latencyByRif.containsKey(l)) {
                return latencyByRif.get(l);
            }
            if (r <= 20 && latencyByRif.containsKey(r)) {
                return latencyByRif.get(r);
            }
            l--;
            r++;
        }

        return new long[32];
    }

    static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;

        Semaphore capacity = new Semaphore(20);

        AtomicInteger rif = new AtomicInteger(0);

        Map<Integer, long[]> latencyByRif = new ConcurrentHashMap<>();
        Map<Integer, AtomicInteger> indexByRif = new ConcurrentHashMap<>();

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

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/work", exchange -> {
            try {
                capacity.acquire();
                int arrivalRif = rif.incrementAndGet();
                long start = System.currentTimeMillis();
                try {
                    Thread.sleep(slow.get() ? 500 : 100);
                    byte[] response = "OK".getBytes();
                    exchange.sendResponseHeaders(200, response.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    long latency = System.currentTimeMillis() - start;
                    latencyByRif.computeIfAbsent(arrivalRif, k -> new long[32]);
                    indexByRif.computeIfAbsent(arrivalRif, k -> new AtomicInteger(0));
                    int index = indexByRif.get(arrivalRif).getAndIncrement() % 32;
                    latencyByRif.get(arrivalRif)[index] = latency;
                    rif.decrementAndGet();
                    capacity.release();
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "Error handling /work request", e);
            }
        });

        server.createContext("/probe", exchange -> {
            try {
                int currentRif = rif.get();
                long[] buffer = latencyByRif.get(currentRif);
                if (buffer == null) {
                    buffer = findNearestBucket(latencyByRif, currentRif);
                }
                long[] bufferCopy = Arrays.copyOf(buffer, 32);
                Arrays.sort(bufferCopy);

                long medianLatency = bufferCopy[16];

                String body = "{\"rif\":" + rif.get() + ",\"latencyEstimateMillis\":" + medianLatency + "}";
                byte[] bytes = body.getBytes();
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            } catch (Exception e) {
                logger.log(Level.WARNING, "Error handling /probe request", e);
            }
        });

        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        logger.info("Backend started on port " + port);
    }
}