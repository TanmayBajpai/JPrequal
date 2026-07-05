package io.jprequal.proxy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.jprequal.core.PrequalConfig;
import io.jprequal.core.PrequalSelector;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProxyServer {
    private static final Logger logger = Logger.getLogger(ProxyServer.class.getName());

    private static final Set<String> SKIP_REQUEST_HEADERS = Set.of(
            "connection", "content-length", "expect", "host", "upgrade",
            "transfer-encoding", "te", "keep-alive", "proxy-connection");
    private static final Set<String> SKIP_RESPONSE_HEADERS = Set.of(
            "connection", "content-length", "transfer-encoding", "keep-alive");
    private static final Set<String> NON_RETRYABLE_METHODS = Set.of("POST", "PATCH");

    static void main(String[] args) throws IOException {
        String configPath = args.length > 0 ? args[0] : "proxy.conf";

        Properties config = new Properties();
        try (InputStream in = new FileInputStream(configPath)) {
            config.load(in);
        }

        int port;
        int maxRetries;
        int requestTimeoutMs;
        PrequalConfig prequalConfig;
        try {
            port = Integer.parseInt(config.getProperty("port", "8080"));
            if (port <= 0 || port > 65535)
                throw new IllegalArgumentException("port must be in [1, 65535]");

            maxRetries = Integer.parseInt(config.getProperty("max_retries", "2"));
            if (maxRetries < 0)
                throw new IllegalArgumentException("max_retries cannot be negative");

            requestTimeoutMs = Integer.parseInt(config.getProperty("request_timeout_ms", "5000"));
            if (requestTimeoutMs <= 0)
                throw new IllegalArgumentException("request_timeout_ms must be greater than 0");

            List<String> replicas = Arrays.stream(config.getProperty("replicas", "").split(","))
                    .map(String::trim).filter(s -> !s.isEmpty()).toList();

            prequalConfig = new PrequalConfig(
                    replicas,
                    Integer.parseInt(config.getProperty("max_size", "4")),
                    Double.parseDouble(config.getProperty("probing_rate", "2")),
                    Double.parseDouble(config.getProperty("rremove", "1")),
                    Double.parseDouble(config.getProperty("delta", "1.0")),
                    Integer.parseInt(config.getProperty("probe_timeout_ms", "1000")),
                    Integer.parseInt(config.getProperty("max_failures", "5")),
                    Double.parseDouble(config.getProperty("q_rif", "0.84")),
                    Long.parseLong(config.getProperty("probe_staleness_ms", "1000")),
                    Long.parseLong(config.getProperty("recovery_interval_ms", "5000")));
        } catch (IllegalArgumentException e) {
            logger.severe("Invalid configuration in " + configPath + ": " + e.getMessage());
            System.exit(1);
            return;
        }

        PrequalSelector selector = new PrequalSelector(prequalConfig);

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(requestTimeoutMs))
                .build();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", exchange -> {
            try {
                handle(exchange, selector, client, maxRetries, requestTimeoutMs);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Unexpected error handling request", e);
            } finally {
                Thread.ofVirtual().start(selector::fireProbes);
                exchange.close();
            }
        });

        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        logger.info("Proxy started on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down proxy...");
            server.stop(5);
            selector.close();
            logger.info("Proxy stopped");
        }));
    }

    private static void handle(HttpExchange exchange, PrequalSelector selector, HttpClient client,
                               int maxRetries, int requestTimeoutMs) throws IOException {
        String method = exchange.getRequestMethod();
        byte[] requestBody = exchange.getRequestBody().readAllBytes();
        int maxAttempts = NON_RETRYABLE_METHODS.contains(method) ? 1 : maxRetries + 1;

        HttpResponse<byte[]> response = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            String replica = selector.select();
            try {
                HttpRequest.Builder builder = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + replica + exchange.getRequestURI()))
                        .timeout(Duration.ofMillis(requestTimeoutMs))
                        .method(method, requestBody.length > 0
                                ? HttpRequest.BodyPublishers.ofByteArray(requestBody)
                                : HttpRequest.BodyPublishers.noBody());

                exchange.getRequestHeaders().forEach((name, values) -> {
                    if (!SKIP_REQUEST_HEADERS.contains(name.toLowerCase()))
                        values.forEach(v -> builder.header(name, v));
                });

                response = client.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());
                if (response.statusCode() < 500) {
                    selector.reportQueryOutcome(replica, true);
                    break;
                }
                selector.reportQueryOutcome(replica, false);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                selector.reportQueryOutcome(replica, false);
                logger.log(Level.WARNING, "Request to " + replica + " failed on attempt " + attempt, e);
            }
        }

        if (response == null || response.statusCode() >= 500) {
            byte[] error = "Service unavailable".getBytes();
            exchange.sendResponseHeaders(503, error.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(error);
            }
            return;
        }

        response.headers().map().forEach((name, values) -> {
            if (!name.startsWith(":") && !SKIP_RESPONSE_HEADERS.contains(name.toLowerCase()))
                exchange.getResponseHeaders().put(name, values);
        });

        byte[] body = response.body();
        if (body.length == 0) {
            exchange.sendResponseHeaders(response.statusCode(), -1);
        } else {
            exchange.sendResponseHeaders(response.statusCode(), body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}