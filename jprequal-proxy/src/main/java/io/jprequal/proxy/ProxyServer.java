package io.jprequal.proxy;

import com.sun.net.httpserver.HttpServer;
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
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProxyServer {
    private static final Logger logger = Logger.getLogger(ProxyServer.class.getName());

    static void main(String[] args) throws IOException {
        String configPath = args.length > 0 ? args[0] : "JPrequal.conf";

        Properties config = new Properties();
        try (InputStream in = new FileInputStream(configPath)) {
            config.load(in);
        }

        int port = Integer.parseInt(config.getProperty("port", "8080"));
        int maxSize = Integer.parseInt(config.getProperty("max_size", "16"));
        int probingRate = Integer.parseInt(config.getProperty("probing_rate", "2"));
        int rremove = Integer.parseInt(config.getProperty("rremove", "1"));
        double delta = Double.parseDouble(config.getProperty("delta", "1.0"));
        int probeTimeout = Integer.parseInt(config.getProperty("probe_timeout_ms", "1000"));
        int maxFailures = Integer.parseInt(config.getProperty("max_failures", "5"));
        int maxRetries = Integer.parseInt(config.getProperty("max_retries", "2"));

        List<String> replicas = Arrays.stream(config.getProperty("replicas", "").split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();

        PrequalSelector selector = new PrequalSelector(replicas, maxSize, probingRate, rremove, delta, probeTimeout, maxFailures);

        HttpClient client = HttpClient.newHttpClient();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/work", exchange -> {
            try {
                int attempts = 0;
                HttpResponse<byte[]> response = null;

                while (attempts <= maxRetries) {
                    String selectedReplica = selector.select();
                    try {
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create("http://" + selectedReplica + "/work"))
                                .GET()
                                .build();
                        response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
                        if (response.statusCode() < 500) break;
                    } catch (Exception e) {
                        logger.log(Level.WARNING, "Request to replica failed on attempt " + attempts, e);
                    }
                    attempts++;
                }

                if (response == null || response.statusCode() >= 500) {
                    byte[] error = "Service unavailable".getBytes();
                    exchange.sendResponseHeaders(503, error.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(error);
                    }
                } else {
                    byte[] body = response.body();
                    exchange.sendResponseHeaders(response.statusCode(), body.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(body);
                    }
                }

                Thread.ofVirtual().start(selector::fireProbes);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Unexpected error handling request", e);
            }
        });

        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        logger.info("Proxy started on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down proxy...");
            server.stop(5);
            logger.info("Proxy stopped");
        }));
    }
}