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

public class ProxyServer {
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

        List<String> replicas = Arrays.asList(config.getProperty("replicas", "").split(","))
                .stream().map(String::trim).filter(s -> !s.isEmpty()).toList();

        PrequalSelector selector = new PrequalSelector(replicas, maxSize, probingRate, rremove, delta);

        HttpClient client = HttpClient.newHttpClient();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/work", exchange -> {
            try {
                String selectedReplica = selector.select();

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://" + selectedReplica + "/work"))
                        .GET()
                        .build();

                HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());

                byte[] body = response.body();

                exchange.sendResponseHeaders(response.statusCode(), body.length);

                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }

                Thread.ofVirtual().start(selector::fireProbes);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.println("Proxy started on port " + port);
    }
}