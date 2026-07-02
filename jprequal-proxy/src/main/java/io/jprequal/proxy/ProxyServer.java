package io.jprequal.proxy;

import com.sun.net.httpserver.HttpServer;
import io.jprequal.core.RoundRobinSelector;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

public class ProxyServer {
    static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);

        List<String> replicas = Arrays.asList(args).subList(1, args.length);

        RoundRobinSelector selector = new RoundRobinSelector();

        HttpClient client = HttpClient.newHttpClient();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/work", exchange -> {
            try {
                String selectedReplica = selector.select(replicas);

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
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.println("Proxy started on port " + port);
    }
}