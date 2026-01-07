package varga.tarn.yarn;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.yarn.api.records.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;

public class DiscoveryServer {
    private static final Logger log = LoggerFactory.getLogger(DiscoveryServer.class);
    private final HttpServer httpServer;
    private final List<Container> runningContainers;
    private final TarnConfig config;

    public DiscoveryServer(TarnConfig config, List<Container> runningContainers) throws IOException {
        this.config = config;
        this.runningContainers = runningContainers;
        this.httpServer = HttpServer.create(new InetSocketAddress(config.bindAddress, config.amPort), 0);
        
        this.httpServer.createContext("/instances", new InstancesHandler());
        this.httpServer.setExecutor(null);
    }

    public void start() {
        httpServer.start();
        log.info("Service Discovery HTTP Server started on {}:{}", config.bindAddress, config.amPort);
    }

    public void stop() {
        httpServer.stop(0);
    }

    public int getPort() {
        return httpServer.getAddress().getPort();
    }

    private class InstancesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (config.apiToken != null && !config.apiToken.isEmpty()) {
                String providedToken = exchange.getRequestHeaders().getFirst("X-TARN-Token");
                if (!config.apiToken.equals(providedToken)) {
                    log.warn("Unauthorized access attempt to /instances from {}", exchange.getRemoteAddress());
                    exchange.sendResponseHeaders(401, -1);
                    return;
                }
            }
            StringBuilder sb = new StringBuilder();
            synchronized (runningContainers) {
                for (Container c : runningContainers) {
                    sb.append(c.getNodeId().getHost()).append(":").append(config.tritonPort).append("\n");
                }
            }
            String response = sb.toString();
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
}
