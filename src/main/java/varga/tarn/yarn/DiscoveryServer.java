package varga.tarn.yarn;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DiscoveryServer {
    private static final Logger log = LoggerFactory.getLogger(DiscoveryServer.class);
    private final HttpServer httpServer;
    private final ApplicationMaster am;
    private final TarnConfig config;
    private final Configuration freeMarkerConfig;

    public DiscoveryServer(TarnConfig config, ApplicationMaster am) throws IOException {
        this.config = config;
        this.am = am;
        this.httpServer = HttpServer.create(new InetSocketAddress(config.bindAddress, config.amPort), 0);
        
        // Initialize FreeMarker
        this.freeMarkerConfig = new Configuration(Configuration.VERSION_2_3_32);
        this.freeMarkerConfig.setClassLoaderForTemplateLoading(this.getClass().getClassLoader(), "templates");
        this.freeMarkerConfig.setDefaultEncoding("UTF-8");
        this.freeMarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        this.freeMarkerConfig.setLogTemplateExceptions(false);
        this.freeMarkerConfig.setWrapUncheckedExceptions(true);
        this.freeMarkerConfig.setFallbackOnNullLoopVariable(false);

        this.httpServer.createContext("/instances", new InstancesHandler());
        this.httpServer.createContext("/dashboard", new DashboardHandler());
        this.httpServer.createContext("/metrics", new PrometheusHandler());
        this.httpServer.createContext("/health", new GlobalHealthHandler());
        this.httpServer.setExecutor(null);
    }

    public void start() {
        httpServer.start();
        log.info("TARN HTTP Server started on {}:{}", config.bindAddress, config.amPort);
    }

    public void stop() {
        httpServer.stop(0);
    }

    public int getPort() {
        return httpServer.getAddress().getPort();
    }

    private boolean isAuthorized(HttpExchange exchange) throws IOException {
        if (config.apiToken != null && !config.apiToken.isEmpty()) {
            String providedToken = exchange.getRequestHeaders().getFirst("X-TARN-Token");
            // Also allow token in query param for easier dashboard access
            if (providedToken == null) {
                String query = exchange.getRequestURI().getQuery();
                if (query != null && query.contains("token=" + config.apiToken)) {
                    return true;
                }
            }
            if (!config.apiToken.equals(providedToken)) {
                log.warn("Unauthorized access attempt to {} from {}", exchange.getRequestURI().getPath(), exchange.getRemoteAddress());
                exchange.sendResponseHeaders(401, -1);
                return false;
            }
        }
        return true;
    }

    private class InstancesHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthorized(exchange)) return;

            StringBuilder sb = new StringBuilder();
            List<Container> containers = am.getRunningContainers();
            synchronized (containers) {
                for (Container c : containers) {
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

    private class DashboardHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthorized(exchange)) return;

            Map<String, Object> model = new HashMap<>();
            
            // Global Resources
            Resource res = am.getAvailableResources();
            Map<String, Object> resMap = new HashMap<>();
            resMap.put("memorySize", res.getMemorySize());
            resMap.put("virtualCores", res.getVirtualCores());
            model.put("availableResources", resMap);
            model.put("targetNumContainers", am.getTargetNumContainers());

            // Containers
            List<Map<String, Object>> containerModels = new ArrayList<>();
            List<Container> containers = am.getRunningContainers();
            synchronized (containers) {
                for (Container c : containers) {
                    Map<String, Object> cm = new HashMap<>();
                    cm.put("id", c.getId().toString());
                    cm.put("host", c.getNodeId().getHost());
                    cm.put("load", am.getMetricsCollector().fetchContainerLoad(c.getNodeId().getHost()));
                    cm.put("ready", am.getMetricsCollector().isContainerReady(c.getNodeId().getHost(), config.tritonPort));
                    cm.put("memory", c.getResource().getMemorySize());
                    cm.put("vcores", c.getResource().getVirtualCores());
                    cm.put("gpus", am.getMetricsCollector().fetchGpuMetricsStructured(c.getNodeId().getHost()));
                    containerModels.add(cm);
                }
            }
            model.put("containers", containerModels);

            // HDFS Models
            model.put("availableModels", am.getAvailableModels());

            // Samples for models (still useful but user wanted to remove GPU sample)
            if (!containers.isEmpty()) {
                String firstHost = containers.get(0).getNodeId().getHost();
                model.put("sampleHost", firstHost);
                model.put("loadedModelsJson", am.getMetricsCollector().fetchLoadedModels(firstHost, config.tritonPort));
            }

            try (StringWriter out = new StringWriter()) {
                Template temp = freeMarkerConfig.getTemplate("dashboard.ftl");
                temp.process(model, out);
                String response = out.toString();
                
                exchange.getResponseHeaders().set("Content-Type", "text/html");
                byte[] responseBytes = response.getBytes("UTF-8");
                exchange.sendResponseHeaders(200, responseBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(responseBytes);
                }
            } catch (Exception e) {
                log.error("Error rendering dashboard template", e);
                String errorMsg = "Internal Server Error: " + e.getMessage();
                try {
                    exchange.sendResponseHeaders(500, errorMsg.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(errorMsg.getBytes());
                    }
                } catch (IOException io) {
                    log.error("Failed to send error response", io);
                }
            }
        }
    }

    private class PrometheusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthorized(exchange)) return;

            StringBuilder sb = new StringBuilder();
            sb.append("# HELP tarn_target_containers Target number of containers\n");
            sb.append("# TYPE tarn_target_containers gauge\n");
            sb.append("tarn_target_containers ").append(am.getTargetNumContainers()).append("\n");

            List<Container> containers = am.getRunningContainers();
            sb.append("# HELP tarn_running_containers Number of running containers\n");
            sb.append("# TYPE tarn_running_containers gauge\n");
            sb.append("tarn_running_containers ").append(containers.size()).append("\n");

            synchronized (containers) {
                for (Container c : containers) {
                    String host = c.getNodeId().getHost();
                    String cid = c.getId().toString();
                    double load = am.getMetricsCollector().fetchContainerLoad(host);
                    
                    sb.append("tarn_container_load{container_id=\"").append(cid).append("\",host=\"").append(host).append("\"} ").append(load).append("\n");
                    
                    Map<String, Map<String, String>> gpuMetrics = am.getMetricsCollector().fetchGpuMetricsStructured(host);
                    for (Map.Entry<String, Map<String, String>> gpuEntry : gpuMetrics.entrySet()) {
                        String gpuId = gpuEntry.getKey();
                        for (Map.Entry<String, String> metricEntry : gpuEntry.getValue().entrySet()) {
                            String metricName = metricEntry.getKey();
                            String value = metricEntry.getValue();
                            sb.append("tarn_gpu_").append(metricName)
                              .append("{container_id=\"").append(cid)
                              .append("\",host=\"").append(host)
                              .append("\",gpu=\"").append(gpuId)
                              .append("\"} ").append(value).append("\n");
                        }
                    }
                }
            }

            String response = sb.toString();
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    private class GlobalHealthHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // No auth for health check typically, or same as metrics
            List<Container> containers = am.getRunningContainers();
            boolean atLeastOneReady = false;
            synchronized (containers) {
                for (Container c : containers) {
                    if (am.getMetricsCollector().isContainerReady(c.getNodeId().getHost(), config.tritonPort)) {
                        atLeastOneReady = true;
                        break;
                    }
                }
            }

            if (atLeastOneReady) {
                String response = "OK";
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            } else {
                String response = "NO_INSTANCES_READY";
                exchange.sendResponseHeaders(503, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        }
    }
}
