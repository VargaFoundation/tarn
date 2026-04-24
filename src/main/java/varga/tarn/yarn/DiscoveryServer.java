package varga.tarn.yarn;

/*-
 * #%L
 * Tarn
 * %%
 * Copyright (C) 2025 - 2026 Varga Foundation
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Pattern;

public class DiscoveryServer {
    private static final Logger log = LoggerFactory.getLogger(DiscoveryServer.class);
    // Redact env values whose key hints at a credential.
    private static final Pattern SENSITIVE_ENV_KEY = Pattern.compile(
            ".*(KEY|TOKEN|PASSWORD|SECRET|CREDENTIAL|PASSPHRASE|AUTH).*",
            Pattern.CASE_INSENSITIVE);
    // Cap query-string length to avoid resource abuse via oversized params.
    private static final int MAX_QUERY_LENGTH = 4096;

    private final HttpServer httpServer;
    private final ApplicationMaster am;
    private final TarnConfig config;
    private final Configuration freeMarkerConfig;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Map<String, CircuitBreaker> healthCheckCircuitBreakers = new java.util.concurrent.ConcurrentHashMap<>();
    // Pre-computed for constant-time compare. Null when auth is disabled.
    private final byte[] apiTokenBytes;

    public DiscoveryServer(TarnConfig config, ApplicationMaster am) throws IOException {
        this(config, am, null);
    }

    /**
     * Full constructor. {@code hadoopConf} is used only when TLS is enabled (to load the
     * keystore via HDFS + resolve the password through the credential provider). Pass null in
     * tests that run in plain-HTTP mode.
     */
    public DiscoveryServer(TarnConfig config, ApplicationMaster am, org.apache.hadoop.conf.Configuration hadoopConf) throws IOException {
        this.config = config;
        this.am = am;
        this.apiTokenBytes = (config.apiToken != null && !config.apiToken.isEmpty())
                ? config.apiToken.getBytes(StandardCharsets.UTF_8)
                : null;
        InetSocketAddress addr = new InetSocketAddress(config.bindAddress, config.amPort);
        if (config.tlsEnabled) {
            try {
                HttpsServer https = HttpsServer.create(addr, 0);
                HttpsConfigurator cfg = TlsContextLoader.buildConfigurator(config,
                        hadoopConf != null ? hadoopConf : new org.apache.hadoop.conf.Configuration());
                https.setHttpsConfigurator(cfg);
                this.httpServer = https;
                log.info("TLS enabled on AM port {}", config.amPort);
            } catch (Exception e) {
                throw new IOException("Failed to initialize TLS on AM port " + config.amPort, e);
            }
        } else {
            this.httpServer = HttpServer.create(addr, 0);
        }

        // Initialize FreeMarker
        this.freeMarkerConfig = new Configuration(Configuration.VERSION_2_3_32);
        this.freeMarkerConfig.setClassLoaderForTemplateLoading(this.getClass().getClassLoader(), "templates");
        this.freeMarkerConfig.setDefaultEncoding("UTF-8");
        this.freeMarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        this.freeMarkerConfig.setLogTemplateExceptions(false);
        this.freeMarkerConfig.setWrapUncheckedExceptions(true);
        this.freeMarkerConfig.setFallbackOnNullLoopVariable(false);

        com.sun.net.httpserver.Filter securityHeaders = new SecurityHeadersFilter(config.tlsEnabled);
        createSecuredContext("/instances", new InstancesHandler(), securityHeaders);
        createSecuredContext("/dashboard", new DashboardHandler(), securityHeaders);
        createSecuredContext("/metrics", new PrometheusHandler(), securityHeaders);
        createSecuredContext("/health", new GlobalHealthHandler(), securityHeaders);
        createSecuredContext("/config", new ConfigHandler(), securityHeaders);
        createSecuredContext("/authorize", new AuthorizeHandler(), securityHeaders);
        createSecuredContext("/alerts", new AlertsHandler(), securityHeaders);
        this.httpServer.setExecutor(null);
    }

    private void createSecuredContext(String path, HttpHandler handler, com.sun.net.httpserver.Filter filter) {
        com.sun.net.httpserver.HttpContext ctx = this.httpServer.createContext(path, handler);
        ctx.getFilters().add(filter);
    }

    /**
     * Applies defense-in-depth HTTP security headers to every response before the handler
     * writes any status. HSTS is only sent over TLS (per RFC 6797 §7.2).
     */
    static final class SecurityHeadersFilter extends com.sun.net.httpserver.Filter {
        private final boolean tls;
        SecurityHeadersFilter(boolean tls) { this.tls = tls; }
        @Override public String description() { return "tarn-security-headers"; }
        @Override
        public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
            var headers = exchange.getResponseHeaders();
            headers.set("X-Content-Type-Options", "nosniff");
            headers.set("X-Frame-Options", "DENY");
            headers.set("Referrer-Policy", "no-referrer");
            headers.set("Cache-Control", "no-store");
            if (tls) {
                headers.set("Strict-Transport-Security", "max-age=31536000; includeSubDomains");
            }
            chain.doFilter(exchange);
        }
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
        if (apiTokenBytes == null) {
            return true;
        }
        // Accept only the header. Query-string tokens leak via logs, referrers, and browser history.
        String providedToken = exchange.getRequestHeaders().getFirst("X-TARN-Token");
        if (providedToken == null || !constantTimeEquals(apiTokenBytes, providedToken.getBytes(StandardCharsets.UTF_8))) {
            log.warn("Unauthorized access attempt to {} from {}",
                    exchange.getRequestURI().getPath(), exchange.getRemoteAddress());
            exchange.sendResponseHeaders(401, -1);
            return false;
        }
        return true;
    }

    /**
     * Timing-attack-safe equality. Hashes both sides to a fixed-length digest so lengths can
     * never leak through the compare, then uses MessageDigest.isEqual for the final check.
     */
    private static boolean constantTimeEquals(byte[] a, byte[] b) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return MessageDigest.isEqual(md.digest(a), md.digest(b));
        } catch (Exception e) {
            return false;
        }
    }

    private Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null) return params;
        if (query.length() > MAX_QUERY_LENGTH) {
            log.warn("Query string too large ({} bytes), rejecting params", query.length());
            return params;
        }
        for (String pair : query.split("&")) {
            int idx = pair.indexOf('=');
            if (idx <= 0 || idx >= pair.length() - 1) continue;
            String key = urlDecode(pair.substring(0, idx));
            String value = urlDecode(pair.substring(idx + 1));
            params.put(key, value);
        }
        return params;
    }

    private static String urlDecode(String s) {
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return s;
        }
    }

    private static String redactIfSensitive(String key, String value) {
        if (key == null) return value;
        return SENSITIVE_ENV_KEY.matcher(key).matches() ? "***REDACTED***" : value;
    }

    private String getRequestUser(HttpExchange exchange) {
        String user = exchange.getRequestHeaders().getFirst("X-TARN-User");
        if (user == null || user.isEmpty()) {
            try {
                user = UserGroupInformation.getCurrentUser().getShortUserName();
            } catch (IOException e) {
                user = "anonymous";
            }
        }
        return user;
    }

    private java.util.Set<String> getUserGroups(String user) {
        java.util.Set<String> groups = new java.util.HashSet<>();
        try {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
            groups.addAll(java.util.Arrays.asList(ugi.getGroupNames()));
        } catch (Exception e) {
            // Fallback
        }
        return groups;
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
            model.put("zkEnabled", config.zkEnsemble != null && !config.zkEnsemble.isEmpty());
            model.put("zkPath", config.zkPath);

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
            List<String> allModels = am.getAvailableModels();
            List<String> authorizedModels = new ArrayList<>();
            String user = getRequestUser(exchange);
            java.util.Set<String> groups = getUserGroups(user);

            for (String m : allModels) {
                if (am.getRangerAuthorizer().isAllowed(user, groups, "list", m)) {
                    authorizedModels.add(m);
                }
            }
            model.put("availableModels", authorizedModels);
            model.put("rangerEnabled", config.rangerService != null && !config.rangerService.isEmpty());

            // Multi-LoRA: map of base -> list of visible adapters, filtered by Ranger
            // (resource = "base#lora" combined name to allow per-adapter policies).
            Map<String, List<String>> loraMap = am.getAvailableLoraAdapters();
            Map<String, List<String>> authorizedLoras = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, List<String>> e : loraMap.entrySet()) {
                String base = e.getKey();
                if (!authorizedModels.contains(base)) continue; // Hide adapters for hidden bases.
                List<String> visible = new ArrayList<>();
                for (String lora : e.getValue()) {
                    if (am.getRangerAuthorizer().isAllowed(user, groups, "list", base + "#" + lora)) {
                        visible.add(lora);
                    }
                }
                if (!visible.isEmpty()) {
                    authorizedLoras.put(base, visible);
                }
            }
            model.put("loraAdapters", authorizedLoras);

            // Samples for models
            if (!containers.isEmpty()) {
                String firstHost = containers.get(0).getNodeId().getHost();
                model.put("sampleHost", firstHost);
                String rawModelsJson = am.getMetricsCollector().fetchLoadedModels(firstHost, config.tritonPort);

                // Filter loaded models based on metadata permission
                try {
                    List<Map<String, Object>> modelsList = objectMapper.readValue(rawModelsJson, new TypeReference<List<Map<String, Object>>>() {
                    });
                    List<Map<String, Object>> filteredModelsList = new ArrayList<>();
                    for (Map<String, Object> m : modelsList) {
                        String modelName = (String) m.get("name");
                        if (am.getRangerAuthorizer().isAllowed(user, groups, "metadata", modelName)) {
                            filteredModelsList.add(m);
                        }
                    }
                    model.put("loadedModelsJson", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(filteredModelsList));
                } catch (Exception e) {
                    log.warn("Failed to filter loaded models: {}", e.getMessage());
                    model.put("loadedModelsJson", rawModelsJson);
                }
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

            MetricsCollector mc = am.getMetricsCollector();
            StringBuilder sb = new StringBuilder();
            sb.append("# HELP tarn_target_containers Target number of containers\n");
            sb.append("# TYPE tarn_target_containers gauge\n");
            sb.append("tarn_target_containers ").append(am.getTargetNumContainers()).append("\n");

            List<Container> containers = am.getRunningContainers();
            sb.append("# HELP tarn_running_containers Number of running containers\n");
            sb.append("# TYPE tarn_running_containers gauge\n");
            sb.append("tarn_running_containers ").append(containers.size()).append("\n");

            // Queue depth for predictive scaling
            sb.append("# HELP tarn_queue_depth_total Total queue depth across all containers\n");
            sb.append("# TYPE tarn_queue_depth_total gauge\n");
            sb.append("tarn_queue_depth_total ").append(mc.getTotalQueueDepth()).append("\n");

            synchronized (containers) {
                for (Container c : containers) {
                    String host = c.getNodeId().getHost();
                    String cid = c.getId().toString();
                    double load = mc.fetchContainerLoad(host);

                    sb.append("tarn_container_load{container_id=\"").append(cid).append("\",host=\"").append(host).append("\"} ").append(load).append("\n");

                    // Container startup time
                    Long startupTime = mc.getContainerStartupTime(cid);
                    if (startupTime != null) {
                        sb.append("# HELP tarn_container_startup_ms Container startup time in milliseconds\n");
                        sb.append("# TYPE tarn_container_startup_ms gauge\n");
                        sb.append("tarn_container_startup_ms{container_id=\"").append(cid).append("\"} ").append(startupTime).append("\n");
                    }

                    // Queue depth per container
                    int queueDepth = mc.getQueueDepth(cid);
                    sb.append("tarn_container_queue_depth{container_id=\"").append(cid).append("\",host=\"").append(host).append("\"} ").append(queueDepth).append("\n");

                    Map<String, Map<String, String>> gpuMetrics = mc.fetchGpuMetricsStructured(host);
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

            // Native Prometheus histogram — aggregatable across instances via histogram_quantile().
            // The old per-instance p50/p95/p99 gauges are kept for dashboard compatibility but
            // consumers should migrate to the histogram for cross-replica rollups.
            sb.append("\n# HELP tarn_inference_latency_seconds Inference latency in seconds\n");
            sb.append("# TYPE tarn_inference_latency_seconds histogram\n");
            for (String model : mc.getTrackedModels()) {
                long[] buckets = mc.getHistogramBucketsCumulative(model);
                long count = mc.getHistogramCount(model);
                double sum = mc.getHistogramSum(model);
                if (buckets != null) {
                    for (int i = 0; i < MetricsCollector.LATENCY_BUCKETS_SECONDS.length; i++) {
                        sb.append("tarn_inference_latency_seconds_bucket{model=\"").append(model)
                                .append("\",le=\"").append(MetricsCollector.LATENCY_BUCKETS_SECONDS[i]).append("\"} ")
                                .append(buckets[i]).append("\n");
                    }
                }
                sb.append("tarn_inference_latency_seconds_bucket{model=\"").append(model).append("\",le=\"+Inf\"} ").append(count).append("\n");
                sb.append("tarn_inference_latency_seconds_sum{model=\"").append(model).append("\"} ").append(sum).append("\n");
                sb.append("tarn_inference_latency_seconds_count{model=\"").append(model).append("\"} ").append(count).append("\n");
            }

            // Per-instance percentile approximation (dashboard helper, deprecated for scraping).
            sb.append("\n# HELP tarn_inference_latency_p50_ms DEPRECATED: use histogram. Per-instance p50.\n");
            sb.append("# TYPE tarn_inference_latency_p50_ms gauge\n");
            sb.append("# HELP tarn_inference_latency_p95_ms DEPRECATED: use histogram. Per-instance p95.\n");
            sb.append("# TYPE tarn_inference_latency_p95_ms gauge\n");
            sb.append("# HELP tarn_inference_latency_p99_ms DEPRECATED: use histogram. Per-instance p99.\n");
            sb.append("# TYPE tarn_inference_latency_p99_ms gauge\n");
            for (String model : mc.getTrackedModels()) {
                Map<String, Double> percentiles = mc.getLatencyPercentiles(model);
                sb.append("tarn_inference_latency_p50_ms{model=\"").append(model).append("\"} ").append(percentiles.get("p50")).append("\n");
                sb.append("tarn_inference_latency_p95_ms{model=\"").append(model).append("\"} ").append(percentiles.get("p95")).append("\n");
                sb.append("tarn_inference_latency_p99_ms{model=\"").append(model).append("\"} ").append(percentiles.get("p99")).append("\n");
            }

            // Total inference requests split by success/error — lets you compute error rate
            // in Prometheus (sum(rate(tarn_inference_requests_total{status="error"}[5m])) / ...).
            sb.append("\n# HELP tarn_inference_requests_total Total inference requests counted by model and outcome\n");
            sb.append("# TYPE tarn_inference_requests_total counter\n");
            for (String model : mc.getTrackedModels()) {
                long total = mc.getRequestCount(model);
                long errors = mc.getErrorCount(model);
                long success = Math.max(0L, total - errors);
                sb.append("tarn_inference_requests_total{model=\"").append(model).append("\",status=\"success\"} ").append(success).append("\n");
                sb.append("tarn_inference_requests_total{model=\"").append(model).append("\",status=\"error\"} ").append(errors).append("\n");
            }

            // Error rate per model — kept as gauge for dashboards; Prometheus users should
            // derive it from the counter above.
            sb.append("\n# HELP tarn_model_error_rate Error rate per model (0.0 to 1.0)\n");
            sb.append("# TYPE tarn_model_error_rate gauge\n");
            for (Map.Entry<String, Double> entry : mc.getAllErrorRates().entrySet()) {
                sb.append("tarn_model_error_rate{model=\"").append(entry.getKey()).append("\"} ").append(entry.getValue()).append("\n");
            }

            // LLM token accounting — drives per-user chargeback. Labels: user, model.
            // Counter semantics (monotonically increasing) — use rate() / increase() in PromQL.
            sb.append("\n# HELP tarn_tokens_in_total Total prompt tokens consumed\n");
            sb.append("# TYPE tarn_tokens_in_total counter\n");
            for (Map.Entry<String, Long> e : mc.getTokensIn().entrySet()) {
                String[] um = e.getKey().split("\\|", 2);
                String u = um.length > 0 ? um[0] : "unknown";
                String m = um.length > 1 ? um[1] : "unknown";
                sb.append("tarn_tokens_in_total{user=\"").append(u).append("\",model=\"").append(m).append("\"} ").append(e.getValue()).append("\n");
            }
            sb.append("\n# HELP tarn_tokens_out_total Total completion tokens generated\n");
            sb.append("# TYPE tarn_tokens_out_total counter\n");
            for (Map.Entry<String, Long> e : mc.getTokensOut().entrySet()) {
                String[] um = e.getKey().split("\\|", 2);
                String u = um.length > 0 ? um[0] : "unknown";
                String m = um.length > 1 ? um[1] : "unknown";
                sb.append("tarn_tokens_out_total{user=\"").append(u).append("\",model=\"").append(m).append("\"} ").append(e.getValue()).append("\n");
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
            // Ranger degraded = instant 503 : a regulated cluster must not serve traffic when
            // authorization is unavailable.
            RangerAuthorizer ra = am.getRangerAuthorizer();
            if (ra != null && ra.isDegraded() && config.rangerStrict) {
                String response = "RANGER_UNAVAILABLE";
                exchange.sendResponseHeaders(503, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
                return;
            }

            List<Container> containers = am.getRunningContainers();
            boolean atLeastOneReady = false;
            synchronized (containers) {
                for (Container c : containers) {
                    String host = c.getNodeId().getHost();
                    CircuitBreaker cb = healthCheckCircuitBreakers.computeIfAbsent(
                            host, k -> CircuitBreaker.forHealthCheck(k));

                    if (!cb.allowRequest()) {
                        log.debug("Circuit breaker OPEN for host {}, skipping health check", host);
                        continue;
                    }

                    try {
                        boolean ready = am.getMetricsCollector().isContainerReady(host, config.tritonPort);
                        if (ready) {
                            cb.onSuccess();
                            atLeastOneReady = true;
                            break;
                        } else {
                            cb.onFailure();
                        }
                    } catch (Exception e) {
                        cb.onFailure();
                        log.warn("Health check failed for host {}: {}", host, e.getMessage());
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

    private class ConfigHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthorized(exchange)) return;

            StringBuilder sb = new StringBuilder();
            sb.append("modelRepository: ").append(config.modelRepository).append("\n");
            sb.append("tritonImage: ").append(config.tritonImage).append("\n");
            sb.append("tritonPort: ").append(config.tritonPort).append("\n");
            sb.append("grpcPort: ").append(config.grpcPort).append("\n");
            sb.append("metricsPort: ").append(config.metricsPort).append("\n");
            sb.append("amPort: ").append(config.amPort).append("\n");
            sb.append("bindAddress: ").append(config.bindAddress).append("\n");
            sb.append("containerMemory: ").append(config.containerMemory).append("\n");
            sb.append("containerVCores: ").append(config.containerVCores).append("\n");
            sb.append("tensorParallelism: ").append(config.tensorParallelism).append("\n");
            sb.append("pipelineParallelism: ").append(config.pipelineParallelism).append("\n");
            sb.append("placementTag: ").append(config.placementTag).append("\n");
            sb.append("dockerNetwork: ").append(config.dockerNetwork).append("\n");
            sb.append("dockerPrivileged: ").append(config.dockerPrivileged).append("\n");
            sb.append("dockerDelayedRemoval: ").append(config.dockerDelayedRemoval).append("\n");
            sb.append("scaleUpThreshold: ").append(config.scaleUpThreshold).append("\n");
            sb.append("scaleDownThreshold: ").append(config.scaleDownThreshold).append("\n");
            sb.append("minContainers: ").append(config.minContainers).append("\n");
            sb.append("maxContainers: ").append(config.maxContainers).append("\n");
            sb.append("scaleCooldownMs: ").append(config.scaleCooldownMs).append("\n");

            if (!config.customEnv.isEmpty()) {
                sb.append("\nCustom Environment:\n");
                for (Map.Entry<String, String> entry : config.customEnv.entrySet()) {
                    sb.append("  ").append(entry.getKey())
                            .append("=").append(redactIfSensitive(entry.getKey(), entry.getValue()))
                            .append("\n");
                }
            }
            // Never expose the API token itself, even though the endpoint is auth-protected.
            sb.append("\napiTokenSet: ").append(config.apiToken != null && !config.apiToken.isEmpty()).append("\n");

            String response = sb.toString();
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    private class AuthorizeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthorized(exchange)) return;

            Map<String, String> params = parseQueryParams(exchange.getRequestURI().getQuery());
            String user = params.getOrDefault("user", getRequestUser(exchange));
            String action = params.get("action");
            String modelName = params.get("model");

            if (action == null || modelName == null) {
                String error = "Missing 'action' or 'model' parameter";
                exchange.sendResponseHeaders(400, error.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(error.getBytes());
                }
                return;
            }

            Set<String> groups = getUserGroups(user);
            boolean allowed = am.getRangerAuthorizer().isAllowed(user, groups, action, modelName);

            String response = String.valueOf(allowed);
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    private class AlertsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!isAuthorized(exchange)) return;

            MetricsCollector mc = am.getMetricsCollector();
            List<MetricsCollector.AlertEvent> alerts = mc.getRecentAlerts();

            StringBuilder sb = new StringBuilder();
            sb.append("# HELP tarn_alerts_total Total number of alerts\n");
            sb.append("# TYPE tarn_alerts_total counter\n");
            sb.append("tarn_alerts_total ").append(alerts.size()).append("\n\n");

            sb.append("# Recent alerts (last 100)\n");
            for (MetricsCollector.AlertEvent alert : alerts) {
                sb.append("# [").append(alert.timestamp).append("] ")
                        .append(alert.severity.toUpperCase()).append(" - ")
                        .append(alert.alertType).append(": ")
                        .append(alert.message).append("\n");
            }

            // Prometheus-style alert metrics
            long containerFailures = alerts.stream()
                    .filter(a -> "container_failure".equals(a.alertType)).count();
            long scalingEvents = alerts.stream()
                    .filter(a -> "scaling_event".equals(a.alertType)).count();

            sb.append("\n# HELP tarn_container_failures_total Total container failures\n");
            sb.append("# TYPE tarn_container_failures_total counter\n");
            sb.append("tarn_container_failures_total ").append(containerFailures).append("\n");

            sb.append("# HELP tarn_scaling_events_total Total scaling events\n");
            sb.append("# TYPE tarn_scaling_events_total counter\n");
            sb.append("tarn_scaling_events_total ").append(scalingEvents).append("\n");

            String response = sb.toString();
            exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }
}
