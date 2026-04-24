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


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetricsCollector {
    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);
    // Hostname syntax RFC 1123 + IPv4. Refuses `..`, scheme prefixes, `@`, spaces, etc.
    private static final Pattern SAFE_HOST = Pattern.compile(
            "^(?!.*\\.\\.)([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)(\\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$");

    /**
     * Prometheus histogram bucket boundaries in seconds. Chosen to give useful resolution across
     * the inference range we care about — from batch ONNX classifiers (sub-10 ms) to LLM chat
     * completions (multi-second). Exposed publicly so the Prometheus endpoint can emit them.
     */
    public static final double[] LATENCY_BUCKETS_SECONDS = {
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0
    };

    private final HttpClient httpClient;
    private final int metricsPort;
    // Cached host resolution — refused hosts stay refused across fetches.
    private final Map<String, Boolean> hostAllowCache = new ConcurrentHashMap<>();

    // Enriched metrics storage
    private final Map<String, List<Double>> inferenceLatencies = new ConcurrentHashMap<>();
    private final Map<String, Long> containerStartTimes = new ConcurrentHashMap<>();
    private final Map<String, Long> containerReadyTimes = new ConcurrentHashMap<>();
    private final Map<String, Long> errorCountsByModel = new ConcurrentHashMap<>();
    private final Map<String, Long> requestCountsByModel = new ConcurrentHashMap<>();
    private final Map<String, Integer> queueDepthByContainer = new ConcurrentHashMap<>();

    // Prometheus-native histogram state: cumulative bucket counts, total sum, total count.
    // Lifecycle-wise these grow forever per-model; counters-in-a-counter-world is intentional.
    private final Map<String, long[]> histogramBucketsByModel = new ConcurrentHashMap<>();
    private final Map<String, Double> histogramSumByModel = new ConcurrentHashMap<>();
    private final Map<String, Long> histogramCountByModel = new ConcurrentHashMap<>();

    // Alerting state
    private final List<AlertEvent> alertEvents = Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_LATENCY_SAMPLES = 1000;

    public MetricsCollector(int metricsPort) {
        this.metricsPort = metricsPort;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public MetricsCollector(int metricsPort, HttpClient httpClient) {
        this.metricsPort = metricsPort;
        this.httpClient = httpClient;
    }

    // Container startup tracking
    public void recordContainerStart(String containerId) {
        containerStartTimes.put(containerId, System.currentTimeMillis());
    }

    public void recordContainerReady(String containerId) {
        containerReadyTimes.put(containerId, System.currentTimeMillis());
    }

    public Long getContainerStartupTime(String containerId) {
        Long startTime = containerStartTimes.get(containerId);
        Long readyTime = containerReadyTimes.get(containerId);
        if (startTime != null && readyTime != null) {
            return readyTime - startTime;
        }
        return null;
    }

    // Inference latency tracking
    public void recordInferenceLatency(String model, double latencyMs) {
        double seconds = latencyMs / 1000.0;
        // 1) Cumulative Prometheus histogram buckets — aggregatable across instances via histogram_quantile.
        long[] buckets = histogramBucketsByModel.computeIfAbsent(model,
                k -> new long[LATENCY_BUCKETS_SECONDS.length]);
        synchronized (buckets) {
            for (int i = 0; i < LATENCY_BUCKETS_SECONDS.length; i++) {
                if (seconds <= LATENCY_BUCKETS_SECONDS[i]) {
                    buckets[i]++;
                }
            }
        }
        histogramSumByModel.merge(model, seconds, Double::sum);
        histogramCountByModel.merge(model, 1L, Long::sum);

        // 2) Bounded window for on-the-fly percentile display in the dashboard — not scraped.
        inferenceLatencies.computeIfAbsent(model, k -> Collections.synchronizedList(new ArrayList<>()));
        List<Double> latencies = inferenceLatencies.get(model);
        synchronized (latencies) {
            latencies.add(latencyMs);
            if (latencies.size() > MAX_LATENCY_SAMPLES) {
                latencies.remove(0);
            }
        }
    }

    /** Snapshot of cumulative bucket counts for Prometheus export. Caller must not mutate. */
    public long[] getHistogramBucketsCumulative(String model) {
        long[] src = histogramBucketsByModel.get(model);
        if (src == null) return null;
        synchronized (src) {
            return src.clone();
        }
    }

    public double getHistogramSum(String model) {
        return histogramSumByModel.getOrDefault(model, 0.0);
    }

    public long getHistogramCount(String model) {
        return histogramCountByModel.getOrDefault(model, 0L);
    }

    public long getRequestCount(String model) {
        return requestCountsByModel.getOrDefault(model, 0L);
    }

    public long getErrorCount(String model) {
        return errorCountsByModel.getOrDefault(model, 0L);
    }

    // Token accounting (OpenAI `usage` field) — keyed by (user, model) so you can chargeback.
    // Exposed as counters in /metrics so downstream billing can consume them directly.
    private final Map<String, Long> tokensInByUserModel = new ConcurrentHashMap<>();
    private final Map<String, Long> tokensOutByUserModel = new ConcurrentHashMap<>();

    public void recordTokens(String user, String model, long promptTokens, long completionTokens) {
        String key = safe(user) + "|" + safe(model);
        if (promptTokens > 0) tokensInByUserModel.merge(key, promptTokens, Long::sum);
        if (completionTokens > 0) tokensOutByUserModel.merge(key, completionTokens, Long::sum);
    }

    public Map<String, Long> getTokensIn() {
        return new LinkedHashMap<>(tokensInByUserModel);
    }

    public Map<String, Long> getTokensOut() {
        return new LinkedHashMap<>(tokensOutByUserModel);
    }

    private static String safe(String s) {
        return s == null ? "unknown" : s.replace('|', '_');
    }

    public Map<String, Double> getLatencyPercentiles(String model) {
        Map<String, Double> percentiles = new LinkedHashMap<>();
        List<Double> latencies = inferenceLatencies.get(model);
        if (latencies == null || latencies.isEmpty()) {
            percentiles.put("p50", 0.0);
            percentiles.put("p95", 0.0);
            percentiles.put("p99", 0.0);
            return percentiles;
        }

        List<Double> sorted;
        synchronized (latencies) {
            sorted = new ArrayList<>(latencies);
        }
        Collections.sort(sorted);

        percentiles.put("p50", getPercentile(sorted, 50));
        percentiles.put("p95", getPercentile(sorted, 95));
        percentiles.put("p99", getPercentile(sorted, 99));
        return percentiles;
    }

    private double getPercentile(List<Double> sortedList, int percentile) {
        if (sortedList.isEmpty()) return 0.0;
        int index = (int) Math.ceil(percentile / 100.0 * sortedList.size()) - 1;
        return sortedList.get(Math.max(0, Math.min(index, sortedList.size() - 1)));
    }

    // Error rate tracking
    public void recordModelRequest(String model, boolean success) {
        requestCountsByModel.merge(model, 1L, Long::sum);
        if (!success) {
            errorCountsByModel.merge(model, 1L, Long::sum);
        }
    }

    public double getErrorRate(String model) {
        long requests = requestCountsByModel.getOrDefault(model, 0L);
        long errors = errorCountsByModel.getOrDefault(model, 0L);
        if (requests == 0) return 0.0;
        return (double) errors / requests;
    }

    public Map<String, Double> getAllErrorRates() {
        Map<String, Double> rates = new LinkedHashMap<>();
        for (String model : requestCountsByModel.keySet()) {
            rates.put(model, getErrorRate(model));
        }
        return rates;
    }

    // Queue depth tracking
    public void updateQueueDepth(String containerId, int depth) {
        queueDepthByContainer.put(containerId, depth);
    }

    public int getQueueDepth(String containerId) {
        return queueDepthByContainer.getOrDefault(containerId, 0);
    }

    public int getTotalQueueDepth() {
        return queueDepthByContainer.values().stream().mapToInt(Integer::intValue).sum();
    }

    // Alerting
    public void recordAlert(String alertType, String message, String severity) {
        AlertEvent event = new AlertEvent(alertType, message, severity, Instant.now());
        alertEvents.add(event);
        // Keep only last 100 alerts
        while (alertEvents.size() > 100) {
            alertEvents.remove(0);
        }
        log.warn("ALERT [{}] {}: {}", severity, alertType, message);
    }

    public List<AlertEvent> getRecentAlerts() {
        return new ArrayList<>(alertEvents);
    }

    public void recordContainerFailure(String containerId, String reason) {
        recordAlert("container_failure", "Container " + containerId + " failed: " + reason, "critical");
    }

    public void recordScalingEvent(String eventType, int fromCount, int toCount) {
        recordAlert("scaling_event", eventType + " from " + fromCount + " to " + toCount + " containers", "info");
    }

    // Alert event class
    public static class AlertEvent {
        public final String alertType;
        public final String message;
        public final String severity;
        public final Instant timestamp;

        public AlertEvent(String alertType, String message, String severity, Instant timestamp) {
            this.alertType = alertType;
            this.message = message;
            this.severity = severity;
            this.timestamp = timestamp;
        }
    }

    // Get all enriched metrics for a model
    public Map<String, Object> getEnrichedMetrics(String model) {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("latencyPercentiles", getLatencyPercentiles(model));
        metrics.put("errorRate", getErrorRate(model));
        metrics.put("requestCount", requestCountsByModel.getOrDefault(model, 0L));
        metrics.put("errorCount", errorCountsByModel.getOrDefault(model, 0L));
        return metrics;
    }

    public Set<String> getTrackedModels() {
        Set<String> models = new HashSet<>();
        models.addAll(inferenceLatencies.keySet());
        models.addAll(requestCountsByModel.keySet());
        return models;
    }

    /**
     * Validates a host string to prevent SSRF via loopback/link-local/metadata endpoints.
     * Only public unicast / site-local addresses reachable by DNS (or literal IPv4) pass.
     * Result is cached — YARN-provided NodeManager hostnames don't change mid-application.
     */
    public boolean isHostAllowed(String host) {
        if (host == null || host.isEmpty() || host.length() > 253) return false;
        Boolean cached = hostAllowCache.get(host);
        if (cached != null) return cached;

        boolean allowed = checkHostAllowed(host);
        hostAllowCache.put(host, allowed);
        if (!allowed) {
            log.warn("Refusing to fetch from disallowed host (SSRF guard): {}", host);
        }
        return allowed;
    }

    private boolean checkHostAllowed(String host) {
        if (!SAFE_HOST.matcher(host).matches()) return false;
        try {
            InetAddress addr = InetAddress.getByName(host);
            // Block obvious SSRF targets: loopback, link-local (AWS/GCP metadata 169.254.x), any-local, multicast.
            if (addr.isLoopbackAddress()) return false;
            if (addr.isLinkLocalAddress()) return false;
            if (addr.isAnyLocalAddress()) return false;
            if (addr.isMulticastAddress()) return false;
            return true;
        } catch (UnknownHostException e) {
            return false;
        }
    }

    public boolean isContainerReady(String host, int tritonPort) {
        if (!isHostAllowed(host)) return false;
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + ":" + tritonPort + "/v2/health/ready"))
                    .timeout(Duration.ofSeconds(2))
                    .build();
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    public double fetchContainerLoad(String host) {
        String metrics = fetchRawMetrics(host);
        return parseLoadFromMetrics(metrics);
    }

    public String fetchRawMetrics(String host) {
        if (!isHostAllowed(host)) return "";
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + ":" + metricsPort + "/metrics"))
                    .timeout(Duration.ofSeconds(3))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return response.body();
            }
        } catch (Exception e) {
            log.warn("Failed to fetch metrics from {}: {}", host, e.getMessage());
        }
        return "";
    }

    public String fetchLoadedModels(String host, int tritonPort) {
        if (!isHostAllowed(host)) return "[]";
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + ":" + tritonPort + "/v2/repository/index"))
                    .POST(HttpRequest.BodyPublishers.noBody()) // Triton index is often a POST
                    .timeout(Duration.ofSeconds(3))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return response.body();
            }
        } catch (Exception e) {
            log.warn("Failed to fetch models from {}: {}", host, e.getMessage());
        }
        return "[]";
    }

    public Map<String, Map<String, String>> fetchGpuMetricsStructured(String host) {
        Map<String, Map<String, String>> gpus = new LinkedHashMap<>();
        String metrics = fetchRawMetrics(host);
        if (metrics.isEmpty()) return gpus;

        Pattern p = Pattern.compile("(nv_gpu_[a-z_]+)\\{gpu=\"(\\d+)\"\\}\\s+([\\d.e+]+)");
        Matcher m = p.matcher(metrics);
        while (m.find()) {
            String metric = m.group(1).replace("nv_gpu_", "");
            String gpuId = m.group(2);
            String value = m.group(3);

            gpus.computeIfAbsent(gpuId, k -> new LinkedHashMap<>()).put(metric, value);
        }
        return gpus;
    }

    public double parseLoadFromMetrics(String metrics) {
        if (metrics == null || metrics.isEmpty()) return 0.0;

        // Try to find GPU utilization first
        Pattern gpuPattern = Pattern.compile("nv_gpu_utilization\\{[^}]*\\}\\s+([\\d.]+)");
        Matcher gpuMatcher = gpuPattern.matcher(metrics);
        double totalGpuLoad = 0;
        int gpuCount = 0;
        while (gpuMatcher.find()) {
            totalGpuLoad += Double.parseDouble(gpuMatcher.group(1));
            gpuCount++;
        }
        if (gpuCount > 0) return (totalGpuLoad / gpuCount) / 100.0;

        // Fallback to a simple heuristic if no GPU metrics:
        if (metrics.contains("nv_inference_request_success")) {
            return 0.3; // Placeholder for "active"
        }

        return 0.0;
    }

    /**
     * Parse the sum of {@code nv_inference_pending_request_count} samples across all
     * {@code model=*} labels in the Triton metrics payload. This is the authoritative
     * "pending queue size" used by queue-aware scaling.
     */
    public int parseQueueDepthFromMetrics(String metrics) {
        if (metrics == null || metrics.isEmpty()) return 0;
        Pattern p = Pattern.compile("nv_inference_pending_request_count\\{[^}]*\\}\\s+([\\d.]+)");
        Matcher m = p.matcher(metrics);
        int total = 0;
        while (m.find()) {
            try {
                total += (int) Double.parseDouble(m.group(1));
            } catch (NumberFormatException ignore) {
                // Corrupt metric line — skip without blowing up scaling.
            }
        }
        return total;
    }

    /**
     * Fetch fresh Triton metrics once, update the per-container queue depth cache, and return
     * the measured depth. Callers doing one-shot reads in the scaling loop avoid double
     * scrapes this way.
     */
    public int refreshQueueDepth(String containerId, String host) {
        String raw = fetchRawMetrics(host);
        int depth = parseQueueDepthFromMetrics(raw);
        updateQueueDepth(containerId, depth);
        return depth;
    }
}
