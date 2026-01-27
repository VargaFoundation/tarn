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

import java.net.URI;
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
    private final HttpClient httpClient;
    private final int metricsPort;

    // Enriched metrics storage
    private final Map<String, List<Double>> inferenceLatencies = new ConcurrentHashMap<>();
    private final Map<String, Long> containerStartTimes = new ConcurrentHashMap<>();
    private final Map<String, Long> containerReadyTimes = new ConcurrentHashMap<>();
    private final Map<String, Long> errorCountsByModel = new ConcurrentHashMap<>();
    private final Map<String, Long> requestCountsByModel = new ConcurrentHashMap<>();
    private final Map<String, Integer> queueDepthByContainer = new ConcurrentHashMap<>();

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
        inferenceLatencies.computeIfAbsent(model, k -> Collections.synchronizedList(new ArrayList<>()));
        List<Double> latencies = inferenceLatencies.get(model);
        synchronized (latencies) {
            latencies.add(latencyMs);
            if (latencies.size() > MAX_LATENCY_SAMPLES) {
                latencies.remove(0);
            }
        }
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

    public boolean isContainerReady(String host, int tritonPort) {
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
}
