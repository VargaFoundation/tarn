package varga.tarn.yarn;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetricsCollector {
    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);
    private final HttpClient httpClient;
    private final int metricsPort;

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

    public double fetchContainerLoad(String host) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + ":" + metricsPort + "/metrics"))
                    .timeout(Duration.ofSeconds(3))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return parseLoadFromMetrics(response.body());
            }
        } catch (Exception e) {
            log.warn("Failed to fetch metrics from {}: {}", host, e.getMessage());
        }
        return 0.0;
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
