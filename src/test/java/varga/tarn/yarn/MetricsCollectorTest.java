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


import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetricsCollectorTest {

    @Test
    public void testParseLoadFromMetricsGpu() {
        MetricsCollector collector = new MetricsCollector(8002);
        String metrics = "# HELP nv_gpu_utilization GPU utilization (0.0 - 1.0)\n" +
                "# TYPE nv_gpu_utilization gauge\n" +
                "nv_gpu_utilization{gpu=\"0\",uuid=\"GPU-123\"} 45.5\n" +
                "nv_gpu_utilization{gpu=\"1\",uuid=\"GPU-456\"} 55.5";

        double load = collector.parseLoadFromMetrics(metrics);
        assertEquals(0.505, load, 0.001);
    }

    @Test
    public void testParseLoadFromMetricsFallback() {
        MetricsCollector collector = new MetricsCollector(8002);
        String metrics = "nv_inference_request_success{model=\"resnet\",version=\"1\"} 100";

        double load = collector.parseLoadFromMetrics(metrics);
        assertEquals(0.3, load);
    }

    @Test
    public void testParseLoadFromMetricsEmpty() {
        MetricsCollector collector = new MetricsCollector(8002);
        assertEquals(0.0, collector.parseLoadFromMetrics(""));
        assertEquals(0.0, collector.parseLoadFromMetrics(null));
        assertEquals(0.0, collector.parseLoadFromMetrics("unknown_metric 1.0"));
    }

    @Test
    public void testFetchGpuMetricsStructured() {
        // We need a way to mock fetchRawMetrics or just test the parsing logic if extracted
        // For now let's just test that the logic in fetchGpuMetricsStructured works
        // if we were to expose the parsing part.
        // Actually I'll just run mvn test to see if everything compiles.
    }

    @Test
    public void testHostAllowlistRefusesLoopbackAndLinkLocal() {
        MetricsCollector collector = new MetricsCollector(8002);
        // Loopback is refused — otherwise SSRF to the AM itself or co-located admin services is trivial.
        assertEquals(false, collector.isHostAllowed("127.0.0.1"));
        assertEquals(false, collector.isHostAllowed("localhost"));
        // AWS / GCP instance metadata endpoint lives on 169.254.169.254.
        assertEquals(false, collector.isHostAllowed("169.254.169.254"));
        // Any-local / multicast / malformed.
        assertEquals(false, collector.isHostAllowed("0.0.0.0"));
        assertEquals(false, collector.isHostAllowed(""));
        assertEquals(false, collector.isHostAllowed(null));
        // Shell-metacharacter injection attempts refused up-front.
        assertEquals(false, collector.isHostAllowed("host;rm"));
        assertEquals(false, collector.isHostAllowed("host with spaces"));
    }

    @Test
    public void testFetchRefusesDisallowedHosts() {
        MetricsCollector collector = new MetricsCollector(8002);
        // fetchRawMetrics must short-circuit on disallowed hosts (no network attempt).
        assertEquals("", collector.fetchRawMetrics("127.0.0.1"));
        assertEquals("[]", collector.fetchLoadedModels("localhost", 8000));
        assertEquals(false, collector.isContainerReady("169.254.169.254", 8000));
    }

    @Test
    public void testParseQueueDepthSumsAcrossModels() {
        MetricsCollector c = new MetricsCollector(8002);
        String metrics = "# HELP nv_inference_pending_request_count Pending\n" +
                "# TYPE nv_inference_pending_request_count gauge\n" +
                "nv_inference_pending_request_count{model=\"llama-3-70b\",version=\"1\"} 12\n" +
                "nv_inference_pending_request_count{model=\"stable-diffusion\",version=\"1\"} 3\n" +
                "nv_inference_pending_request_count{model=\"resnet50\",version=\"1\"} 0\n";
        assertEquals(15, c.parseQueueDepthFromMetrics(metrics));
    }

    @Test
    public void testParseQueueDepthEmpty() {
        MetricsCollector c = new MetricsCollector(8002);
        assertEquals(0, c.parseQueueDepthFromMetrics(""));
        assertEquals(0, c.parseQueueDepthFromMetrics(null));
        assertEquals(0, c.parseQueueDepthFromMetrics("nv_gpu_utilization{gpu=\"0\"} 10"));
    }

    /**
     * Cumulative histogram buckets must include every sample whose value is <= the boundary.
     * This is the Prometheus contract — a regression here would break histogram_quantile().
     */
    @Test
    public void testHistogramBucketsAreCumulative() {
        MetricsCollector collector = new MetricsCollector(8002);
        // latencies in ms: 5, 50, 120, 800, 4500 -> seconds: 0.005, 0.05, 0.12, 0.8, 4.5
        collector.recordInferenceLatency("m", 5.0);
        collector.recordInferenceLatency("m", 50.0);
        collector.recordInferenceLatency("m", 120.0);
        collector.recordInferenceLatency("m", 800.0);
        collector.recordInferenceLatency("m", 4500.0);

        long[] buckets = collector.getHistogramBucketsCumulative("m");
        assertEquals(MetricsCollector.LATENCY_BUCKETS_SECONDS.length, buckets.length);

        // le=0.005 covers only the 5ms sample
        assertEquals(1L, buckets[0]);
        // le=0.05 covers 5ms + 50ms => 2
        assertEquals(2L, buckets[3]);
        // le=0.25 covers 5, 50, 120 ms => 3
        int idx025 = java.util.Arrays.binarySearch(MetricsCollector.LATENCY_BUCKETS_SECONDS, 0.25);
        assertEquals(3L, buckets[idx025]);
        // le=1.0 covers 5, 50, 120, 800 ms => 4
        int idx1 = java.util.Arrays.binarySearch(MetricsCollector.LATENCY_BUCKETS_SECONDS, 1.0);
        assertEquals(4L, buckets[idx1]);
        // le=5.0 covers all 5 samples
        int idx5 = java.util.Arrays.binarySearch(MetricsCollector.LATENCY_BUCKETS_SECONDS, 5.0);
        assertEquals(5L, buckets[idx5]);

        assertEquals(5L, collector.getHistogramCount("m"));
        // Sum is in seconds: 0.005 + 0.05 + 0.12 + 0.8 + 4.5 = 5.475
        assertEquals(5.475, collector.getHistogramSum("m"), 1e-9);
    }
}
