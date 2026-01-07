package varga.tarn.yarn;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

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
}
