package varga.tarn.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

public class DiscoveryServerTest {

    @Test
    public void testDiscoveryEndpoint() throws Exception {
        TarnConfig config = new TarnConfig();
        config.amPort = 0; // Random port
        config.apiToken = "test-token";
        config.tritonPort = 8000;
        config.bindAddress = "127.0.0.1";

        List<Container> containers = new ArrayList<>();
        Container mockContainer = mock(Container.class);
        NodeId mockNodeId = mock(NodeId.class);
        org.apache.hadoop.yarn.api.records.ContainerId mockContainerId = mock(org.apache.hadoop.yarn.api.records.ContainerId.class);
        when(mockContainerId.toString()).thenReturn("container_123");
        when(mockNodeId.getHost()).thenReturn("host1");
        when(mockContainer.getNodeId()).thenReturn(mockNodeId);
        when(mockContainer.getId()).thenReturn(mockContainerId);
        org.apache.hadoop.yarn.api.records.Resource mockResource = mock(org.apache.hadoop.yarn.api.records.Resource.class);
        when(mockResource.getMemorySize()).thenReturn(4096L);
        when(mockResource.getVirtualCores()).thenReturn(2);
        when(mockContainer.getResource()).thenReturn(mockResource);
        containers.add(mockContainer);

        ApplicationMaster mockAm = mock(ApplicationMaster.class);
        MetricsCollector mockMetrics = mock(MetricsCollector.class);
        RangerAuthorizer mockAuthorizer = mock(RangerAuthorizer.class);
        
        when(mockMetrics.fetchLoadedModels(anyString(), anyInt())).thenReturn("[]");
        when(mockMetrics.fetchContainerLoad(anyString())).thenReturn(0.5);
        when(mockMetrics.fetchGpuMetricsStructured(anyString())).thenReturn(new java.util.HashMap<>());
        
        when(mockAuthorizer.isAllowed(anyString(), anySet(), anyString(), anyString())).thenReturn(true);
        
        when(mockAm.getRunningContainers()).thenReturn(containers);
        when(mockAm.getAvailableModels()).thenReturn(new ArrayList<>());
        when(mockAm.getMetricsCollector()).thenReturn(mockMetrics);
        when(mockAm.getRangerAuthorizer()).thenReturn(mockAuthorizer);
        when(mockAm.getAvailableResources()).thenReturn(org.apache.hadoop.yarn.api.records.Resource.newInstance(1024, 1));

        DiscoveryServer server = new DiscoveryServer(config, mockAm);
        server.start();
        int actualPort = server.getPort();

        try {
            HttpClient client = HttpClient.newHttpClient();
            
            // 1. Unauthenticated request
            HttpRequest req1 = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + actualPort + "/instances"))
                    .build();
            HttpResponse<String> resp1 = client.send(req1, HttpResponse.BodyHandlers.ofString());
            assertEquals(401, resp1.statusCode());

            // 2. Authenticated request
            HttpRequest req2 = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + actualPort + "/instances"))
                    .header("X-TARN-Token", "test-token")
                    .build();
            HttpResponse<String> resp2 = client.send(req2, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp2.statusCode());
            assertTrue(resp2.body().contains("host1:8000"));

            // 3. Dashboard request
            HttpRequest req3 = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + actualPort + "/dashboard?token=test-token"))
                    .build();
            HttpResponse<String> resp3 = client.send(req3, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp3.statusCode());
            assertTrue(resp3.body().contains("TARN Dashboard"));
            assertTrue(resp3.body().contains("host1"));
            assertTrue(resp3.body().contains("4096 MB"));
            assertTrue(resp3.body().contains("2 vCores"));

            // 4. Prometheus metrics request
            HttpRequest req4 = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + actualPort + "/metrics?token=test-token"))
                    .build();
            HttpResponse<String> resp4 = client.send(req4, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp4.statusCode());
            assertTrue(resp4.body().contains("tarn_target_containers"));
            assertTrue(resp4.body().contains("tarn_running_containers 1"));
            assertTrue(resp4.body().contains("tarn_container_load{container_id=\"container_123\",host=\"host1\"} 0.5"));

            // 5. Config request
            HttpRequest req5 = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + actualPort + "/config?token=test-token"))
                    .build();
            HttpResponse<String> resp5 = client.send(req5, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp5.statusCode());
            assertTrue(resp5.body().contains("tritonPort: 8000"));
            assertTrue(resp5.body().contains("amPort:"));

        } finally {
            server.stop();
        }
    }
}
