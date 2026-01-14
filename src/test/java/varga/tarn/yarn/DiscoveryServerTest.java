/*
 * Copyright © 2008 Varga Foundation (contact@varga.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package varga.tarn.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        when(mockMetrics.fetchLoadedModels(anyString(), anyInt())).thenReturn("[{\"name\":\"model1\",\"state\":\"READY\"}, {\"name\":\"model2\",\"state\":\"READY\"}]");
        when(mockMetrics.fetchContainerLoad(anyString())).thenReturn(0.5);
        when(mockMetrics.fetchGpuMetricsStructured(anyString())).thenReturn(new java.util.HashMap<>());

        when(mockAuthorizer.isAllowed(anyString(), anySet(), eq("list"), anyString())).thenReturn(true);
        when(mockAuthorizer.isAllowed(anyString(), anySet(), eq("metadata"), eq("model1"))).thenReturn(true);
        when(mockAuthorizer.isAllowed(anyString(), anySet(), eq("metadata"), eq("model2"))).thenReturn(false);
        when(mockAuthorizer.isAllowed(anyString(), anySet(), eq("infer"), eq("model1"))).thenReturn(true);

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

            // 6. Authorize request (Allowed)
            HttpRequest req6 = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + actualPort + "/authorize?token=test-token&model=model1&action=infer"))
                    .build();
            HttpResponse<String> resp6 = client.send(req6, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp6.statusCode());
            assertEquals("true", resp6.body());

            // 7. Authorize request (Denied)
            HttpRequest req7 = HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + actualPort + "/authorize?token=test-token&model=model2&action=metadata"))
                    .build();
            HttpResponse<String> resp7 = client.send(req7, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp7.statusCode());
            assertEquals("false", resp7.body());

            // 8. Filtered Dashboard content
            assertTrue(resp3.body().contains("model1"));
            assertFalse(resp3.body().contains("model2"));

        } finally {
            server.stop();
        }
    }
}
