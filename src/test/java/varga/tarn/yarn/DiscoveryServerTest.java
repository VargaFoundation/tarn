package varga.tarn.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
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
        when(mockNodeId.getHost()).thenReturn("host1");
        when(mockContainer.getNodeId()).thenReturn(mockNodeId);
        containers.add(mockContainer);

        DiscoveryServer server = new DiscoveryServer(config, containers);
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

        } finally {
            server.stop();
        }
    }
}
