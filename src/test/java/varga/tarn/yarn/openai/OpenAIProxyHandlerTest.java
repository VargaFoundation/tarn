package varga.tarn.yarn.openai;

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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import varga.tarn.yarn.ApplicationMaster;
import varga.tarn.yarn.MetricsCollector;
import varga.tarn.yarn.RangerAuthorizer;
import varga.tarn.yarn.TarnConfig;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * End-to-end tests for the OpenAI proxy. A real mini HTTP server plays the role of Triton's
 * openai_frontend on 127.0.0.1 so we exercise the proxy pipeline for real — authz, routing,
 * streaming, content propagation. The SSRF guard in MetricsCollector does not apply here
 * because queue-depth lookups are by container id, not host.
 */
public class OpenAIProxyHandlerTest {

    private HttpServer fakeTriton;
    private OpenAIProxyServer proxy;
    private TarnConfig config;
    private ApplicationMaster mockAm;
    private RangerAuthorizer mockRanger;
    private MetricsCollector metrics;
    private final AtomicInteger tritonHits = new AtomicInteger();
    private final ObjectMapper om = new ObjectMapper();

    @BeforeEach
    void setup() throws Exception {
        // 1. Fake Triton on 127.0.0.1 — choose an ephemeral port.
        fakeTriton = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeTriton.createContext("/v1/chat/completions", ex -> {
            tritonHits.incrementAndGet();
            byte[] reqBody = ex.getRequestBody().readAllBytes();
            JsonNode req = om.readTree(reqBody);
            assertEquals("alice", ex.getRequestHeaders().getFirst("X-Forwarded-User"),
                    "identity must propagate downstream");
            if (req.has("stream") && req.get("stream").asBoolean()) {
                // Emit three SSE events then [DONE].
                ex.getResponseHeaders().set("Content-Type", "text/event-stream");
                ex.sendResponseHeaders(200, 0);
                try (var os = ex.getResponseBody()) {
                    os.write("data: {\"delta\":{\"content\":\"he\"}}\n\n".getBytes(StandardCharsets.UTF_8));
                    os.flush();
                    os.write("data: {\"delta\":{\"content\":\"llo\"}}\n\n".getBytes(StandardCharsets.UTF_8));
                    os.flush();
                    os.write("data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8));
                }
            } else {
                byte[] resp = "{\"id\":\"cmpl-x\",\"choices\":[{\"message\":{\"content\":\"ok\"}}]}".getBytes(StandardCharsets.UTF_8);
                ex.getResponseHeaders().set("Content-Type", "application/json");
                ex.sendResponseHeaders(200, resp.length);
                try (var os = ex.getResponseBody()) { os.write(resp); }
            }
        });
        fakeTriton.start();

        // 2. Mock AM + Ranger + real MetricsCollector.
        mockAm = mock(ApplicationMaster.class);
        mockRanger = mock(RangerAuthorizer.class);
        metrics = new MetricsCollector(8002);

        Container c = mock(Container.class);
        NodeId nid = mock(NodeId.class);
        ContainerId cid = mock(ContainerId.class);
        when(cid.toString()).thenReturn("container_1");
        when(nid.getHost()).thenReturn("127.0.0.1");
        when(c.getId()).thenReturn(cid);
        when(c.getNodeId()).thenReturn(nid);
        List<Container> list = new ArrayList<>();
        list.add(c);
        when(mockAm.getRunningContainers()).thenReturn(list);
        when(mockAm.getMetricsCollector()).thenReturn(metrics);
        when(mockAm.getRangerAuthorizer()).thenReturn(mockRanger);
        when(mockAm.getAvailableModels()).thenReturn(List.of("llama-3-70b", "stable-diffusion"));

        // 3. Proxy config: use the fake Triton port as tritonPort.
        config = new TarnConfig();
        config.bindAddress = "127.0.0.1";
        config.openaiProxyPort = 0; // random
        config.openaiProxyEnabled = true;
        config.tritonPort = fakeTriton.getAddress().getPort();
        config.tlsEnabled = false;
        proxy = new OpenAIProxyServer(config, mockAm, null);
        proxy.start();
    }

    @AfterEach
    void teardown() {
        if (proxy != null) proxy.stop();
        if (fakeTriton != null) fakeTriton.stop(0);
    }

    private String proxyUrl(String path) {
        return "http://127.0.0.1:" + proxy.getPort() + path;
    }

    @Test
    public void listsModelsFilteredByRanger() throws Exception {
        when(mockRanger.isAllowed(anyString(), anySet(), eq("list"), eq("llama-3-70b"), anyString())).thenReturn(true);
        when(mockRanger.isAllowed(anyString(), anySet(), eq("list"), eq("stable-diffusion"), anyString())).thenReturn(false);

        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/models"))).build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        JsonNode body = om.readTree(resp.body());
        assertEquals("list", body.get("object").asText());
        assertEquals(1, body.get("data").size());
        assertEquals("llama-3-70b", body.get("data").get(0).get("id").asText());
    }

    @Test
    public void deniedInferReturns403() throws Exception {
        when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), eq("llama-3-70b"), anyString())).thenReturn(false);

        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                        .header("Content-Type", "application/json")
                        .header("X-Forwarded-User", "alice")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"model\":\"llama-3-70b\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(403, resp.statusCode());
        assertEquals(0, tritonHits.get(), "denied requests must never reach Triton");
        JsonNode body = om.readTree(resp.body());
        assertEquals("permission_denied", body.get("error").get("type").asText());
    }

    @Test
    public void allowedInferProxiesAndCountsMetrics() throws Exception {
        when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), eq("llama-3-70b"), anyString())).thenReturn(true);

        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                        .header("Content-Type", "application/json")
                        .header("X-Forwarded-User", "alice")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"model\":\"llama-3-70b\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertEquals(1, tritonHits.get());
        assertTrue(resp.body().contains("cmpl-x"));
        // Latency histogram should have recorded one sample.
        assertEquals(1L, metrics.getHistogramCount("llama-3-70b"));
        assertEquals(1L, metrics.getRequestCount("llama-3-70b"));
    }

    @Test
    public void streamingResponseIsRelayedWithoutBuffering() throws Exception {
        when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), eq("llama-3-70b"), anyString())).thenReturn(true);

        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                        .header("Content-Type", "application/json")
                        .header("X-Forwarded-User", "alice")
                        .timeout(Duration.ofSeconds(5))
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"model\":\"llama-3-70b\",\"stream\":true,\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("he"));
        assertTrue(resp.body().contains("llo"));
        assertTrue(resp.body().contains("[DONE]"));
        assertTrue(resp.headers().firstValue("Content-Type").orElse("").startsWith("text/event-stream"));
    }

    @Test
    public void loraSuffixIsCheckedAgainstRangerSeparately() throws Exception {
        // Base allowed, LoRA denied.
        when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), eq("llama-3-70b"), anyString())).thenReturn(true);
        when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), eq("llama-3-70b#customer-support"), anyString())).thenReturn(false);

        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                        .header("Content-Type", "application/json")
                        .header("X-Forwarded-User", "alice")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"model\":\"llama-3-70b#customer-support\",\"messages\":[]}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(403, resp.statusCode());
        assertEquals(0, tritonHits.get());
    }

    @Test
    public void returns503WhenNoContainers() throws Exception {
        when(mockAm.getRunningContainers()).thenReturn(new ArrayList<>());
        when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), anyString(), anyString())).thenReturn(true);

        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("{\"model\":\"llama-3-70b\"}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(503, resp.statusCode());
    }

    @Test
    public void invalidBodyReturns400() throws Exception {
        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("not-json"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(400, resp.statusCode());
    }

    @Test
    public void tokenUsageFromResponseIsAccounted() throws Exception {
        // Replace fake Triton handler with one that returns an OpenAI usage block.
        fakeTriton.removeContext("/v1/chat/completions");
        fakeTriton.createContext("/v1/chat/completions", exh -> {
            tritonHits.incrementAndGet();
            byte[] resp = ("{\"id\":\"cmpl-1\",\"choices\":[{\"message\":{\"content\":\"ok\"}}]," +
                    "\"usage\":{\"prompt_tokens\":42,\"completion_tokens\":17,\"total_tokens\":59}}")
                    .getBytes(StandardCharsets.UTF_8);
            exh.getResponseHeaders().set("Content-Type", "application/json");
            exh.sendResponseHeaders(200, resp.length);
            try (var os = exh.getResponseBody()) { os.write(resp); }
        });
        when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), eq("llama-3-70b"), anyString())).thenReturn(true);

        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                        .header("Content-Type", "application/json")
                        .header("X-Forwarded-User", "alice")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"model\":\"llama-3-70b\",\"messages\":[]}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());

        // Token counters must be accumulated per (user, model).
        assertEquals(42L, (long) metrics.getTokensIn().get("alice|llama-3-70b"));
        assertEquals(17L, (long) metrics.getTokensOut().get("alice|llama-3-70b"));
    }

    @Test
    public void clientIpPropagatesToRanger() throws Exception {
        // Verify X-Forwarded-For (set by Knox / ingress) is the IP passed to Ranger audit.
        when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), eq("llama-3-70b"), anyString())).thenReturn(true);

        HttpResponse<String> resp = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                        .header("Content-Type", "application/json")
                        .header("X-Forwarded-User", "alice")
                        .header("X-Forwarded-For", "203.0.113.7, 10.0.0.1")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"model\":\"llama-3-70b\",\"messages\":[]}"))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());

        // First value in the XFF chain is the client. Ranger must receive it verbatim.
        org.mockito.ArgumentCaptor<String> ip = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(mockRanger, atLeastOnce()).isAllowed(
                anyString(), anySet(), eq("infer"), eq("llama-3-70b"), ip.capture());
        assertEquals("203.0.113.7", ip.getValue());
    }

    @Test
    public void shadowEndpointReceivesAtSampleRate1() throws Exception {
        // Build a second fake Triton, point the proxy's shadow config to it, verify it sees
        // the mirrored request while the primary still serves the client.
        java.util.concurrent.atomic.AtomicInteger shadowHits = new java.util.concurrent.atomic.AtomicInteger();
        HttpServer shadow = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        shadow.createContext("/v1/chat/completions", exh -> {
            shadowHits.incrementAndGet();
            assertEquals("true", exh.getRequestHeaders().getFirst("X-TARN-Shadow"));
            byte[] reply = "{}".getBytes(StandardCharsets.UTF_8);
            exh.sendResponseHeaders(200, reply.length);
            try (var os = exh.getResponseBody()) { os.write(reply); }
        });
        shadow.start();
        try {
            config.shadowEndpoint = "http://127.0.0.1:" + shadow.getAddress().getPort();
            config.shadowSampleRate = 1.0;
            // Rebuild proxy to pick up new config (handler reads config at handle time? It reads
            // on every request, so changing config fields works without restart).
            when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), eq("llama-3-70b"), anyString())).thenReturn(true);

            HttpResponse<String> resp = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                            .header("Content-Type", "application/json")
                            .header("X-Forwarded-User", "alice")
                            .POST(HttpRequest.BodyPublishers.ofString(
                                    "{\"model\":\"llama-3-70b\",\"messages\":[]}"))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, resp.statusCode());
            assertEquals(1, tritonHits.get(), "primary must receive the request");
            // Give the async shadow up to 2s to arrive.
            long deadline = System.currentTimeMillis() + 2000;
            while (shadowHits.get() == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertEquals(1, shadowHits.get(), "shadow must also receive the mirrored request");
        } finally {
            shadow.stop(0);
        }
    }

    @Test
    public void quotaExceededReturns429WithRetryAfter() throws Exception {
        // Cap any caller to 1 request per minute; send 2 and verify the 2nd is 429.
        varga.tarn.yarn.QuotaEnforcer quotas = new varga.tarn.yarn.QuotaEnforcer();
        quotas.setGlobalLimit(1);
        when(mockAm.getQuotaEnforcer()).thenReturn(quotas);
        when(mockRanger.isAllowed(anyString(), anySet(), eq("infer"), eq("llama-3-70b"), anyString())).thenReturn(true);

        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(proxyUrl("/v1/chat/completions")))
                .header("Content-Type", "application/json")
                .header("X-Forwarded-User", "alice")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"model\":\"llama-3-70b\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"))
                .build();

        HttpResponse<String> r1 = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r1.statusCode());

        HttpResponse<String> r2 = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(429, r2.statusCode());
        assertTrue(r2.headers().firstValue("Retry-After").isPresent(),
                "429 must include Retry-After header");
        // Triton received only the first call.
        assertEquals(1, tritonHits.get());
    }
}
