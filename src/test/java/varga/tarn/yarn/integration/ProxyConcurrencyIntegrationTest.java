package varga.tarn.yarn.integration;

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

import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import varga.tarn.yarn.ApplicationMaster;
import varga.tarn.yarn.MetricsCollector;
import varga.tarn.yarn.QuotaEnforcer;
import varga.tarn.yarn.RangerAuthorizer;
import varga.tarn.yarn.TarnConfig;
import varga.tarn.yarn.openai.OpenAIProxyServer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Stress test the OpenAI proxy with concurrent clients to catch race conditions in quota
 * enforcement, metric updates, and response relay. A concurrency regression in any of these
 * would be silently corruption, not a visible failure — so this covers what the unit tests
 * intentionally can't.
 */
public class ProxyConcurrencyIntegrationTest {

    private HttpServer fakeTriton;
    private OpenAIProxyServer proxy;
    private ApplicationMaster mockAm;
    private MetricsCollector metrics;
    private QuotaEnforcer quotaEnforcer;
    private final AtomicInteger tritonHits = new AtomicInteger();

    @BeforeEach
    void setup() throws Exception {
        fakeTriton = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        fakeTriton.createContext("/v1/chat/completions", ex -> {
            tritonHits.incrementAndGet();
            byte[] resp = ("{\"id\":\"cmpl\",\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":20}}")
                    .getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, resp.length);
            try (var os = ex.getResponseBody()) { os.write(resp); }
        });
        // Larger thread pool so concurrent requests don't self-serialize on the fake server.
        fakeTriton.setExecutor(Executors.newFixedThreadPool(16));
        fakeTriton.start();

        mockAm = mock(ApplicationMaster.class);
        RangerAuthorizer ranger = mock(RangerAuthorizer.class);
        when(ranger.isAllowed(anyString(), anySet(), eq("infer"), anyString(), anyString())).thenReturn(true);
        metrics = new MetricsCollector(8002);

        Container c = mock(Container.class);
        NodeId nid = mock(NodeId.class);
        ContainerId cid = mock(ContainerId.class);
        when(cid.toString()).thenReturn("c1");
        when(nid.getHost()).thenReturn("127.0.0.1");
        when(c.getId()).thenReturn(cid);
        when(c.getNodeId()).thenReturn(nid);
        List<Container> containers = new ArrayList<>();
        containers.add(c);
        when(mockAm.getRunningContainers()).thenReturn(containers);
        when(mockAm.getMetricsCollector()).thenReturn(metrics);
        when(mockAm.getRangerAuthorizer()).thenReturn(ranger);
        when(mockAm.getAvailableModels()).thenReturn(List.of("m"));
        quotaEnforcer = new QuotaEnforcer();
        when(mockAm.getQuotaEnforcer()).thenReturn(quotaEnforcer);

        TarnConfig config = new TarnConfig();
        config.bindAddress = "127.0.0.1";
        config.openaiProxyPort = 0;
        config.openaiProxyEnabled = true;
        config.tritonPort = fakeTriton.getAddress().getPort();
        proxy = new OpenAIProxyServer(config, mockAm, null);
        proxy.start();
    }

    @AfterEach
    void teardown() {
        if (proxy != null) proxy.stop();
        if (fakeTriton != null) fakeTriton.stop(0);
    }

    @Test
    public void tokenCountsAccumulateCorrectlyUnderConcurrency() throws Exception {
        int clients = 20;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        HttpClient httpClient = HttpClient.newHttpClient();
        String url = "http://127.0.0.1:" + proxy.getPort() + "/v1/chat/completions";
        String body = "{\"model\":\"m\",\"messages\":[]}";

        List<CompletableFuture<Integer>> futs = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            final int idx = i;
            futs.add(CompletableFuture.supplyAsync(() -> {
                try {
                    HttpResponse<String> resp = httpClient.send(
                            HttpRequest.newBuilder().uri(URI.create(url))
                                    .header("Content-Type", "application/json")
                                    .header("X-Forwarded-User", "user-" + (idx % 4))
                                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                            HttpResponse.BodyHandlers.ofString());
                    return resp.statusCode();
                } catch (Exception e) {
                    return -1;
                }
            }, pool));
        }
        for (CompletableFuture<Integer> f : futs) assertEquals(200, f.get(30, TimeUnit.SECONDS));

        assertEquals(clients, tritonHits.get(), "every request must reach upstream");
        // Total tokens across all user buckets must equal clients * {prompt_tokens, completion_tokens}.
        long totalIn = metrics.getTokensIn().values().stream().mapToLong(Long::longValue).sum();
        long totalOut = metrics.getTokensOut().values().stream().mapToLong(Long::longValue).sum();
        assertEquals(10L * clients, totalIn, "prompt tokens must not drop under concurrency");
        assertEquals(20L * clients, totalOut, "completion tokens must not drop under concurrency");

        // 4 distinct users -> 4 buckets; each received 5 requests.
        assertEquals(4, metrics.getTokensIn().size());

        pool.shutdownNow();
    }

    @Test
    public void quotaEnforcementHoldsUnderConcurrency() throws Exception {
        // Set a strict 5 rpm cap on the enforcer the handler is already using. Fire 20
        // concurrent requests and expect exactly 5 accepted (200) and 15 rate-limited (429).
        quotaEnforcer.setGlobalLimit(5);

        int clients = 20;
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        HttpClient httpClient = HttpClient.newHttpClient();
        String url = "http://127.0.0.1:" + proxy.getPort() + "/v1/chat/completions";
        String body = "{\"model\":\"m\",\"messages\":[]}";

        List<CompletableFuture<Integer>> futs = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            futs.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return httpClient.send(
                            HttpRequest.newBuilder().uri(URI.create(url))
                                    .header("Content-Type", "application/json")
                                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                            HttpResponse.BodyHandlers.discarding()).statusCode();
                } catch (Exception e) {
                    return -1;
                }
            }, pool));
        }
        int ok = 0, limited = 0;
        for (CompletableFuture<Integer> f : futs) {
            int s = f.get(30, TimeUnit.SECONDS);
            if (s == 200) ok++;
            else if (s == 429) limited++;
        }
        assertEquals(5, ok, "exactly 5 requests (the capacity) must succeed");
        assertEquals(15, limited, "the remaining 15 must receive 429");
        pool.shutdownNow();
    }

}
