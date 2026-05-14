package varga.tarn.operator;

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
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exercises the canary auto-promotion gate end-to-end against a fake Prometheus HTTP
 * server. Two scenarios: the canary clears the SLO thresholds and gets promoted to 100%,
 * and the canary blows them and is left untouched.
 */
@EnableKubernetesMockClient(crud = true)
public class CanaryAnalysisTest {

    KubernetesClient client;
    private HttpServer prom;
    private int promPort;
    private volatile String promBody;

    @BeforeEach
    void setup() throws Exception {
        prom = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        prom.createContext("/api/v1/query", ex -> {
            byte[] body = promBody.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(200, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        prom.start();
        promPort = prom.getAddress().getPort();
    }

    @AfterEach
    void teardown() {
        if (prom != null) prom.stop(0);
    }

    private TritonDeployment buildCr(String name, double canaryWeight) {
        TritonDeployment cr = new TritonDeployment();
        cr.setMetadata(new ObjectMetaBuilder()
                .withName(name).withNamespace("ns").withUid("uid-" + name).withGeneration(1L).build());
        TritonDeploymentSpec spec = new TritonDeploymentSpec();
        spec.setImage("triton:1");
        spec.setModelRepository("s3://m");
        spec.setReplicas(2);
        TritonDeploymentSpec.TrafficVariant stable = new TritonDeploymentSpec.TrafficVariant("stable", 95);
        TritonDeploymentSpec.TrafficVariant canary = new TritonDeploymentSpec.TrafficVariant("canary", (int) canaryWeight);
        TritonDeploymentSpec.CanaryAnalysis a = new TritonDeploymentSpec.CanaryAnalysis();
        a.setObservationWindowSec(1); // 1 second window so the test doesn't wait 5 minutes.
        a.setPrometheusUrl("http://127.0.0.1:" + promPort);
        canary.setAnalysis(a);
        spec.setTraffic(Arrays.asList(stable, canary));
        cr.setSpec(spec);
        return cr;
    }

    private String fakePromResponse(double value) {
        return "{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{"
                + "\"metric\":{},\"value\":[1.0,\"" + value + "\"]}]}}";
    }

    @Test
    public void promotesCanaryWhenSloMet() throws Exception {
        TritonDeployment cr = buildCr("good", 5);
        // The promotion path mutates the spec and calls update() — the CR must exist in
        // the mock apiserver. We create it once and let the reconciler patch it in place.
        client.resources(TritonDeployment.class).inNamespace("ns").resource(cr).create();
        TritonDeploymentReconciler r = new TritonDeploymentReconciler(client);

        TritonDeploymentStatus s1 = new TritonDeploymentStatus();
        cr.setStatus(s1);
        r.maybeRunCanaryAnalysis(cr, s1);
        assertEquals("Running", s1.getCanaryAnalysis().getState());
        s1.getCanaryAnalysis().setStartTime(Instant.now().minusSeconds(5).toString());

        promBody = fakePromResponse(0.001);
        r.maybeRunCanaryAnalysis(cr, s1);
        assertEquals("Succeeded", s1.getCanaryAnalysis().getState());
        assertEquals(0, (int) cr.getSpec().getTraffic().get(0).getWeight(), "stable must be drained");
        assertEquals(100, (int) cr.getSpec().getTraffic().get(1).getWeight(), "canary must be promoted");
    }

    @Test
    public void leavesWeightsAloneWhenSloBlown() throws Exception {
        TritonDeployment cr = buildCr("bad", 5);
        TritonDeploymentReconciler r = new TritonDeploymentReconciler(client);

        TritonDeploymentStatus s = new TritonDeploymentStatus();
        cr.setStatus(s);
        r.maybeRunCanaryAnalysis(cr, s);
        s.getCanaryAnalysis().setStartTime(Instant.now().minusSeconds(5).toString());

        // Error rate of 10% — well over the 1% default threshold.
        promBody = fakePromResponse(0.10);
        r.maybeRunCanaryAnalysis(cr, s);
        assertEquals("Failed", s.getCanaryAnalysis().getState());
        // Weights untouched: operator humans investigate and decide whether to retry.
        assertEquals(95, (int) cr.getSpec().getTraffic().get(0).getWeight());
        assertEquals(5, (int) cr.getSpec().getTraffic().get(1).getWeight());
    }
}
