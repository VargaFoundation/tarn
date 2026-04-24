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

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Direct reconciler tests against a fabric8 mock Kubernetes API server — no real cluster
 * required. Covers: Deployment + Service creation, OwnerReferences for cascade delete,
 * accelerator resource mapping, OpenAI proxy port wiring, validation errors.
 */
@EnableKubernetesMockClient(crud = true)
public class TritonDeploymentReconcilerTest {

    // Injected by the fabric8 annotation processor.
    KubernetesClient client;

    private TritonDeployment newCr(String name, String ns,
                                   java.util.function.Consumer<TritonDeploymentSpec> cfg) {
        TritonDeployment cr = new TritonDeployment();
        cr.setMetadata(new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(ns)
                .withUid("uid-" + name)
                .withGeneration(1L)
                .build());
        TritonDeploymentSpec spec = new TritonDeploymentSpec();
        spec.setImage("nvcr.io/nvidia/tritonserver:24.09-py3");
        spec.setModelRepository("s3://models");
        spec.setReplicas(2);
        cfg.accept(spec);
        cr.setSpec(spec);
        return cr;
    }

    @Test
    public void createsDeploymentAndServiceWithOwnerReferences() {
        TritonDeployment cr = newCr("llama", "ns1", s -> {});
        TritonDeploymentReconciler r = new TritonDeploymentReconciler(client);

        TritonDeploymentStatus status = r.reconcile(cr);

        Deployment d = client.apps().deployments().inNamespace("ns1").withName("llama").get();
        assertNotNull(d, "Deployment must be created");
        assertEquals(2, d.getSpec().getReplicas());
        assertEquals("nvcr.io/nvidia/tritonserver:24.09-py3",
                d.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals(1, d.getMetadata().getOwnerReferences().size());
        assertEquals("uid-llama", d.getMetadata().getOwnerReferences().get(0).getUid());
        assertTrue(d.getMetadata().getOwnerReferences().get(0).getController(),
                "OwnerReference must be controller=true so cascade delete works");

        Service s = client.services().inNamespace("ns1").withName("llama").get();
        assertNotNull(s, "Service must be created");
        // Default: http, grpc, metrics (no openai because proxy disabled).
        assertEquals(3, s.getSpec().getPorts().size());
        assertTrue(s.getSpec().getPorts().stream().anyMatch(p -> "http".equals(p.getName())));
        assertTrue(s.getSpec().getPorts().stream().anyMatch(p -> "grpc".equals(p.getName())));
        assertTrue(s.getSpec().getPorts().stream().anyMatch(p -> "metrics".equals(p.getName())));

        assertEquals(2, status.getTargetReplicas());
        // The mock server never starts real pods, so readyReplicas stays at 0 -> Reconciling.
        assertEquals(TritonDeploymentStatus.PHASE_RECONCILING, status.getPhase());
    }

    @Test
    public void acceleratorResourceMappingRespectsType() {
        TritonDeployment cr = newCr("amd-llama", "ns", s -> {
            TritonDeploymentSpec.Accelerator a = new TritonDeploymentSpec.Accelerator();
            a.setType("amd_gpu");
            a.setCount(2);
            s.setAccelerator(a);
            s.setTensorParallelism(2);
        });
        new TritonDeploymentReconciler(client).reconcile(cr);

        Deployment d = client.apps().deployments().inNamespace("ns").withName("amd-llama").get();
        var reqs = d.getSpec().getTemplate().getSpec().getContainers().get(0).getResources();
        // amd.com/gpu with count=2 × tp=2 × pp=1 = 4 devices.
        assertEquals("4", reqs.getLimits().get("amd.com/gpu").getAmount());
        assertNull(reqs.getLimits().get("nvidia.com/gpu"));
    }

    @Test
    public void cpuOnlyDoesNotRequestAccelerators() {
        TritonDeployment cr = newCr("cpu", "ns", s -> {
            TritonDeploymentSpec.Accelerator a = new TritonDeploymentSpec.Accelerator();
            a.setType("cpu_only");
            s.setAccelerator(a);
        });
        new TritonDeploymentReconciler(client).reconcile(cr);

        Deployment d = client.apps().deployments().inNamespace("ns").withName("cpu").get();
        var reqs = d.getSpec().getTemplate().getSpec().getContainers().get(0).getResources();
        assertNull(reqs.getLimits().get("nvidia.com/gpu"));
        assertNull(reqs.getLimits().get("amd.com/gpu"));
        assertNotNull(reqs.getLimits().get("memory"));
    }

    @Test
    public void openaiProxyEnabledAddsPortAndEnv() {
        TritonDeployment cr = newCr("chat", "ns", s -> {
            TritonDeploymentSpec.OpenaiProxy op = new TritonDeploymentSpec.OpenaiProxy();
            op.setEnabled(true);
            op.setPort(9100);
            s.setOpenaiProxy(op);
        });
        new TritonDeploymentReconciler(client).reconcile(cr);

        Deployment d = client.apps().deployments().inNamespace("ns").withName("chat").get();
        var container = d.getSpec().getTemplate().getSpec().getContainers().get(0);
        assertTrue(container.getPorts().stream().anyMatch(p -> "openai".equals(p.getName()) && p.getContainerPort() == 9100));
        assertTrue(container.getEnv().stream()
                .anyMatch(e -> "OPENAI_PROXY_ENABLED".equals(e.getName()) && "true".equals(e.getValue())));
        Service s = client.services().inNamespace("ns").withName("chat").get();
        assertTrue(s.getSpec().getPorts().stream()
                .anyMatch(p -> "openai".equals(p.getName()) && p.getPort() == 9100));
    }

    @Test
    public void validationFailsOnMissingRequiredFields() {
        TritonDeployment cr = new TritonDeployment();
        cr.setMetadata(new ObjectMetaBuilder().withName("bad").withNamespace("ns").withUid("x").build());
        cr.setSpec(new TritonDeploymentSpec()); // image and modelRepository missing

        TritonDeploymentStatus status = new TritonDeploymentReconciler(client).reconcile(cr);

        assertEquals(TritonDeploymentStatus.PHASE_FAILED, status.getPhase());
        // No Deployment should have been created.
        assertNull(client.apps().deployments().inNamespace("ns").withName("bad").get());
    }

    @Test
    public void envPassthroughAppendsCustomEntries() {
        TritonDeployment cr = newCr("env", "ns", s -> {
            s.setEnv(Arrays.asList(
                    new TritonDeploymentSpec.EnvVar("TARN_TOKEN", "secret"),
                    new TritonDeploymentSpec.EnvVar("CUSTOM_FLAG", "yes")));
        });
        new TritonDeploymentReconciler(client).reconcile(cr);
        var envs = client.apps().deployments().inNamespace("ns").withName("env").get()
                .getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertTrue(envs.stream().anyMatch(e -> "TARN_TOKEN".equals(e.getName())));
        assertTrue(envs.stream().anyMatch(e -> "CUSTOM_FLAG".equals(e.getName()) && "yes".equals(e.getValue())));
    }

    @Test
    public void cleanupRemovesDeploymentAndService() {
        TritonDeployment cr = newCr("gone", "ns", s -> {});
        TritonDeploymentReconciler r = new TritonDeploymentReconciler(client);
        r.reconcile(cr);

        assertNotNull(client.apps().deployments().inNamespace("ns").withName("gone").get());
        assertNotNull(client.services().inNamespace("ns").withName("gone").get());

        r.cleanup("ns", "gone");

        assertNull(client.apps().deployments().inNamespace("ns").withName("gone").get());
        assertNull(client.services().inNamespace("ns").withName("gone").get());
    }
}
