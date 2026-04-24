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
        // A Warning event should have been emitted for ops visibility.
        assertTrue(client.v1().events().inNamespace("ns").list().getItems().stream()
                .anyMatch(e -> "Warning".equals(e.getType()) && "InvalidSpec".equals(e.getReason())),
                "InvalidSpec Warning event must be emitted");
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
    public void weightedTrafficSplitCreatesProportionalDeployments() {
        TritonDeployment cr = newCr("llm", "ns", s -> {
            s.setReplicas(10);
            s.setTraffic(Arrays.asList(
                    new TritonDeploymentSpec.TrafficVariant("v1", 90),
                    new TritonDeploymentSpec.TrafficVariant("v2", 10)));
        });
        TritonDeploymentStatus status = new TritonDeploymentReconciler(client).reconcile(cr);

        var d1 = client.apps().deployments().inNamespace("ns").withName("llm-v1").get();
        var d2 = client.apps().deployments().inNamespace("ns").withName("llm-v2").get();
        assertNotNull(d1, "variant v1 deployment must exist");
        assertNotNull(d2, "variant v2 deployment must exist");
        // 90/10 split over 10 replicas -> 9/1 exactly (largest-remainder).
        assertEquals(9, d1.getSpec().getReplicas());
        assertEquals(1, d2.getSpec().getReplicas());
        // The no-variant Deployment must NOT exist in multi-cohort mode.
        assertNull(client.apps().deployments().inNamespace("ns").withName("llm").get());
        assertEquals(10, status.getTargetReplicas());
    }

    @Test
    public void trafficVariantOverridesImageAndModelRepository() {
        TritonDeployment cr = newCr("canary", "ns", s -> {
            s.setReplicas(4);
            TritonDeploymentSpec.TrafficVariant v2 = new TritonDeploymentSpec.TrafficVariant("v2", 25);
            v2.setImage("nvcr.io/nvidia/tritonserver:25.03-py3");
            v2.setModelRepository("s3://models/v2");
            s.setTraffic(Arrays.asList(
                    new TritonDeploymentSpec.TrafficVariant("v1", 75),
                    v2));
        });
        new TritonDeploymentReconciler(client).reconcile(cr);

        var d1 = client.apps().deployments().inNamespace("ns").withName("canary-v1").get();
        var d2 = client.apps().deployments().inNamespace("ns").withName("canary-v2").get();
        // v1 inherits top-level spec, v2 overrides.
        assertEquals("nvcr.io/nvidia/tritonserver:24.09-py3",
                d1.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());
        assertEquals("nvcr.io/nvidia/tritonserver:25.03-py3",
                d2.getSpec().getTemplate().getSpec().getContainers().get(0).getImage());

        // MODEL_REPOSITORY env differs between variants.
        var envs2 = d2.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        assertTrue(envs2.stream().anyMatch(e -> "MODEL_REPOSITORY".equals(e.getName())
                && "s3://models/v2".equals(e.getValue())));
    }

    @Test
    public void switchingToMultiCohortDeletesTheOldSingleDeployment() {
        // First reconcile: single cohort — creates "demo" Deployment.
        TritonDeployment cr = newCr("demo", "ns", s -> s.setReplicas(3));
        TritonDeploymentReconciler r = new TritonDeploymentReconciler(client);
        r.reconcile(cr);
        assertNotNull(client.apps().deployments().inNamespace("ns").withName("demo").get());

        // Second reconcile: CR updated with traffic split — old Deployment must be pruned.
        cr.getSpec().setTraffic(Arrays.asList(
                new TritonDeploymentSpec.TrafficVariant("primary", 1)));
        r.reconcile(cr);

        assertNotNull(client.apps().deployments().inNamespace("ns").withName("demo-primary").get());
        assertNull(client.apps().deployments().inNamespace("ns").withName("demo").get(),
                "legacy single-cohort Deployment must be pruned when switching to variants");
    }

    @Test
    public void removingAVariantPrunesItsDeployment() {
        TritonDeployment cr = newCr("clean", "ns", s -> {
            s.setReplicas(4);
            s.setTraffic(Arrays.asList(
                    new TritonDeploymentSpec.TrafficVariant("v1", 50),
                    new TritonDeploymentSpec.TrafficVariant("v2", 50)));
        });
        TritonDeploymentReconciler r = new TritonDeploymentReconciler(client);
        r.reconcile(cr);
        assertNotNull(client.apps().deployments().inNamespace("ns").withName("clean-v1").get());
        assertNotNull(client.apps().deployments().inNamespace("ns").withName("clean-v2").get());

        // Drop v2 from the traffic list.
        cr.getSpec().setTraffic(Arrays.asList(
                new TritonDeploymentSpec.TrafficVariant("v1", 100)));
        r.reconcile(cr);

        assertNotNull(client.apps().deployments().inNamespace("ns").withName("clean-v1").get());
        assertNull(client.apps().deployments().inNamespace("ns").withName("clean-v2").get(),
                "removed variant must be cleaned up");
    }

    @Test
    public void cohortPlannerDistributesExactly() {
        // Pure unit test of the planner: 7 replicas across 90/10 -> 6+1 (rounds down then fills).
        TritonDeploymentSpec spec = new TritonDeploymentSpec();
        spec.setReplicas(7);
        spec.setTraffic(Arrays.asList(
                new TritonDeploymentSpec.TrafficVariant("a", 90),
                new TritonDeploymentSpec.TrafficVariant("b", 10)));
        var plan = TritonDeploymentReconciler.planCohortReplicas(spec);
        assertEquals(6, plan.get("a"));
        assertEquals(1, plan.get("b"));
        assertEquals(7, plan.values().stream().mapToInt(Integer::intValue).sum());

        // 100 replicas across 3 weights -> should total exactly 100.
        spec.setReplicas(100);
        spec.setTraffic(Arrays.asList(
                new TritonDeploymentSpec.TrafficVariant("a", 50),
                new TritonDeploymentSpec.TrafficVariant("b", 30),
                new TritonDeploymentSpec.TrafficVariant("c", 20)));
        plan = TritonDeploymentReconciler.planCohortReplicas(spec);
        assertEquals(100, plan.values().stream().mapToInt(Integer::intValue).sum());
        assertEquals(50, plan.get("a"));
        assertEquals(30, plan.get("b"));
        assertEquals(20, plan.get("c"));
    }

    @Test
    public void gatewayHttpRouteEmittedWithPreciseWeights() {
        TritonDeployment cr = newCr("chat", "ns", s -> {
            s.setReplicas(10);
            s.setTraffic(Arrays.asList(
                    new TritonDeploymentSpec.TrafficVariant("v1", 95),
                    new TritonDeploymentSpec.TrafficVariant("v2", 5)));
            TritonDeploymentSpec.Gateway gw = new TritonDeploymentSpec.Gateway();
            gw.setEnabled(true);
            TritonDeploymentSpec.ParentRef pr = new TritonDeploymentSpec.ParentRef();
            pr.setName("public-gw");
            pr.setNamespace("gateway-system");
            gw.setParentRefs(Arrays.asList(pr));
            gw.setHostnames(Arrays.asList("chat.example.com"));
            s.setGateway(gw);
        });
        new TritonDeploymentReconciler(client).reconcile(cr);

        // Per-variant Services must exist for HTTPRoute to reference them.
        assertNotNull(client.services().inNamespace("ns").withName("chat-v1").get());
        assertNotNull(client.services().inNamespace("ns").withName("chat-v2").get());

        var rdc = new io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext.Builder()
                .withGroup("gateway.networking.k8s.io").withVersion("v1")
                .withKind("HTTPRoute").withPlural("httproutes").withNamespaced(true).build();
        var route = client.genericKubernetesResources(rdc).inNamespace("ns").withName("chat").get();
        assertNotNull(route, "HTTPRoute must be created when gateway is enabled");
        @SuppressWarnings("unchecked")
        var spec = (java.util.Map<String, Object>) route.getAdditionalProperties().get("spec");
        assertNotNull(spec);
        // parentRefs propagated.
        @SuppressWarnings("unchecked")
        var parentRefs = (java.util.List<java.util.Map<String, Object>>) spec.get("parentRefs");
        assertEquals("public-gw", parentRefs.get(0).get("name"));
        assertEquals("gateway-system", parentRefs.get(0).get("namespace"));
        // Hostnames propagated.
        assertEquals(Arrays.asList("chat.example.com"), spec.get("hostnames"));
        // Backend weights are the *raw* values from the spec — precise, not rounded.
        @SuppressWarnings("unchecked")
        var rules = (java.util.List<java.util.Map<String, Object>>) spec.get("rules");
        @SuppressWarnings("unchecked")
        var backends = (java.util.List<java.util.Map<String, Object>>) rules.get(0).get("backendRefs");
        assertEquals(95, backends.get(0).get("weight"));
        assertEquals(5, backends.get(1).get("weight"));
        assertEquals("chat-v1", backends.get(0).get("name"));
        assertEquals("chat-v2", backends.get(1).get("name"));
    }

    @Test
    public void gatewayDisabledLeavesHttpRouteAbsent() {
        // Default path: gateway disabled → no HTTPRoute, single aggregate Service only.
        TritonDeployment cr = newCr("plain", "ns", s -> {
            s.setReplicas(2);
            s.setTraffic(Arrays.asList(
                    new TritonDeploymentSpec.TrafficVariant("v1", 50),
                    new TritonDeploymentSpec.TrafficVariant("v2", 50)));
        });
        new TritonDeploymentReconciler(client).reconcile(cr);

        assertNotNull(client.services().inNamespace("ns").withName("plain").get(),
                "aggregate Service must still exist");
        // No per-variant Services, no HTTPRoute.
        assertNull(client.services().inNamespace("ns").withName("plain-v1").get());
        var rdc = new io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext.Builder()
                .withGroup("gateway.networking.k8s.io").withVersion("v1")
                .withKind("HTTPRoute").withPlural("httproutes").withNamespaced(true).build();
        assertNull(client.genericKubernetesResources(rdc).inNamespace("ns").withName("plain").get());
    }

    @Test
    public void cohortPlannerHonoursWeightZero() {
        TritonDeploymentSpec spec = new TritonDeploymentSpec();
        spec.setReplicas(10);
        // v2 drained (weight=0) but kept reachable for header-targeted testing.
        spec.setTraffic(Arrays.asList(
                new TritonDeploymentSpec.TrafficVariant("v1", 1),
                new TritonDeploymentSpec.TrafficVariant("v2", 0)));
        var plan = TritonDeploymentReconciler.planCohortReplicas(spec);
        assertEquals(10, plan.get("v1"));
        assertEquals(0, plan.get("v2"));
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
