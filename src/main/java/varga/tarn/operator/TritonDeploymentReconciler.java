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

import io.fabric8.kubernetes.api.model.ConfigMapEnvSourceBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reconciles a {@link TritonDeployment} CR into a native K8s Deployment + Service pair.
 *
 * <p><b>Contract:</b>
 * <ul>
 *   <li>For each TritonDeployment, desired state is a Deployment named after the CR with a
 *       single Triton container, plus a Service exposing the Triton ports.</li>
 *   <li>OwnerReference on the Deployment/Service points back at the CR, so {@code kubectl
 *       delete td my-llama} cascades automatically.</li>
 *   <li>Status.phase mirrors the Deployment's readiness: Pending → Reconciling → Ready.</li>
 * </ul>
 *
 * <p>This is a simple imperative reconciler — not a JOSDK Operator. The operator main loop
 * (see {@link TarnOperator}) runs {@link #reconcile} on every CR event.
 */
public class TritonDeploymentReconciler {

    private static final Logger log = LoggerFactory.getLogger(TritonDeploymentReconciler.class);
    /** Selector label applied to every pod/Service we create, identifying the owning CR. */
    public static final String LABEL_INSTANCE = "tarn.varga.io/instance";
    public static final String LABEL_COMPONENT = "tarn.varga.io/component";
    public static final String COMPONENT_TRITON = "triton";

    private final KubernetesClient client;

    public TritonDeploymentReconciler(KubernetesClient client) {
        this.client = client;
    }

    /**
     * Brings the cluster to the desired state for a single CR. Idempotent: safe to call on
     * every event. Returns the new status to be written back on the CR.
     */
    public TritonDeploymentStatus reconcile(TritonDeployment cr) {
        String name = cr.getMetadata().getName();
        String ns = cr.getMetadata().getNamespace();
        TritonDeploymentSpec spec = cr.getSpec();
        TritonDeploymentStatus status = cr.getStatus() == null
                ? new TritonDeploymentStatus() : cr.getStatus();
        status.setObservedGeneration(cr.getMetadata().getGeneration());
        status.setTargetReplicas(spec.effectiveReplicas());

        if (spec.getImage() == null || spec.getImage().isEmpty()
                || spec.getModelRepository() == null || spec.getModelRepository().isEmpty()) {
            status.setPhase(TritonDeploymentStatus.PHASE_FAILED);
            status.getConditions().add(new TritonDeploymentStatus.Condition(
                    "Ready", "False", "InvalidSpec",
                    "spec.image and spec.modelRepository are required"));
            return status;
        }

        try {
            Deployment desired = buildDeployment(cr);
            applyDeployment(ns, desired);

            Service svc = buildService(cr);
            applyService(ns, svc);

            // Read back actual state to populate status.
            Deployment current = client.apps().deployments().inNamespace(ns).withName(name).get();
            int ready = 0;
            if (current != null && current.getStatus() != null) {
                DeploymentStatus ds = current.getStatus();
                ready = ds.getReadyReplicas() == null ? 0 : ds.getReadyReplicas();
            }
            status.setReadyReplicas(ready);
            if (ready >= spec.effectiveReplicas() && spec.effectiveReplicas() > 0) {
                status.setPhase(TritonDeploymentStatus.PHASE_READY);
                replaceCondition(status, "Ready", "True", "DeploymentReady",
                        ready + "/" + spec.effectiveReplicas() + " replicas ready");
            } else {
                status.setPhase(TritonDeploymentStatus.PHASE_RECONCILING);
                replaceCondition(status, "Ready", "False", "WaitingForReplicas",
                        ready + "/" + spec.effectiveReplicas() + " replicas ready");
            }
            log.info("Reconciled {}/{}: phase={} ready={}/{}",
                    ns, name, status.getPhase(), ready, spec.effectiveReplicas());
        } catch (Exception e) {
            log.error("Reconcile failed for {}/{}: {}", ns, name, e.getMessage(), e);
            status.setPhase(TritonDeploymentStatus.PHASE_DEGRADED);
            replaceCondition(status, "Ready", "False", "ReconcileError",
                    e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return status;
    }

    /** Deletes a CR's dependent resources. Called when a CR is removed from the cluster. */
    public void cleanup(String namespace, String name) {
        client.apps().deployments().inNamespace(namespace).withName(name).delete();
        client.services().inNamespace(namespace).withName(name).delete();
        log.info("Cleaned up resources for {}/{}", namespace, name);
    }

    /**
     * Create the Deployment or replace it if it already exists. We don't rely on SSA because
     * the fabric8 mock server in tests doesn't implement it, and older real clusters (&lt;1.22)
     * don't either. A read-then-create-or-replace is explicit and universally supported.
     */
    private void applyDeployment(String ns, Deployment desired) {
        var ops = client.apps().deployments().inNamespace(ns).withName(desired.getMetadata().getName());
        Deployment existing = ops.get();
        if (existing == null) {
            client.apps().deployments().inNamespace(ns).resource(desired).create();
        } else {
            // Preserve server-assigned fields we shouldn't stomp on.
            desired.getMetadata().setResourceVersion(existing.getMetadata().getResourceVersion());
            client.apps().deployments().inNamespace(ns).resource(desired).update();
        }
    }

    private void applyService(String ns, Service desired) {
        var ops = client.services().inNamespace(ns).withName(desired.getMetadata().getName());
        Service existing = ops.get();
        if (existing == null) {
            client.services().inNamespace(ns).resource(desired).create();
        } else {
            desired.getMetadata().setResourceVersion(existing.getMetadata().getResourceVersion());
            // ClusterIP, once assigned, must be preserved to avoid a validation error.
            if (desired.getSpec() != null && existing.getSpec() != null) {
                desired.getSpec().setClusterIP(existing.getSpec().getClusterIP());
                desired.getSpec().setClusterIPs(existing.getSpec().getClusterIPs());
            }
            client.services().inNamespace(ns).resource(desired).update();
        }
    }

    // ----------------------------------------------------------------------
    // Desired state builders
    // ----------------------------------------------------------------------

    Deployment buildDeployment(TritonDeployment cr) {
        String name = cr.getMetadata().getName();
        TritonDeploymentSpec spec = cr.getSpec();
        Map<String, String> labels = commonLabels(cr);

        Container container = new ContainerBuilder()
                .withName("triton")
                .withImage(spec.getImage())
                .withEnv(buildEnvVars(spec))
                .withResources(buildResourceRequirements(spec))
                .addNewPort().withName("http").withContainerPort(8000).withProtocol("TCP").endPort()
                .addNewPort().withName("grpc").withContainerPort(8001).withProtocol("TCP").endPort()
                .addNewPort().withName("metrics").withContainerPort(8002).withProtocol("TCP").endPort()
                .addAllToPorts(openaiPortIfEnabled(spec))
                // Triton's /v2/health/ready is the authoritative readiness signal.
                .withNewReadinessProbe()
                    .withNewHttpGet()
                        .withPath("/v2/health/ready")
                        .withPort(new IntOrString("http"))
                    .endHttpGet()
                    .withPeriodSeconds(5)
                    .withFailureThreshold(60)   // allow up to 5min to warm up a large model
                .endReadinessProbe()
                .withNewLivenessProbe()
                    .withNewHttpGet()
                        .withPath("/v2/health/live")
                        .withPort(new IntOrString("http"))
                    .endHttpGet()
                    .withPeriodSeconds(15)
                    .withFailureThreshold(4)
                .endLivenessProbe()
                .build();

        return new DeploymentBuilder()
                .withMetadata(ownedMeta(cr, name))
                .withNewSpec()
                    .withReplicas(spec.effectiveReplicas())
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(labels).build())
                    .withNewTemplate()
                        .withNewMetadata().withLabels(labels).endMetadata()
                        .withNewSpec()
                            .withContainers(container)
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();
    }

    Service buildService(TritonDeployment cr) {
        String name = cr.getMetadata().getName();
        TritonDeploymentSpec spec = cr.getSpec();
        List<ServicePort> ports = new ArrayList<>();
        ports.add(new ServicePortBuilder().withName("http").withPort(8000)
                .withTargetPort(new IntOrString("http")).withProtocol("TCP").build());
        ports.add(new ServicePortBuilder().withName("grpc").withPort(8001)
                .withTargetPort(new IntOrString("grpc")).withProtocol("TCP").build());
        ports.add(new ServicePortBuilder().withName("metrics").withPort(8002)
                .withTargetPort(new IntOrString("metrics")).withProtocol("TCP").build());
        if (Boolean.TRUE.equals(effectiveProxyEnabled(spec))) {
            int openaiPort = spec.getOpenaiProxy() != null && spec.getOpenaiProxy().getPort() != null
                    ? spec.getOpenaiProxy().getPort() : 9000;
            ports.add(new ServicePortBuilder().withName("openai").withPort(openaiPort)
                    .withTargetPort(new IntOrString("openai")).withProtocol("TCP").build());
        }
        return new ServiceBuilder()
                .withMetadata(ownedMeta(cr, name))
                .withNewSpec()
                    .withSelector(commonLabels(cr))
                    .withPorts(ports)
                    .withType("ClusterIP")
                .endSpec()
                .build();
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    private ObjectMeta ownedMeta(TritonDeployment cr, String name) {
        return new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(cr.getMetadata().getNamespace())
                .withLabels(commonLabels(cr))
                .withOwnerReferences(new OwnerReferenceBuilder()
                        .withApiVersion(cr.getApiVersion())
                        .withKind(cr.getKind())
                        .withName(cr.getMetadata().getName())
                        .withUid(cr.getMetadata().getUid())
                        .withController(true)
                        .withBlockOwnerDeletion(true)
                        .build())
                .build();
    }

    private Map<String, String> commonLabels(TritonDeployment cr) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put(LABEL_INSTANCE, cr.getMetadata().getName());
        labels.put(LABEL_COMPONENT, COMPONENT_TRITON);
        return labels;
    }

    private List<EnvVar> buildEnvVars(TritonDeploymentSpec spec) {
        List<EnvVar> out = new ArrayList<>();
        out.add(new EnvVar("MODEL_REPOSITORY", spec.getModelRepository(), null));
        out.add(new EnvVar("TENSOR_PARALLELISM", String.valueOf(spec.effectiveTensorParallelism()), null));
        out.add(new EnvVar("PIPELINE_PARALLELISM", String.valueOf(spec.effectivePipelineParallelism()), null));
        if (Boolean.TRUE.equals(effectiveProxyEnabled(spec))) {
            out.add(new EnvVar("OPENAI_PROXY_ENABLED", "true", null));
        }
        if (spec.getEnv() != null) {
            for (TritonDeploymentSpec.EnvVar e : spec.getEnv()) {
                out.add(new EnvVar(e.getName(), e.getValue(), null));
            }
        }
        return out;
    }

    private io.fabric8.kubernetes.api.model.ResourceRequirements buildResourceRequirements(TritonDeploymentSpec spec) {
        TritonDeploymentSpec.Resources r = spec.getResources() == null
                ? new TritonDeploymentSpec.Resources() : spec.getResources();
        Map<String, Quantity> requests = new LinkedHashMap<>();
        requests.put("memory", new Quantity(r.getMemory()));
        requests.put("cpu", new Quantity(r.getCpu()));
        Map<String, Quantity> limits = new LinkedHashMap<>(requests);
        TritonDeploymentSpec.Accelerator acc = spec.effectiveAccelerator();
        String accelResource = acc.kubernetesResourceName();
        if (accelResource != null) {
            int accelCount = acc.getCount() == null ? 1 : acc.getCount();
            int total = Math.max(1, accelCount * spec.effectiveTensorParallelism() * spec.effectivePipelineParallelism());
            limits.put(accelResource, new Quantity(String.valueOf(total)));
            requests.put(accelResource, new Quantity(String.valueOf(total)));
        }
        return new ResourceRequirementsBuilder()
                .withRequests(requests)
                .withLimits(limits)
                .build();
    }

    private List<io.fabric8.kubernetes.api.model.ContainerPort> openaiPortIfEnabled(TritonDeploymentSpec spec) {
        List<io.fabric8.kubernetes.api.model.ContainerPort> list = new ArrayList<>();
        if (Boolean.TRUE.equals(effectiveProxyEnabled(spec))) {
            int port = spec.getOpenaiProxy() != null && spec.getOpenaiProxy().getPort() != null
                    ? spec.getOpenaiProxy().getPort() : 9000;
            list.add(new io.fabric8.kubernetes.api.model.ContainerPortBuilder()
                    .withName("openai")
                    .withContainerPort(port)
                    .withProtocol("TCP")
                    .build());
        }
        return list;
    }

    private Boolean effectiveProxyEnabled(TritonDeploymentSpec spec) {
        return spec.getOpenaiProxy() != null && Boolean.TRUE.equals(spec.getOpenaiProxy().getEnabled());
    }

    private void replaceCondition(TritonDeploymentStatus status, String type, String state,
                                  String reason, String message) {
        status.getConditions().removeIf(c -> type.equals(c.getType()));
        status.getConditions().add(
                new TritonDeploymentStatus.Condition(type, state, reason, message));
    }

    // Silence unused-import warnings; these helpers are here as a template for the next wave
    // of features (ConfigMap-backed quotas refs, ServiceAccount binding, etc.).
    @SuppressWarnings("unused")
    private static Object silence(ConfigMapEnvSourceBuilder x) { return x; }
}
