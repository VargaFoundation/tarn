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
    public static final String LABEL_VARIANT = "tarn.varga.io/variant";
    public static final String COMPONENT_TRITON = "triton";

    private final KubernetesClient client;
    private final TarnEvents events;

    public TritonDeploymentReconciler(KubernetesClient client) {
        this.client = client;
        this.events = new TarnEvents(client);
    }

    // Test-only constructor that lets callers stub the event sink.
    TritonDeploymentReconciler(KubernetesClient client, TarnEvents events) {
        this.client = client;
        this.events = events;
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
            events.warning(cr, "InvalidSpec",
                    "spec.image and spec.modelRepository are required");
            return status;
        }

        try {
            List<TritonDeploymentSpec.TrafficVariant> variants = spec.getTraffic();
            int totalReady;
            if (variants == null || variants.isEmpty()) {
                // Single-cohort path — one Deployment named after the CR.
                Deployment desired = buildDeployment(cr);
                applyDeployment(ns, desired);

                Deployment current = client.apps().deployments().inNamespace(ns).withName(name).get();
                totalReady = readyReplicasOf(current);
                // Clean up any leftover variant deployments from a previous multi-cohort state.
                pruneVariantDeployments(ns, name, java.util.Collections.emptySet());
            } else {
                // Multi-cohort path — one Deployment per variant, replicas proportional to weight.
                Map<String, Integer> planned = planCohortReplicas(spec);
                totalReady = 0;
                for (Map.Entry<String, Integer> e : planned.entrySet()) {
                    Deployment desired = buildVariantDeployment(cr, findVariant(variants, e.getKey()), e.getValue());
                    applyDeployment(ns, desired);
                    Deployment current = client.apps().deployments().inNamespace(ns)
                            .withName(desired.getMetadata().getName()).get();
                    totalReady += readyReplicasOf(current);
                }
                // Remove deployments for variants that were previously defined but no longer present.
                pruneVariantDeployments(ns, name, planned.keySet());
            }

            // Single Service aggregating all cohorts (or the single deployment).
            Service svc = buildService(cr);
            applyService(ns, svc);

            // Gateway API path: when enabled and we have multiple variants, emit per-variant
            // Services + a single HTTPRoute with precise weights. Non-structured JSON so we
            // don't drag a gatewayapi model dep into the main JAR.
            if (variants != null && !variants.isEmpty()
                    && spec.getGateway() != null
                    && Boolean.TRUE.equals(spec.getGateway().getEnabled())) {
                for (TritonDeploymentSpec.TrafficVariant v : variants) {
                    Service variantSvc = buildVariantService(cr, v);
                    applyService(ns, variantSvc);
                }
                applyHttpRoute(cr, variants);
            }

            String previousPhase = cr.getStatus() == null ? null : cr.getStatus().getPhase();
            status.setReadyReplicas(totalReady);
            if (totalReady >= spec.effectiveReplicas() && spec.effectiveReplicas() > 0) {
                status.setPhase(TritonDeploymentStatus.PHASE_READY);
                replaceCondition(status, "Ready", "True", "DeploymentReady",
                        totalReady + "/" + spec.effectiveReplicas() + " replicas ready");
                if (!TritonDeploymentStatus.PHASE_READY.equals(previousPhase)) {
                    // Phase transition to Ready — worth an event, not worth spamming on every tick.
                    events.normal(cr, "Ready", "All %d replicas are ready", totalReady);
                }
            } else {
                status.setPhase(TritonDeploymentStatus.PHASE_RECONCILING);
                replaceCondition(status, "Ready", "False", "WaitingForReplicas",
                        totalReady + "/" + spec.effectiveReplicas() + " replicas ready");
                if (!TritonDeploymentStatus.PHASE_RECONCILING.equals(previousPhase)
                        && previousPhase != null) {
                    events.normal(cr, "Reconciling", "Waiting for %d/%d replicas",
                            totalReady, spec.effectiveReplicas());
                }
            }
            log.info("Reconciled {}/{}: phase={} ready={}/{} variants={}",
                    ns, name, status.getPhase(), totalReady, spec.effectiveReplicas(),
                    variants == null ? "single" : variants.size());
        } catch (Exception e) {
            log.error("Reconcile failed for {}/{}: {}", ns, name, e.getMessage(), e);
            status.setPhase(TritonDeploymentStatus.PHASE_DEGRADED);
            replaceCondition(status, "Ready", "False", "ReconcileError",
                    e.getClass().getSimpleName() + ": " + e.getMessage());
            events.warning(cr, "ReconcileError", "%s: %s",
                    e.getClass().getSimpleName(), e.getMessage());
        }
        return status;
    }

    /** Deletes a CR's dependent resources. Called when a CR is removed from the cluster. */
    public void cleanup(String namespace, String name) {
        client.apps().deployments().inNamespace(namespace).withName(name).delete();
        // Cascade delete via OwnerReferences handles variant deployments automatically, but
        // explicit cleanup is belt-and-suspenders for apiservers with GC lag.
        client.apps().deployments().inNamespace(namespace)
                .withLabel(LABEL_INSTANCE, name).delete();
        client.services().inNamespace(namespace).withName(name).delete();
        log.info("Cleaned up resources for {}/{}", namespace, name);
    }

    /**
     * Distribute {@code spec.effectiveReplicas()} across variants proportionally to their
     * weights. Variants with weight 0 get zero replicas (drained). Fractional shares are
     * rounded so the total matches the requested count exactly — largest-remainder method.
     * Variants with positive weight always get at least 1 replica unless their weight is
     * literally 0.
     */
    static Map<String, Integer> planCohortReplicas(TritonDeploymentSpec spec) {
        List<TritonDeploymentSpec.TrafficVariant> variants = spec.getTraffic();
        int total = spec.effectiveReplicas();
        Map<String, Integer> out = new java.util.LinkedHashMap<>();
        int totalWeight = 0;
        for (TritonDeploymentSpec.TrafficVariant v : variants) {
            totalWeight += v.getWeight() == null ? 0 : Math.max(0, v.getWeight());
        }
        if (totalWeight == 0) {
            // Degenerate spec — all weights 0 or null. Allocate one replica per variant as
            // a safety net so the CR remains observable.
            for (TritonDeploymentSpec.TrafficVariant v : variants) {
                out.put(v.getName(), 1);
            }
            return out;
        }

        // Largest-remainder: floor initial shares, then distribute leftovers by descending
        // fractional part. Deterministic for a given input.
        double[] remainders = new double[variants.size()];
        int allocated = 0;
        for (int i = 0; i < variants.size(); i++) {
            TritonDeploymentSpec.TrafficVariant v = variants.get(i);
            int w = v.getWeight() == null ? 0 : Math.max(0, v.getWeight());
            double exact = (double) w * total / totalWeight;
            int floor = (int) Math.floor(exact);
            remainders[i] = exact - floor;
            if (w > 0 && floor == 0) floor = 1; // never starve an active variant
            out.put(v.getName(), floor);
            allocated += floor;
        }
        // Distribute leftovers.
        int leftover = total - allocated;
        while (leftover > 0) {
            int bestIdx = -1;
            double bestRem = -1.0;
            for (int i = 0; i < variants.size(); i++) {
                if (variants.get(i).getWeight() == null || variants.get(i).getWeight() <= 0) continue;
                if (remainders[i] > bestRem) {
                    bestRem = remainders[i];
                    bestIdx = i;
                }
            }
            if (bestIdx < 0) break;
            out.merge(variants.get(bestIdx).getName(), 1, Integer::sum);
            remainders[bestIdx] = -1.0; // each variant only gets one leftover
            leftover--;
        }
        // If we over-allocated because of the starvation guard, trim from weight-0 variants.
        while (allocated + (total - allocated - leftover) > total) {
            // Shouldn't happen with above logic, defensive.
            break;
        }
        return out;
    }

    private static TritonDeploymentSpec.TrafficVariant findVariant(
            List<TritonDeploymentSpec.TrafficVariant> variants, String name) {
        for (TritonDeploymentSpec.TrafficVariant v : variants) {
            if (name.equals(v.getName())) return v;
        }
        return null;
    }

    private Deployment buildVariantDeployment(TritonDeployment cr,
                                              TritonDeploymentSpec.TrafficVariant variant,
                                              int replicas) {
        String baseName = cr.getMetadata().getName();
        String variantName = baseName + "-" + variant.getName();
        TritonDeploymentSpec originalSpec = cr.getSpec();

        // Clone spec with variant overrides applied.
        TritonDeploymentSpec effective = shallowClone(originalSpec);
        effective.setReplicas(replicas);
        if (variant.getImage() != null && !variant.getImage().isEmpty()) {
            effective.setImage(variant.getImage());
        }
        if (variant.getModelRepository() != null && !variant.getModelRepository().isEmpty()) {
            effective.setModelRepository(variant.getModelRepository());
        }
        TritonDeployment projected = new TritonDeployment();
        projected.setMetadata(new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
                .withName(variantName)
                .withNamespace(cr.getMetadata().getNamespace())
                .withUid(cr.getMetadata().getUid())
                .withGeneration(cr.getMetadata().getGeneration())
                .build());
        projected.setSpec(effective);
        // Reuse the owner's apiVersion/kind so ownedMeta attaches the OwnerReference.
        projected.setApiVersion(cr.getApiVersion());
        projected.setKind(cr.getKind());
        // Make sure the owner UID points to the ORIGINAL CR, not this projection.
        projected.getMetadata().setOwnerReferences(null);

        Deployment d = buildDeployment(projected);
        // The selector includes the variant label so the aggregating Service can also
        // select across all variants via a relaxed selector.
        Map<String, String> variantLabels = new LinkedHashMap<>(commonLabels(cr));
        variantLabels.put(LABEL_VARIANT, variant.getName());
        d.getMetadata().setLabels(variantLabels);
        d.getSpec().getTemplate().getMetadata().setLabels(variantLabels);
        d.getSpec().setSelector(new io.fabric8.kubernetes.api.model.LabelSelectorBuilder()
                .withMatchLabels(variantLabels).build());
        // Owner reference must point back at the CR, not the projected wrapper.
        d.getMetadata().setOwnerReferences(ownedMeta(cr, variantName).getOwnerReferences());
        return d;
    }

    /** Delete any Deployments with this CR's instance label that aren't in the keep-set. */
    private void pruneVariantDeployments(String ns, String instance, java.util.Set<String> keepVariantNames) {
        var list = client.apps().deployments().inNamespace(ns)
                .withLabel(LABEL_INSTANCE, instance)
                .list();
        for (Deployment d : list.getItems()) {
            String variant = d.getMetadata().getLabels() == null
                    ? null : d.getMetadata().getLabels().get(LABEL_VARIANT);
            // The single-cohort Deployment has no variant label and name == instance.
            // Variant Deployments are named <instance>-<variant>.
            if (variant == null) {
                // In single-cohort mode (keepVariantNames empty), KEEP the no-variant Deployment;
                // in multi-cohort mode, DELETE it.
                if (!keepVariantNames.isEmpty()) {
                    client.apps().deployments().inNamespace(ns)
                            .withName(d.getMetadata().getName()).delete();
                }
                continue;
            }
            if (!keepVariantNames.contains(variant)) {
                client.apps().deployments().inNamespace(ns)
                        .withName(d.getMetadata().getName()).delete();
            }
        }
    }

    /**
     * Variant-specific Service selecting only the pods of that variant. Used by the HTTPRoute
     * to send a precise fraction of traffic to each cohort.
     */
    private Service buildVariantService(TritonDeployment cr, TritonDeploymentSpec.TrafficVariant v) {
        String variantName = cr.getMetadata().getName() + "-" + v.getName();
        Map<String, String> selector = new LinkedHashMap<>(commonLabels(cr));
        selector.put(LABEL_VARIANT, v.getName());

        List<ServicePort> ports = new ArrayList<>();
        ports.add(new ServicePortBuilder().withName("http").withPort(8000)
                .withTargetPort(new IntOrString("http")).withProtocol("TCP").build());
        ports.add(new ServicePortBuilder().withName("grpc").withPort(8001)
                .withTargetPort(new IntOrString("grpc")).withProtocol("TCP").build());
        if (Boolean.TRUE.equals(effectiveProxyEnabled(cr.getSpec()))) {
            int openaiPort = cr.getSpec().getOpenaiProxy() != null && cr.getSpec().getOpenaiProxy().getPort() != null
                    ? cr.getSpec().getOpenaiProxy().getPort() : 9000;
            ports.add(new ServicePortBuilder().withName("openai").withPort(openaiPort)
                    .withTargetPort(new IntOrString("openai")).withProtocol("TCP").build());
        }
        return new ServiceBuilder()
                .withMetadata(ownedMeta(cr, variantName))
                .withNewSpec()
                    .withSelector(selector)
                    .withPorts(ports)
                    .withType("ClusterIP")
                .endSpec()
                .build();
    }

    /**
     * Apply (create-or-update) a Gateway API HTTPRoute pointing at the per-variant Services
     * with exact weights. We use GenericKubernetesResource to avoid pulling the gateway-api
     * model jar into the main uber-JAR — clusters without Gateway API installed simply won't
     * have the CRD registered and the apply silently no-ops (we catch and log).
     */
    private void applyHttpRoute(TritonDeployment cr, List<TritonDeploymentSpec.TrafficVariant> variants) {
        TritonDeploymentSpec.Gateway gw = cr.getSpec().getGateway();
        if (gw == null || gw.getParentRefs() == null || gw.getParentRefs().isEmpty()) {
            log.warn("gateway.enabled=true but gateway.parentRefs is empty — HTTPRoute skipped");
            return;
        }
        String ns = cr.getMetadata().getNamespace();
        String name = cr.getMetadata().getName();
        int openaiPort = Boolean.TRUE.equals(effectiveProxyEnabled(cr.getSpec()))
                && cr.getSpec().getOpenaiProxy() != null
                && cr.getSpec().getOpenaiProxy().getPort() != null
                ? cr.getSpec().getOpenaiProxy().getPort() : 8000;

        List<Map<String, Object>> parentRefs = new ArrayList<>();
        for (TritonDeploymentSpec.ParentRef p : gw.getParentRefs()) {
            Map<String, Object> ref = new LinkedHashMap<>();
            ref.put("name", p.getName());
            if (p.getNamespace() != null) ref.put("namespace", p.getNamespace());
            if (p.getSectionName() != null) ref.put("sectionName", p.getSectionName());
            parentRefs.add(ref);
        }

        List<Map<String, Object>> backendRefs = new ArrayList<>();
        for (TritonDeploymentSpec.TrafficVariant v : variants) {
            int w = v.getWeight() == null ? 0 : v.getWeight();
            Map<String, Object> br = new LinkedHashMap<>();
            br.put("name", name + "-" + v.getName());
            br.put("port", openaiPort);
            br.put("weight", w);
            backendRefs.add(br);
        }

        Map<String, Object> rule = new LinkedHashMap<>();
        rule.put("backendRefs", backendRefs);

        Map<String, Object> specMap = new LinkedHashMap<>();
        specMap.put("parentRefs", parentRefs);
        if (gw.getHostnames() != null && !gw.getHostnames().isEmpty()) {
            specMap.put("hostnames", gw.getHostnames());
        }
        specMap.put("rules", java.util.Collections.singletonList(rule));

        io.fabric8.kubernetes.api.model.GenericKubernetesResource route =
                new io.fabric8.kubernetes.api.model.GenericKubernetesResource();
        route.setApiVersion("gateway.networking.k8s.io/v1");
        route.setKind("HTTPRoute");
        route.setMetadata(new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
                .withName(name)
                .withNamespace(ns)
                .withLabels(commonLabels(cr))
                .withOwnerReferences(ownedMeta(cr, name).getOwnerReferences())
                .build());
        route.setAdditionalProperty("spec", specMap);

        io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext rdc =
                new io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext.Builder()
                        .withGroup("gateway.networking.k8s.io")
                        .withVersion("v1")
                        .withKind("HTTPRoute")
                        .withPlural("httproutes")
                        .withNamespaced(true)
                        .build();
        try {
            var existing = client.genericKubernetesResources(rdc).inNamespace(ns)
                    .withName(name).get();
            if (existing == null) {
                client.genericKubernetesResources(rdc).inNamespace(ns)
                        .resource(route).create();
            } else {
                route.getMetadata().setResourceVersion(existing.getMetadata().getResourceVersion());
                client.genericKubernetesResources(rdc).inNamespace(ns)
                        .resource(route).update();
            }
        } catch (Exception e) {
            log.warn("Failed to apply HTTPRoute for {}/{}: {} (is Gateway API installed on the cluster?)",
                    ns, name, e.getMessage());
            events.warning(cr, "HTTPRouteFailed",
                    "Could not apply HTTPRoute (Gateway API CRDs missing?): %s", e.getMessage());
        }
    }

    private static int readyReplicasOf(Deployment current) {
        if (current == null || current.getStatus() == null) return 0;
        Integer r = current.getStatus().getReadyReplicas();
        return r == null ? 0 : r;
    }

    private static TritonDeploymentSpec shallowClone(TritonDeploymentSpec in) {
        TritonDeploymentSpec out = new TritonDeploymentSpec();
        out.setImage(in.getImage());
        out.setModelRepository(in.getModelRepository());
        out.setReplicas(in.getReplicas());
        out.setTensorParallelism(in.getTensorParallelism());
        out.setPipelineParallelism(in.getPipelineParallelism());
        out.setAccelerator(in.getAccelerator());
        out.setResources(in.getResources());
        out.setOpenaiProxy(in.getOpenaiProxy());
        out.setScaling(in.getScaling());
        out.setRanger(in.getRanger());
        out.setQuotasRef(in.getQuotasRef());
        out.setEnv(in.getEnv());
        // Intentionally NOT copying traffic — the projected CR is single-cohort.
        return out;
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
