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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * Declarative spec for {@link TritonDeployment}. Fields intentionally mirror the CRD schema;
 * absent fields fall back to the CRD's {@code default} or to reconciler constants.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TritonDeploymentSpec {

    private String image;
    private String modelRepository;
    private Integer replicas;
    private Integer tensorParallelism;
    private Integer pipelineParallelism;
    private Accelerator accelerator;
    private Resources resources;
    private OpenaiProxy openaiProxy;
    private Scaling scaling;
    private Ranger ranger;
    private QuotasRef quotasRef;
    private List<EnvVar> env;
    private List<TrafficVariant> traffic;
    private Gateway gateway;

    // --- Getters / setters (plain POJO, no Lombok here — fabric8 serialization is strict) ---

    public String getImage() { return image; }
    public void setImage(String image) { this.image = image; }
    public String getModelRepository() { return modelRepository; }
    public void setModelRepository(String modelRepository) { this.modelRepository = modelRepository; }
    public Integer getReplicas() { return replicas; }
    public void setReplicas(Integer replicas) { this.replicas = replicas; }
    public Integer getTensorParallelism() { return tensorParallelism; }
    public void setTensorParallelism(Integer tp) { this.tensorParallelism = tp; }
    public Integer getPipelineParallelism() { return pipelineParallelism; }
    public void setPipelineParallelism(Integer pp) { this.pipelineParallelism = pp; }
    public Accelerator getAccelerator() { return accelerator; }
    public void setAccelerator(Accelerator a) { this.accelerator = a; }
    public Resources getResources() { return resources; }
    public void setResources(Resources r) { this.resources = r; }
    public OpenaiProxy getOpenaiProxy() { return openaiProxy; }
    public void setOpenaiProxy(OpenaiProxy op) { this.openaiProxy = op; }
    public Scaling getScaling() { return scaling; }
    public void setScaling(Scaling s) { this.scaling = s; }
    public Ranger getRanger() { return ranger; }
    public void setRanger(Ranger r) { this.ranger = r; }
    public QuotasRef getQuotasRef() { return quotasRef; }
    public void setQuotasRef(QuotasRef q) { this.quotasRef = q; }
    public List<EnvVar> getEnv() { return env; }
    public void setEnv(List<EnvVar> env) { this.env = env; }
    public List<TrafficVariant> getTraffic() { return traffic; }
    public void setTraffic(List<TrafficVariant> traffic) { this.traffic = traffic; }
    public Gateway getGateway() { return gateway; }
    public void setGateway(Gateway gateway) { this.gateway = gateway; }

    // --- Effective getters with defaults applied — used by the reconciler ---

    public int effectiveReplicas() {
        return replicas == null ? 1 : replicas;
    }

    public int effectiveTensorParallelism() {
        return tensorParallelism == null ? 1 : tensorParallelism;
    }

    public int effectivePipelineParallelism() {
        return pipelineParallelism == null ? 1 : pipelineParallelism;
    }

    public Accelerator effectiveAccelerator() {
        return accelerator == null ? new Accelerator() : accelerator;
    }

    // --- Nested types — each mirrors a sub-object in the CRD schema ---

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Accelerator {
        private String type = "nvidia_gpu";
        private Integer count = 1;
        private String sliceSize;
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public Integer getCount() { return count; }
        public void setCount(Integer c) { this.count = c; }
        public String getSliceSize() { return sliceSize; }
        public void setSliceSize(String s) { this.sliceSize = s; }

        /** K8s resource name for the configured accelerator type. Null means CPU-only. */
        public String kubernetesResourceName() {
            if (type == null) return "nvidia.com/gpu";
            switch (type) {
                case "amd_gpu": return "amd.com/gpu";
                case "intel_gaudi": return "habana.ai/gaudi";
                case "aws_neuron": return "aws.amazon.com/neuron";
                case "cpu_only": return null;
                case "nvidia_gpu":
                default: return "nvidia.com/gpu";
            }
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Resources {
        private String memory = "4Gi";
        private String cpu = "2";
        public String getMemory() { return memory; }
        public void setMemory(String m) { this.memory = m; }
        public String getCpu() { return cpu; }
        public void setCpu(String c) { this.cpu = c; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OpenaiProxy {
        private Boolean enabled = false;
        private Integer port = 9000;
        public Boolean getEnabled() { return enabled; }
        public void setEnabled(Boolean b) { this.enabled = b; }
        public Integer getPort() { return port; }
        public void setPort(Integer p) { this.port = p; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Scaling {
        private String mode = "composite";
        private Integer minReplicas = 1;
        private Integer maxReplicas = 10;
        private Double upThreshold = 0.7;
        private Double downThreshold = 0.2;
        public String getMode() { return mode; }
        public void setMode(String m) { this.mode = m; }
        public Integer getMinReplicas() { return minReplicas; }
        public void setMinReplicas(Integer n) { this.minReplicas = n; }
        public Integer getMaxReplicas() { return maxReplicas; }
        public void setMaxReplicas(Integer n) { this.maxReplicas = n; }
        public Double getUpThreshold() { return upThreshold; }
        public void setUpThreshold(Double v) { this.upThreshold = v; }
        public Double getDownThreshold() { return downThreshold; }
        public void setDownThreshold(Double v) { this.downThreshold = v; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Ranger {
        private String serviceName;
        private Boolean strict = true;
        public String getServiceName() { return serviceName; }
        public void setServiceName(String s) { this.serviceName = s; }
        public Boolean getStrict() { return strict; }
        public void setStrict(Boolean b) { this.strict = b; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class QuotasRef {
        private String configMapName;
        private String key = "quotas.json";
        public String getConfigMapName() { return configMapName; }
        public void setConfigMapName(String n) { this.configMapName = n; }
        public String getKey() { return key; }
        public void setKey(String k) { this.key = k; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class EnvVar {
        private String name;
        private String value;
        public EnvVar() {}
        public EnvVar(String name, String value) { this.name = name; this.value = value; }
        public String getName() { return name; }
        public void setName(String n) { this.name = n; }
        public String getValue() { return value; }
        public void setValue(String v) { this.value = v; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Gateway {
        private Boolean enabled = false;
        private List<ParentRef> parentRefs;
        private List<String> hostnames;
        public Boolean getEnabled() { return enabled; }
        public void setEnabled(Boolean b) { this.enabled = b; }
        public List<ParentRef> getParentRefs() { return parentRefs; }
        public void setParentRefs(List<ParentRef> p) { this.parentRefs = p; }
        public List<String> getHostnames() { return hostnames; }
        public void setHostnames(List<String> h) { this.hostnames = h; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ParentRef {
        private String name;
        private String namespace;
        private String sectionName;
        public String getName() { return name; }
        public void setName(String n) { this.name = n; }
        public String getNamespace() { return namespace; }
        public void setNamespace(String n) { this.namespace = n; }
        public String getSectionName() { return sectionName; }
        public void setSectionName(String s) { this.sectionName = s; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TrafficVariant {
        private String name;
        private Integer weight;
        private String modelRepository;
        private String image;
        public TrafficVariant() {}
        public TrafficVariant(String name, int weight) { this.name = name; this.weight = weight; }
        public String getName() { return name; }
        public void setName(String n) { this.name = n; }
        public Integer getWeight() { return weight; }
        public void setWeight(Integer w) { this.weight = w; }
        public String getModelRepository() { return modelRepository; }
        public void setModelRepository(String m) { this.modelRepository = m; }
        public String getImage() { return image; }
        public void setImage(String i) { this.image = i; }
    }

    // Unused but kept to satisfy compilers that want Map<String,String> coercion in tests.
    @SuppressWarnings("unused")
    private Map<String, String> __compat;
}
