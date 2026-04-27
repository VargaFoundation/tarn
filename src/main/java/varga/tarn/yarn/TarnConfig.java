package varga.tarn.yarn;

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


import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;

public class TarnConfig {
    public String modelRepository;
    public String tritonImage;
    public int tritonPort;
    public int grpcPort;
    public int metricsPort;
    public int amPort;
    public String bindAddress;
    public String apiToken;
    public int containerMemory;
    public int containerVCores;
    public int tensorParallelism;
    public int pipelineParallelism;
    public String secretsPath;
    public String placementTag;
    public String dockerNetwork;
    public boolean dockerPrivileged;
    public boolean dockerDelayedRemoval;
    public String dockerMounts;
    public String dockerPorts;
    public String jarPath;
    public String zkEnsemble;
    public String zkPath;
    public String rangerService;
    public String rangerAppId;
    public boolean rangerAudit;
    public boolean rangerStrict;
    public boolean zkRequired;
    // Local path to a JAAS config file (e.g. for SASL/Kerberos ZK auth). The Client uploads
    // it to HDFS as a LocalResource and sets JAVA_TOOL_OPTIONS on the AM container so the
    // JVM picks it up automatically. Null = no JAAS staging.
    public String zkJaasPath;
    public long drainTimeoutMs;
    public long monitorIntervalMs;
    // TLS for the AM HTTP server. When enabled, --tls-keystore must be set.
    public boolean tlsEnabled;
    public String tlsKeystorePath;
    public String tlsKeystorePasswordAlias;
    public String tlsKeystoreType;
    // OpenAI proxy configuration — zero impact when unset.
    public boolean openaiProxyEnabled;
    public int openaiProxyPort;
    // Observability.
    public String otelEndpoint;
    // Scaling strategy: gpu_util | queue_depth | composite. Composite is safe default for
    // mixed workloads where GPU% and queue depth each matter in different regimes.
    public String scaleMode;
    // Number of pending requests per container treated as "full" for queue-normalized load.
    // Should match (or slightly under-run) the backend batching width.
    public int queueCapacityPerContainer;
    // Warmup window after container start during which ZK registration is withheld.
    public long warmupTimeoutMs;
    public long warmupPollIntervalMs;
    // Quota rules source: HDFS path to a JSON file (see QuotaEnforcer for format). Null disables.
    public String quotasPath;
    // Accelerator hardware type to request from YARN. NVIDIA_GPU | AMD_GPU | INTEL_GAUDI | AWS_NEURON | CPU_ONLY.
    public String acceleratorType;
    // Fractional slice size when using MIG-partitioned GPUs (e.g. "0.5" for a half-GPU slice).
    public String gpuSliceSize;
    // Shadow traffic mirror: when set, a fraction of inference requests is copied (fire-and-forget)
    // to this host:port. Responses are discarded; only latency/error metrics are recorded.
    public String shadowEndpoint;
    public double shadowSampleRate;
    public Map<String, String> customEnv = new HashMap<>();

    // Client daemon port (health endpoint when running as foreground monitor)
    public int clientPort;

    // Scaling properties
    public double scaleUpThreshold;
    public double scaleDownThreshold;
    public int minContainers;
    public int maxContainers;
    public long scaleCooldownMs;

    public TarnConfig() {
        // Defaults from environment or static defaults
        tritonImage = getEnv("TRITON_IMAGE", "nvcr.io/nvidia/tritonserver:24.09-py3");
        modelRepository = getEnv("MODEL_REPOSITORY", "hdfs:///models");
        tritonPort = Integer.parseInt(getEnv("TRITON_PORT", "8000"));
        grpcPort = Integer.parseInt(getEnv("GRPC_PORT", "8001"));
        metricsPort = Integer.parseInt(getEnv("METRICS_PORT", "8002"));
        amPort = Integer.parseInt(getEnv("AM_PORT", "8888"));
        bindAddress = getEnv("BIND_ADDRESS", "0.0.0.0");
        apiToken = getEnv("TARN_TOKEN", "");
        containerMemory = Integer.parseInt(getEnv("CONTAINER_MEMORY", "4096"));
        containerVCores = Integer.parseInt(getEnv("CONTAINER_VCORES", "2"));
        tensorParallelism = Integer.parseInt(getEnv("TENSOR_PARALLELISM", "1"));
        pipelineParallelism = Integer.parseInt(getEnv("PIPELINE_PARALLELISM", "1"));
        secretsPath = getEnv("SECRETS_PATH", null);
        placementTag = getEnv("PLACEMENT_TAG", "nvidia");
        dockerNetwork = getEnv("DOCKER_NETWORK", "host");
        dockerPrivileged = Boolean.parseBoolean(getEnv("DOCKER_PRIVILEGED", "false"));
        dockerDelayedRemoval = Boolean.parseBoolean(getEnv("DOCKER_DELAYED_REMOVAL", "false"));
        dockerMounts = getEnv("DOCKER_MOUNTS", null);
        dockerPorts = getEnv("DOCKER_PORTS", null);
        zkEnsemble = getEnv("ZK_ENSEMBLE", null);
        zkPath = getEnv("ZK_PATH", "/services/triton/instances");
        rangerService = getEnv("RANGER_SERVICE", null);
        rangerAppId = getEnv("RANGER_APP_ID", "tarn");
        rangerAudit = Boolean.parseBoolean(getEnv("RANGER_AUDIT", "true"));
        // Default to strict mode when Ranger is configured: fail-closed if plugin init fails.
        rangerStrict = Boolean.parseBoolean(getEnv("RANGER_STRICT", rangerService != null ? "true" : "false"));
        // Default to required-ZK when an ensemble is configured.
        zkRequired = Boolean.parseBoolean(getEnv("ZK_REQUIRED", zkEnsemble != null ? "true" : "false"));
        zkJaasPath = getEnv("ZK_JAAS", null);
        drainTimeoutMs = Long.parseLong(getEnv("DRAIN_TIMEOUT_MS", "30000"));
        monitorIntervalMs = Long.parseLong(getEnv("MONITOR_INTERVAL_MS", "15000"));
        tlsEnabled = Boolean.parseBoolean(getEnv("TLS_ENABLED", "false"));
        tlsKeystorePath = getEnv("TLS_KEYSTORE_PATH", null);
        tlsKeystorePasswordAlias = getEnv("TLS_KEYSTORE_PASSWORD_ALIAS", "tarn.tls.keystore.password");
        tlsKeystoreType = getEnv("TLS_KEYSTORE_TYPE", "JKS");
        openaiProxyEnabled = Boolean.parseBoolean(getEnv("OPENAI_PROXY_ENABLED", "false"));
        openaiProxyPort = Integer.parseInt(getEnv("OPENAI_PROXY_PORT", "9000"));
        otelEndpoint = getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", null);
        scaleMode = getEnv("SCALE_MODE", "composite");
        queueCapacityPerContainer = Integer.parseInt(getEnv("QUEUE_CAPACITY_PER_CONTAINER", "16"));
        // Warmup: how long to wait post-start for Triton to load all models and answer /v2/health/ready.
        // ZK registration is delayed until this is satisfied so Knox never routes to cold backends.
        warmupTimeoutMs = Long.parseLong(getEnv("WARMUP_TIMEOUT_MS", "120000"));
        warmupPollIntervalMs = Long.parseLong(getEnv("WARMUP_POLL_INTERVAL_MS", "2000"));
        quotasPath = getEnv("QUOTAS_PATH", null);
        acceleratorType = getEnv("ACCELERATOR_TYPE", "NVIDIA_GPU");
        gpuSliceSize = getEnv("GPU_SLICE_SIZE", null);
        shadowEndpoint = getEnv("SHADOW_ENDPOINT", null);
        shadowSampleRate = Double.parseDouble(getEnv("SHADOW_SAMPLE_RATE", "0.0"));
        clientPort = Integer.parseInt(getEnv("CLIENT_PORT", "8889"));

        scaleUpThreshold = Double.parseDouble(getEnv("SCALE_UP_THRESHOLD", "0.7"));
        scaleDownThreshold = Double.parseDouble(getEnv("SCALE_DOWN_THRESHOLD", "0.2"));
        minContainers = Integer.parseInt(getEnv("MIN_CONTAINERS", "1"));
        maxContainers = Integer.parseInt(getEnv("MAX_CONTAINERS", "10"));
        scaleCooldownMs = Long.parseLong(getEnv("SCALE_COOLDOWN_MS", "60000"));
    }

    private String getEnv(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null) ? val : defaultValue;
    }

    public void parseArgs(String[] args) throws ParseException {
        Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine line = parser.parse(options, args);

        if (line.hasOption("model-repository")) modelRepository = line.getOptionValue("model-repository");
        if (line.hasOption("image")) tritonImage = line.getOptionValue("image");
        if (line.hasOption("port")) tritonPort = Integer.parseInt(line.getOptionValue("port"));
        if (line.hasOption("grpc-port")) grpcPort = Integer.parseInt(line.getOptionValue("grpc-port"));
        if (line.hasOption("metrics-port")) metricsPort = Integer.parseInt(line.getOptionValue("metrics-port"));
        if (line.hasOption("am-port")) amPort = Integer.parseInt(line.getOptionValue("am-port"));
        if (line.hasOption("address")) bindAddress = line.getOptionValue("address");
        if (line.hasOption("token")) apiToken = line.getOptionValue("token");
        if (line.hasOption("tp")) tensorParallelism = Integer.parseInt(line.getOptionValue("tp"));
        if (line.hasOption("pp")) pipelineParallelism = Integer.parseInt(line.getOptionValue("pp"));
        if (line.hasOption("secrets")) secretsPath = line.getOptionValue("secrets");
        if (line.hasOption("placement-tag")) placementTag = line.getOptionValue("placement-tag");
        if (line.hasOption("docker-network")) dockerNetwork = line.getOptionValue("docker-network");
        if (line.hasOption("docker-privileged")) dockerPrivileged = true;
        if (line.hasOption("docker-delayed-removal")) dockerDelayedRemoval = true;
        if (line.hasOption("docker-mounts")) dockerMounts = line.getOptionValue("docker-mounts");
        if (line.hasOption("docker-ports")) dockerPorts = line.getOptionValue("docker-ports");
        if (line.hasOption("zk-ensemble")) zkEnsemble = line.getOptionValue("zk-ensemble");
        if (line.hasOption("zk-path")) zkPath = line.getOptionValue("zk-path");
        if (line.hasOption("ranger-service")) rangerService = line.getOptionValue("ranger-service");
        if (line.hasOption("ranger-app-id")) rangerAppId = line.getOptionValue("ranger-app-id");
        if (line.hasOption("ranger-audit")) rangerAudit = true;
        if (line.hasOption("jar")) jarPath = line.getOptionValue("jar");
        if (line.hasOption("env")) {
            String[] envs = line.getOptionValues("env");
            for (String env : envs) {
                int index = env.indexOf('=');
                if (index > 0) {
                    customEnv.put(env.substring(0, index), env.substring(index + 1));
                }
            }
        }

        if (line.hasOption("scale-up")) scaleUpThreshold = Double.parseDouble(line.getOptionValue("scale-up"));
        if (line.hasOption("scale-down")) scaleDownThreshold = Double.parseDouble(line.getOptionValue("scale-down"));
        if (line.hasOption("min-instances")) minContainers = Integer.parseInt(line.getOptionValue("min-instances"));
        if (line.hasOption("max-instances")) maxContainers = Integer.parseInt(line.getOptionValue("max-instances"));
        if (line.hasOption("cooldown")) scaleCooldownMs = Long.parseLong(line.getOptionValue("cooldown"));
        if (line.hasOption("client-port")) clientPort = Integer.parseInt(line.getOptionValue("client-port"));
        if (line.hasOption("ranger-strict")) rangerStrict = true;
        if (line.hasOption("zk-required")) zkRequired = true;
        if (line.hasOption("zk-jaas")) zkJaasPath = line.getOptionValue("zk-jaas");
        if (line.hasOption("drain-timeout-ms")) drainTimeoutMs = Long.parseLong(line.getOptionValue("drain-timeout-ms"));
        if (line.hasOption("monitor-interval-ms")) monitorIntervalMs = Long.parseLong(line.getOptionValue("monitor-interval-ms"));
        if (line.hasOption("tls-enabled")) tlsEnabled = true;
        if (line.hasOption("tls-keystore")) tlsKeystorePath = line.getOptionValue("tls-keystore");
        if (line.hasOption("tls-keystore-type")) tlsKeystoreType = line.getOptionValue("tls-keystore-type");
        if (line.hasOption("tls-keystore-password-alias")) tlsKeystorePasswordAlias = line.getOptionValue("tls-keystore-password-alias");
        if (line.hasOption("openai-proxy-enabled")) openaiProxyEnabled = true;
        if (line.hasOption("openai-proxy-port")) openaiProxyPort = Integer.parseInt(line.getOptionValue("openai-proxy-port"));
        if (line.hasOption("otel-endpoint")) otelEndpoint = line.getOptionValue("otel-endpoint");
        if (line.hasOption("scale-mode")) scaleMode = line.getOptionValue("scale-mode");
        if (line.hasOption("queue-capacity-per-container")) queueCapacityPerContainer = Integer.parseInt(line.getOptionValue("queue-capacity-per-container"));
        if (line.hasOption("warmup-timeout-ms")) warmupTimeoutMs = Long.parseLong(line.getOptionValue("warmup-timeout-ms"));
        if (line.hasOption("warmup-poll-interval-ms")) warmupPollIntervalMs = Long.parseLong(line.getOptionValue("warmup-poll-interval-ms"));
        if (line.hasOption("quotas")) quotasPath = line.getOptionValue("quotas");
        if (line.hasOption("accelerator-type")) acceleratorType = line.getOptionValue("accelerator-type");
        if (line.hasOption("gpu-slice-size")) gpuSliceSize = line.getOptionValue("gpu-slice-size");
        if (line.hasOption("shadow-endpoint")) shadowEndpoint = line.getOptionValue("shadow-endpoint");
        if (line.hasOption("shadow-sample-rate")) shadowSampleRate = Double.parseDouble(line.getOptionValue("shadow-sample-rate"));

        validate();
    }

    /**
     * Fail-fast validation of user-supplied paths and numerics. Run after parseArgs to reject
     * unsafe inputs before any command construction or YARN submission.
     */
    public void validate() {
        TritonCommandBuilder.requireSafePath("model-repository", modelRepository);
        TritonCommandBuilder.requireSafePath("secrets", secretsPath);
        if (tritonPort <= 0 || tritonPort > 65535) throw new IllegalArgumentException("tritonPort out of range");
        if (grpcPort <= 0 || grpcPort > 65535) throw new IllegalArgumentException("grpcPort out of range");
        if (metricsPort <= 0 || metricsPort > 65535) throw new IllegalArgumentException("metricsPort out of range");
        if (amPort <= 0 || amPort > 65535) throw new IllegalArgumentException("amPort out of range");
        if (minContainers < 0) throw new IllegalArgumentException("minContainers < 0");
        if (maxContainers < minContainers) throw new IllegalArgumentException("maxContainers < minContainers");
        if (scaleUpThreshold <= 0 || scaleUpThreshold > 1.0) throw new IllegalArgumentException("scaleUpThreshold must be in (0, 1]");
        if (scaleDownThreshold < 0 || scaleDownThreshold >= scaleUpThreshold) throw new IllegalArgumentException("scaleDownThreshold must be in [0, scaleUpThreshold)");
        if (tlsEnabled && (tlsKeystorePath == null || tlsKeystorePath.isEmpty())) {
            throw new IllegalArgumentException("--tls-enabled requires --tls-keystore");
        }
        if (openaiProxyEnabled && (openaiProxyPort <= 0 || openaiProxyPort > 65535)) {
            throw new IllegalArgumentException("openaiProxyPort out of range");
        }
        if (openaiProxyEnabled && openaiProxyPort == amPort) {
            throw new IllegalArgumentException("openaiProxyPort must differ from amPort");
        }
        if (gpuSliceSize != null && !gpuSliceSize.isEmpty()) {
            // Accept NVIDIA MIG profiles (e.g. 1g.10gb, 2g.20gb, 3g.40gb, 7g.80gb) OR a decimal
            // fraction for MPS/time-sharing (e.g. 0.5). Anything else is a typo and would
            // trigger an obscure YARN scheduler error at allocation time.
            if (!gpuSliceSize.matches("^(\\d+g\\.\\d+gb|0?\\.\\d+|1\\.0)$")) {
                throw new IllegalArgumentException(
                        "Invalid --gpu-slice-size: expected MIG profile like '1g.10gb' or fraction like '0.5', got '" + gpuSliceSize + "'");
            }
        }
        if (shadowSampleRate < 0.0 || shadowSampleRate > 1.0) {
            throw new IllegalArgumentException("shadow-sample-rate must be in [0, 1]");
        }
        if (shadowSampleRate > 0.0 && (shadowEndpoint == null || shadowEndpoint.isEmpty())) {
            throw new IllegalArgumentException("shadow-sample-rate > 0 requires --shadow-endpoint");
        }
    }

    public static Options getOptions() {
        Options options = new Options();
        options.addOption("m", "model-repository", true, "Path to model repository (hdfs:///... or /...)");
        options.addOption("i", "image", true, "Triton Docker image");
        options.addOption("p", "port", true, "Triton HTTP port");
        options.addOption("gp", "grpc-port", true, "Triton GRPC port");
        options.addOption("mp", "metrics-port", true, "Triton metrics port");
        options.addOption("ap", "am-port", true, "AM HTTP port");
        options.addOption("a", "address", true, "Bind address");
        options.addOption("t", "token", true, "Security token for API");
        options.addOption("tp", "tensor-parallelism", true, "Tensor parallelism");
        options.addOption("pp", "pipeline-parallelism", true, "Pipeline parallelism");
        options.addOption("s", "secrets", true, "HDFS path to JKS/JCEKS secrets file");
        options.addOption("pt", "placement-tag", true, "Placement tag for anti-affinity (default: nvidia)");
        options.addOption("dn", "docker-network", true, "Docker network (default: host)");
        options.addOption("dp", "docker-privileged", false, "Run docker in privileged mode");
        options.addOption("ddr", "docker-delayed-removal", false, "Delayed removal of docker containers");
        options.addOption("dm", "docker-mounts", true, "Docker mounts (comma-separated)");
        options.addOption("dports", "docker-ports", true, "Docker port mapping (host_port:container_port,...)");
        options.addOption("ze", "zk-ensemble", true, "ZooKeeper ensemble (e.g. host1:2181,host2:2181)");
        options.addOption("zp", "zk-path", true, "ZooKeeper path for instance registration (default: /services/triton/instances)");
        options.addOption("rs", "ranger-service", true, "Apache Ranger service name");
        options.addOption("ra", "ranger-app-id", true, "Apache Ranger App ID (default: tarn)");
        options.addOption("raudit", "ranger-audit", false, "Enable Apache Ranger auditing");
        options.addOption(null, "ranger-strict", false, "Deny-by-default if Ranger plugin fails to initialize (recommended in regulated clusters)");
        options.addOption(null, "zk-required", false, "Fail the AM if ZooKeeper is unreachable (recommended when Knox depends on ZK discovery)");
        options.addOption(null, "zk-jaas", true, "Local path to a JAAS config for SASL/Kerberos ZooKeeper auth — uploaded to HDFS and set as -Djava.security.auth.login.config on the AM JVM");
        options.addOption(null, "drain-timeout-ms", true, "Max wait for in-flight inferences before stopping a container during scale-down (default 30000)");
        options.addOption(null, "monitor-interval-ms", true, "Interval between scaling evaluations in ms (default 15000)");
        options.addOption(null, "tls-enabled", false, "Serve AM endpoints over HTTPS (requires --tls-keystore)");
        options.addOption(null, "tls-keystore", true, "HDFS path or local path to the TLS keystore (JKS/PKCS12)");
        options.addOption(null, "tls-keystore-type", true, "Keystore type (JKS or PKCS12, default JKS)");
        options.addOption(null, "tls-keystore-password-alias", true, "JCEKS alias holding the keystore password (default tarn.tls.keystore.password)");
        options.addOption(null, "openai-proxy-enabled", false, "Enable OpenAI-compatible /v1 proxy endpoints (requires a Triton openai_frontend container)");
        options.addOption(null, "openai-proxy-port", true, "Port for the OpenAI proxy (default 9000)");
        options.addOption(null, "otel-endpoint", true, "OTLP gRPC endpoint for trace export (e.g. http://collector:4317)");
        options.addOption(null, "scale-mode", true, "Scaling signal: gpu_util | queue_depth | composite (default composite)");
        options.addOption(null, "queue-capacity-per-container", true, "Pending requests per container treated as 'full' for queue-normalized load (default 16)");
        options.addOption(null, "warmup-timeout-ms", true, "Max time to wait for a container to become warm before registering in ZK (default 120000)");
        options.addOption(null, "warmup-poll-interval-ms", true, "Warmup readiness poll interval in ms (default 2000)");
        options.addOption(null, "quotas", true, "HDFS/local path to quotas JSON file (see QuotaEnforcer for format)");
        options.addOption(null, "accelerator-type", true, "Accelerator: nvidia_gpu (default) | amd_gpu | intel_gaudi | aws_neuron | cpu_only");
        options.addOption(null, "gpu-slice-size", true, "Fractional GPU slice when MIG is enabled (e.g. '0.5' or '1g.10gb')");
        options.addOption(null, "shadow-endpoint", true, "Shadow backend URL for A/B offline comparison (e.g. http://triton-v2:8000). Responses discarded.");
        options.addOption(null, "shadow-sample-rate", true, "Fraction of inferences mirrored to the shadow endpoint (0.0 - 1.0)");
        options.addOption("j", "jar", true, "Path to the application JAR (local, will be uploaded to HDFS)");

        Option envOption = new Option("e", "env", true, "Custom environment variables (KEY=VALUE)");
        envOption.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(envOption);

        options.addOption("su", "scale-up", true, "Scale up threshold (0.0-1.0)");
        options.addOption("sd", "scale-down", true, "Scale down threshold (0.0-1.0)");
        options.addOption("min", "min-instances", true, "Minimum number of instances");
        options.addOption("max", "max-instances", true, "Maximum number of instances");
        options.addOption("c", "cooldown", true, "Scale cooldown in ms");
        options.addOption("cp", "client-port", true, "Client health endpoint port (default: 8889)");
        return options;
    }
}
