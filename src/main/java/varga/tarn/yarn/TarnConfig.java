package varga.tarn.yarn;

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
    public Map<String, String> customEnv = new HashMap<>();
    
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
        options.addOption("j", "jar", true, "Path to the application JAR (local, will be uploaded to HDFS)");
        
        Option envOption = new Option("e", "env", true, "Custom environment variables (KEY=VALUE)");
        envOption.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(envOption);
        
        options.addOption("su", "scale-up", true, "Scale up threshold (0.0-1.0)");
        options.addOption("sd", "scale-down", true, "Scale down threshold (0.0-1.0)");
        options.addOption("min", "min-instances", true, "Minimum number of instances");
        options.addOption("max", "max-instances", true, "Maximum number of instances");
        options.addOption("c", "cooldown", true, "Scale cooldown in ms");
        return options;
    }
}
