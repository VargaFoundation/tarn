package varga.tarn.yarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.util.Map;

public class TarnConfig {
    public String modelRepositoryHdfs;
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

    public TarnConfig() {
        // Defaults from environment or static defaults
        tritonImage = getEnv("TRITON_IMAGE", "nvcr.io/nvidia/tritonserver:24.09-py3");
        modelRepositoryHdfs = getEnv("MODEL_REPOSITORY_HDFS", "hdfs:///models");
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
    }

    private String getEnv(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null) ? val : defaultValue;
    }

    public void parseArgs(String[] args) throws ParseException {
        Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        CommandLine line = parser.parse(options, args);

        if (line.hasOption("model-repository")) modelRepositoryHdfs = line.getOptionValue("model-repository");
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
    }

    public static Options getOptions() {
        Options options = new Options();
        options.addOption("m", "model-repository", true, "HDFS path to model repository");
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
        return options;
    }
}
