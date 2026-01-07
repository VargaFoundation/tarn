package varga.tarn.yarn;

import org.apache.commons.cli.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.util.Map;

public class TarnConfig {
    public String modelRepositoryHdfs;
    public String tritonImage;
    public int tritonPort;
    public int metricsPort;
    public int amPort;
    public String bindAddress;
    public String apiToken;
    public int containerMemory;
    public int containerVCores;

    public TarnConfig() {
        // Defaults from environment or static defaults
        tritonImage = getEnv("TRITON_IMAGE", "nvcr.io/nvidia/tritonserver:24.09-py3");
        modelRepositoryHdfs = getEnv("MODEL_REPOSITORY_HDFS", "hdfs:///models");
        tritonPort = Integer.parseInt(getEnv("TRITON_PORT", "8000"));
        metricsPort = Integer.parseInt(getEnv("METRICS_PORT", "8002"));
        amPort = Integer.parseInt(getEnv("AM_PORT", "8888"));
        bindAddress = getEnv("BIND_ADDRESS", "0.0.0.0");
        apiToken = getEnv("TARN_TOKEN", "");
        containerMemory = Integer.parseInt(getEnv("CONTAINER_MEMORY", "4096"));
        containerVCores = Integer.parseInt(getEnv("CONTAINER_VCORES", "2"));
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
        if (line.hasOption("metrics-port")) metricsPort = Integer.parseInt(line.getOptionValue("metrics-port"));
        if (line.hasOption("am-port")) amPort = Integer.parseInt(line.getOptionValue("am-port"));
        if (line.hasOption("address")) bindAddress = line.getOptionValue("address");
        if (line.hasOption("token")) apiToken = line.getOptionValue("token");
    }

    public static Options getOptions() {
        Options options = new Options();
        options.addOption("m", "model-repository", true, "HDFS path to model repository");
        options.addOption("i", "image", true, "Triton Docker image");
        options.addOption("p", "port", true, "Triton HTTP port");
        options.addOption("mp", "metrics-port", true, "Triton metrics port");
        options.addOption("ap", "am-port", true, "AM HTTP port");
        options.addOption("a", "address", true, "Bind address");
        options.addOption("t", "token", true, "Security token for API");
        return options;
    }
}
