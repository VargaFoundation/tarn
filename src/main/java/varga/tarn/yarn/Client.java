package varga.tarn.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * YARN Client for submitting the Triton application.
 */
public class Client {

    private static final Logger log = LoggerFactory.getLogger(Client.class);

    private Configuration conf;
    private YarnClient yarnClient;

    public Client() {
        conf = new YarnConfiguration();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
    }

    public void run(String[] args) throws Exception {
        TarnConfig config = new TarnConfig();
        try {
            config.parseArgs(args);
        } catch (ParseException exp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("yarn jar tarn.jar varga.tarn.yarn.Client", TarnConfig.getOptions());
            throw exp;
        }

        String modelHdfsPath = config.modelRepositoryHdfs;
        String tritonImage = config.tritonImage;
        String tritonPort = String.valueOf(config.tritonPort);
        String metricsPort = String.valueOf(config.metricsPort);
        String amPort = String.valueOf(config.amPort);
        String bindAddress = config.bindAddress;
        String token = config.apiToken;
        String secretsPath = config.secretsPath;

        yarnClient.start();
        log.info("YARN Client started");

        YarnClientApplication app = yarnClient.createApplication();
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();

        appContext.setApplicationName("Triton-on-YARN");
        appContext.setApplicationTags(Collections.singleton("TARN"));

        // Resource requirements for the ApplicationMaster
        Resource resource = Records.newRecord(Resource.class);
        resource.setMemorySize(1024);
        resource.setVirtualCores(1);
        appContext.setResource(resource);

        // Define the AM Container
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // Environment variables for the AM
        Map<String, String> env = new HashMap<>();
        env.put("MODEL_REPOSITORY_HDFS", modelHdfsPath);
        env.put("TRITON_IMAGE", tritonImage);
        env.put("TRITON_PORT", tritonPort);
        env.put("METRICS_PORT", metricsPort);
        env.put("AM_PORT", amPort);
        env.put("BIND_ADDRESS", bindAddress);
        if (!token.isEmpty()) {
            env.put("TARN_TOKEN", token);
        }
        if (secretsPath != null) {
            env.put("SECRETS_PATH", secretsPath);
        }
        env.put("TENSOR_PARALLELISM", String.valueOf(config.tensorParallelism));
        env.put("PIPELINE_PARALLELISM", String.valueOf(config.pipelineParallelism));
        env.put("PLACEMENT_TAG", config.placementTag);
        env.put("DOCKER_NETWORK", config.dockerNetwork);
        env.put("DOCKER_PRIVILEGED", String.valueOf(config.dockerPrivileged));
        env.put("DOCKER_DELAYED_REMOVAL", String.valueOf(config.dockerDelayedRemoval));
        if (config.dockerMounts != null) {
            env.put("DOCKER_MOUNTS", config.dockerMounts);
        }
        if (config.dockerPorts != null) {
            env.put("DOCKER_PORTS", config.dockerPorts);
        }
        
        // Add classpath to environment
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$())
            .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        env.put("CLASSPATH", classPathEnv.toString());
        amContainer.setEnvironment(env);

        // Command to launch the ApplicationMaster
        List<String> commands = Collections.singletonList(
                ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java" +
                        " -Xmx512m" +
                        " varga.tarn.yarn.ApplicationMaster" +
                        " --model-repository " + modelHdfsPath +
                        " --image " + tritonImage +
                        " --port " + tritonPort +
                        " --metrics-port " + metricsPort +
                        " --am-port " + amPort +
                        " --address " + bindAddress +
                        (token.isEmpty() ? "" : " --token " + token) +
                        (secretsPath == null ? "" : " --secrets " + secretsPath) +
                        " --tp " + config.tensorParallelism +
                        " --pp " + config.pipelineParallelism +
                        " --placement-tag " + config.placementTag +
                        " --docker-network " + config.dockerNetwork +
                        (config.dockerPrivileged ? " --docker-privileged" : "") +
                        (config.dockerDelayedRemoval ? " --docker-delayed-removal" : "") +
                        (config.dockerMounts == null ? "" : " --docker-mounts " + config.dockerMounts) +
                        (config.dockerPorts == null ? "" : " --docker-ports " + config.dockerPorts) +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr"
        );
        amContainer.setCommands(commands);

        // In a production environment, the application JAR would be uploaded to HDFS 
        // and added here as a LocalResource. 
        // For this implementation, we assume the JAR is already available or handled by YARN.
        Map<String, LocalResource> localResources = new HashMap<>();
        amContainer.setLocalResources(localResources);

        appContext.setAMContainerSpec(amContainer);

        log.info("Submitting application {} to ResourceManager", appId);
        yarnClient.submitApplication(appContext);
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client();
        try {
            client.run(args);
        } catch (Exception e) {
            log.error("Failed to run YARN Client", e);
            System.exit(1);
        }
    }
}
