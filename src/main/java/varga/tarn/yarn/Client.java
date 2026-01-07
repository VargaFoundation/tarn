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
        Options options = new Options();
        options.addOption("m", "model-repository", true, "HDFS path to model repository");
        options.addOption("i", "image", true, "Triton Docker image");
        options.addOption("p", "port", true, "Triton HTTP port (default 8000)");
        options.addOption("mp", "metrics-port", true, "Triton metrics port (default 8002)");
        options.addOption("ap", "am-port", true, "AM HTTP port (default 8888)");
        options.addOption("a", "address", true, "Bind address (default 0.0.0.0)");
        options.addOption("t", "token", true, "Security token for AM API");

        CommandLineParser parser = new PosixParser();
        CommandLine line;
        try {
            line = parser.parse(options, args);
        } catch (ParseException exp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("yarn jar tarn.jar varga.tarn.yarn.Client", options);
            throw exp;
        }

        String modelHdfsPath = line.getOptionValue("model-repository", "hdfs:///models");
        String tritonImage = line.getOptionValue("image", "nvcr.io/nvidia/tritonserver:24.09-py3");
        String tritonPort = line.getOptionValue("port", "8000");
        String metricsPort = line.getOptionValue("metrics-port", "8002");
        String amPort = line.getOptionValue("am-port", "8888");
        String bindAddress = line.getOptionValue("address", "0.0.0.0");
        String token = line.getOptionValue("token", "");

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
