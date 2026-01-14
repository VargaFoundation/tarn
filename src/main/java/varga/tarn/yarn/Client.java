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


import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

        String modelPath = config.modelRepository;
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
        env.put("MODEL_REPOSITORY", modelPath);
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
        if (config.zkEnsemble != null) {
            env.put("ZK_ENSEMBLE", config.zkEnsemble);
        }
        if (config.zkPath != null) {
            env.put("ZK_PATH", config.zkPath);
        }
        env.put("DOCKER_PRIVILEGED", String.valueOf(config.dockerPrivileged));
        env.put("DOCKER_DELAYED_REMOVAL", String.valueOf(config.dockerDelayedRemoval));
        if (config.dockerMounts != null) {
            env.put("DOCKER_MOUNTS", config.dockerMounts);
        }
        if (config.dockerPorts != null) {
            env.put("DOCKER_PORTS", config.dockerPorts);
        }
        env.put("SCALE_UP_THRESHOLD", String.valueOf(config.scaleUpThreshold));
        env.put("SCALE_DOWN_THRESHOLD", String.valueOf(config.scaleDownThreshold));
        env.put("MIN_CONTAINERS", String.valueOf(config.minContainers));
        env.put("MAX_CONTAINERS", String.valueOf(config.maxContainers));
        env.put("SCALE_COOLDOWN_MS", String.valueOf(config.scaleCooldownMs));
        if (config.rangerService != null) {
            env.put("RANGER_SERVICE", config.rangerService);
        }
        if (config.rangerAppId != null) {
            env.put("RANGER_APP_ID", config.rangerAppId);
        }
        env.put("RANGER_AUDIT", String.valueOf(config.rangerAudit));

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
        StringBuilder amCommand = new StringBuilder();
        amCommand.append(ApplicationConstants.Environment.JAVA_HOME.$()).append("/bin/java")
                .append(" -Xmx512m")
                .append(" varga.tarn.yarn.ApplicationMaster")
                .append(" --model-repository ").append(modelPath)
                .append(" --image ").append(tritonImage)
                .append(" --port ").append(tritonPort)
                .append(" --metrics-port ").append(metricsPort)
                .append(" --am-port ").append(amPort)
                .append(" --address ").append(bindAddress);

        if (!token.isEmpty()) amCommand.append(" --token ").append(token);
        if (secretsPath != null) amCommand.append(" --secrets ").append(secretsPath);

        amCommand.append(" --tp ").append(config.tensorParallelism)
                .append(" --pp ").append(config.pipelineParallelism)
                .append(" --placement-tag ").append(config.placementTag)
                .append(" --docker-network ").append(config.dockerNetwork);

        if (config.zkEnsemble != null) {
            amCommand.append(" --zk-ensemble ").append(config.zkEnsemble);
        }
        if (config.zkPath != null) {
            amCommand.append(" --zk-path ").append(config.zkPath);
        }

        if (config.dockerPrivileged) amCommand.append(" --docker-privileged");
        if (config.dockerDelayedRemoval) amCommand.append(" --docker-delayed-removal");
        if (config.dockerMounts != null) amCommand.append(" --docker-mounts ").append(config.dockerMounts);
        if (config.dockerPorts != null) amCommand.append(" --docker-ports ").append(config.dockerPorts);

        amCommand.append(" --scale-up ").append(config.scaleUpThreshold)
                .append(" --scale-down ").append(config.scaleDownThreshold)
                .append(" --min-instances ").append(config.minContainers)
                .append(" --max-instances ").append(config.maxContainers)
                .append(" --cooldown ").append(config.scaleCooldownMs);

        if (config.rangerService != null) amCommand.append(" --ranger-service ").append(config.rangerService);
        if (config.rangerAppId != null) amCommand.append(" --ranger-app-id ").append(config.rangerAppId);
        if (config.rangerAudit) amCommand.append(" --ranger-audit");

        for (Map.Entry<String, String> entry : config.customEnv.entrySet()) {
            amCommand.append(" --env ").append(entry.getKey()).append("=").append(entry.getValue());
        }

        amCommand.append(" 1>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/AppMaster.stdout")
                .append(" 2>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/AppMaster.stderr");

        amContainer.setCommands(Collections.singletonList(amCommand.toString()));

        // Handle application JAR
        Map<String, LocalResource> localResources = new HashMap<>();
        if (config.jarPath != null && !config.jarPath.isEmpty()) {
            setupAppJar(new Path(config.jarPath), localResources, appId);
        }
        amContainer.setLocalResources(localResources);

        appContext.setAMContainerSpec(amContainer);

        log.info("Submitting application {} to ResourceManager", appId);
        yarnClient.submitApplication(appContext);
    }

    private void setupAppJar(Path jarPath, Map<String, LocalResource> localResources, ApplicationId appId) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path destPath = new Path(fs.getHomeDirectory(), ".tarn/jars/" + appId.toString() + "/tarn.jar");

        log.info("Uploading JAR from {} to {}", jarPath, destPath);
        fs.copyFromLocalFile(false, true, jarPath, destPath);

        FileStatus destStatus = fs.getFileStatus(destPath);
        LocalResource jarResource = Records.newRecord(LocalResource.class);
        jarResource.setResource(URL.fromPath(destPath));
        jarResource.setSize(destStatus.getLen());
        jarResource.setTimestamp(destStatus.getModificationTime());
        jarResource.setType(LocalResourceType.FILE);
        jarResource.setVisibility(LocalResourceVisibility.APPLICATION);

        localResources.put("tarn.jar", jarResource);
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
