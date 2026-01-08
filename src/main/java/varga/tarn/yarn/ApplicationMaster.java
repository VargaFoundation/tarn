package varga.tarn.yarn;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.api.resource.PlacementConstraints;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ApplicationMaster for Triton on YARN.
 * Manages Triton container lifecycle and horizontal scaling.
 */
public class ApplicationMaster {

    private static final Logger log = LoggerFactory.getLogger(ApplicationMaster.class);

    private final Configuration conf;
    private final TarnConfig config;
    private final MetricsCollector metricsCollector;
    private final ScalingPolicy scalingPolicy;
    private final List<Container> runningContainers = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger targetNumContainers = new AtomicInteger(1);
    private final AtomicLong allocationRequestIdCounter = new AtomicLong(0);

    private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
    private NMClientAsync nmClient;
    private DiscoveryServer discoveryServer;
    private PlacementConstraint tritonConstraint;

    public ApplicationMaster() {
        this.conf = new YarnConfiguration();
        this.config = new TarnConfig();
        this.metricsCollector = new MetricsCollector(config.metricsPort);
        this.scalingPolicy = new ScalingPolicy();
    }

    public void init(String[] args) throws Exception {
        config.parseArgs(args);
    }

    public void run() throws Exception {
        log.info("Starting ApplicationMaster...");
        
        discoveryServer = new DiscoveryServer(config, this);
        discoveryServer.start();

        // Initialize RM Client
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
        amRMClient.init(conf);
        amRMClient.start();

        // Initialize NM Client
        nmClient = NMClientAsync.createNMClientAsync(new NMCallbackHandler());
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        String appMasterHostname = InetAddress.getLocalHost().getHostName();

        // Anti-affinity constraint: no more than 1 container with the configured placement tag per node
        tritonConstraint = PlacementConstraints.targetNotIn(
                PlacementConstraints.NODE,
                PlacementConstraints.PlacementTargets.allocationTag(config.placementTag)
        ).build();
        Map<Set<String>, PlacementConstraint> constraints = Collections.singletonMap(
                Collections.singleton(config.placementTag), tritonConstraint);

        amRMClient.registerApplicationMaster(appMasterHostname, config.amPort,
                "http://" + appMasterHostname + ":" + config.amPort, constraints);
        log.info("ApplicationMaster registered with RM at {}:{} with anti-affinity constraints on tag '{}'", 
                appMasterHostname, config.amPort, config.placementTag);

        // Initial request for Triton containers
        requestContainers();

        // Main loop for monitoring and auto-scaling
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(15000);
                monitorMetricsAndScale();
            } catch (InterruptedException e) {
                log.info("AM interrupted, shutting down...");
                break;
            }
        }

        shutdown();
    }

    private void shutdown() throws Exception {
        if (amRMClient != null) {
            amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Shutdown", "");
            amRMClient.stop();
        }
        if (nmClient != null) {
            nmClient.stop();
        }
        if (discoveryServer != null) {
            discoveryServer.stop();
        }
    }

    private void requestContainers() {
        int currentCount = runningContainers.size();
        int needed = targetNumContainers.get() - currentCount;

        if (needed > 0) {
            log.info("Requesting {} additional containers", needed);
            Resource capability = Resource.newInstance(config.containerMemory, config.containerVCores);
            int numGpus = config.tensorParallelism * config.pipelineParallelism;
            if (numGpus > 0) {
                try {
                    capability.setResourceValue("yarn.io/gpu", numGpus);
                } catch (NoSuchMethodError e) {
                    log.warn("GPU resource request not supported by this YARN version");
                }
            }
            Priority priority = Priority.newInstance(0);

            for (int i = 0; i < needed; i++) {
                SchedulingRequest schedulingRequest = SchedulingRequest.newBuilder()
                        .priority(priority)
                        .allocationRequestId(allocationRequestIdCounter.incrementAndGet())
                        .resourceSizing(ResourceSizing.newInstance(1, capability))
                        .allocationTags(Collections.singleton(config.placementTag))
                        .placementConstraintExpression(tritonConstraint)
                        .build();
                amRMClient.addSchedulingRequests(Collections.singletonList(schedulingRequest));
            }
        }
    }

    private void monitorMetricsAndScale() {
        int currentCount = runningContainers.size();
        int currentTarget = targetNumContainers.get();
        log.info("Monitoring instances... Current active: {}, Target: {}", currentCount, currentTarget);
        
        // 1. Handle failover (ensure we have enough containers for current target)
        if (currentCount < currentTarget) {
            log.info("Current count {} below target {}, requesting more...", currentCount, currentTarget);
            requestContainers();
        }

        // 2. Handle auto-scaling
        double avgLoad = getAverageLoad();
        int newTarget = scalingPolicy.calculateTarget(currentTarget, avgLoad);
        
        if (newTarget > currentTarget) {
            targetNumContainers.set(newTarget);
            requestContainers();
        } else if (newTarget < currentTarget) {
            targetNumContainers.set(newTarget);
            stopExtraContainer();
        }
    }

    private void stopExtraContainer() {
        synchronized (runningContainers) {
            if (!runningContainers.isEmpty()) {
                Container cToStop = runningContainers.get(0);
                nmClient.stopContainerAsync(cToStop.getId(), cToStop.getNodeId());
                // Note: removal from runningContainers happens in onContainersCompleted
            }
        }
    }

    private double getAverageLoad() {
        if (runningContainers.isEmpty()) return 0.0;

        double totalLoad = 0;
        int count = 0;
        synchronized (runningContainers) {
            for (Container c : runningContainers) {
                totalLoad += metricsCollector.fetchContainerLoad(c.getNodeId().getHost());
                count++;
            }
        }
        return count > 0 ? totalLoad / count : 0.0;
    }

    private class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
        @Override
        public void onContainersAllocated(List<Container> containers) {
            for (Container container : containers) {
                log.info("Container allocated: {}. Launching Triton...", container.getId());
                launchTriton(container);
                runningContainers.add(container);
            }
        }

        private void launchTriton(Container container) {
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

            Map<String, String> env = new HashMap<>();
            env.put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
            env.put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", config.tritonImage);
            env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE", "true");
            if (config.dockerNetwork != null && !config.dockerNetwork.isEmpty()) {
                env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK", config.dockerNetwork);
            }
            if (config.dockerPrivileged) {
                env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_PRIVILEGED_CONTAINER", "true");
            }
            if (config.dockerDelayedRemoval) {
                env.put("YARN_CONTAINER_RUNTIME_DOCKER_DELAYED_REMOVAL", "true");
            }
            if (config.dockerMounts != null && !config.dockerMounts.isEmpty()) {
                env.put("YARN_CONTAINER_RUNTIME_DOCKER_MOUNTS", config.dockerMounts);
            }
            if (config.dockerPorts != null && !config.dockerPorts.isEmpty()) {
                env.put("YARN_CONTAINER_RUNTIME_DOCKER_PORTS_MAPPING", config.dockerPorts);
            }
            env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME", container.getId().toString());
            env.put("YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS", "true");
            
            // Add secrets to environment if available
            if (config.secretsPath != null) {
                try {
                    Configuration secretConf = new Configuration(conf);
                    String providerPath = config.secretsPath;
                    if (!providerPath.contains("://")) {
                        providerPath = "jceks://hdfs" + providerPath;
                    } else if (providerPath.startsWith("hdfs://")) {
                        providerPath = providerPath.replace("hdfs://", "jceks://hdfs");
                    }
                    secretConf.set("hadoop.security.credential.provider.path", providerPath);
                    
                    char[] hfToken = secretConf.getPassword("huggingface.token");
                    if (hfToken != null) {
                        env.put("HUGGING_FACE_HUB_TOKEN", new String(hfToken));
                        log.info("Loaded Hugging Face token from secrets");
                    }
                } catch (Exception e) {
                    log.warn("Failed to load secrets from {}: {}", config.secretsPath, e.getMessage());
                }
            }

            ctx.setEnvironment(env);

            String modelPath = "/models";
            int worldSize = config.tensorParallelism * config.pipelineParallelism;
            
            StringBuilder sb = new StringBuilder();
            if (config.modelRepository != null && !config.modelRepository.isEmpty()) {
                if (config.modelRepository.startsWith("hdfs:///")) {
                    sb.append("mkdir -p ").append(modelPath).append(" && ");
                    sb.append("hadoop fs -copyToLocal ").append(config.modelRepository).append("/* ").append(modelPath).append(" && ");
                } else if (config.modelRepository.startsWith("/")) {
                    modelPath = config.modelRepository;
                }
            }

            if (config.secretsPath != null && !config.secretsPath.isEmpty()) {
                sb.append("mkdir -p /secrets && ");
                sb.append("hadoop fs -copyToLocal ").append(config.secretsPath).append(" /secrets/secrets.jks && ");
            }

            if (worldSize > 1) {
                sb.append("mpirun --allow-run-as-root ");
                for (int i = 0; i < worldSize; i++) {
                    if (i > 0) sb.append(" : ");
                    sb.append("-n 1 tritonserver ");
                    sb.append("--id=rank").append(i).append(" ");
                    sb.append("--model-repository=").append(modelPath).append(" ");
                    sb.append("--backend-config=python,shm-region-prefix-name=rank").append(i).append("_ ");
                    
                    if (i == 0) {
                        sb.append("--http-port=").append(config.tritonPort).append(" ");
                        sb.append("--grpc-port=").append(config.grpcPort).append(" ");
                        sb.append("--metrics-port=").append(config.metricsPort).append(" ");
                        sb.append("--http-address=").append(config.bindAddress).append(" ");
                        sb.append("--metrics-address=").append(config.bindAddress).append(" ");
                        sb.append("--allow-cpu-metrics=false --allow-gpu-metrics=false --allow-metrics=true --metrics-interval-ms=1000 ");
                        sb.append("--model-load-thread-count=2 --strict-readiness=true ");
                    } else {
                        sb.append("--http-port=").append(config.tritonPort + i * 10).append(" ");
                        sb.append("--grpc-port=").append(config.grpcPort + i * 10).append(" ");
                        sb.append("--allow-http=false --allow-grpc=false --allow-metrics=false ");
                        sb.append("--log-info=false --log-warning=false --model-control-mode=explicit --load-model=tensorrt_llm ");
                        sb.append("--model-load-thread-count=2 ");
                    }
                }
            } else {
                sb.append("tritonserver ")
                  .append("--model-repository=").append(modelPath).append(" ")
                  .append("--http-port=").append(config.tritonPort).append(" ")
                  .append("--grpc-port=").append(config.grpcPort).append(" ")
                  .append("--metrics-port=").append(config.metricsPort).append(" ")
                  .append("--http-address=").append(config.bindAddress).append(" ")
                  .append("--metrics-address=").append(config.bindAddress)
                  .append(" --allow-cpu-metrics=false --allow-gpu-metrics=false --allow-metrics=true --metrics-interval-ms=1000")
                  .append(" --model-load-thread-count=2 --strict-readiness=true");
            }

            String launchCommand = sb.toString();

            ctx.setCommands(Collections.singletonList(
                    launchCommand +
                            " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                            " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
            ));
            nmClient.startContainerAsync(container, ctx);
        }

        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            for (ContainerStatus status : statuses) {
                log.info("Container completed: {}", status.getContainerId());
                runningContainers.removeIf(c -> c.getId().equals(status.getContainerId()));
            }
        }

        @Override public void onShutdownRequest() { log.info("Shutdown requested"); }
        @Override public void onNodesUpdated(List<NodeReport> updatedNodes) {}
        @Override public void onError(Throwable e) { log.error("RM Error", e); }
        @Override public void onContainersUpdated(List<UpdatedContainer> containers) {}

        @Override
        public float getProgress() {
            int target = targetNumContainers.get();
            return target <= 0 ? 0.0f : (float) runningContainers.size() / target;
        }
    }

    private class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {
        @Override public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceKeys) { log.info("Container started: {}", containerId); }
        @Override public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {}
        @Override public void onContainerStopped(ContainerId containerId) {
            log.info("Container stopped: {}", containerId);
            runningContainers.removeIf(c -> c.getId().equals(containerId));
        }
        @Override public void onStartContainerError(ContainerId containerId, Throwable t) { log.error("Start error for {}", containerId, t); }
        @Override public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {}
        @Override public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {}
        @Override public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {}
        @Override public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {}
        @Override public void onGetContainerStatusError(ContainerId containerId, Throwable t) {}
        @Override public void onStopContainerError(ContainerId containerId, Throwable t) {}
    }

    public List<Container> getRunningContainers() {
        return runningContainers;
    }

    public List<String> getAvailableModels() {
        List<String> models = new ArrayList<>();
        try {
            if (config.modelRepository != null) {
                Path modelPath = new Path(config.modelRepository);
                FileSystem fs = modelPath.getFileSystem(conf);
                if (fs.exists(modelPath)) {
                    FileStatus[] statuses = fs.listStatus(modelPath);
                    for (FileStatus status : statuses) {
                        if (status.isDirectory()) {
                            models.add(status.getPath().getName());
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("Failed to list models from repository", e);
        }
        return models;
    }

    public Resource getAvailableResources() {
        if (amRMClient != null) {
            return amRMClient.getAvailableResources();
        }
        return Resource.newInstance(0, 0);
    }

    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }

    public int getTargetNumContainers() {
        return targetNumContainers.get();
    }

    public TarnConfig getConfig() {
        return config;
    }

    public static void main(String[] args) throws Exception {
        ApplicationMaster am = new ApplicationMaster();
        am.init(args);
        am.run();
    }
}
