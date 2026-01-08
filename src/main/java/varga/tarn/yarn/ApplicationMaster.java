package varga.tarn.yarn;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
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
    private MetricsCollector metricsCollector;
    private ScalingPolicy scalingPolicy;
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
    }

    public void init(String[] args) throws Exception {
        config.parseArgs(args);
        this.metricsCollector = new MetricsCollector(config.metricsPort);
        this.scalingPolicy = new ScalingPolicy(
                config.scaleUpThreshold,
                config.scaleDownThreshold,
                config.minContainers,
                config.maxContainers,
                config.scaleCooldownMs
        );
        this.targetNumContainers.set(config.minContainers);
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

        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, config.amPort,
                "http://" + appMasterHostname + ":" + config.amPort, constraints);
        
        // Recover running containers if this is an AM restart
        List<Container> previousContainers = response.getContainersFromPreviousAttempts();
        if (previousContainers != null && !previousContainers.isEmpty()) {
            log.info("Recovered {} containers from previous attempt", previousContainers.size());
            for (Container c : previousContainers) {
                runningContainers.add(c);
            }
        }

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

            String launchCommand = new TritonCommandBuilder()
                    .modelRepository(config.modelRepository)
                    .httpPort(config.tritonPort)
                    .grpcPort(config.grpcPort)
                    .metricsPort(config.metricsPort)
                    .bindAddress(config.bindAddress)
                    .tensorParallelism(config.tensorParallelism)
                    .pipelineParallelism(config.pipelineParallelism)
                    .secretsPath(config.secretsPath)
                    .build();

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
