package varga.tarn.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
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

    private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
    private NMClientAsync nmClient;
    private DiscoveryServer discoveryServer;

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
        
        discoveryServer = new DiscoveryServer(config, runningContainers);
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
        amRMClient.registerApplicationMaster(appMasterHostname, config.amPort, 
                "http://" + appMasterHostname + ":" + config.amPort);
        log.info("ApplicationMaster registered with RM at {}:{}", appMasterHostname, config.amPort);

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
            Resource capability = Records.newRecord(Resource.class);
            capability.setMemorySize(config.containerMemory);
            capability.setVirtualCores(config.containerVCores);
            Priority priority = Priority.newInstance(0);

            for (int i = 0; i < needed; i++) {
                AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, priority);
                amRMClient.addContainerRequest(request);
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
            ctx.setEnvironment(env);

            String modelPath = "/models";
            String tritonArgs = " --model-repository=" + modelPath + 
                               " --http-port=" + config.tritonPort + 
                               " --metrics-port=" + config.metricsPort +
                               " --http-address=" + config.bindAddress +
                               " --metrics-address=" + config.bindAddress;

            String launchCommand;
            if (config.modelRepositoryHdfs != null && !config.modelRepositoryHdfs.isEmpty()) {
                launchCommand = "mkdir -p " + modelPath + " && " +
                        "hadoop fs -copyToLocal " + config.modelRepositoryHdfs + "/* " + modelPath + " && " +
                        "tritonserver" + tritonArgs;
            } else {
                launchCommand = "tritonserver" + tritonArgs;
            }

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

    public TarnConfig getConfig() {
        return config;
    }

    public static void main(String[] args) throws Exception {
        ApplicationMaster am = new ApplicationMaster();
        am.init(args);
        am.run();
    }
}
