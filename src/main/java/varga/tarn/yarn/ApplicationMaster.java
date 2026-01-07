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

import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

/**
 * ApplicationMaster for Triton on YARN.
 * Manages Triton container lifecycle and horizontal scaling.
 */
public class ApplicationMaster {

    private static final Logger log = LoggerFactory.getLogger(ApplicationMaster.class);

    private Configuration conf;
    private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
    private NMClientAsync nmClient;
    private HttpServer httpServer;
    
    private AtomicInteger targetNumContainers = new AtomicInteger(1);
    private List<Container> runningContainers = Collections.synchronizedList(new ArrayList<>());
    
    // Configurations
    private String tritonImage = System.getenv("TRITON_IMAGE") != null ? 
            System.getenv("TRITON_IMAGE") : "nvcr.io/nvidia/tritonserver:24.09-py3";
    private String modelRepositoryHdfs = System.getenv("MODEL_REPOSITORY_HDFS");
    private int containerMemory = Integer.parseInt(System.getenv().getOrDefault("CONTAINER_MEMORY", "4096"));
    private int containerVCores = Integer.parseInt(System.getenv().getOrDefault("CONTAINER_VCORES", "2"));

    public ApplicationMaster() {
        conf = new YarnConfiguration();
    }

    public void run() throws Exception {
        log.info("Starting ApplicationMaster...");
        startHttpServer();

        // Initialize RM Client
        RMCallbackHandler rmCallbackHandler = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, rmCallbackHandler);
        amRMClient.init(conf);
        amRMClient.start();

        // Initialize NM Client
        NMCallbackHandler nmCallbackHandler = new NMCallbackHandler();
        nmClient = NMClientAsync.createNMClientAsync(nmCallbackHandler);
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        amRMClient.registerApplicationMaster("", 0, "");
        log.info("ApplicationMaster registered with RM");

        // Initial request for Triton containers
        requestContainers();

        // Main loop for monitoring and auto-scaling
        while (true) {
            try {
                Thread.sleep(15000);
                monitorMetricsAndScale();
            } catch (InterruptedException e) {
                log.info("AM interrupted, shutting down...");
                break;
            }
        }
        
        // Cleanup
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Shutdown", "");
        amRMClient.stop();
        nmClient.stop();
        httpServer.stop(0);
    }

    private void startHttpServer() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(8888), 0);
        httpServer.createContext("/instances", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                StringBuilder sb = new StringBuilder();
                synchronized (runningContainers) {
                    for (Container c : runningContainers) {
                        sb.append(c.getNodeId().getHost()).append(":8000\n");
                    }
                }
                String response = sb.toString();
                exchange.getResponseHeaders().set("Content-Type", "text/plain");
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        });
        httpServer.setExecutor(null);
        httpServer.start();
        log.info("Service Discovery HTTP Server started on port 8888");
    }

    private void requestContainers() {
        int currentCount = runningContainers.size();
        int needed = targetNumContainers.get() - currentCount;
        
        if (needed > 0) {
            log.info("Requesting {} additional containers", needed);
            Resource capability = Records.newRecord(Resource.class);
            capability.setMemorySize(containerMemory);
            capability.setVirtualCores(containerVCores);
            Priority priority = Priority.newInstance(0);

            for (int i = 0; i < needed; i++) {
                AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, priority);
                amRMClient.addContainerRequest(request);
            }
        }
    }

    private void monitorMetricsAndScale() {
        log.info("Monitoring instances... Current active: {}", runningContainers.size());
        double avgLoad = Math.random(); // Simulated
        
        if (avgLoad > 0.7 && targetNumContainers.get() < 10) {
            log.info("High load detected ({}), scaling up...", avgLoad);
            targetNumContainers.incrementAndGet();
            requestContainers();
        } else if (avgLoad < 0.2 && targetNumContainers.get() > 1) {
            log.info("Low load detected ({}), scaling down...", avgLoad);
            targetNumContainers.decrementAndGet();
            if (!runningContainers.isEmpty()) {
                Container cToStop = runningContainers.get(0);
                nmClient.stopContainerAsync(cToStop.getId(), cToStop.getNodeId());
            }
        }
    }

    /**
     * Callback handler for ResourceManager events.
     */
    private class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
        @Override
        public void onContainersAllocated(List<Container> containers) {
            for (Container container : containers) {
                log.info("Container allocated: {}. Launching Triton...", container.getId());
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                
                Map<String, String> env = new HashMap<>();
                env.put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
                env.put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", tritonImage);
                env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE", "true");
                ctx.setEnvironment(env);

                String modelPath = "/models";
                String launchCommand;
                if (modelRepositoryHdfs != null && !modelRepositoryHdfs.isEmpty()) {
                    launchCommand = "mkdir -p " + modelPath + " && " +
                                    "hadoop fs -copyToLocal " + modelRepositoryHdfs + "/* " + modelPath + " && " +
                                    "tritonserver --model-repository=" + modelPath;
                } else {
                    launchCommand = "tritonserver --model-repository=" + modelPath;
                }

                List<String> commands = Collections.singletonList(
                    launchCommand +
                    " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                    " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                );
                ctx.setCommands(commands);
                nmClient.startContainerAsync(container, ctx);
                runningContainers.add(container);
            }
        }

        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            for (ContainerStatus status : statuses) {
                log.info("Container completed: {}", status.getContainerId());
                runningContainers.removeIf(c -> c.getId().equals(status.getContainerId()));
            }
        }

        @Override
        public void onShutdownRequest() {
            log.info("Shutdown requested");
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {}

        @Override
        public float getProgress() {
            return 0.5f;
        }

        @Override
        public void onError(Throwable e) {
            log.error("RM Error", e);
        }

        // Required by some Hadoop 3 versions
        public void onContainersUpdated(List<UpdatedContainer> containers) {}
    }

    /**
     * Callback handler for NodeManager events.
     */
    private class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {
        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceKeys) {
            log.info("Container started: {}", containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {}

        @Override
        public void onContainerStopped(ContainerId containerId) {
            log.info("Container stopped: {}", containerId);
            runningContainers.removeIf(c -> c.getId().equals(containerId));
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            log.error("Start error for {}", containerId, t);
        }

        @Override
        public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {}

        @Override
        public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {}

        @Override
        public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {}

        @Override
        public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {}

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {}

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {}
    }

    public static void main(String[] args) throws Exception {
        ApplicationMaster am = new ApplicationMaster();
        am.run();
    }
}
