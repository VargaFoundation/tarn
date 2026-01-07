package varga.tarn.yarn;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.cli.*;
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
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    private String tritonImage;
    private String modelRepositoryHdfs;
    private int tritonPort;
    private int metricsPort;
    int amPort;
    String bindAddress;
    private String apiToken;
    private int containerMemory;
    private int containerVCores;
    private HttpClient httpClient;

    public ApplicationMaster() {
        conf = new YarnConfiguration();
        // Default values from environment or defaults
        tritonImage = System.getenv("TRITON_IMAGE") != null ?
                System.getenv("TRITON_IMAGE") : "nvcr.io/nvidia/tritonserver:24.09-py3";
        modelRepositoryHdfs = System.getenv("MODEL_REPOSITORY_HDFS");
        tritonPort = Integer.parseInt(System.getenv().getOrDefault("TRITON_PORT", "8000"));
        metricsPort = Integer.parseInt(System.getenv().getOrDefault("METRICS_PORT", "8002"));
        amPort = Integer.parseInt(System.getenv().getOrDefault("AM_PORT", "8888"));
        bindAddress = System.getenv().getOrDefault("BIND_ADDRESS", "0.0.0.0");
        apiToken = System.getenv("TARN_TOKEN");
        containerMemory = Integer.parseInt(System.getenv().getOrDefault("CONTAINER_MEMORY", "4096"));
        containerVCores = Integer.parseInt(System.getenv().getOrDefault("CONTAINER_VCORES", "2"));
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public void init(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("m", "model-repository", true, "HDFS path to model repository");
        options.addOption("i", "image", true, "Triton Docker image");
        options.addOption("p", "port", true, "Triton HTTP port");
        options.addOption("mp", "metrics-port", true, "Triton metrics port");
        options.addOption("ap", "am-port", true, "AM HTTP port");
        options.addOption("a", "address", true, "Bind address for HTTP servers");
        options.addOption("t", "token", true, "Security token for API");

        CommandLineParser parser = new PosixParser();
        CommandLine line = parser.parse(options, args);

        if (line.hasOption("model-repository")) {
            modelRepositoryHdfs = line.getOptionValue("model-repository");
        }
        if (line.hasOption("image")) {
            tritonImage = line.getOptionValue("image");
        }
        if (line.hasOption("port")) {
            tritonPort = Integer.parseInt(line.getOptionValue("port"));
        }
        if (line.hasOption("metrics-port")) {
            metricsPort = Integer.parseInt(line.getOptionValue("metrics-port"));
        }
        if (line.hasOption("am-port")) {
            amPort = Integer.parseInt(line.getOptionValue("am-port"));
        }
        if (line.hasOption("address")) {
            bindAddress = line.getOptionValue("address");
        }
        if (line.hasOption("token")) {
            apiToken = line.getOptionValue("token");
        }
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
        String appMasterHostname = InetAddress.getLocalHost().getHostName();
        amRMClient.registerApplicationMaster(appMasterHostname, amPort, "http://" + appMasterHostname + ":" + amPort);
        log.info("ApplicationMaster registered with RM at {}:{}", appMasterHostname, amPort);

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
        httpServer = HttpServer.create(new InetSocketAddress(bindAddress, amPort), 0);
        httpServer.createContext("/instances", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (apiToken != null && !apiToken.isEmpty()) {
                    String providedToken = exchange.getRequestHeaders().getFirst("X-TARN-Token");
                    if (!apiToken.equals(providedToken)) {
                        log.warn("Unauthorized access attempt to /instances from {}", exchange.getRemoteAddress());
                        exchange.sendResponseHeaders(401, -1);
                        return;
                    }
                }
                StringBuilder sb = new StringBuilder();
                synchronized (runningContainers) {
                    for (Container c : runningContainers) {
                        sb.append(c.getNodeId().getHost()).append(":").append(tritonPort).append("\n");
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
        double avgLoad = getAverageLoad();

        if (avgLoad > 0.7 && targetNumContainers.get() < 10) {
            log.info("High load detected ({}), scaling up...", String.format("%.2f", avgLoad));
            targetNumContainers.incrementAndGet();
            requestContainers();
        } else if (avgLoad < 0.2 && targetNumContainers.get() > 1) {
            log.info("Low load detected ({}), scaling down...", String.format("%.2f", avgLoad));
            targetNumContainers.decrementAndGet();
            if (!runningContainers.isEmpty()) {
                Container cToStop = runningContainers.get(0);
                nmClient.stopContainerAsync(cToStop.getId(), cToStop.getNodeId());
            }
        }
    }

    private double getAverageLoad() {
        if (runningContainers.isEmpty()) return 0.0;

        double totalLoad = 0;
        int count = 0;
        synchronized (runningContainers) {
            for (Container c : runningContainers) {
                double load = fetchContainerLoad(c.getNodeId().getHost());
                totalLoad += load;
                count++;
            }
        }
        return count > 0 ? totalLoad / count : 0.0;
    }

    private double fetchContainerLoad(String host) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + host + ":" + metricsPort + "/metrics"))
                    .timeout(Duration.ofSeconds(3))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                return parseLoadFromMetrics(response.body());
            }
        } catch (Exception e) {
            log.warn("Failed to fetch metrics from {}: {}", host, e.getMessage());
        }
        return 0.0;
    }

    private double parseLoadFromMetrics(String metrics) {
        // Try to find GPU utilization first
        Pattern gpuPattern = Pattern.compile("nv_gpu_utilization\\{[^}]*\\}\\s+([\\d.]+)");
        Matcher gpuMatcher = gpuPattern.matcher(metrics);
        double totalGpuLoad = 0;
        int gpuCount = 0;
        while (gpuMatcher.find()) {
            totalGpuLoad += Double.parseDouble(gpuMatcher.group(1));
            gpuCount++;
        }
        if (gpuCount > 0) return (totalGpuLoad / gpuCount) / 100.0;

        // Fallback to a simple heuristic if no GPU metrics: 
        // use nv_inference_request_duration_us per model? 
        // For simplicity, if we see any activity, we return a base load or 0.
        // A more advanced AM would track the rate of change.
        if (metrics.contains("nv_inference_request_success")) {
            // If the metric exists, we consider it at least working.
            // In a real world, we'd compare with previous value.
            return 0.3; // Placeholder for "active"
        }

        return 0.0;
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
                String tritonArgs = " --model-repository=" + modelPath + 
                                   " --http-port=" + tritonPort + 
                                   " --metrics-port=" + metricsPort +
                                   " --http-address=" + bindAddress +
                                   " --metrics-address=" + bindAddress;

                if (modelRepositoryHdfs != null && !modelRepositoryHdfs.isEmpty()) {
                    launchCommand = "mkdir -p " + modelPath + " && " +
                            "hadoop fs -copyToLocal " + modelRepositoryHdfs + "/* " + modelPath + " && " +
                            "tritonserver" + tritonArgs;
                } else {
                    launchCommand = "tritonserver" + tritonArgs;
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
        public void onNodesUpdated(List<NodeReport> updatedNodes) {
        }

        @Override
        public float getProgress() {
            int target = targetNumContainers.get();
            if (target <= 0) return 0.0f;
            return (float) runningContainers.size() / target;
        }

        @Override
        public void onError(Throwable e) {
            log.error("RM Error", e);
        }

        // Required by some Hadoop 3 versions
        public void onContainersUpdated(List<UpdatedContainer> containers) {
        }
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
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        }

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
        public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {
        }

        @Override
        public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {
        }

        @Override
        public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {
        }

        @Override
        public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
        }
    }

    public static void main(String[] args) throws Exception {
        ApplicationMaster am = new ApplicationMaster();
        am.init(args);
        am.run();
    }
}
