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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
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
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    private RangerAuthorizer rangerAuthorizer;
    private PlacementConstraint tritonConstraint;
    private CuratorFramework zkClient;
    private final RetryPolicy zkRetryPolicy = RetryPolicy.defaultPolicy();
    private ScheduledExecutorService monitorExecutor;
    private ScheduledExecutorService drainExecutor;
    // Timeout used when blocking on ZK connect at startup.
    private static final int ZK_CONNECT_TIMEOUT_SECONDS = 30;

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

        rangerAuthorizer = new RangerAuthorizer(config);
        if (rangerAuthorizer.isDegraded()) {
            String msg = "Ranger plugin requested (service=" + config.rangerService + ") but initialization failed";
            metricsCollector.recordAlert("ranger_degraded", msg,
                    config.rangerStrict ? "critical" : "warning");
            if (config.rangerStrict) {
                throw new IllegalStateException(msg + " and --ranger-strict is set — refusing to start");
            }
        }

        initZookeeper();

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
                registerInZooKeeper(c);
            }
        }

        log.info("ApplicationMaster registered with RM at {}:{} with anti-affinity constraints on tag '{}'",
                appMasterHostname, config.amPort, config.placementTag);

        // Initial request for Triton containers
        requestContainers();

        // Schedule the scaling loop instead of a tight Thread.sleep — interval is configurable
        // and failure in one tick doesn't prevent the next from running.
        long intervalMs = config.monitorIntervalMs > 0 ? config.monitorIntervalMs : 15000L;
        monitorExecutor = Executors.newSingleThreadScheduledExecutor(daemonFactory("tarn-monitor"));
        drainExecutor = Executors.newScheduledThreadPool(2, daemonFactory("tarn-drain"));
        monitorExecutor.scheduleAtFixedRate(this::safeMonitorTick,
                intervalMs, intervalMs, TimeUnit.MILLISECONDS);

        // Block the main thread; monitor runs on its own executor.
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Thread.sleep(Long.MAX_VALUE);
            }
        } catch (InterruptedException e) {
            log.info("AM interrupted, shutting down...");
        }

        shutdown();
    }

    private static ThreadFactory daemonFactory(String prefix) {
        AtomicInteger count = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + count.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    private void safeMonitorTick() {
        try {
            monitorMetricsAndScale();
        } catch (Throwable t) {
            log.error("Monitor tick failed (will retry next interval)", t);
        }
    }

    /**
     * Initialize the Curator client, block up to ZK_CONNECT_TIMEOUT_SECONDS, and attach a
     * connection state listener that re-registers ephemeral znodes on session reconnect so
     * Knox does not lose track of live containers during a transient ZK outage.
     */
    private void initZookeeper() {
        if (config.zkEnsemble == null || config.zkEnsemble.isEmpty()) {
            return;
        }
        log.info("Initializing ZooKeeper client with ensemble: {}", config.zkEnsemble);
        zkClient = CuratorFrameworkFactory.newClient(config.zkEnsemble, new ExponentialBackoffRetry(1000, 3));
        zkClient.getConnectionStateListenable().addListener(newConnectionStateListener());
        zkClient.start();

        try {
            if (!zkClient.blockUntilConnected(ZK_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new TimeoutException("ZK connect timeout after " + ZK_CONNECT_TIMEOUT_SECONDS + "s");
            }
            if (zkClient.checkExists().forPath(config.zkPath) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(config.zkPath);
            }
            log.info("ZooKeeper client initialized and base path ensured: {}", config.zkPath);
        } catch (Exception e) {
            metricsCollector.recordAlert("zk_init_failed",
                    "ZooKeeper initialization failed: " + e.getMessage(),
                    config.zkRequired ? "critical" : "warning");
            if (config.zkRequired) {
                throw new IllegalStateException(
                        "ZooKeeper init failed and --zk-required is set — refusing to start", e);
            }
            log.warn("ZooKeeper init failed; continuing without registration (Knox discovery will be broken)", e);
        }
    }

    private ConnectionStateListener newConnectionStateListener() {
        return (client, newState) -> {
            log.info("ZooKeeper connection state changed: {}", newState);
            if (newState == ConnectionState.RECONNECTED) {
                metricsCollector.recordAlert("zk_reconnected",
                        "ZooKeeper session reconnected; re-registering " + runningContainers.size() + " container(s)",
                        "info");
                // Snapshot the list to avoid holding the lock during network I/O.
                List<Container> snapshot;
                synchronized (runningContainers) {
                    snapshot = new ArrayList<>(runningContainers);
                }
                for (Container c : snapshot) {
                    registerInZooKeeper(c);
                }
            } else if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
                metricsCollector.recordAlert("zk_connection_lost",
                        "ZooKeeper connection " + newState,
                        newState == ConnectionState.LOST ? "critical" : "warning");
            }
        };
    }

    private void shutdown() throws Exception {
        if (monitorExecutor != null) {
            monitorExecutor.shutdownNow();
        }
        if (drainExecutor != null) {
            drainExecutor.shutdownNow();
        }
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
        if (rangerAuthorizer != null) {
            rangerAuthorizer.stop();
        }
        if (zkClient != null) {
            zkClient.close();
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
            metricsCollector.recordScalingEvent("scale_up", currentTarget, newTarget);
            requestContainers();
        } else if (newTarget < currentTarget) {
            targetNumContainers.set(newTarget);
            metricsCollector.recordScalingEvent("scale_down", currentTarget, newTarget);
            stopExtraContainer();
        }
    }

    private void stopExtraContainer() {
        Container cToStop;
        synchronized (runningContainers) {
            if (runningContainers.isEmpty()) return;
            // Prefer the least-loaded container for drain — minimizes disruption.
            cToStop = pickContainerToDrain();
        }
        if (cToStop != null) {
            gracefulStop(cToStop);
        }
    }

    private Container pickContainerToDrain() {
        // Called under synchronized(runningContainers).
        Container best = runningContainers.get(0);
        int bestDepth = metricsCollector.getQueueDepth(best.getId().toString());
        for (int i = 1; i < runningContainers.size(); i++) {
            Container c = runningContainers.get(i);
            int d = metricsCollector.getQueueDepth(c.getId().toString());
            if (d < bestDepth) {
                best = c;
                bestDepth = d;
            }
        }
        return best;
    }

    /**
     * Remove the container from ZK first (so Knox and HAProxy stop routing to it),
     * wait up to drainTimeoutMs for its queue to drain, then ask NM to stop it.
     * Runs on the drainExecutor so the monitor loop is not blocked.
     */
    private void gracefulStop(Container container) {
        if (drainExecutor == null || drainExecutor.isShutdown()) {
            // Fallback to immediate stop if we haven't started the executor yet.
            nmClient.stopContainerAsync(container.getId(), container.getNodeId());
            return;
        }
        drainExecutor.submit(() -> {
            String cid = container.getId().toString();
            log.info("Draining container {} (timeout={}ms)", cid, config.drainTimeoutMs);
            // 1. Deregister first so traffic stops arriving.
            unregisterFromZooKeeper(container.getId());

            // 2. Poll queue depth until 0 or timeout.
            long deadline = System.currentTimeMillis() + config.drainTimeoutMs;
            long pollMs = 500;
            while (System.currentTimeMillis() < deadline) {
                int depth = metricsCollector.getQueueDepth(cid);
                if (depth <= 0) break;
                try {
                    Thread.sleep(pollMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            int remaining = metricsCollector.getQueueDepth(cid);
            if (remaining > 0) {
                metricsCollector.recordAlert("drain_timeout",
                        "Container " + cid + " still had " + remaining + " in-flight requests at drain timeout",
                        "warning");
            }

            // 3. Tell NM to stop. Removal from runningContainers happens in onContainersCompleted.
            nmClient.stopContainerAsync(container.getId(), container.getNodeId());
        });
    }

    private double getAverageLoad() {
        // Snapshot hosts under the lock; never do network I/O while holding it.
        List<String> hosts;
        synchronized (runningContainers) {
            if (runningContainers.isEmpty()) return 0.0;
            hosts = new ArrayList<>(runningContainers.size());
            for (Container c : runningContainers) {
                hosts.add(c.getNodeId().getHost());
            }
        }

        List<CompletableFuture<Double>> futures = new ArrayList<>(hosts.size());
        for (String host : hosts) {
            futures.add(CompletableFuture.supplyAsync(
                    () -> metricsCollector.fetchContainerLoad(host), monitorExecutor));
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Load fetch timed out or failed: {}", e.getMessage());
            // Fall through: we'll still aggregate whatever completed.
        }

        double total = 0.0;
        int count = 0;
        for (CompletableFuture<Double> f : futures) {
            if (!f.isDone() || f.isCompletedExceptionally()) continue;
            try {
                total += f.getNow(0.0);
                count++;
            } catch (Exception ignore) {
                // Shouldn't happen since we checked isDone/exceptionally.
            }
        }
        return count > 0 ? total / count : 0.0;
    }

    private void registerInZooKeeper(Container container) {
        if (zkClient == null) return;

        String host = container.getNodeId().getHost();
        String containerId = container.getId().toString();
        String path = config.zkPath + "/" + containerId;
        String data = host + ":" + config.tritonPort;

        try {
            zkRetryPolicy.executeVoid(() -> {
                try {
                    if (zkClient.checkExists().forPath(path) != null) {
                        zkClient.delete().forPath(path);
                    }
                    zkClient.create()
                            .withMode(CreateMode.EPHEMERAL)
                            .forPath(path, data.getBytes());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "registerInZooKeeper:" + containerId);
            log.info("Registered container {} in ZooKeeper at {}: {}", containerId, path, data);
        } catch (Exception e) {
            log.error("Failed to register container {} in ZooKeeper after retries", containerId, e);
        }
    }

    private void unregisterFromZooKeeper(ContainerId containerId) {
        if (zkClient == null) return;

        String path = config.zkPath + "/" + containerId.toString();
        try {
            zkRetryPolicy.executeVoid(() -> {
                try {
                    if (zkClient.checkExists().forPath(path) != null) {
                        zkClient.delete().forPath(path);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, "unregisterFromZooKeeper:" + containerId);
            log.info("Unregistered container {} from ZooKeeper at {}", containerId, path);
        } catch (Exception e) {
            log.error("Failed to unregister container {} from ZooKeeper after retries", containerId, e);
        }
    }

    private class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
        @Override
        public void onContainersAllocated(List<Container> containers) {
            for (Container container : containers) {
                log.info("Container allocated: {}. Launching Triton...", container.getId());
                launchTriton(container);
                runningContainers.add(container);
                registerInZooKeeper(container);
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

            // Apply custom environment variables
            for (Map.Entry<String, String> entry : config.customEnv.entrySet()) {
                env.put(entry.getKey(), entry.getValue());
            }

            env.put("YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_HOSTNAME", container.getId().toString());
            env.put("YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS", "true");

            // Add secrets from JCEKS to environment
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

                    // Standard Hugging Face token
                    char[] hfToken = secretConf.getPassword("huggingface.token");
                    if (hfToken != null) {
                        env.put("HUGGING_FACE_HUB_TOKEN", new String(hfToken));
                        log.info("Loaded Hugging Face token from secrets");
                    }

                    // Dynamic secrets mapping: any alias starting with "tarn.env." will be mapped to env var
                    // e.g. tarn.env.AWS_ACCESS_KEY_ID -> AWS_ACCESS_KEY_ID
                    try {
                        // Use reflection or direct call if Hadoop version allows
                        java.util.List<String> aliases = org.apache.hadoop.security.alias.CredentialProviderFactory.getProviders(secretConf)
                                .stream()
                                .flatMap(p -> {
                                    try {
                                        return p.getAliases().stream();
                                    } catch (IOException e) {
                                        return java.util.stream.Stream.empty();
                                    }
                                })
                                .toList();

                        for (String alias : aliases) {
                            if (alias.startsWith("tarn.env.")) {
                                String envVar = alias.substring("tarn.env.".length());
                                char[] value = secretConf.getPassword(alias);
                                if (value != null) {
                                    env.put(envVar, new String(value));
                                    log.info("Loaded secret {} as env var {}", alias, envVar);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Failed to list additional aliases from secrets: {}", e.getMessage());
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
            metricsCollector.recordContainerStart(container.getId().toString());
            nmClient.startContainerAsync(container, ctx);
        }

        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            for (ContainerStatus status : statuses) {
                log.info("Container completed: {}. State: {}. Exit Status: {}. Diagnostics: {}",
                        status.getContainerId(), status.getState(), status.getExitStatus(), status.getDiagnostics());
                if (status.getExitStatus() != 0) {
                    metricsCollector.recordContainerFailure(status.getContainerId().toString(), 
                            "Exit status: " + status.getExitStatus() + ", " + status.getDiagnostics());
                }
                runningContainers.removeIf(c -> c.getId().equals(status.getContainerId()));
                unregisterFromZooKeeper(status.getContainerId());
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
        public void onError(Throwable e) {
            log.error("RM Error", e);
        }

        @Override
        public void onContainersUpdated(List<UpdatedContainer> containers) {
        }

        @Override
        public float getProgress() {
            int target = targetNumContainers.get();
            return target <= 0 ? 0.0f : (float) runningContainers.size() / target;
        }
    }

    private class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {
        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceKeys) {
            log.info("Container started: {}", containerId);
            metricsCollector.recordContainerReady(containerId.toString());
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            log.info("Container stopped: {}", containerId);
            runningContainers.removeIf(c -> c.getId().equals(containerId));
            unregisterFromZooKeeper(containerId);
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            log.error("Start error for {}", containerId, t);
            metricsCollector.recordContainerFailure(containerId.toString(), "Start error: " + t.getMessage());
            unregisterFromZooKeeper(containerId);
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

    public RangerAuthorizer getRangerAuthorizer() {
        return rangerAuthorizer;
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
