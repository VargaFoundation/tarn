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
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
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
import varga.tarn.yarn.openai.OpenAIProxyServer;

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
    private QuotaEnforcer quotaEnforcer;
    private PlacementConstraint tritonConstraint;
    private CuratorFramework zkClient;
    private final RetryPolicy zkRetryPolicy = RetryPolicy.defaultPolicy();
    private ScheduledExecutorService monitorExecutor;
    private ScheduledExecutorService drainExecutor;
    private OpenAIProxyServer openaiProxy;
    private NodeCache quotasNodeCache;
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
                config.scaleCooldownMs,
                LoadSignal.ScalingMode.parse(config.scaleMode)
        );
        this.targetNumContainers.set(config.minContainers);
    }

    public void run() throws Exception {
        log.info("Starting ApplicationMaster...");

        discoveryServer = new DiscoveryServer(config, this, conf);
        discoveryServer.start();

        if (config.openaiProxyEnabled) {
            openaiProxy = new OpenAIProxyServer(config, this, conf);
            openaiProxy.start();
        }

        rangerAuthorizer = new RangerAuthorizer(config);
        quotaEnforcer = new QuotaEnforcer();
        loadQuotasFromConfig();
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
            startConfigWatchers();
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

    /**
     * Sets up Curator {@link NodeCache}s on config znodes so quota rules can be updated
     * without restarting the AM. Today we watch {@code {zkPath}/config/quotas}; scaling
     * thresholds can be added here in the same pattern.
     *
     * <p>Operators push an update with e.g.
     * {@code zkCli.sh set /services/triton/config/quotas '{"rules":[...]}'} and all AM
     * replicas react within the Curator event latency (typically &lt; 100ms).
     */
    private void startConfigWatchers() {
        if (zkClient == null) return;
        String configRoot = configRootPath();
        try {
            if (zkClient.checkExists().forPath(configRoot) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(configRoot);
            }
            String quotasPath = configRoot + "/quotas";
            if (zkClient.checkExists().forPath(quotasPath) == null) {
                zkClient.create().forPath(quotasPath, new byte[0]);
            }
            quotasNodeCache = new NodeCache(zkClient, quotasPath);
            quotasNodeCache.getListenable().addListener(new NodeCacheListener() {
                @Override public void nodeChanged() {
                    byte[] data = quotasNodeCache.getCurrentData() == null
                            ? null : quotasNodeCache.getCurrentData().getData();
                    if (data == null || data.length == 0) {
                        log.info("ZK quotas znode cleared; reloading from disk if configured");
                        loadQuotasFromConfig();
                        return;
                    }
                    String json = new String(data, java.nio.charset.StandardCharsets.UTF_8);
                    log.info("ZK quotas znode updated ({} bytes), hot-reloading", data.length);
                    quotaEnforcer.loadFromJson(json);
                    metricsCollector.recordAlert("quota_reloaded",
                            "Quota rules hot-reloaded from ZK (" + data.length + " bytes)", "info");
                }
            });
            quotasNodeCache.start(true);
            log.info("ZK config watcher started at {}", quotasPath);
        } catch (Exception e) {
            log.warn("Failed to install ZK config watchers: {}", e.getMessage());
        }
    }

    private String configRootPath() {
        // Sibling of the instances path: /services/triton/config alongside /services/triton/instances.
        int lastSlash = config.zkPath.lastIndexOf('/');
        String parent = lastSlash > 0 ? config.zkPath.substring(0, lastSlash) : "/services/triton";
        return parent + "/config";
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
        if (openaiProxy != null) {
            openaiProxy.stop();
        }
        if (rangerAuthorizer != null) {
            rangerAuthorizer.stop();
        }
        if (quotasNodeCache != null) {
            try { quotasNodeCache.close(); } catch (Exception ignore) {}
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

            AcceleratorType accelerator = AcceleratorType.parse(config.acceleratorType);
            if (accelerator.requiresAcceleratorResource()) {
                int accelCount = Math.max(1, config.tensorParallelism * config.pipelineParallelism);
                try {
                    capability.setResourceValue(accelerator.yarnResourceName(), accelCount);
                    if (config.gpuSliceSize != null && !config.gpuSliceSize.isEmpty()) {
                        log.info("Requesting {} x {} ({}) per container, slice profile '{}' "
                                        + "(MIG partitioning must be enabled on NodeManagers for fractional scheduling)",
                                accelCount, accelerator.yarnResourceName(), accelerator, config.gpuSliceSize);
                    } else {
                        log.info("Requesting {} x {} ({}) per container",
                                accelCount, accelerator.yarnResourceName(), accelerator);
                    }
                } catch (NoSuchMethodError e) {
                    log.warn("Accelerator resource {} not supported by this YARN version",
                            accelerator.yarnResourceName());
                } catch (org.apache.hadoop.yarn.exceptions.ResourceNotFoundException e) {
                    log.error("Accelerator resource {} is not declared in resource-types.xml — "
                            + "scheduling will fail. Add it to the cluster config or set "
                            + "--accelerator-type=cpu_only.", accelerator.yarnResourceName());
                }
            } else {
                log.info("CPU-only mode: no accelerator resource requested");
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

        // 2. Build a composite load signal (GPU + queue depth) in parallel across containers.
        LoadSignal signal = buildLoadSignal(currentCount);
        int newTarget = scalingPolicy.calculateTarget(currentTarget, signal);

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

    /**
     * Parallel scrape of GPU utilization and pending queue depth across all running
     * containers. Aggregates into a {@link LoadSignal} for {@link ScalingPolicy}. A per-host
     * failure never stalls the whole decision — stale data is better than no decision.
     */
    private LoadSignal buildLoadSignal(int numContainers) {
        List<Container> snapshot;
        synchronized (runningContainers) {
            snapshot = new ArrayList<>(runningContainers);
        }
        if (snapshot.isEmpty()) {
            return new LoadSignal(0.0, 0, 0.0, 0, config.queueCapacityPerContainer);
        }

        List<CompletableFuture<double[]>> futures = new ArrayList<>(snapshot.size());
        for (Container c : snapshot) {
            String host = c.getNodeId().getHost();
            String cid = c.getId().toString();
            futures.add(CompletableFuture.supplyAsync(() -> {
                String raw = metricsCollector.fetchRawMetrics(host);
                double gpu = metricsCollector.parseLoadFromMetrics(raw);
                int depth = metricsCollector.parseQueueDepthFromMetrics(raw);
                metricsCollector.updateQueueDepth(cid, depth);
                return new double[]{gpu, depth};
            }, monitorExecutor));
        }
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Load signal fetch partial/timeout: {}", e.getMessage());
        }
        double gpuSum = 0.0;
        int depthSum = 0;
        int ok = 0;
        for (CompletableFuture<double[]> f : futures) {
            if (!f.isDone() || f.isCompletedExceptionally()) continue;
            try {
                double[] pair = f.getNow(null);
                if (pair == null) continue;
                gpuSum += pair[0];
                depthSum += (int) pair[1];
                ok++;
            } catch (Exception ignored) {
            }
        }
        double avgGpu = ok > 0 ? gpuSum / ok : 0.0;
        return new LoadSignal(avgGpu, depthSum, 0.0, numContainers, config.queueCapacityPerContainer);
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

    /**
     * Polls {@code /v2/health/ready} on the container until it succeeds, then registers the
     * container in ZooKeeper so Knox starts routing to it. Runs on the drainExecutor so the
     * YARN callback thread is freed immediately.
     *
     * <p>The critical guarantee: a container is never visible via ZK discovery before it can
     * answer a real inference. Previously registration happened in {@code onContainersAllocated}
     * (before Triton even started), causing connection-refused errors during cluster warmup.
     */
    private void scheduleWarmupAndRegister(ContainerId containerId) {
        Container container = findContainerById(containerId);
        if (container == null) {
            log.warn("scheduleWarmupAndRegister: container {} not found in runningContainers", containerId);
            return;
        }
        if (drainExecutor == null || drainExecutor.isShutdown()) {
            // AM shutdown race — skip.
            return;
        }
        drainExecutor.submit(() -> runWarmupThenRegister(container));
    }

    private Container findContainerById(ContainerId id) {
        synchronized (runningContainers) {
            for (Container c : runningContainers) {
                if (c.getId().equals(id)) return c;
            }
        }
        return null;
    }

    private void runWarmupThenRegister(Container container) {
        String host = container.getNodeId().getHost();
        String cid = container.getId().toString();
        long start = System.currentTimeMillis();
        long deadline = start + config.warmupTimeoutMs;
        long poll = Math.max(250L, config.warmupPollIntervalMs);
        int attempt = 0;
        while (System.currentTimeMillis() < deadline) {
            attempt++;
            if (metricsCollector.isContainerReady(host, config.tritonPort)) {
                long took = System.currentTimeMillis() - start;
                log.info("Container {} passed warmup in {}ms ({} probes)", cid, took, attempt);
                metricsCollector.recordAlert("warmup_ok",
                        "Container " + cid + " ready after " + took + "ms",
                        "info");
                registerInZooKeeper(container);
                return;
            }
            try {
                Thread.sleep(poll);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        // Timeout: register anyway to avoid wedging capacity, but warn loudly.
        metricsCollector.recordAlert("warmup_timeout",
                "Container " + cid + " did not pass /v2/health/ready within "
                        + config.warmupTimeoutMs + "ms — registering to preserve capacity, expect cold-start latency",
                "warning");
        registerInZooKeeper(container);
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

    /**
     * Build a YARN LocalResource map for an HDFS model repository (or single archive). Walks the
     * tree and registers each file under {@code models/<relative-path>}; YARN's NM downloads them
     * to the container's working directory before {@code docker run}, and the docker container
     * runtime mounts them in (when {@code YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS=true}).
     *
     * <p>If the path points at a {@code .tar.gz}/{@code .zip} file, it is registered as ARCHIVE
     * type and YARN extracts it under {@code ./models/} on the NM. Otherwise files are localized
     * individually preserving the directory layout.
     */
    private Map<String, LocalResource> buildHdfsModelLocalResources(String hdfsUri) throws IOException {
        Map<String, LocalResource> resources = new HashMap<>();
        Path root = new Path(hdfsUri);
        FileSystem fs = root.getFileSystem(conf);
        if (!fs.exists(root)) {
            throw new IOException("Model repository does not exist: " + hdfsUri);
        }

        FileStatus rootStatus = fs.getFileStatus(root);
        String name = root.getName().toLowerCase();
        if (rootStatus.isFile() && (name.endsWith(".tar.gz") || name.endsWith(".tgz") || name.endsWith(".zip"))) {
            LocalResource res = Records.newRecord(LocalResource.class);
            res.setResource(URL.fromPath(root));
            res.setSize(rootStatus.getLen());
            res.setTimestamp(rootStatus.getModificationTime());
            res.setType(LocalResourceType.ARCHIVE);
            res.setVisibility(LocalResourceVisibility.APPLICATION);
            resources.put("models", res);
            return resources;
        }
        if (!rootStatus.isDirectory()) {
            throw new IOException("Model repository must be a directory or .tar.gz/.tgz/.zip archive: " + hdfsUri);
        }

        // Recursive walk over the model dir; register each FILE as a LocalResource preserving
        // the relative directory structure under models/...
        org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it = fs.listFiles(root, true);
        String rootUriPath = root.toUri().getPath();
        while (it.hasNext()) {
            org.apache.hadoop.fs.LocatedFileStatus f = it.next();
            String relPath = f.getPath().toUri().getPath();
            if (relPath.startsWith(rootUriPath + "/")) {
                relPath = relPath.substring(rootUriPath.length() + 1);
            } else if (relPath.equals(rootUriPath)) {
                continue;
            }
            LocalResource res = Records.newRecord(LocalResource.class);
            res.setResource(URL.fromPath(f.getPath()));
            res.setSize(f.getLen());
            res.setTimestamp(f.getModificationTime());
            res.setType(LocalResourceType.FILE);
            res.setVisibility(LocalResourceVisibility.APPLICATION);
            resources.put("models/" + relPath, res);
        }
        return resources;
    }

    private class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
        @Override
        public void onContainersAllocated(List<Container> containers) {
            for (Container container : containers) {
                log.info("Container allocated: {}. Launching Triton...", container.getId());
                launchTriton(container);
                runningContainers.add(container);
                // Registration in ZooKeeper is DEFERRED to post-warmup (see scheduleWarmup
                // triggered in NMCallbackHandler.onContainerStarted). Registering here would
                // have Knox route traffic to a container that has not yet loaded models.
            }
        }

        private void launchTriton(Container container) {
            ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

            Map<String, String> env = new HashMap<>();
            env.put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
            env.put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", config.tritonImage);
            // Keep the image entrypoint disabled so YARN wraps the launch command with `bash -c`.
            // With use-entry-point=true the image entrypoint receives the full launch string as a
            // single arg and `exec "$@"`s it as one binary path — which crashes with exit 127.
            env.put("YARN_CONTAINER_RUNTIME_DOCKER_RUN_OVERRIDE_DISABLE", "false");
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

            // Propagate the AM's HDFS delegation tokens so the NM can localize HDFS resources
            // (model repo files, jar) on behalf of the application user on a Kerberized cluster.
            try {
                org.apache.hadoop.security.Credentials creds =
                        org.apache.hadoop.security.UserGroupInformation.getCurrentUser().getCredentials();
                if (creds != null && creds.numberOfTokens() > 0) {
                    org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer();
                    creds.writeTokenStorageToStream(dob);
                    ctx.setTokens(java.nio.ByteBuffer.wrap(dob.getData(), 0, dob.getLength()));
                }
            } catch (IOException e) {
                log.warn("Failed to attach AM credentials to child container: {}", e.getMessage());
            }

            // For hdfs:// model repos, register the model tree as YARN LocalResources so the NM
            // localizes them to the container's working directory before docker run. The Triton
            // image has no `hadoop` CLI, so doing the fetch from inside the container (the old
            // approach) is broken. With YARN_CONTAINER_RUNTIME_DOCKER_LOCAL_RESOURCE_MOUNTS=true,
            // the localized files are auto-mounted into the docker container; we point Triton at
            // ./models inside the container.
            if (config.modelRepository != null && config.modelRepository.startsWith("hdfs://")) {
                try {
                    Map<String, LocalResource> modelResources = buildHdfsModelLocalResources(config.modelRepository);
                    if (!modelResources.isEmpty()) {
                        ctx.setLocalResources(modelResources);
                        log.info("Registered {} HDFS model files as YARN LocalResources for container {}",
                                modelResources.size(), container.getId());
                    } else {
                        log.warn("HDFS model path {} produced no LocalResources (empty dir?)",
                                config.modelRepository);
                    }
                } catch (IOException e) {
                    log.error("Failed to localize HDFS model repo {}: {}",
                            config.modelRepository, e.getMessage(), e);
                    metricsCollector.recordContainerFailure(container.getId().toString(),
                            "Model localization failed: " + e.getMessage());
                    return;
                }
            }

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
            log.info("Container started: {}. Beginning warmup...", containerId);
            metricsCollector.recordContainerReady(containerId.toString());
            scheduleWarmupAndRegister(containerId);
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

    /**
     * Reads {@code lora.json} from the root of the model repository, a {@code {base: [lora, ...]}}
     * mapping. Triton's {@code openai_frontend} consumes this file natively; TARN re-reads it to
     * populate the dashboard and to apply Ranger policies to LoRA adapters as separate resources.
     * Cached result is recomputed every call — the file is small and operators rotate adapters
     * without restarting the AM.
     */
    public Map<String, List<String>> getAvailableLoraAdapters() {
        Map<String, List<String>> empty = Collections.emptyMap();
        if (config.modelRepository == null || config.modelRepository.isEmpty()) return empty;
        try {
            Path loraPath = new Path(config.modelRepository, "lora.json");
            FileSystem fs = loraPath.getFileSystem(conf);
            if (!fs.exists(loraPath)) return empty;
            try (java.io.InputStream in = fs.open(loraPath)) {
                byte[] bytes = in.readAllBytes();
                com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
                return om.readValue(bytes,
                        new com.fasterxml.jackson.core.type.TypeReference<Map<String, List<String>>>() { });
            }
        } catch (Exception e) {
            log.warn("Failed to read lora.json from model repository: {}", e.getMessage());
            return empty;
        }
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

    public QuotaEnforcer getQuotaEnforcer() {
        return quotaEnforcer;
    }

    /**
     * Writes a new quota JSON to the shared ZK config znode so every AM replica picks it up
     * through its {@link NodeCache} listener. This is the multi-replica write path used by the
     * admin endpoint; a pure-local reload happens on a subsequent NodeCache event.
     */
    public boolean publishQuotasToZk(String json) {
        if (zkClient == null) {
            // Single-instance fallback: apply locally so the operator sees the effect even
            // when ZK isn't configured.
            quotaEnforcer.loadFromJson(json);
            return false;
        }
        try {
            String path = configRootPath() + "/quotas";
            if (zkClient.checkExists().forPath(path) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(path);
            }
            zkClient.setData().forPath(path, json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return true;
        } catch (Exception e) {
            log.error("Failed to publish quotas to ZK: {}", e.getMessage());
            // Degrade gracefully: apply locally even if ZK write failed.
            quotaEnforcer.loadFromJson(json);
            return false;
        }
    }

    /**
     * Reads the quota JSON file from HDFS (or local) and replaces the in-memory rule set.
     * Called on startup and again by the hot-reload watcher (P2.8).
     */
    public void loadQuotasFromConfig() {
        if (config.quotasPath == null || config.quotasPath.isEmpty()) {
            return;
        }
        try {
            Path p = new Path(config.quotasPath);
            FileSystem fs = p.getFileSystem(conf);
            if (!fs.exists(p)) {
                log.warn("Quotas path {} does not exist", config.quotasPath);
                return;
            }
            byte[] bytes;
            try (java.io.InputStream in = fs.open(p)) {
                bytes = in.readAllBytes();
            }
            quotaEnforcer.loadFromJson(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.error("Failed to load quotas from {}: {}", config.quotasPath, e.getMessage());
        }
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
