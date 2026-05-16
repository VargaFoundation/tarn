package varga.tarn.yarn.shared;

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
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ZooKeeper-backed {@link SharedState} that reuses the AM's existing Curator client.
 *
 * <p>Membership is an ephemeral znode under {@code <base>/members/<uuid>}, maintained across
 * reconnects by the {@link PersistentNode} recipe; a {@link CuratorCache} over the members path
 * keeps the live-replica count (and this replica's sorted index, for fair-share remainder) in two
 * volatile fields, updated on cache events — no per-request ZooKeeper traffic.
 *
 * <p>During a ZK outage the recipes self-heal on reconnect; meanwhile the cached count is reused,
 * so rate limiting keeps enforcing (fail-functional rather than fail-open) with the last-known
 * share, and affinity reads continue from the cache mirror.
 */
public final class ZkSharedState implements SharedState {

    private static final Logger log = LoggerFactory.getLogger(ZkSharedState.class);

    private final CuratorFramework client;
    private final String basePath;
    private final String membersPath;
    private final String affinityPath;
    private final String myId = UUID.randomUUID().toString();

    private volatile int liveCount = 1;
    private volatile int myIndex = 0;

    private PersistentNode memberNode;
    private CuratorCache membersCache;
    private ZkAffinityStore affinityStore;
    private final ZkRateLimitStore rateLimitStore;

    public ZkSharedState(CuratorFramework client, String basePath) {
        this.client = client;
        this.basePath = basePath;
        this.membersPath = basePath + "/members";
        this.affinityPath = basePath + "/affinity";
        this.rateLimitStore = new ZkRateLimitStore(() -> liveCount, () -> myIndex);
    }

    @Override
    public void start() throws Exception {
        ensurePath(membersPath);
        ensurePath(affinityPath);

        memberNode = new PersistentNode(client, CreateMode.EPHEMERAL, false,
                membersPath + "/" + myId, myId.getBytes(StandardCharsets.UTF_8));
        memberNode.start();
        memberNode.waitForInitialCreate(15, TimeUnit.SECONDS);

        membersCache = CuratorCache.build(client, membersPath);
        membersCache.listenable().addListener((type, oldData, data) -> recomputeMembership());
        membersCache.start();
        recomputeMembership();

        affinityStore = new ZkAffinityStore(client, affinityPath);
        affinityStore.start();

        log.info("ZooKeeper shared-state started at {} (replicaId={}, liveReplicas={})",
                basePath, myId, liveCount);
    }

    /** Recomputes live-replica count and this replica's sorted index from the members cache. */
    private void recomputeMembership() {
        List<String> names = new ArrayList<>();
        for (ChildData cd : membersCache.stream().collect(Collectors.toList())) {
            String p = cd.getPath();
            if (p.length() > membersPath.length()
                    && p.startsWith(membersPath + "/")
                    && p.indexOf('/', membersPath.length() + 1) < 0) {
                names.add(p.substring(membersPath.length() + 1));
            }
        }
        Collections.sort(names);
        int idx = names.indexOf(myId);
        this.myIndex = Math.max(0, idx);
        this.liveCount = Math.max(1, names.size());
    }

    private void ensurePath(String path) {
        try {
            if (client.checkExists().forPath(path) == null) {
                client.create().creatingParentsIfNeeded().forPath(path);
            }
        } catch (KeeperException.NodeExistsException ignore) {
            // raced with another replica — fine
        } catch (Exception e) {
            log.warn("Failed to ensure ZK path {}: {}", path, e.toString());
        }
    }

    @Override public String mode() { return "zk"; }

    @Override public int liveReplicaCount() { return liveCount; }

    @Override public String replicaId() { return myId; }

    @Override public RateLimitStore rateLimits() { return rateLimitStore; }

    @Override public AffinityStore affinity() { return affinityStore; }

    @Override
    public void close() {
        if (affinityStore != null) {
            try { affinityStore.close(); } catch (Exception ignore) { /* shutting down */ }
        }
        if (membersCache != null) {
            try { membersCache.close(); } catch (Exception ignore) { /* shutting down */ }
        }
        if (memberNode != null) {
            try { memberNode.close(); } catch (Exception ignore) { /* shutting down */ }
        }
    }
}
