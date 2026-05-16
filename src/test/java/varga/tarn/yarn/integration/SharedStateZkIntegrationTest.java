package varga.tarn.yarn.integration;

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
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import varga.tarn.yarn.QuotaEnforcer;
import varga.tarn.yarn.shared.ZkSharedState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Multi-replica integration tests for the ZooKeeper-backed {@link varga.tarn.yarn.shared.SharedState}.
 * Each "replica" is a {@link ZkSharedState} over its own Curator client against a single embedded
 * {@link TestingServer}, so several replicas run inside one JVM — the same trick used by
 * {@link ZooKeeperIntegrationTest}.
 *
 * <p>These lock down the correctness wins of {@code --shared-state=zk}: fair-share rate limiting
 * and quotas summing to the configured ceiling across replicas, live-membership tracking, shared
 * conversation affinity (cross-replica and restart-surviving), write-throttling, and recovery of
 * membership after a ZooKeeper bounce.
 */
public class SharedStateZkIntegrationTest {

    private static final String BASE = "/services/triton/shared";

    private TestingServer zkServer;
    private final List<CuratorFramework> clients = new ArrayList<>();
    private final List<ZkSharedState> providers = new ArrayList<>();

    @BeforeEach
    void setup() throws Exception {
        zkServer = new TestingServer(true);
    }

    @AfterEach
    void teardown() throws Exception {
        for (ZkSharedState p : providers) {
            try { p.close(); } catch (Exception ignore) { }
        }
        for (CuratorFramework c : clients) {
            try { c.close(); } catch (Exception ignore) { }
        }
        if (zkServer != null) zkServer.close();
    }

    private ZkSharedState newReplica() throws Exception {
        CuratorFramework c = CuratorFrameworkFactory.newClient(
                zkServer.getConnectString(), new ExponentialBackoffRetry(100, 3));
        c.start();
        assertTrue(c.blockUntilConnected(5, TimeUnit.SECONDS));
        clients.add(c);
        ZkSharedState s = new ZkSharedState(c, BASE);
        s.start();
        providers.add(s);
        return s;
    }

    private static void awaitTrue(BooleanSupplier cond, long timeoutMs, String msg)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(50);
        }
        fail(msg);
    }

    @Test
    public void globalRateLimitIsFairSharedAcrossReplicas() throws Exception {
        ZkSharedState a = newReplica();
        ZkSharedState b = newReplica();
        ZkSharedState c = newReplica();
        awaitTrue(() -> a.liveReplicaCount() == 3 && b.liveReplicaCount() == 3 && c.liveReplicaCount() == 3,
                10_000, "membership did not settle to 3 replicas");

        int total = 0;
        for (ZkSharedState s : List.of(a, b, c)) {
            int ok = 0;
            for (int i = 0; i < 9; i++) {
                if (s.rateLimits().acquireGlobal(9) == 0L) ok++;
            }
            assertEquals(3, ok, "each replica should admit cap/N = 9/3 = 3");
            total += ok;
        }
        assertEquals(9, total, "the per-replica shares must sum to the cluster-wide cap");
    }

    @Test
    public void quotaIsFairSharedThroughQuotaEnforcer() throws Exception {
        ZkSharedState a = newReplica();
        ZkSharedState b = newReplica();
        awaitTrue(() -> a.liveReplicaCount() == 2 && b.liveReplicaCount() == 2,
                10_000, "membership did not settle to 2 replicas");

        QuotaEnforcer qeA = new QuotaEnforcer();
        qeA.setRateLimitStore(a.rateLimits());
        qeA.setGlobalLimit(6); // match-all rule, 6 rpm cluster-wide
        QuotaEnforcer qeB = new QuotaEnforcer();
        qeB.setRateLimitStore(b.rateLimits());
        qeB.setGlobalLimit(6);

        int okA = 0;
        for (int i = 0; i < 6; i++) {
            if (qeA.check("alice", Collections.emptySet(), "m").allowed) okA++;
        }
        int okB = 0;
        for (int i = 0; i < 6; i++) {
            if (qeB.check("alice", Collections.emptySet(), "m").allowed) okB++;
        }
        assertEquals(3, okA, "replica A admits half of the 6 rpm");
        assertEquals(3, okB, "replica B admits the other half");
        assertEquals(6, okA + okB, "the rpm holds cluster-wide, not per-replica");
    }

    @Test
    public void liveReplicaCountTracksMembershipChurn() throws Exception {
        ZkSharedState a = newReplica();
        ZkSharedState b = newReplica();
        ZkSharedState c = newReplica();
        awaitTrue(() -> a.liveReplicaCount() == 3, 10_000, "did not reach 3 replicas");

        // A replica leaves: its PersistentNode (ephemeral) is removed on close.
        c.close();
        providers.remove(c);
        awaitTrue(() -> a.liveReplicaCount() == 2 && b.liveReplicaCount() == 2,
                12_000, "count should drop to 2 after a replica leaves");
    }

    @Test
    public void affinityIsVisibleAcrossReplicas() throws Exception {
        ZkSharedState a = newReplica();
        ZkSharedState b = newReplica();
        String container = "container_e1_1_01_000007";
        a.affinity().put("conv-1", container, 60_000L);
        awaitTrue(() -> container.equals(b.affinity().get("conv-1")),
                5_000, "affinity written on replica A should be readable on replica B");
    }

    @Test
    public void affinitySurvivesProviderRestart() throws Exception {
        ZkSharedState a = newReplica();
        String container = "container_e1_1_01_000009";
        a.affinity().put("conv-2", container, 60_000L);
        awaitTrue(() -> container.equals(a.affinity().get("conv-2")), 5_000, "write did not land");

        a.close();
        providers.remove(a);

        // A fresh replica re-attaches to the persisted affinity entry.
        ZkSharedState a2 = newReplica();
        awaitTrue(() -> container.equals(a2.affinity().get("conv-2")),
                5_000, "affinity must survive a provider restart (persistent znode)");
    }

    @Test
    public void repeatedAffinityPutsAreThrottled() throws Exception {
        ZkSharedState a = newReplica();
        String affinityPath = BASE + "/affinity";
        a.affinity().put("conv-3", "container_a", 60_000L);
        awaitTrue(() -> a.affinity().get("conv-3") != null, 5_000, "first write did not land");

        List<String> children = clients.get(0).getChildren().forPath(affinityPath);
        assertEquals(1, children.size(), "exactly one affinity znode expected");
        String znode = affinityPath + "/" + children.get(0);
        int versionBefore = clients.get(0).checkExists().forPath(znode).getVersion();

        // Same container, within half the TTL: must be coalesced (no ZK rewrite).
        for (int i = 0; i < 5; i++) {
            a.affinity().put("conv-3", "container_a", 60_000L);
        }
        int versionAfter = clients.get(0).checkExists().forPath(znode).getVersion();
        assertEquals(versionBefore, versionAfter,
                "repeated puts of the same container within half-TTL must not rewrite the znode");
    }

    @Test
    public void membershipRecoversAfterZooKeeperBounce() throws Exception {
        ZkSharedState a = newReplica();
        String memberPath = BASE + "/members/" + a.replicaId();
        awaitTrue(() -> {
            try {
                return clients.get(0).checkExists().forPath(memberPath) != null;
            } catch (Exception e) {
                return false;
            }
        }, 5_000, "member node should exist after start");

        // Force a session loss + reconnect; the PersistentNode recipe must recreate the ephemeral.
        zkServer.restart();

        awaitTrue(() -> {
            try {
                return clients.get(0).checkExists().forPath(memberPath) != null;
            } catch (Exception e) {
                return false;
            }
        }, 20_000, "PersistentNode should recreate the membership ephemeral after a ZK bounce");
        // Rate limiting is still functional after recovery.
        assertEquals(0L, a.rateLimits().acquireGlobal(3));
    }
}
