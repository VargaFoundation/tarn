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
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import varga.tarn.yarn.QuotaEnforcer;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests against an embedded ZooKeeper (Curator TestingServer).
 *
 * <p>These lock down the two hardest-to-mock behaviours:
 * <ul>
 *   <li>Quota hot-reload: write quota JSON to a config znode, verify {@link QuotaEnforcer}
 *       rules update within one Curator event without a restart.</li>
 *   <li>Ephemeral instance registration: write container entries, check that ZK removes them
 *       automatically when the session closes (simulating AM crash).</li>
 *   <li>Reconnect re-registration: bounce the server to force a session loss, verify the
 *       client detects the disconnect and re-establishes.</li>
 * </ul>
 *
 * <p>These exercise real Curator wiring rather than mocks, so they catch races and API
 * misuse that a pure unit test would hide.
 */
public class ZooKeeperIntegrationTest {

    private TestingServer zkServer;
    private CuratorFramework client;

    @BeforeEach
    void setup() throws Exception {
        zkServer = new TestingServer(true);
        client = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                new ExponentialBackoffRetry(100, 3));
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
    }

    @AfterEach
    void teardown() throws Exception {
        if (client != null) client.close();
        if (zkServer != null) zkServer.close();
    }

    @Test
    public void quotaHotReloadPropagatesViaZkEvent() throws Exception {
        String path = "/services/triton/config/quotas";
        client.create().creatingParentsIfNeeded().forPath(path, new byte[0]);

        QuotaEnforcer enforcer = new QuotaEnforcer();
        CountDownLatch updateSeen = new CountDownLatch(1);
        NodeCache cache = new NodeCache(client, path);
        cache.getListenable().addListener(new NodeCacheListener() {
            @Override public void nodeChanged() {
                byte[] data = cache.getCurrentData() == null ? null : cache.getCurrentData().getData();
                if (data != null && data.length > 0) {
                    enforcer.loadFromJson(new String(data, StandardCharsets.UTF_8));
                    updateSeen.countDown();
                }
            }
        });
        cache.start(true);

        // Before reload: no rules, anything goes.
        assertTrue(enforcer.check("alice", Collections.emptySet(), "m").allowed);

        // Push new rules.
        byte[] json = "{\"rules\":[{\"user\":\"alice\",\"model\":\"*\",\"requestsPerMinute\":1}]}"
                .getBytes(StandardCharsets.UTF_8);
        client.setData().forPath(path, json);

        assertTrue(updateSeen.await(5, TimeUnit.SECONDS),
                "NodeCache did not fire within 5s of ZK update");
        // After reload: alice gets 1 rpm, second call refused.
        assertTrue(enforcer.check("alice", Collections.emptySet(), "m").allowed);
        assertFalse(enforcer.check("alice", Collections.emptySet(), "m").allowed,
                "rule must take effect after hot-reload");
        cache.close();
    }

    @Test
    public void ephemeralInstancesVanishOnSessionClose() throws Exception {
        String base = "/services/triton/instances";
        client.create().creatingParentsIfNeeded().forPath(base);

        // Open a second client (simulates the AM) and create an ephemeral znode.
        CuratorFramework amClient = CuratorFrameworkFactory.newClient(
                zkServer.getConnectString(), new ExponentialBackoffRetry(100, 3));
        amClient.start();
        assertTrue(amClient.blockUntilConnected(5, TimeUnit.SECONDS));
        amClient.create().withMode(CreateMode.EPHEMERAL).forPath(base + "/container_1",
                "host1:8000".getBytes(StandardCharsets.UTF_8));

        assertNotNull(client.checkExists().forPath(base + "/container_1"));
        assertEquals(1, client.getChildren().forPath(base).size());

        // Close the AM's session — its ephemerals must disappear.
        amClient.close();

        // Ephemeral removal can lag by the session timeout; allow up to 10s.
        long deadline = System.currentTimeMillis() + 10_000L;
        while (System.currentTimeMillis() < deadline
                && client.checkExists().forPath(base + "/container_1") != null) {
            Thread.sleep(100);
        }
        assertNull(client.checkExists().forPath(base + "/container_1"),
                "ephemeral znode must be removed after owning client closes");
    }

    @Test
    public void connectionStateListenerFiresOnReconnect() throws Exception {
        AtomicInteger reconnects = new AtomicInteger();
        CountDownLatch reconnected = new CountDownLatch(1);
        client.getConnectionStateListenable().addListener((c, state) -> {
            if (state == ConnectionState.RECONNECTED) {
                reconnects.incrementAndGet();
                reconnected.countDown();
            }
        });
        // Force a disconnect by restarting the embedded server.
        zkServer.restart();
        assertTrue(reconnected.await(10, TimeUnit.SECONDS),
                "ConnectionStateListener RECONNECTED should fire after server restart");
        assertTrue(reconnects.get() >= 1);
    }
}
