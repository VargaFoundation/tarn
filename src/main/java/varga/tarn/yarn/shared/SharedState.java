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
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Backend that makes throttling and routing state correct across horizontally-scaled replicas.
 *
 * <p>Two modes:
 * <ul>
 *   <li>{@code local} (default): single-replica semantics. {@link #rateLimits()} and
 *       {@link #affinity()} return {@code null}, so {@link varga.tarn.yarn.QuotaEnforcer},
 *       {@link varga.tarn.yarn.GlobalRateLimiter} and
 *       {@link varga.tarn.yarn.openai.ConversationAffinity} keep their in-process state — byte
 *       for byte the historical behaviour. This is the right choice on YARN (one Application
 *       Master) and for K8s deployments with {@code replicaCount: 1}.</li>
 *   <li>{@code zk}: reuses the existing Curator client. Live-replica count comes from ephemeral
 *       membership znodes; rate limits and quotas are enforced fair-share (each replica gets
 *       {@code ceil(limit / liveReplicas)}); conversation affinity is shared so a follow-up turn
 *       on any replica resolves the same container.</li>
 * </ul>
 *
 * <p>A Redis-backed mode (precise counters + native TTL) is a planned follow-up behind these same
 * interfaces; it is intentionally not wired yet so the default uber-JAR carries no extra deps.
 */
public interface SharedState extends AutoCloseable {

    /** {@code "local"} or {@code "zk"} — surfaced on the dashboard and {@code /metrics}. */
    String mode();

    /** Number of live replicas sharing this state. Always {@code >= 1}; {@code local} returns 1. */
    int liveReplicaCount();

    /** Stable identifier for this replica (for membership / debugging). */
    String replicaId();

    /** Rate-limit / quota backend, or {@code null} to use the caller's in-process buckets. */
    RateLimitStore rateLimits();

    /** Affinity backend, or {@code null} to use the caller's in-process map. */
    AffinityStore affinity();

    /**
     * Shared windowed counter for <em>precise</em> budget accounting, or {@code null} when the
     * backend only offers fair-share (local / zk). When present, token/cost budgets are enforced
     * exactly across replicas instead of being divided by the live replica count.
     */
    default WindowedCounter counters() {
        return null;
    }

    /** Joins membership and starts caches. No-op for {@code local}. */
    void start() throws Exception;

    @Override
    void close();

    /**
     * Builds the provider for the configured mode. Falls back to {@code local} (with a warning)
     * when {@code zk} is requested but no Curator client is available, mirroring the graceful
     * degradation of {@code initZookeeper()}.
     */
    static SharedState create(String mode, String sharedPath, CuratorFramework zk,
                              Configuration hadoopConf, String hbaseTable) {
        Logger log = LoggerFactory.getLogger(SharedState.class);
        String m = mode == null ? "local" : mode.trim().toLowerCase(Locale.ROOT);
        switch (m) {
            case "zk":
                if (zk == null) {
                    log.warn("--shared-state=zk but no ZooKeeper ensemble is configured; "
                            + "falling back to local (per-replica) enforcement");
                    return new LocalSharedState();
                }
                return new ZkSharedState(zk, sharedPath);
            case "hbase":
                // Precise (exact) cross-replica enforcement via HBase Increment + TTL.
                return new HBaseSharedState(hadoopConf, hbaseTable);
            case "local":
                return new LocalSharedState();
            default:
                log.warn("Unknown --shared-state '{}'; using local", mode);
                return new LocalSharedState();
        }
    }
}
