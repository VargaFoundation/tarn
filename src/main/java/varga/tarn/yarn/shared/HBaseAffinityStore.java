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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Conversation affinity backed by HBase, leaning on the native per-cell TTL ({@code Put.setTTL}):
 * entries expire on their own, so there is no purge loop and a {@code get} on an expired row simply
 * returns {@code null}. A write per turn refreshes the TTL (HBase is built for write throughput),
 * keeping active conversations pinned.
 *
 * <p>{@code evictByContainer} / {@code purgeExpired} are no-ops: TTL handles expiry, and the proxy
 * re-checks {@code isContainerReady} on the pinned id before using it, so a mapping to a reaped
 * container is harmless (it falls back to least-loaded).
 */
final class HBaseAffinityStore implements AffinityStore {

    private static final Logger log = LoggerFactory.getLogger(HBaseAffinityStore.class);
    static final byte[] CF = Bytes.toBytes("d");
    static final byte[] QUAL = Bytes.toBytes("c");

    private final Connection connection;
    private final TableName tableName;

    HBaseAffinityStore(Connection connection, TableName tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    @Override
    public String get(String conversationId) {
        if (conversationId == null || conversationId.isEmpty()) return null;
        try (Table table = connection.getTable(tableName)) {
            Result r = table.get(new Get(Bytes.toBytes(sha1(conversationId))).addColumn(CF, QUAL));
            byte[] v = r.getValue(CF, QUAL);
            return v == null ? null : Bytes.toString(v);
        } catch (Exception e) {
            log.debug("HBase affinity get failed for {}: {}", conversationId, e.toString());
            return null;
        }
    }

    @Override
    public void put(String conversationId, String containerId, long ttlMs) {
        if (conversationId == null || conversationId.isEmpty() || containerId == null) return;
        try (Table table = connection.getTable(tableName)) {
            Put p = new Put(Bytes.toBytes(sha1(conversationId)))
                    .addColumn(CF, QUAL, Bytes.toBytes(containerId));
            p.setTTL(Math.max(1000L, ttlMs)); // native expiry — no purge loop needed
            table.put(p);
        } catch (Exception e) {
            log.debug("HBase affinity put failed for {}: {}", conversationId, e.toString());
        }
    }

    @Override
    public int evictByContainer(String containerId) {
        return 0; // TTL + the proxy's readiness re-check make stale mappings harmless
    }

    @Override
    public int purgeExpired() {
        return 0; // native per-cell TTL
    }

    @Override
    public int size() {
        return 0; // not tracked (would require a full table scan)
    }

    private static String sha1(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(d.length * 2);
            for (byte b : d) {
                sb.append(Character.forDigit((b >> 4) & 0xf, 16));
                sb.append(Character.forDigit(b & 0xf, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            return Integer.toHexString(s.hashCode());
        }
    }
}
