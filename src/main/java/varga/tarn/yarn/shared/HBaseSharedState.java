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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * HBase-backed {@link SharedState}: <em>precise</em> cross-replica enforcement, Hadoop-native (no
 * external Redis). A single auto-created table holds atomically-incremented windowed counters
 * (rate limit, quotas, token/cost budgets) and TTL'd conversation affinity, all in one column
 * family with per-cell TTLs.
 *
 * <p>Because counters are exact, there is no fair-share division and {@link #liveReplicaCount()} is
 * not used for enforcement (it reports 1; the {@code zk} backend is the one that tracks live
 * replicas). The HBase connection is read from {@code hbase-site.xml} on the classpath merged with
 * the AM's Hadoop {@link Configuration}.
 */
public final class HBaseSharedState implements SharedState {

    private static final Logger log = LoggerFactory.getLogger(HBaseSharedState.class);
    // Backstop TTL on the column family (2 days); per-cell TTLs (window length / affinity TTL) are
    // shorter and take precedence — this just bounds anything written without an explicit TTL.
    private static final int CF_TTL_SECONDS = 2 * 24 * 60 * 60;

    private final Configuration conf;
    private final TableName tableName;
    private final String replicaId = UUID.randomUUID().toString();

    private Connection connection;
    private HBaseWindowedCounter counter;
    private HBaseAffinityStore affinityStore;
    private RateLimitStore rateLimitStore;

    public HBaseSharedState(Configuration hadoopConf, String table) {
        this.conf = HBaseConfiguration.create(hadoopConf);
        this.tableName = TableName.valueOf(table == null || table.isEmpty() ? "tarn_shared" : table);
    }

    @Override
    public void start() throws Exception {
        connection = ConnectionFactory.createConnection(conf);
        ensureTable();
        counter = new HBaseWindowedCounter(connection, tableName);
        affinityStore = new HBaseAffinityStore(connection, tableName);
        rateLimitStore = new CountingRateLimitStore(counter);
        log.info("HBase shared-state started (table={}, replicaId={})", tableName, replicaId);
    }

    private void ensureTable() throws Exception {
        try (Admin admin = connection.getAdmin()) {
            if (!admin.tableExists(tableName)) {
                admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder
                                .newBuilder(HBaseWindowedCounter.CF)
                                .setTimeToLive(CF_TTL_SECONDS)
                                .build())
                        .build());
                log.info("Created HBase table {}", tableName);
            }
        }
    }

    @Override public String mode() { return "hbase"; }

    /** Not tracked: enforcement is precise, so no fair-share division is needed. */
    @Override public int liveReplicaCount() { return 1; }

    @Override public String replicaId() { return replicaId; }

    @Override public RateLimitStore rateLimits() { return rateLimitStore; }

    @Override public AffinityStore affinity() { return affinityStore; }

    @Override public WindowedCounter counters() { return counter; }

    @Override
    public void close() {
        if (connection != null) {
            try { connection.close(); } catch (Exception ignore) { /* shutting down */ }
        }
    }
}
