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
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link WindowedCounter} backed by HBase — the Hadoop-native equivalent of Redis
 * {@code INCR}/{@code EXPIRE}. Each window is a distinct row ({@code key@windowId}); the atomic
 * server-side {@code Increment} gives exact cross-replica counts with no read-modify-write race,
 * and a per-cell TTL ({@code Increment.setTTL}) lets old windows expire on their own.
 *
 * <p>Reads/writes fail-open (return {@code 0}) on a transient HBase error: for protective ceilings,
 * staying available beats hard-failing inference because the counter store hiccupped.
 */
final class HBaseWindowedCounter implements WindowedCounter {

    private static final Logger log = LoggerFactory.getLogger(HBaseWindowedCounter.class);
    static final byte[] CF = Bytes.toBytes("d");
    static final byte[] QUAL = Bytes.toBytes("v");

    private final Connection connection;
    private final TableName tableName;

    HBaseWindowedCounter(Connection connection, TableName tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    @Override
    public long incrementAndGet(String key, long delta, long windowMs) {
        try (Table table = connection.getTable(tableName)) {
            Increment inc = new Increment(row(key, windowMs)).addColumn(CF, QUAL, delta);
            inc.setTTL(Math.max(1000L, windowMs * 2)); // cell outlives its window, then expires
            Result r = table.increment(inc);
            byte[] v = r.getValue(CF, QUAL);
            return v == null ? delta : Bytes.toLong(v);
        } catch (Exception e) {
            log.warn("HBase increment failed for {} (failing open): {}", key, e.toString());
            return 0L;
        }
    }

    @Override
    public long get(String key, long windowMs) {
        try (Table table = connection.getTable(tableName)) {
            Result r = table.get(new Get(row(key, windowMs)).addColumn(CF, QUAL));
            byte[] v = r.getValue(CF, QUAL);
            return v == null ? 0L : Bytes.toLong(v);
        } catch (Exception e) {
            log.warn("HBase get failed for {} (failing open): {}", key, e.toString());
            return 0L;
        }
    }

    private static byte[] row(String key, long windowMs) {
        long win = System.currentTimeMillis() / Math.max(1L, windowMs);
        return Bytes.toBytes(key + "@" + win);
    }
}
