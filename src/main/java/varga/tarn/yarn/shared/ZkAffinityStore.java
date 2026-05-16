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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.stream.Collectors;

/**
 * ZooKeeper-backed conversation affinity. Reads are served from a local {@link CuratorCache}
 * mirror (no per-turn round-trip); writes are throttled to collapse the common "many turns, same
 * container" case to roughly one write per half-TTL. Eviction and purge issue idempotent deletes,
 * so concurrent replicas racing on the same expired entry is harmless.
 *
 * <p>Entry layout: one znode per conversation at {@code <base>/<sha1(conversationId)>} whose data
 * is {@code "<containerId>|<expiryEpochMs>"}. Conversation ids are hashed so arbitrary client
 * values are legal znode names and bounded in length.
 */
final class ZkAffinityStore implements AffinityStore {

    private static final Logger log = LoggerFactory.getLogger(ZkAffinityStore.class);

    private final CuratorFramework client;
    private final String basePath;
    private final CuratorCache cache;

    ZkAffinityStore(CuratorFramework client, String basePath) {
        this.client = client;
        this.basePath = basePath;
        this.cache = CuratorCache.build(client, basePath);
    }

    void start() {
        cache.start();
    }

    void close() {
        cache.close();
    }

    @Override
    public String get(String conversationId) {
        if (conversationId == null || conversationId.isEmpty()) return null;
        String path = pathFor(conversationId);
        ChildData cd = cache.get(path).orElse(null);
        if (cd == null) return null;
        String[] parts = decode(cd.getData());
        if (parts == null) return null;
        if (System.currentTimeMillis() > parseLong(parts[1])) {
            deleteQuietly(path); // lazy expiry; ignore races
            return null;
        }
        return parts[0];
    }

    @Override
    public void put(String conversationId, String containerId, long ttlMs) {
        if (conversationId == null || conversationId.isEmpty() || containerId == null) return;
        long now = System.currentTimeMillis();
        String path = pathFor(conversationId);

        // Throttle off the local cache mirror (no extra unbounded state): if the same container
        // is already recorded with more than half its TTL remaining, it was written less than
        // half a TTL ago, so skip the rewrite. This collapses "many turns, same container" to
        // ~one write per half-TTL while staying bounded by the cache (which purgeExpired prunes).
        ChildData cd = cache.get(path).orElse(null);
        if (cd != null) {
            String[] parts = decode(cd.getData());
            if (parts != null && containerId.equals(parts[0]) && (parseLong(parts[1]) - now) > (ttlMs / 2)) {
                return;
            }
        }

        String data = containerId + "|" + (now + ttlMs);
        try {
            client.create().orSetData().creatingParentsIfNeeded()
                    .forPath(path, data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            // Best-effort: a failed write just means the next turn may not reuse the KV cache.
            log.debug("affinity put failed for {}: {}", conversationId, e.toString());
        }
    }

    @Override
    public int evictByContainer(String containerId) {
        if (containerId == null) return 0;
        int removed = 0;
        for (ChildData cd : entries()) {
            String[] parts = decode(cd.getData());
            if (parts != null && containerId.equals(parts[0])) {
                deleteQuietly(cd.getPath());
                removed++;
            }
        }
        return removed;
    }

    @Override
    public int purgeExpired() {
        long now = System.currentTimeMillis();
        int removed = 0;
        for (ChildData cd : entries()) {
            String[] parts = decode(cd.getData());
            if (parts != null && now > parseLong(parts[1])) {
                deleteQuietly(cd.getPath());
                removed++;
            }
        }
        return removed;
    }

    @Override
    public int size() {
        return entries().size();
    }

    /** Snapshot of direct child entries (collected so we can delete while iterating). */
    private List<ChildData> entries() {
        return cache.stream()
                .filter(cd -> isEntry(cd.getPath()))
                .collect(Collectors.toList());
    }

    private boolean isEntry(String path) {
        return path.length() > basePath.length()
                && path.startsWith(basePath + "/")
                && path.indexOf('/', basePath.length() + 1) < 0;
    }

    private String pathFor(String conversationId) {
        return basePath + "/" + sha1(conversationId);
    }

    private void deleteQuietly(String path) {
        try {
            client.delete().quietly().forPath(path);
        } catch (Exception ignore) {
            // idempotent — another replica may have removed it already
        }
    }

    private static String[] decode(byte[] data) {
        if (data == null || data.length == 0) return null;
        String s = new String(data, StandardCharsets.UTF_8);
        int sep = s.lastIndexOf('|');
        if (sep <= 0 || sep == s.length() - 1) return null;
        return new String[]{s.substring(0, sep), s.substring(sep + 1)};
    }

    private static long parseLong(String s) {
        try {
            return Long.parseLong(s.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
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
