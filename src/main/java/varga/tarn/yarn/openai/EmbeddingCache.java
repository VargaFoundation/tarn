package varga.tarn.yarn.openai;

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

import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded LRU cache of embedding responses. Embeddings are deterministic — the same model and
 * input always produce the same vector — so an exact-match cache is safe and, for RAG-style
 * workloads that re-embed the same documents and queries, very effective at skipping GPU work.
 *
 * <p>Keyed on a hash of the raw request body (model + input + any params are all in there), so any
 * difference is a clean miss, never a wrong hit. Opt-in via {@code --embedding-cache-size} (0
 * disables); per-process (a hit on one replica is cheap to recompute on another), so no shared
 * backend is needed. Entries store the upstream response bytes and content type verbatim.
 */
public final class EmbeddingCache {

    public static final class Entry {
        public final byte[] body;
        public final String contentType;

        public Entry(byte[] body, String contentType) {
            this.body = body;
            this.contentType = contentType;
        }
    }

    /** Access-order map that evicts the least-recently-used entry past the cap. */
    private static final class LruMap extends LinkedHashMap<String, Entry> {
        private static final long serialVersionUID = 1L;
        private final int max;

        LruMap(int max) {
            super(16, 0.75f, true);
            this.max = max;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Entry> eldest) {
            return size() > max;
        }
    }

    private final int maxEntries;
    private final LruMap lru;

    public EmbeddingCache(int maxEntries) {
        this.maxEntries = maxEntries;
        this.lru = new LruMap(Math.max(1, maxEntries));
    }

    public boolean isEnabled() {
        return maxEntries > 0;
    }

    public synchronized Entry get(String key) {
        return key == null ? null : lru.get(key);
    }

    public synchronized void put(String key, byte[] body, String contentType) {
        if (key == null || body == null || maxEntries <= 0) return;
        lru.put(key, new Entry(body, contentType));
    }

    public synchronized int size() {
        return lru.size();
    }

    /** Stable cache key for a request body (model, input and params are all encoded in it). */
    public static String keyFor(byte[] requestBody) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(requestBody);
            StringBuilder sb = new StringBuilder(d.length * 2);
            for (byte b : d) {
                sb.append(Character.forDigit((b >> 4) & 0xf, 16));
                sb.append(Character.forDigit(b & 0xf, 16));
            }
            return sb.toString();
        } catch (Exception e) {
            // SHA-256 is always available; degrade to a non-crypto key rather than failing.
            return Integer.toHexString(java.util.Arrays.hashCode(requestBody));
        }
    }
}
