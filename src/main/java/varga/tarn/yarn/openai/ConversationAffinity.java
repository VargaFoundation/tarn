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

import org.apache.hadoop.yarn.api.records.ContainerId;
import varga.tarn.yarn.shared.AffinityStore;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps client-supplied {@code X-Conversation-Id} headers to the Triton container that
 * served the conversation last, so follow-up turns can reuse the in-process KV cache
 * instead of recomputing the context. Entries expire after a configurable TTL — the
 * common case for chat is "a few minutes between turns", well below an hour, so the
 * default of one hour is a reasonable balance between cache reuse and stale state.
 *
 * <p>The map is unbounded; cleanup runs lazily on the read path (entry-by-entry as we
 * look them up) and via {@link #purgeExpired} from the AM monitor loop. When a container
 * is reaped by YARN, the AM calls {@link #evictByContainer} so we don't hand back a
 * dead destination.
 */
public final class ConversationAffinity {

    private final long ttlMs;
    // Optional shared backend. When installed (--shared-state=zk), affinity is shared across
    // replicas and survives a restart; null = the in-process map below (single-replica default).
    private volatile AffinityStore store;

    public ConversationAffinity(long ttlMs) {
        this.ttlMs = ttlMs;
    }

    /** Installs a shared affinity backend; pass {@code null} to keep the in-process map. */
    public void setAffinityStore(AffinityStore store) {
        this.store = store;
    }

    private static final class Entry {
        final ContainerId containerId;
        final long expiryMs;
        Entry(ContainerId c, long expiry) { this.containerId = c; this.expiryMs = expiry; }
    }

    private final Map<String, Entry> byConversation = new ConcurrentHashMap<>();

    /** Returns the recorded container id or {@code null} if absent / expired. */
    public ContainerId get(String conversationId) {
        if (conversationId == null || conversationId.isEmpty()) return null;
        AffinityStore s = store;
        if (s != null) {
            String cid = s.get(conversationId);
            return cid == null ? null : toContainerId(cid);
        }
        Entry e = byConversation.get(conversationId);
        if (e == null) return null;
        if (System.currentTimeMillis() > e.expiryMs) {
            byConversation.remove(conversationId, e);
            return null;
        }
        return e.containerId;
    }

    /** Records (or refreshes) the affinity. Caller has already verified the container is alive. */
    public void put(String conversationId, ContainerId containerId) {
        if (conversationId == null || conversationId.isEmpty() || containerId == null) return;
        AffinityStore s = store;
        if (s != null) {
            s.put(conversationId, containerId.toString(), ttlMs);
            return;
        }
        byConversation.put(conversationId, new Entry(containerId, System.currentTimeMillis() + ttlMs));
    }

    /** Drops every entry pinning the given container — called when the container is reaped. */
    public int evictByContainer(ContainerId containerId) {
        if (containerId == null) return 0;
        AffinityStore s = store;
        if (s != null) return s.evictByContainer(containerId.toString());
        int removed = 0;
        Iterator<Map.Entry<String, Entry>> it = byConversation.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Entry> e = it.next();
            if (containerId.equals(e.getValue().containerId)) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    /** Removes expired entries. Called periodically — cheap, O(entries). */
    public int purgeExpired() {
        AffinityStore s = store;
        if (s != null) return s.purgeExpired();
        long now = System.currentTimeMillis();
        int removed = 0;
        Iterator<Map.Entry<String, Entry>> it = byConversation.entrySet().iterator();
        while (it.hasNext()) {
            if (now > it.next().getValue().expiryMs) {
                it.remove();
                removed++;
            }
        }
        return removed;
    }

    public int size() {
        AffinityStore s = store;
        if (s != null) return s.size();
        return byConversation.size();
    }

    private static ContainerId toContainerId(String s) {
        try {
            return ContainerId.fromString(s);
        } catch (Exception e) {
            return null;
        }
    }
}
