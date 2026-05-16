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

/**
 * Cluster-shared conversation→container affinity, so a follow-up turn that lands on a different
 * replica (or after a restart) still resolves the container that holds the warm KV cache.
 *
 * <p>Container ids are exchanged as strings to keep YARN types out of the shared layer;
 * {@link varga.tarn.yarn.openai.ConversationAffinity} converts to/from {@code ContainerId}.
 * Reads are expected to be served from a local mirror (no per-turn round-trip); see
 * {@link varga.tarn.yarn.openai.ConversationAffinity} for the surrounding contract.
 */
public interface AffinityStore {

    /** @return the recorded container id, or {@code null} if absent / expired. */
    String get(String conversationId);

    /** Records (or refreshes) the affinity with the given TTL. Implementations may throttle writes. */
    void put(String conversationId, String containerId, long ttlMs);

    /** Drops every entry pinning the given container; returns the number removed. */
    int evictByContainer(String containerId);

    /** Removes expired entries; returns the number removed. */
    int purgeExpired();

    /** Approximate number of live affinity entries (for metrics / dashboard). */
    int size();
}
