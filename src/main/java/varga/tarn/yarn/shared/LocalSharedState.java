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
 * Single-replica provider: no shared backend. Returns {@code null} stores so the components keep
 * their in-process buckets / affinity map, and reports a live-replica count of 1. This is the
 * default and is byte-for-byte the historical behaviour.
 */
public final class LocalSharedState implements SharedState {

    @Override public String mode() { return "local"; }

    @Override public int liveReplicaCount() { return 1; }

    @Override public String replicaId() { return "local"; }

    @Override public RateLimitStore rateLimits() { return null; }

    @Override public AffinityStore affinity() { return null; }

    @Override public void start() { /* no-op */ }

    @Override public void close() { /* no-op */ }
}
