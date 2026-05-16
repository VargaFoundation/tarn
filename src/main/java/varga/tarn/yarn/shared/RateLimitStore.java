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
 * Cluster-aware backend for rate-limit / quota acquisition. The configured limits are
 * <em>cluster-wide</em> ceilings; the implementation decides how a single replica enforces its
 * share of that ceiling so that the sum across replicas honours the configured value.
 *
 * <p>The token-bucket math, rule matching and the {@code Decision} type stay in
 * {@link varga.tarn.yarn.QuotaEnforcer}/{@link varga.tarn.yarn.GlobalRateLimiter}; this store only
 * answers "allowed, or how long to wait" so the caller's deny-message construction is unchanged.
 *
 * <p>The default (single-replica / no ZooKeeper) behaviour does <strong>not</strong> use a store
 * at all — the components keep their in-process buckets. A store is installed only when a shared
 * backend ({@code --shared-state=zk}) is selected.
 */
public interface RateLimitStore {

    /**
     * Global proxy cap. {@code capacityPerSecond} is the configured cluster-wide rps.
     *
     * @return {@code 0} if the request is allowed, otherwise the milliseconds to wait.
     */
    long acquireGlobal(int capacityPerSecond);

    /**
     * Per-rule quota. {@code key} already encodes the matched rule (e.g. {@code "u:alice|*"}).
     * {@code requestsPerMinute} is the rule's cluster-wide rpm.
     *
     * @return {@code 0} if allowed, otherwise the milliseconds to wait.
     */
    long acquireQuota(String key, int requestsPerMinute);

    /** Drops cached per-key buckets — called when the quota rule set is reloaded. */
    void invalidate();
}
