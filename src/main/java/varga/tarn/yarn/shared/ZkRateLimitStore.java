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

import varga.tarn.yarn.QuotaEnforcer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

/**
 * Fair-share rate limiting across replicas with <strong>zero ZooKeeper I/O on the request
 * path</strong>. Each replica enforces {@code ceil(limit / liveReplicas)} of the cluster-wide
 * ceiling using a local {@link QuotaEnforcer.TokenBucket}; the per-replica share is recomputed
 * lazily from the membership-derived live count (a volatile read), and the bucket is only
 * rebuilt when that share actually changes (i.e. on a scale event).
 *
 * <p>The remainder of an uneven division is handed to the lowest-indexed replicas (by sorted
 * membership) so the shares sum to exactly the configured limit — no over-admission. When
 * {@code liveReplicas > limit} the trailing replicas get a share of 0 (reject all), which is
 * safe for a protective ceiling; deployments needing per-replica admission when {@code limit < N}
 * should use a precise/Redis backend (planned follow-up).
 */
final class ZkRateLimitStore implements RateLimitStore {

    private final IntSupplier liveCount;
    private final IntSupplier replicaIndex;

    // Global bucket guarded by `this`.
    private int globalShare = -1;
    private QuotaEnforcer.TokenBucket globalBucket;

    private final ConcurrentHashMap<String, ShareBucket> quotaBuckets = new ConcurrentHashMap<>();

    ZkRateLimitStore(IntSupplier liveCount, IntSupplier replicaIndex) {
        this.liveCount = liveCount;
        this.replicaIndex = replicaIndex;
    }

    @Override
    public synchronized long acquireGlobal(int capacityPerSecond) {
        if (capacityPerSecond <= 0) return 0L;
        int share = fairShare(capacityPerSecond);
        if (globalBucket == null || share != globalShare) {
            globalBucket = new QuotaEnforcer.TokenBucket(share, 1000L);
            globalShare = share;
        }
        return globalBucket.tryAcquire();
    }

    @Override
    public long acquireQuota(String key, int requestsPerMinute) {
        if (requestsPerMinute <= 0) return 0L;
        int share = fairShare(requestsPerMinute);
        ShareBucket sb = quotaBuckets.computeIfAbsent(key, k -> new ShareBucket());
        return sb.acquire(share);
    }

    @Override
    public void invalidate() {
        quotaBuckets.clear();
    }

    /**
     * This replica's slice of a cluster-wide capacity: {@code floor(cap/N)}, plus one for the
     * first {@code (cap mod N)} replicas by sorted membership index so the per-replica shares
     * sum to exactly {@code cap}.
     */
    private int fairShare(int capacity) {
        int n = Math.max(1, liveCount.getAsInt());
        if (n == 1) return capacity;
        int idx = Math.max(0, replicaIndex.getAsInt());
        int base = capacity / n;
        int remainder = capacity % n;
        return base + (idx < remainder ? 1 : 0);
    }

    /** A per-key bucket that rebuilds itself when this replica's fair share changes. */
    private static final class ShareBucket {
        private int share = -1;
        private QuotaEnforcer.TokenBucket bucket;

        synchronized long acquire(int currentShare) {
            if (bucket == null || currentShare != share) {
                bucket = new QuotaEnforcer.TokenBucket(currentShare, 60_000L);
                share = currentShare;
            }
            return bucket.tryAcquire();
        }
    }
}
