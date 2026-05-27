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
 * <em>Precise</em> rate limiter / quota enforcer over a shared {@link WindowedCounter}: every
 * request atomically increments the shared window counter and is allowed while the total stays
 * within the configured ceiling. Unlike the fair-share strategy this is exact across replicas — at
 * the cost of one counter round-trip per request — and needs no live-replica count.
 *
 * <p>Backend-agnostic: works over any {@link WindowedCounter}, so the limiting logic is testable
 * with an in-memory counter and reused by the HBase backend.
 */
final class CountingRateLimitStore implements RateLimitStore {

    private static final long SECOND_MS = 1000L;
    private static final long MINUTE_MS = 60_000L;

    private final WindowedCounter counter;

    CountingRateLimitStore(WindowedCounter counter) {
        this.counter = counter;
    }

    @Override
    public long acquireGlobal(int capacityPerSecond) {
        if (capacityPerSecond <= 0) return 0L;
        long total = counter.incrementAndGet("rl:g", 1, SECOND_MS);
        return total <= capacityPerSecond ? 0L : msToWindowEnd(SECOND_MS);
    }

    @Override
    public long acquireQuota(String key, int requestsPerMinute) {
        if (requestsPerMinute <= 0) return 0L;
        long total = counter.incrementAndGet("rl:q:" + key, 1, MINUTE_MS);
        return total <= requestsPerMinute ? 0L : msToWindowEnd(MINUTE_MS);
    }

    @Override
    public void invalidate() {
        // No-op: windows expire on their own; a rule reload doesn't need to reset shared counters.
    }

    /** Milliseconds until the current fixed window rolls (a safe Retry-After hint). */
    private static long msToWindowEnd(long windowMs) {
        long now = System.currentTimeMillis();
        return Math.max(1L, windowMs - (now % windowMs));
    }
}
