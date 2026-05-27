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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests the precise rate limiter against an in-memory {@link WindowedCounter} — validating the
 * limiting logic without any real store. The HBase wire (atomic Increment + TTL) is exercised
 * separately by the HBase integration test.
 */
public class CountingRateLimitStoreTest {

    /** Deterministic in-memory windowed counter (keys by real time / windowMs). */
    static final class FakeCounter implements WindowedCounter {
        private final Map<String, long[]> m = new HashMap<>();
        @Override public synchronized long incrementAndGet(String key, long delta, long windowMs) {
            long[] v = m.computeIfAbsent(key + "@" + (System.currentTimeMillis() / windowMs), x -> new long[1]);
            v[0] += delta;
            return v[0];
        }
        @Override public synchronized long get(String key, long windowMs) {
            long[] v = m.get(key + "@" + (System.currentTimeMillis() / windowMs));
            return v == null ? 0L : v[0];
        }
    }

    @Test
    public void globalCapIsExact() {
        CountingRateLimitStore s = new CountingRateLimitStore(new FakeCounter());
        assertEquals(0L, s.acquireGlobal(3));
        assertEquals(0L, s.acquireGlobal(3));
        assertEquals(0L, s.acquireGlobal(3));
        assertTrue(s.acquireGlobal(3) > 0L, "4th request in the second must be throttled");
    }

    @Test
    public void quotaCapIsExactPerKey() {
        CountingRateLimitStore s = new CountingRateLimitStore(new FakeCounter());
        assertEquals(0L, s.acquireQuota("u:alice|m", 2));
        assertEquals(0L, s.acquireQuota("u:alice|m", 2));
        assertTrue(s.acquireQuota("u:alice|m", 2) > 0L);
        // A different key has its own counter.
        assertEquals(0L, s.acquireQuota("u:bob|m", 2));
    }

    @Test
    public void zeroCapacityIsUnlimited() {
        CountingRateLimitStore s = new CountingRateLimitStore(new FakeCounter());
        for (int i = 0; i < 100; i++) {
            assertEquals(0L, s.acquireGlobal(0));
            assertEquals(0L, s.acquireQuota("k", 0));
        }
    }
}
