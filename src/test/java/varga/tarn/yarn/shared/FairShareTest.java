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

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the fair-share arithmetic in {@link ZkRateLimitStore} — no ZooKeeper. Drives the
 * store with stubbed live-count / index suppliers and asserts each replica admits exactly its
 * slice, that the slices sum to the configured ceiling, and the {@code capacity < replicas} edge.
 */
public class FairShareTest {

    /** Counts how many of {@code attempts} acquireGlobal calls were admitted (returned 0). */
    private static int admittedGlobal(ZkRateLimitStore store, int capacity, int attempts) {
        int ok = 0;
        for (int i = 0; i < attempts; i++) {
            if (store.acquireGlobal(capacity) == 0L) ok++;
        }
        return ok;
    }

    @Test
    public void singleReplicaGetsFullCapacity() {
        ZkRateLimitStore store = new ZkRateLimitStore(() -> 1, () -> 0);
        assertEquals(5, admittedGlobal(store, 5, 10), "N=1 must enforce the full cap locally");
    }

    @Test
    public void evenSplitAcrossReplicas() {
        // cap=9, N=3 -> each replica admits exactly 3; the three shares sum to 9.
        int total = 0;
        for (int idx = 0; idx < 3; idx++) {
            ZkRateLimitStore store = new ZkRateLimitStore(() -> 3, finalIndex(idx));
            int ok = admittedGlobal(store, 9, 9);
            assertEquals(3, ok, "replica " + idx + " share");
            total += ok;
        }
        assertEquals(9, total, "per-replica shares must sum to the cap");
    }

    @Test
    public void remainderGoesToLowestIndexedReplicas() {
        // cap=5, N=2 -> idx0 gets 3 (base 2 + remainder 1), idx1 gets 2; sum=5.
        ZkRateLimitStore r0 = new ZkRateLimitStore(() -> 2, () -> 0);
        ZkRateLimitStore r1 = new ZkRateLimitStore(() -> 2, () -> 1);
        assertEquals(3, admittedGlobal(r0, 5, 6));
        assertEquals(2, admittedGlobal(r1, 5, 6));
    }

    @Test
    public void capacitySmallerThanReplicasRejectsOnTrailingReplicas() {
        // cap=2, N=3 -> idx0:1, idx1:1, idx2:0 (rejects all); never exceeds the cap globally.
        assertEquals(1, admittedGlobal(new ZkRateLimitStore(() -> 3, () -> 0), 2, 4));
        assertEquals(1, admittedGlobal(new ZkRateLimitStore(() -> 3, () -> 1), 2, 4));
        assertEquals(0, admittedGlobal(new ZkRateLimitStore(() -> 3, () -> 2), 2, 4));
    }

    @Test
    public void quotaSharesSameAsGlobal() {
        // Same arithmetic on the per-rule quota path (60s window): cap=6, N=2 -> 3 each.
        ZkRateLimitStore r0 = new ZkRateLimitStore(() -> 2, () -> 0);
        int ok = 0;
        for (int i = 0; i < 6; i++) {
            if (r0.acquireQuota("u:alice|*", 6) == 0L) ok++;
        }
        assertEquals(3, ok);
    }

    @Test
    public void shareRecomputesWhenMembershipChanges() {
        // Bucket rebuilds when the live count changes mid-flight: N=1 (cap 4) then N=4 (cap 1).
        AtomicInteger n = new AtomicInteger(1);
        ZkRateLimitStore store = new ZkRateLimitStore(n::get, () -> 0);
        assertEquals(0L, store.acquireGlobal(4)); // N=1 -> share 4
        n.set(4);                                  // scale out -> share 1
        // The next call rebuilds the bucket to capacity 1: one admit, then throttled.
        assertEquals(0L, store.acquireGlobal(4));
        assertTrue(store.acquireGlobal(4) > 0L, "after scale-out this replica's share is exhausted");
    }

    private static java.util.function.IntSupplier finalIndex(int idx) {
        return () -> idx;
    }
}
