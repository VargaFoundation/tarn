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
 * A shared, fixed-window counter — the primitive behind <em>precise</em> cross-replica enforcement.
 * Implementations key internally by {@code (key, floor(now / windowMs))} so each window is a fresh
 * counter that expires on its own; callers pass a stable {@code key} and the window length.
 *
 * <p>The reference implementation is {@code HBaseWindowedCounter} (atomic {@code Increment} +
 * per-cell TTL), the Hadoop-native equivalent of a Redis {@code INCR}/{@code EXPIRE}. Because both
 * the rate limiter ({@link CountingRateLimitStore}) and token/cost budgets are expressed purely
 * against this interface, their logic is unit-testable with an in-memory fake, independent of any
 * store.
 */
public interface WindowedCounter {

    /**
     * Atomically adds {@code delta} to the counter for {@code key} in the current window and
     * returns the new window total.
     */
    long incrementAndGet(String key, long delta, long windowMs);

    /** Current window total for {@code key} ({@code 0} if nothing has been counted yet). */
    long get(String key, long windowMs);
}
