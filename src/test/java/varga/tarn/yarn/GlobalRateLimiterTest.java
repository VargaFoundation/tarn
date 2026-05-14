package varga.tarn.yarn;

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

import static org.junit.jupiter.api.Assertions.*;

public class GlobalRateLimiterTest {

    @Test
    public void disabledWhenRpsIsZero() {
        GlobalRateLimiter l = new GlobalRateLimiter(0);
        assertFalse(l.isEnabled());
        // Disabled limiter always allows immediately.
        for (int i = 0; i < 100; i++) {
            assertEquals(0L, l.tryAcquire());
        }
    }

    @Test
    public void allowsUpToCapacityThenRejectsWithinTheSameSecond() {
        // 5 req/s — capacity within a 1s window is 5; the 6th must hit the floor.
        GlobalRateLimiter l = new GlobalRateLimiter(5);
        assertTrue(l.isEnabled());
        for (int i = 0; i < 5; i++) {
            assertEquals(0L, l.tryAcquire(), "request " + i + " must be allowed");
        }
        long wait = l.tryAcquire();
        assertTrue(wait > 0, "6th request inside the same second must be refused");
        assertTrue(wait <= 1000, "wait must point to the next 1s window, got " + wait);
    }

    @Test
    public void refillsAfterWindow() throws Exception {
        GlobalRateLimiter l = new GlobalRateLimiter(2);
        assertEquals(0L, l.tryAcquire());
        assertEquals(0L, l.tryAcquire());
        assertTrue(l.tryAcquire() > 0, "third in window denied");
        Thread.sleep(1_050);
        // After the window rolls over the bucket refills to capacity.
        assertEquals(0L, l.tryAcquire(), "post-refill request must be allowed");
    }
}
