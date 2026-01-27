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

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {

    @Test
    void testSuccessfulOperationNoRetry() throws Exception {
        RetryPolicy policy = new RetryPolicy(3, 10, 100, 2.0);
        AtomicInteger attempts = new AtomicInteger(0);

        String result = policy.execute(() -> {
            attempts.incrementAndGet();
            return "success";
        }, "testOp");

        assertEquals("success", result);
        assertEquals(1, attempts.get());
    }

    @Test
    void testRetryOnFailureThenSuccess() throws Exception {
        RetryPolicy policy = new RetryPolicy(3, 10, 100, 2.0);
        AtomicInteger attempts = new AtomicInteger(0);

        String result = policy.execute(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("Transient failure");
            }
            return "success";
        }, "testOp");

        assertEquals("success", result);
        assertEquals(3, attempts.get());
    }

    @Test
    void testAllRetriesExhausted() {
        RetryPolicy policy = new RetryPolicy(2, 10, 100, 2.0);
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(RuntimeException.class, () -> {
            policy.execute(() -> {
                attempts.incrementAndGet();
                throw new RuntimeException("Persistent failure");
            }, "testOp");
        });

        assertEquals(3, attempts.get()); // initial + 2 retries
    }

    @Test
    void testNonRetryableException() {
        RetryPolicy policy = new RetryPolicy(3, 10, 100, 2.0, 
            e -> !(e instanceof IllegalArgumentException));
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(IllegalArgumentException.class, () -> {
            policy.execute(() -> {
                attempts.incrementAndGet();
                throw new IllegalArgumentException("Non-retryable");
            }, "testOp");
        });

        assertEquals(1, attempts.get()); // No retry for non-retryable
    }

    @Test
    void testDefaultPolicy() {
        RetryPolicy policy = RetryPolicy.defaultPolicy();
        assertEquals(3, policy.getMaxRetries());
        assertEquals(100, policy.getInitialDelayMs());
        assertEquals(5000, policy.getMaxDelayMs());
        assertEquals(2.0, policy.getMultiplier());
    }

    @Test
    void testAggressivePolicy() {
        RetryPolicy policy = RetryPolicy.aggressive();
        assertEquals(5, policy.getMaxRetries());
        assertEquals(200, policy.getInitialDelayMs());
        assertEquals(30000, policy.getMaxDelayMs());
        assertEquals(2.0, policy.getMultiplier());
    }

    @Test
    void testExecuteVoid() throws Exception {
        RetryPolicy policy = new RetryPolicy(3, 10, 100, 2.0);
        AtomicInteger attempts = new AtomicInteger(0);

        policy.executeVoid(() -> {
            if (attempts.incrementAndGet() < 2) {
                throw new RuntimeException("Transient failure");
            }
        }, "testOp");

        assertEquals(2, attempts.get());
    }
}
