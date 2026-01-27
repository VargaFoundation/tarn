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

class CircuitBreakerTest {

    @Test
    void testInitialStateClosed() {
        CircuitBreaker cb = new CircuitBreaker("test", 3, 1000);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertTrue(cb.isClosed());
        assertFalse(cb.isOpen());
    }

    @Test
    void testSuccessfulCallsKeepCircuitClosed() {
        CircuitBreaker cb = new CircuitBreaker("test", 3, 1000);
        
        String result = cb.execute(() -> "success", "fallback");
        
        assertEquals("success", result);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
        assertEquals(0, cb.getFailureCount());
    }

    @Test
    void testCircuitOpensAfterThresholdFailures() {
        CircuitBreaker cb = new CircuitBreaker("test", 3, 1000);
        
        // Simulate 3 failures
        for (int i = 0; i < 3; i++) {
            cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        }
        
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
        assertTrue(cb.isOpen());
    }

    @Test
    void testOpenCircuitReturnsFallback() {
        CircuitBreaker cb = new CircuitBreaker("test", 2, 1000);
        
        // Open the circuit
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        
        assertTrue(cb.isOpen());
        
        // Next call should return fallback without executing
        String result = cb.execute(() -> "success", "fallback");
        assertEquals("fallback", result);
    }

    @Test
    void testCircuitTransitionsToHalfOpenAfterTimeout() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker("test", 2, 50); // 50ms timeout
        
        // Open the circuit
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        
        assertTrue(cb.isOpen());
        
        // Wait for timeout
        Thread.sleep(60);
        
        // Next allowRequest should transition to HALF_OPEN
        assertTrue(cb.allowRequest());
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());
    }

    @Test
    void testHalfOpenSuccessClosesCircuit() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker("test", 2, 50);
        
        // Open the circuit
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        
        // Wait for timeout
        Thread.sleep(60);
        
        // Successful call in HALF_OPEN should close circuit
        String result = cb.execute(() -> "success", "fallback");
        
        assertEquals("success", result);
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
    }

    @Test
    void testHalfOpenFailureReopensCircuit() throws InterruptedException {
        CircuitBreaker cb = new CircuitBreaker("test", 2, 50);
        
        // Open the circuit
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        
        // Wait for timeout
        Thread.sleep(60);
        
        // Trigger transition to HALF_OPEN
        cb.allowRequest();
        assertEquals(CircuitBreaker.State.HALF_OPEN, cb.getState());
        
        // Failure in HALF_OPEN should reopen circuit
        cb.onFailure();
        assertEquals(CircuitBreaker.State.OPEN, cb.getState());
    }

    @Test
    void testManualReset() {
        CircuitBreaker cb = new CircuitBreaker("test", 2, 1000);
        
        // Open the circuit
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        
        assertTrue(cb.isOpen());
        
        // Manual reset
        cb.reset();
        
        assertTrue(cb.isClosed());
        assertEquals(0, cb.getFailureCount());
    }

    @Test
    void testExecuteBoolean() {
        CircuitBreaker cb = new CircuitBreaker("test", 3, 1000);
        
        assertTrue(cb.executeBoolean(() -> true));
        assertFalse(cb.executeBoolean(() -> false));
        assertFalse(cb.executeBoolean(() -> { throw new RuntimeException("fail"); }));
    }

    @Test
    void testForHealthCheckFactory() {
        CircuitBreaker cb = CircuitBreaker.forHealthCheck("triton-node1");
        
        assertEquals("triton-node1", cb.getName());
        assertEquals(CircuitBreaker.State.CLOSED, cb.getState());
    }

    @Test
    void testSuccessResetsFailureCount() {
        CircuitBreaker cb = new CircuitBreaker("test", 5, 1000);
        
        // Accumulate some failures (but not enough to open)
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        cb.execute(() -> { throw new RuntimeException("fail"); }, "fallback");
        assertEquals(2, cb.getFailureCount());
        
        // Success should reset failure count
        cb.execute(() -> "success", "fallback");
        assertEquals(0, cb.getFailureCount());
    }
}
