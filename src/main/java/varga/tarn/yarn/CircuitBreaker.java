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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Circuit Breaker pattern implementation for Triton health checks.
 * Prevents cascade failures by temporarily stopping calls to failing services.
 */
public class CircuitBreaker {
    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    public enum State {
        CLOSED,      // Normal operation, requests pass through
        OPEN,        // Circuit is open, requests fail fast
        HALF_OPEN    // Testing if service has recovered
    }

    private final String name;
    private final int failureThreshold;
    private final long resetTimeoutMs;
    private final int halfOpenMaxCalls;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger halfOpenCalls = new AtomicInteger(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    private final AtomicLong openedAt = new AtomicLong(0);

    public CircuitBreaker(String name, int failureThreshold, long resetTimeoutMs) {
        this(name, failureThreshold, resetTimeoutMs, 1);
    }

    public CircuitBreaker(String name, int failureThreshold, long resetTimeoutMs, int halfOpenMaxCalls) {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.resetTimeoutMs = resetTimeoutMs;
        this.halfOpenMaxCalls = halfOpenMaxCalls;
    }

    /**
     * Creates a default circuit breaker for Triton health checks.
     * Opens after 5 failures, resets after 30 seconds.
     */
    public static CircuitBreaker forHealthCheck(String serviceName) {
        return new CircuitBreaker(serviceName, 5, 30000);
    }

    /**
     * Execute an operation through the circuit breaker.
     *
     * @param operation the operation to execute
     * @param fallback  fallback value if circuit is open
     * @param <T>       return type
     * @return the result of the operation or fallback
     */
    public <T> T execute(Callable<T> operation, T fallback) {
        if (!allowRequest()) {
            log.debug("Circuit breaker {} is OPEN, returning fallback", name);
            return fallback;
        }

        try {
            T result = operation.call();
            onSuccess();
            return result;
        } catch (Exception e) {
            onFailure();
            log.warn("Circuit breaker {} recorded failure: {}", name, e.getMessage());
            return fallback;
        }
    }

    /**
     * Execute a boolean operation (like health check) through the circuit breaker.
     */
    public boolean executeBoolean(Callable<Boolean> operation) {
        return execute(operation, false);
    }

    /**
     * Check if a request should be allowed through.
     */
    public boolean allowRequest() {
        State currentState = state.get();

        if (currentState == State.CLOSED) {
            return true;
        }

        if (currentState == State.OPEN) {
            if (System.currentTimeMillis() - openedAt.get() >= resetTimeoutMs) {
                if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                    log.info("Circuit breaker {} transitioning to HALF_OPEN", name);
                    halfOpenCalls.set(0);
                }
                return true;
            }
            return false;
        }

        // HALF_OPEN state - allow limited requests
        return halfOpenCalls.incrementAndGet() <= halfOpenMaxCalls;
    }

    /**
     * Record a successful operation.
     */
    public void onSuccess() {
        State currentState = state.get();
        
        if (currentState == State.HALF_OPEN) {
            if (state.compareAndSet(State.HALF_OPEN, State.CLOSED)) {
                log.info("Circuit breaker {} transitioning to CLOSED after successful call", name);
                failureCount.set(0);
            }
        } else if (currentState == State.CLOSED) {
            failureCount.set(0);
        }
    }

    /**
     * Record a failed operation.
     */
    public void onFailure() {
        lastFailureTime.set(System.currentTimeMillis());
        State currentState = state.get();

        if (currentState == State.HALF_OPEN) {
            if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
                openedAt.set(System.currentTimeMillis());
                log.warn("Circuit breaker {} transitioning back to OPEN from HALF_OPEN", name);
            }
            return;
        }

        int failures = failureCount.incrementAndGet();
        if (failures >= failureThreshold && currentState == State.CLOSED) {
            if (state.compareAndSet(State.CLOSED, State.OPEN)) {
                openedAt.set(System.currentTimeMillis());
                log.warn("Circuit breaker {} OPENED after {} failures", name, failures);
            }
        }
    }

    /**
     * Force the circuit breaker to reset to closed state.
     */
    public void reset() {
        state.set(State.CLOSED);
        failureCount.set(0);
        halfOpenCalls.set(0);
        log.info("Circuit breaker {} manually reset to CLOSED", name);
    }

    public State getState() {
        return state.get();
    }

    public int getFailureCount() {
        return failureCount.get();
    }

    public String getName() {
        return name;
    }

    public boolean isOpen() {
        return state.get() == State.OPEN;
    }

    public boolean isClosed() {
        return state.get() == State.CLOSED;
    }
}
