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
import java.util.function.Predicate;

/**
 * Retry policy with exponential backoff for ZooKeeper/YARN calls.
 * Provides resilient execution of operations that may fail transiently.
 */
public class RetryPolicy {
    private static final Logger log = LoggerFactory.getLogger(RetryPolicy.class);

    private final int maxRetries;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final double multiplier;
    private final Predicate<Exception> retryableExceptionPredicate;

    public RetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs, double multiplier) {
        this(maxRetries, initialDelayMs, maxDelayMs, multiplier, e -> true);
    }

    public RetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs, double multiplier,
                       Predicate<Exception> retryableExceptionPredicate) {
        this.maxRetries = maxRetries;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.multiplier = multiplier;
        this.retryableExceptionPredicate = retryableExceptionPredicate;
    }

    /**
     * Creates a default retry policy suitable for ZooKeeper/YARN operations.
     * 3 retries, starting at 100ms, max 5s, doubling each time.
     */
    public static RetryPolicy defaultPolicy() {
        return new RetryPolicy(3, 100, 5000, 2.0);
    }

    /**
     * Creates an aggressive retry policy for critical operations.
     * 5 retries, starting at 200ms, max 30s, doubling each time.
     */
    public static RetryPolicy aggressive() {
        return new RetryPolicy(5, 200, 30000, 2.0);
    }

    /**
     * Execute an operation with retry and exponential backoff.
     *
     * @param operation   the operation to execute
     * @param operationName name for logging purposes
     * @param <T>         return type
     * @return the result of the operation
     * @throws Exception if all retries are exhausted
     */
    public <T> T execute(Callable<T> operation, String operationName) throws Exception {
        int attempt = 0;
        long currentDelay = initialDelayMs;
        Exception lastException = null;

        while (attempt <= maxRetries) {
            try {
                return operation.call();
            } catch (Exception e) {
                lastException = e;
                
                if (!retryableExceptionPredicate.test(e)) {
                    log.error("Non-retryable exception for {}: {}", operationName, e.getMessage());
                    throw e;
                }

                if (attempt >= maxRetries) {
                    log.error("All {} retries exhausted for {}: {}", maxRetries, operationName, e.getMessage());
                    throw e;
                }

                log.warn("Attempt {}/{} failed for {}: {}. Retrying in {}ms...",
                        attempt + 1, maxRetries + 1, operationName, e.getMessage(), currentDelay);

                try {
                    Thread.sleep(currentDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }

                currentDelay = Math.min((long) (currentDelay * multiplier), maxDelayMs);
                attempt++;
            }
        }

        throw lastException;
    }

    /**
     * Execute a void operation with retry and exponential backoff.
     *
     * @param operation   the operation to execute
     * @param operationName name for logging purposes
     * @throws Exception if all retries are exhausted
     */
    public void executeVoid(Runnable operation, String operationName) throws Exception {
        execute(() -> {
            operation.run();
            return null;
        }, operationName);
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    public double getMultiplier() {
        return multiplier;
    }
}
