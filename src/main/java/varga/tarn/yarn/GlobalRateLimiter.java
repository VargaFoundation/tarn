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

/**
 * Process-wide cap on the rate at which the OpenAI proxy accepts inference requests. Sits
 * upstream of {@link QuotaEnforcer} (which caps per user / per (user,model)) so a runaway
 * client can't saturate every Triton backend even if their own quota is generous.
 *
 * <p>Implementation reuses {@link QuotaEnforcer.TokenBucket} with a 1-second window so the
 * rate parameter reads as "requests per second". A value of {@code 0} disables the limiter
 * entirely — that's the default, opt-in.
 */
public final class GlobalRateLimiter {

    private final int requestsPerSecond;
    private final QuotaEnforcer.TokenBucket bucket;
    // Optional shared backend. When installed (--shared-state=zk), the cap holds cluster-wide
    // (fair-share across replicas). Null = the in-process bucket below — single-replica default.
    private volatile varga.tarn.yarn.shared.RateLimitStore store;

    public GlobalRateLimiter(int requestsPerSecond) {
        this.requestsPerSecond = requestsPerSecond;
        this.bucket = new QuotaEnforcer.TokenBucket(Math.max(1, requestsPerSecond), 1000L);
    }

    /** Installs a shared rate-limit backend; pass {@code null} to keep the in-process bucket. */
    public void setRateLimitStore(varga.tarn.yarn.shared.RateLimitStore store) {
        this.store = store;
    }

    /** Returns 0 when the request is allowed, or milliseconds to wait until next slot. */
    public long tryAcquire() {
        if (requestsPerSecond <= 0) return 0L;
        varga.tarn.yarn.shared.RateLimitStore s = store;
        return s == null ? bucket.tryAcquire() : s.acquireGlobal(requestsPerSecond);
    }

    public int getRequestsPerSecond() {
        return requestsPerSecond;
    }

    public boolean isEnabled() {
        return requestsPerSecond > 0;
    }
}
