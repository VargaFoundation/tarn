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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-(user, model) request rate limiter backed by in-memory token buckets.
 *
 * <p>Rule selection is first-match in this priority order:
 * <ol>
 *   <li>Exact user + exact model.</li>
 *   <li>Exact user + model pattern {@code *}.</li>
 *   <li>Group match (any of the user's groups) + exact model.</li>
 *   <li>Group match + {@code *}.</li>
 *   <li>Default rule with no user/group constraint.</li>
 *   <li>No rule — the call is unlimited.</li>
 * </ol>
 *
 * <p>Config is a JSON document ({@code quotas.json} on HDFS, or POSTed via {@code /admin/config}).
 * Example:
 * <pre>{@code
 * {
 *   "rules": [
 *     {"user": "alice", "model": "llama-3-70b", "requestsPerMinute": 60},
 *     {"group": "paying-customers", "model": "*", "requestsPerMinute": 300},
 *     {"model": "*", "requestsPerMinute": 10}
 *   ]
 * }
 * }</pre>
 *
 * <p>Why not Redis / distributed counters: for a single AM instance in-memory is accurate
 * and cheap. When we ship horizontally-scaled TARN (P2.1 Operator), swap the implementation
 * behind {@link #check} for a shared store.
 */
public class QuotaEnforcer {

    private static final Logger log = LoggerFactory.getLogger(QuotaEnforcer.class);

    public static final class Decision {
        public final boolean allowed;
        public final long retryAfterMs;
        public final String reason;
        public final String ruleDesc;

        Decision(boolean allowed, long retryAfterMs, String reason, String ruleDesc) {
            this.allowed = allowed;
            this.retryAfterMs = retryAfterMs;
            this.reason = reason;
            this.ruleDesc = ruleDesc;
        }

        public static Decision allow() { return new Decision(true, 0L, null, null); }
        public static Decision deny(long retry, String reason, String ruleDesc) {
            return new Decision(false, retry, reason, ruleDesc);
        }
    }

    /** Single quota rule matched via user / group / model. */
    static final class Rule {
        final String user;          // nullable
        final String group;         // nullable
        final String modelPattern;  // "*" or exact
        final int requestsPerMinute;

        Rule(String user, String group, String modelPattern, int requestsPerMinute) {
            this.user = emptyToNull(user);
            this.group = emptyToNull(group);
            this.modelPattern = (modelPattern == null || modelPattern.isEmpty()) ? "*" : modelPattern;
            this.requestsPerMinute = Math.max(0, requestsPerMinute);
        }

        boolean matches(String u, Set<String> groups, String model) {
            if (user != null && !user.equals(u)) return false;
            if (group != null && (groups == null || !groups.contains(group))) return false;
            return modelPattern.equals("*") || modelPattern.equals(model);
        }

        int specificity() {
            int s = 0;
            if (user != null) s += 4;
            if (group != null) s += 2;
            if (!modelPattern.equals("*")) s += 1;
            return s;
        }
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }

    // Snapshot of rules; replaced atomically on reload. volatile so writes publish visibly.
    private volatile List<Rule> rules = new ArrayList<>();
    // Token buckets keyed by "user|model" for rules that match on user, or "group|..." etc.
    // Buckets are lazily created per-caller when first hit.
    private final Map<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    public void loadFromJson(String json) {
        try {
            ObjectMapper om = new ObjectMapper();
            JsonNode root = om.readTree(json);
            JsonNode arr = root.path("rules");
            List<Rule> parsed = new ArrayList<>();
            if (arr.isArray()) {
                for (JsonNode n : arr) {
                    parsed.add(new Rule(
                            n.path("user").asText(null),
                            n.path("group").asText(null),
                            n.path("model").asText("*"),
                            n.path("requestsPerMinute").asInt(0)));
                }
            }
            // Most-specific rules first so first-match wins.
            parsed.sort((a, b) -> Integer.compare(b.specificity(), a.specificity()));
            this.rules = parsed;
            this.buckets.clear(); // New rules invalidate old buckets.
            log.info("Loaded {} quota rule(s)", parsed.size());
        } catch (Exception e) {
            log.error("Failed to parse quotas JSON: {}", e.getMessage());
        }
    }

    /** Convenience: apply a preset to match-all with a global limit. Used in tests. */
    public void setGlobalLimit(int requestsPerMinute) {
        List<Rule> list = new ArrayList<>();
        list.add(new Rule(null, null, "*", requestsPerMinute));
        this.rules = list;
        this.buckets.clear();
    }

    public Decision check(String user, Set<String> groups, String model) {
        Rule matched = null;
        for (Rule r : rules) {
            if (r.matches(user, groups, model)) {
                matched = r;
                break;
            }
        }
        if (matched == null || matched.requestsPerMinute == 0) {
            // No rule, or a rule that denies explicitly (0 rpm).
            if (matched != null && matched.requestsPerMinute == 0) {
                return Decision.deny(60_000L, "quota_zero", describe(matched));
            }
            return Decision.allow();
        }

        String key = bucketKey(matched, user, groups, model);
        final int rpm = matched.requestsPerMinute;
        TokenBucket bucket = buckets.computeIfAbsent(key,
                k -> new TokenBucket(rpm, 60_000L));
        long waitMs = bucket.tryAcquire();
        if (waitMs == 0) return Decision.allow();
        return Decision.deny(waitMs, "rate_limited", describe(matched));
    }

    private static String bucketKey(Rule r, String user, Set<String> groups, String model) {
        // Keyed so all callers matching the same rule share a bucket — that's the usual
        // contract for group quotas (the group's total pool, not per-user).
        StringBuilder sb = new StringBuilder();
        sb.append(r.user != null ? "u:" + r.user : (r.group != null ? "g:" + r.group : "*"));
        sb.append("|");
        sb.append(r.modelPattern.equals("*") ? "*" : model);
        return sb.toString();
    }

    private static String describe(Rule r) {
        return "rule[user=" + r.user + ",group=" + r.group + ",model=" + r.modelPattern
                + ",rpm=" + r.requestsPerMinute + "]";
    }

    /**
     * Simple fixed-window token bucket — resets at {@code windowMs} intervals.
     * Windowed rather than sliding-window for implementation simplicity; returns
     * {@code 0} on grant, or the milliseconds until the next refill on refusal.
     */
    static final class TokenBucket {
        private final int capacity;
        private final long windowMs;
        private long windowStart;
        private int remaining;

        TokenBucket(int capacity, long windowMs) {
            this.capacity = capacity;
            this.windowMs = windowMs;
            this.windowStart = System.currentTimeMillis();
            this.remaining = capacity;
        }

        synchronized long tryAcquire() {
            long now = System.currentTimeMillis();
            long elapsed = now - windowStart;
            if (elapsed >= windowMs) {
                long windows = elapsed / windowMs;
                windowStart += windows * windowMs;
                remaining = capacity;
            }
            if (remaining > 0) {
                remaining--;
                return 0L;
            }
            return Math.max(1L, (windowStart + windowMs) - now);
        }

        synchronized int getRemaining() { return remaining; }
        synchronized long getWindowStartMs() { return windowStart; }
    }
}
