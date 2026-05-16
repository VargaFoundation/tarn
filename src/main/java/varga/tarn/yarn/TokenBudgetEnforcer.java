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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

/**
 * Per-user daily <em>token</em> budgets — the enforcement counterpart to the per-(user,model)
 * request-rate {@link QuotaEnforcer}. Where quotas cap how fast you call, budgets cap how much you
 * consume: TARN already meters token usage ({@code tarn_tokens_*_total}); this closes the loop by
 * refusing new requests once a user has burned their daily allowance.
 *
 * <p>Rules live in the same JSON document as quotas (so one file, one hot-reload, one admin
 * endpoint) under a top-level {@code budgets} array:
 * <pre>{@code
 * {
 *   "rules":   [ ... per-(user,model) rpm quotas ... ],
 *   "budgets": [
 *     {"user": "alice", "tokensPerDay": 2000000},
 *     {"group": "free-tier", "tokensPerDay": 50000},
 *     {"tokensPerDay": 1000000}
 *   ]
 * }
 * }</pre>
 * First-match selects the user's daily budget by specificity (exact user &gt; group &gt; default);
 * a group rule grants each member that budget (per-user accounting, not a shared pool). Consumption
 * is accumulated per user in a rolling 24h window.
 *
 * <p>Multi-replica: like the rate limiter, budgets are enforced fair-share — when a shared-state
 * backend reports {@code N} live replicas, each replica allows {@code budget / N} so the fleet
 * total stays at or below the configured ceiling (it errs slightly strict, never overspends). A
 * precise shared counter is a planned follow-up; until then exact budgets need a single replica.
 */
public class TokenBudgetEnforcer {

    private static final Logger log = LoggerFactory.getLogger(TokenBudgetEnforcer.class);
    private static final long DAY_MS = 24L * 60L * 60L * 1000L;
    // Defensive bound on the per-user counter map so a deployment that churns through many
    // distinct principals can't grow it without limit; clearing only resets in-window usage.
    private static final int MAX_TRACKED_USERS = 200_000;

    public static final class Decision {
        public final boolean allowed;
        public final long retryAfterMs;
        public final long remaining;
        public final String ruleDesc;

        Decision(boolean allowed, long retryAfterMs, long remaining, String ruleDesc) {
            this.allowed = allowed;
            this.retryAfterMs = retryAfterMs;
            this.remaining = remaining;
            this.ruleDesc = ruleDesc;
        }

        static Decision allow(long remaining) { return new Decision(true, 0L, remaining, null); }
        static Decision deny(long retry, String ruleDesc) { return new Decision(false, retry, 0L, ruleDesc); }
    }

    static final class Rule {
        final String user;   // nullable
        final String group;  // nullable
        final long tokensPerDay;

        Rule(String user, String group, long tokensPerDay) {
            this.user = emptyToNull(user);
            this.group = emptyToNull(group);
            this.tokensPerDay = Math.max(0, tokensPerDay);
        }

        boolean matches(String u, Set<String> groups) {
            if (user != null && !user.equals(u)) return false;
            if (group != null && (groups == null || !groups.contains(group))) return false;
            return true;
        }

        int specificity() {
            int s = 0;
            if (user != null) s += 2;
            if (group != null) s += 1;
            return s;
        }
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }

    private final long windowMs;
    private final ObjectMapper om = new ObjectMapper();
    private volatile List<Rule> rules = new ArrayList<>();
    private final ConcurrentHashMap<String, WindowCounter> counters = new ConcurrentHashMap<>();
    // Live-replica count for fair-share; default 1 (single replica / no shared state).
    private volatile IntSupplier replicaCount = () -> 1;

    public TokenBudgetEnforcer() {
        this(DAY_MS);
    }

    /** Window override is exposed for tests; production uses the 24h default. */
    public TokenBudgetEnforcer(long windowMs) {
        this.windowMs = windowMs > 0 ? windowMs : DAY_MS;
    }

    /** Supplies the live-replica count used to fair-share budgets across the fleet. */
    public void setReplicaCountSupplier(IntSupplier supplier) {
        if (supplier != null) this.replicaCount = supplier;
    }

    public int getRuleCount() {
        return rules.size();
    }

    public void loadFromJson(String json) {
        try {
            JsonNode root = om.readTree(json);
            JsonNode arr = root.path("budgets");
            List<Rule> parsed = new ArrayList<>();
            if (arr.isArray()) {
                for (JsonNode n : arr) {
                    parsed.add(new Rule(
                            n.path("user").asText(null),
                            n.path("group").asText(null),
                            n.path("tokensPerDay").asLong(0)));
                }
            }
            parsed.sort((a, b) -> Integer.compare(b.specificity(), a.specificity()));
            this.rules = parsed;
            this.counters.clear();
            log.info("Loaded {} token-budget rule(s)", parsed.size());
        } catch (Exception e) {
            log.error("Failed to parse budgets JSON: {}", e.getMessage());
        }
    }

    /** Convenience for tests: a single match-all daily budget. */
    public void setGlobalBudget(long tokensPerDay) {
        List<Rule> list = new ArrayList<>();
        list.add(new Rule(null, null, tokensPerDay));
        this.rules = list;
        this.counters.clear();
    }

    /**
     * Decides whether {@code user} may issue another request given their already-consumed tokens
     * in the current window. Soft cap: the request that crosses the line is allowed; subsequent
     * ones are refused until the window rolls.
     */
    public Decision check(String user, Set<String> groups) {
        Rule matched = firstMatch(user, groups);
        if (matched == null || matched.tokensPerDay <= 0) {
            return Decision.allow(Long.MAX_VALUE); // no rule (or unlimited) => allow
        }
        long effective = effectiveBudget(matched.tokensPerDay);
        WindowCounter wc = counters.get(user);
        long consumed = wc == null ? 0L : wc.current();
        if (consumed >= effective) {
            return Decision.deny(wc == null ? windowMs : wc.msUntilReset(), describe(matched));
        }
        return Decision.allow(effective - consumed);
    }

    /** Accounts tokens against {@code user}'s window. No-op when no budgets are configured. */
    public void recordUsage(String user, long tokens) {
        if (tokens <= 0 || user == null || rules.isEmpty()) return;
        if (counters.size() > MAX_TRACKED_USERS) {
            counters.clear(); // defensive: bound the map; resets in-window usage (rare)
        }
        counters.computeIfAbsent(user, k -> new WindowCounter(windowMs)).add(tokens);
    }

    /** Remaining tokens in the current window for the matched budget; -1 when unlimited. */
    public long getRemaining(String user, Set<String> groups) {
        Rule matched = firstMatch(user, groups);
        if (matched == null || matched.tokensPerDay <= 0) return -1L;
        long effective = effectiveBudget(matched.tokensPerDay);
        WindowCounter wc = counters.get(user);
        long consumed = wc == null ? 0L : wc.current();
        return Math.max(0L, effective - consumed);
    }

    private Rule firstMatch(String user, Set<String> groups) {
        for (Rule r : rules) {
            if (r.matches(user, groups)) return r;
        }
        return null;
    }

    private long effectiveBudget(long tokensPerDay) {
        int n = Math.max(1, replicaCount.getAsInt());
        return n == 1 ? tokensPerDay : tokensPerDay / n;
    }

    private static String describe(Rule r) {
        return "budget[user=" + r.user + ",group=" + r.group + ",tokensPerDay=" + r.tokensPerDay + "]";
    }

    /** Rolling fixed-window token accumulator; resets every {@code windowMs}. */
    static final class WindowCounter {
        private final long windowMs;
        private long windowStart;
        private final AtomicLong consumed = new AtomicLong();

        WindowCounter(long windowMs) {
            this.windowMs = windowMs;
            this.windowStart = System.currentTimeMillis();
        }

        synchronized long add(long n) {
            roll();
            return consumed.addAndGet(Math.max(0, n));
        }

        synchronized long current() {
            roll();
            return consumed.get();
        }

        synchronized long msUntilReset() {
            roll();
            return Math.max(1L, (windowStart + windowMs) - System.currentTimeMillis());
        }

        private void roll() {
            long now = System.currentTimeMillis();
            long elapsed = now - windowStart;
            if (elapsed >= windowMs) {
                long windows = elapsed / windowMs;
                windowStart += windows * windowMs;
                consumed.set(0L);
            }
        }
    }
}
