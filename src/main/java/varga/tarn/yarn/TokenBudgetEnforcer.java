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
import varga.tarn.yarn.shared.WindowedCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

/**
 * Daily token <em>and cost</em> budgets — the enforcement counterpart to the per-(user,model)
 * request-rate {@link QuotaEnforcer}. Where quotas cap how fast you call, budgets cap how much you
 * consume; TARN already meters token usage, so this closes the loop by refusing new requests once a
 * caller has burned their daily allowance of tokens or money.
 *
 * <p>Rules live in the same JSON document as quotas, under {@code budgets} (and an optional
 * {@code prices} table for cost budgets):
 * <pre>{@code
 * {
 *   "budgets": [
 *     {"user": "alice", "tokensPerDay": 2000000},
 *     {"user": "alice", "model": "gpt-4", "tokensPerDay": 100000},
 *     {"group": "premium", "costPerDay": 50.0}
 *   ],
 *   "prices": [
 *     {"model": "gpt-4", "inputPer1k": 0.03, "outputPer1k": 0.06},
 *     {"model": "*",     "inputPer1k": 0.001, "outputPer1k": 0.002}
 *   ]
 * }
 * }</pre>
 * First-match selects the budget by specificity (user+model &gt; user &gt; group+model &gt; group
 * &gt; model &gt; default). A model-scoped rule caps that model; a model-agnostic rule caps the
 * user's total across all models. A group rule grants each member that budget (per-user accounting).
 *
 * <p>Consumption is accumulated per user in a rolling 24h window, at two granularities — per
 * {@code (user, model)} and per {@code (user, *)} — so model-scoped and model-agnostic rules each
 * read an O(1) counter. Like the rate limiter, budgets are enforced fair-share across replicas:
 * each replica allows {@code budget / liveReplicas}.
 */
public class TokenBudgetEnforcer {

    private static final Logger log = LoggerFactory.getLogger(TokenBudgetEnforcer.class);
    private static final long DAY_MS = 24L * 60L * 60L * 1000L;
    private static final String ANY = "*";
    // Defensive bound on the counter map so churn through many principals can't grow it unbounded.
    private static final int MAX_TRACKED_KEYS = 400_000;

    public static final class Decision {
        public final boolean allowed;
        public final long retryAfterMs;
        public final String reason;   // null, "token_budget" or "cost_budget"
        public final String ruleDesc;

        Decision(boolean allowed, long retryAfterMs, String reason, String ruleDesc) {
            this.allowed = allowed;
            this.retryAfterMs = retryAfterMs;
            this.reason = reason;
            this.ruleDesc = ruleDesc;
        }

        static Decision allow() { return new Decision(true, 0L, null, null); }
        static Decision deny(long retry, String reason, String ruleDesc) {
            return new Decision(false, retry, reason, ruleDesc);
        }
    }

    static final class Rule {
        final String user;    // nullable
        final String group;   // nullable
        final String model;   // nullable (model-agnostic) or exact / "*"
        final long tokensPerDay;
        final double costPerDay;

        Rule(String user, String group, String model, long tokensPerDay, double costPerDay) {
            this.user = emptyToNull(user);
            this.group = emptyToNull(group);
            this.model = emptyToNull(model);
            this.tokensPerDay = Math.max(0, tokensPerDay);
            this.costPerDay = Math.max(0.0, costPerDay);
        }

        boolean modelScoped() {
            return model != null && !model.equals(ANY);
        }

        boolean matches(String u, Set<String> groups, String reqModel) {
            if (user != null && !user.equals(u)) return false;
            if (group != null && (groups == null || !groups.contains(group))) return false;
            if (modelScoped()) {
                // A model-specific rule only applies when the request's model is known and equal.
                return reqModel != null && model.equals(reqModel);
            }
            return true;
        }

        int specificity() {
            int s = 0;
            if (user != null) s += 4;
            if (group != null) s += 2;
            if (modelScoped()) s += 1;
            return s;
        }
    }

    private static String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }

    private final long windowMs;
    private final ObjectMapper om = new ObjectMapper();
    private volatile List<Rule> rules = new ArrayList<>();
    // model -> [inputPricePer1k, outputPricePer1k]; key "*" is the fallback.
    private volatile Map<String, double[]> prices = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, WindowCounter> counters = new ConcurrentHashMap<>();
    private volatile IntSupplier replicaCount = () -> 1;
    // Optional shared counter for *precise* budgets (e.g. HBase). When present, consumption is
    // exact across replicas and the fair-share division by replica count is bypassed.
    private volatile WindowedCounter sharedCounter;

    /** Installs a shared counter for precise budgets; {@code null} keeps the fair-share path. */
    public void setSharedCounter(WindowedCounter counter) {
        this.sharedCounter = counter;
    }

    public TokenBudgetEnforcer() {
        this(DAY_MS);
    }

    /** Window override is exposed for tests; production uses the 24h default. */
    public TokenBudgetEnforcer(long windowMs) {
        this.windowMs = windowMs > 0 ? windowMs : DAY_MS;
    }

    public void setReplicaCountSupplier(IntSupplier supplier) {
        if (supplier != null) this.replicaCount = supplier;
    }

    public int getRuleCount() {
        return rules.size();
    }

    public void loadFromJson(String json) {
        try {
            JsonNode root = om.readTree(json);
            List<Rule> parsedRules = new ArrayList<>();
            JsonNode budgets = root.path("budgets");
            if (budgets.isArray()) {
                for (JsonNode n : budgets) {
                    parsedRules.add(new Rule(
                            n.path("user").asText(null),
                            n.path("group").asText(null),
                            n.path("model").asText(null),
                            n.path("tokensPerDay").asLong(0),
                            n.path("costPerDay").asDouble(0.0)));
                }
            }
            parsedRules.sort((a, b) -> Integer.compare(b.specificity(), a.specificity()));

            Map<String, double[]> parsedPrices = new ConcurrentHashMap<>();
            JsonNode priceArr = root.path("prices");
            if (priceArr.isArray()) {
                for (JsonNode n : priceArr) {
                    String model = n.path("model").asText(null);
                    if (model == null || model.isEmpty()) continue;
                    parsedPrices.put(model, new double[]{
                            n.path("inputPer1k").asDouble(0.0),
                            n.path("outputPer1k").asDouble(0.0)});
                }
            }

            this.rules = parsedRules;
            this.prices = parsedPrices;
            this.counters.clear();
            log.info("Loaded {} token-budget rule(s), {} model price(s)", parsedRules.size(), parsedPrices.size());
        } catch (Exception e) {
            log.error("Failed to parse budgets JSON: {}", e.getMessage());
        }
    }

    /** Convenience for tests: a single match-all daily token budget. */
    public void setGlobalBudget(long tokensPerDay) {
        List<Rule> list = new ArrayList<>();
        list.add(new Rule(null, null, null, tokensPerDay, 0.0));
        this.rules = list;
        this.counters.clear();
    }

    // ---- check ---------------------------------------------------------------

    /** Model-agnostic check (legacy / when the model is unknown). */
    public Decision check(String user, Set<String> groups) {
        return check(user, groups, null);
    }

    /**
     * Decides whether {@code user} may issue another request for {@code model}. Soft cap: the
     * request that crosses a budget is allowed; subsequent ones are refused until the window rolls.
     */
    public Decision check(String user, Set<String> groups, String model) {
        Rule matched = firstMatch(user, groups, model);
        if (matched == null) return Decision.allow();

        String suffix = matched.modelScoped() ? model : ANY;
        WindowedCounter shared = sharedCounter;
        long tokens;
        double cost;
        long retryMs;
        int n;
        if (shared != null) {
            // Precise: exact shared counters, no fair-share division by replica count.
            tokens = shared.get("bt:" + user + "|" + suffix, windowMs);
            cost = shared.get("bc:" + user + "|" + suffix, windowMs) / 1_000_000.0;
            retryMs = msToWindowEnd(windowMs);
            n = 1;
        } else {
            WindowCounter wc = counters.get(user + "|" + suffix);
            tokens = wc == null ? 0L : wc.currentTokens();
            cost = wc == null ? 0.0 : wc.currentCost();
            retryMs = wc == null ? windowMs : wc.msUntilReset();
            n = Math.max(1, replicaCount.getAsInt());
        }

        if (matched.tokensPerDay > 0) {
            long effective = n == 1 ? matched.tokensPerDay : matched.tokensPerDay / n;
            if (tokens >= effective) {
                return Decision.deny(retryMs, "token_budget", describe(matched));
            }
        }
        if (matched.costPerDay > 0.0) {
            double effective = matched.costPerDay / n;
            if (cost >= effective) {
                return Decision.deny(retryMs, "cost_budget", describe(matched));
            }
        }
        return Decision.allow();
    }

    // ---- record --------------------------------------------------------------

    /** Legacy: account undifferentiated tokens against the user's model-agnostic counter. */
    public void recordUsage(String user, long tokens) {
        if (tokens <= 0 || user == null || rules.isEmpty()) return;
        WindowedCounter shared = sharedCounter;
        if (shared != null) {
            shared.incrementAndGet("bt:" + user + "|" + ANY, tokens, windowMs);
            return;
        }
        bump(user + "|" + ANY, tokens, 0.0);
    }

    /** Accounts tokens and the computed cost against both the (user,model) and (user,*) windows. */
    public void recordUsage(String user, String model, long promptTokens, long completionTokens) {
        if (user == null || rules.isEmpty()) return;
        long tokens = Math.max(0, promptTokens) + Math.max(0, completionTokens);
        if (tokens <= 0) return;
        double cost = costOf(model, promptTokens, completionTokens);
        WindowedCounter shared = sharedCounter;
        if (shared != null) {
            long costMicros = Math.round(cost * 1_000_000.0);
            shared.incrementAndGet("bt:" + user + "|" + ANY, tokens, windowMs);
            if (costMicros > 0) shared.incrementAndGet("bc:" + user + "|" + ANY, costMicros, windowMs);
            if (model != null && !model.isEmpty()) {
                shared.incrementAndGet("bt:" + user + "|" + model, tokens, windowMs);
                if (costMicros > 0) shared.incrementAndGet("bc:" + user + "|" + model, costMicros, windowMs);
            }
            return;
        }
        if (counters.size() > MAX_TRACKED_KEYS) {
            counters.clear(); // defensive bound; resets in-window usage (rare)
        }
        bump(user + "|" + ANY, tokens, cost);
        if (model != null && !model.isEmpty()) {
            bump(user + "|" + model, tokens, cost);
        }
    }

    private void bump(String key, long tokens, double cost) {
        counters.computeIfAbsent(key, k -> new WindowCounter(windowMs)).add(tokens, cost);
    }

    // ---- introspection -------------------------------------------------------

    /** Remaining tokens in the current window for the matched rule; -1 when unlimited. */
    public long getRemaining(String user, Set<String> groups) {
        return getRemaining(user, groups, null);
    }

    public long getRemaining(String user, Set<String> groups, String model) {
        Rule matched = firstMatch(user, groups, model);
        if (matched == null || matched.tokensPerDay <= 0) return -1L;
        String suffix = matched.modelScoped() ? model : ANY;
        WindowedCounter shared = sharedCounter;
        long tokens;
        long effective;
        if (shared != null) {
            tokens = shared.get("bt:" + user + "|" + suffix, windowMs);
            effective = matched.tokensPerDay;
        } else {
            WindowCounter wc = counters.get(user + "|" + suffix);
            tokens = wc == null ? 0L : wc.currentTokens();
            int n = Math.max(1, replicaCount.getAsInt());
            effective = n == 1 ? matched.tokensPerDay : matched.tokensPerDay / n;
        }
        return Math.max(0L, effective - tokens);
    }

    /** Estimated cost of a completion under the configured price table (0 when unpriced). */
    public double costOf(String model, long promptTokens, long completionTokens) {
        double[] p = prices.get(model);
        if (p == null) p = prices.get(ANY);
        if (p == null) return 0.0;
        return (Math.max(0, promptTokens) / 1000.0) * p[0]
                + (Math.max(0, completionTokens) / 1000.0) * p[1];
    }

    private Rule firstMatch(String user, Set<String> groups, String model) {
        for (Rule r : rules) {
            if (r.matches(user, groups, model)) return r;
        }
        return null;
    }

    /** Milliseconds until the current fixed window rolls (Retry-After hint for the precise path). */
    private static long msToWindowEnd(long windowMs) {
        long now = System.currentTimeMillis();
        return Math.max(1L, windowMs - (now % windowMs));
    }

    private static String describe(Rule r) {
        return "budget[user=" + r.user + ",group=" + r.group + ",model=" + r.model
                + ",tokensPerDay=" + r.tokensPerDay + ",costPerDay=" + r.costPerDay + "]";
    }

    /** Rolling fixed-window accumulator of tokens and cost; resets every {@code windowMs}. */
    static final class WindowCounter {
        private final long windowMs;
        private long windowStart;
        private long tokens;
        private double cost;

        WindowCounter(long windowMs) {
            this.windowMs = windowMs;
            this.windowStart = System.currentTimeMillis();
        }

        synchronized void add(long t, double c) {
            roll();
            tokens += Math.max(0, t);
            cost += Math.max(0.0, c);
        }

        synchronized long currentTokens() {
            roll();
            return tokens;
        }

        synchronized double currentCost() {
            roll();
            return cost;
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
                tokens = 0L;
                cost = 0.0;
            }
        }
    }
}
