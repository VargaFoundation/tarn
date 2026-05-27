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
import varga.tarn.yarn.shared.WindowedCounter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class TokenBudgetEnforcerTest {

    private static final Set<String> NO_GROUPS = Collections.emptySet();

    @Test
    public void noRulesMeansUnlimited() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        assertTrue(be.check("alice", NO_GROUPS).allowed);
        be.recordUsage("alice", 1_000_000); // ignored when no rules configured
        assertTrue(be.check("alice", NO_GROUPS).allowed);
        assertEquals(-1L, be.getRemaining("alice", NO_GROUPS));
    }

    @Test
    public void perUserBudgetSoftCaps() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.loadFromJson("{\"budgets\":[{\"user\":\"alice\",\"tokensPerDay\":100}]}");
        assertEquals(1, be.getRuleCount());

        assertTrue(be.check("alice", NO_GROUPS).allowed);
        be.recordUsage("alice", 60);
        assertTrue(be.check("alice", NO_GROUPS).allowed, "still under budget");
        assertEquals(40L, be.getRemaining("alice", NO_GROUPS));

        be.recordUsage("alice", 50); // total 110 >= 100
        TokenBudgetEnforcer.Decision d = be.check("alice", NO_GROUPS);
        assertFalse(d.allowed, "over budget => denied");
        assertTrue(d.retryAfterMs > 0);
        assertNotNull(d.ruleDesc);
        assertEquals(0L, be.getRemaining("alice", NO_GROUPS));
    }

    @Test
    public void budgetsAreParsedFromTheSharedQuotasDocument() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        // Same blob a QuotaEnforcer would read; only the "budgets" key is consumed here.
        be.loadFromJson("{\"rules\":[{\"model\":\"*\",\"requestsPerMinute\":10}],"
                + "\"budgets\":[{\"user\":\"bob\",\"tokensPerDay\":5}]}");
        assertEquals(1, be.getRuleCount());
        be.recordUsage("bob", 5);
        assertFalse(be.check("bob", NO_GROUPS).allowed);
    }

    @Test
    public void groupBudgetIsPerMemberNotShared() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.loadFromJson("{\"budgets\":[{\"group\":\"premium\",\"tokensPerDay\":100}]}");
        Set<String> premium = Set.of("premium");

        be.recordUsage("alice", 100);
        assertFalse(be.check("alice", premium).allowed, "alice exhausted her own budget");
        assertTrue(be.check("bob", premium).allowed, "bob has his own per-member budget");

        // A user not in the group has no matching rule => unlimited.
        assertTrue(be.check("carol", NO_GROUPS).allowed);
    }

    @Test
    public void mostSpecificRuleWins() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.loadFromJson("{\"budgets\":["
                + "{\"group\":\"premium\",\"tokensPerDay\":1000},"
                + "{\"user\":\"alice\",\"tokensPerDay\":50}]}");
        Set<String> premium = Set.of("premium");
        be.recordUsage("alice", 50);
        assertFalse(be.check("alice", premium).allowed, "alice's user rule (50) overrides the group rule (1000)");
    }

    @Test
    public void budgetIsFairSharedByReplicaCount() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.setReplicaCountSupplier(() -> 2); // effective budget halves per replica
        be.setGlobalBudget(100);
        assertEquals(50L, be.getRemaining("alice", NO_GROUPS));
        be.recordUsage("alice", 50);
        assertFalse(be.check("alice", NO_GROUPS).allowed, "per-replica share (100/2=50) is exhausted");
    }

    @Test
    public void windowResetsAllowance() throws Exception {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer(200L); // 200ms window for the test
        be.setGlobalBudget(10);
        be.recordUsage("alice", 10);
        assertFalse(be.check("alice", NO_GROUPS).allowed);
        Thread.sleep(260L);
        assertTrue(be.check("alice", NO_GROUPS).allowed, "budget should reset when the window rolls");
    }

    @Test
    public void perModelBudgetCapsOnlyThatModel() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.loadFromJson("{\"budgets\":[{\"user\":\"alice\",\"model\":\"gpt-4\",\"tokensPerDay\":100}]}");
        be.recordUsage("alice", "gpt-4", 60, 40); // 100 tokens on gpt-4
        assertFalse(be.check("alice", NO_GROUPS, "gpt-4").allowed, "gpt-4 budget exhausted");
        assertEquals("token_budget", be.check("alice", NO_GROUPS, "gpt-4").reason);
        // No rule matches a different model => unlimited there.
        assertTrue(be.check("alice", NO_GROUPS, "llama-3").allowed);
    }

    @Test
    public void modelSpecificRuleOverridesAgnosticForThatModel() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.loadFromJson("{\"budgets\":["
                + "{\"user\":\"alice\",\"tokensPerDay\":1000},"
                + "{\"user\":\"alice\",\"model\":\"gpt-4\",\"tokensPerDay\":50}]}");
        be.recordUsage("alice", "gpt-4", 50, 0); // bumps (alice|gpt-4) and (alice|*) by 50
        assertFalse(be.check("alice", NO_GROUPS, "gpt-4").allowed, "tight gpt-4 budget (50) hit");
        assertTrue(be.check("alice", NO_GROUPS, "llama-3").allowed, "agnostic budget (1000) still has room");
    }

    @Test
    public void costBudgetUsesPriceTable() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.loadFromJson("{"
                + "\"budgets\":[{\"user\":\"alice\",\"costPerDay\":1.0}],"
                + "\"prices\":[{\"model\":\"gpt-4\",\"inputPer1k\":10.0,\"outputPer1k\":20.0}]}");
        // cost = 50/1000*10 + 25/1000*20 = 0.5 + 0.5 = 1.0
        assertEquals(1.0, be.costOf("gpt-4", 50, 25), 1e-9);
        be.recordUsage("alice", "gpt-4", 50, 25);
        TokenBudgetEnforcer.Decision d = be.check("alice", NO_GROUPS, "gpt-4");
        assertFalse(d.allowed);
        assertEquals("cost_budget", d.reason);
    }

    @Test
    public void wildcardPriceIsFallback() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.loadFromJson("{"
                + "\"budgets\":[{\"user\":\"alice\",\"costPerDay\":0.002}],"
                + "\"prices\":[{\"model\":\"*\",\"inputPer1k\":0.001,\"outputPer1k\":0.002}]}");
        // unpriced model 'x' falls back to '*': 1000/1000*0.001 + 500/1000*0.002 = 0.002
        be.recordUsage("alice", "x", 1000, 500);
        assertEquals("cost_budget", be.check("alice", NO_GROUPS, "x").reason);
    }

    // --- precise (shared-counter) path -----------------------------------------------------

    /** Deterministic in-memory windowed counter, standing in for the HBase counter. */
    static final class FakeCounter implements WindowedCounter {
        private final Map<String, long[]> m = new HashMap<>();
        @Override public synchronized long incrementAndGet(String key, long delta, long windowMs) {
            long[] v = m.computeIfAbsent(key + "@" + (System.currentTimeMillis() / windowMs), x -> new long[1]);
            v[0] += delta;
            return v[0];
        }
        @Override public synchronized long get(String key, long windowMs) {
            long[] v = m.get(key + "@" + (System.currentTimeMillis() / windowMs));
            return v == null ? 0L : v[0];
        }
    }

    @Test
    public void preciseBudgetIsExactAndIgnoresReplicaCount() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.setSharedCounter(new FakeCounter());
        be.setReplicaCountSupplier(() -> 4); // must be ignored in precise mode
        be.setGlobalBudget(100);

        be.recordUsage("alice", "m", 60, 0);
        // If the replica count were applied, the cap would be 25 and 60 would already exceed it.
        assertTrue(be.check("alice", NO_GROUPS, "m").allowed, "precise cap is the full 100, not 100/4");
        be.recordUsage("alice", "m", 50, 0); // total 110 > 100
        assertEquals("token_budget", be.check("alice", NO_GROUPS, "m").reason);
    }

    @Test
    public void preciseCostBudgetAccumulatesAcrossCalls() {
        TokenBudgetEnforcer be = new TokenBudgetEnforcer();
        be.setSharedCounter(new FakeCounter());
        be.loadFromJson("{"
                + "\"budgets\":[{\"user\":\"alice\",\"costPerDay\":1.0}],"
                + "\"prices\":[{\"model\":\"gpt-4\",\"inputPer1k\":10.0,\"outputPer1k\":20.0}]}");
        // Two calls of 0.5 each => 1.0 total, hitting the cost budget exactly.
        be.recordUsage("alice", "gpt-4", 50, 0);  // 0.5
        assertTrue(be.check("alice", NO_GROUPS, "gpt-4").allowed);
        be.recordUsage("alice", "gpt-4", 0, 25);  // +0.5 => 1.0
        assertEquals("cost_budget", be.check("alice", NO_GROUPS, "gpt-4").reason);
    }
}
