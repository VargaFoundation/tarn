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

import java.util.Collections;
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
}
