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

public class QuotaEnforcerTest {

    @Test
    public void noRulesAllowsEverything() {
        QuotaEnforcer q = new QuotaEnforcer();
        assertTrue(q.check("alice", Collections.emptySet(), "any").allowed);
    }

    @Test
    public void rateLimitBlocksAfterCapacity() {
        QuotaEnforcer q = new QuotaEnforcer();
        q.loadFromJson("{\"rules\":[{\"user\":\"alice\",\"model\":\"m\",\"requestsPerMinute\":3}]}");
        assertTrue(q.check("alice", Set.of(), "m").allowed);
        assertTrue(q.check("alice", Set.of(), "m").allowed);
        assertTrue(q.check("alice", Set.of(), "m").allowed);
        QuotaEnforcer.Decision d4 = q.check("alice", Set.of(), "m");
        assertFalse(d4.allowed, "4th request must be rate-limited");
        assertEquals("rate_limited", d4.reason);
        assertTrue(d4.retryAfterMs > 0 && d4.retryAfterMs <= 60_000L);
    }

    @Test
    public void zeroRpmDeniesImmediately() {
        QuotaEnforcer q = new QuotaEnforcer();
        q.loadFromJson("{\"rules\":[{\"user\":\"banned\",\"model\":\"*\",\"requestsPerMinute\":0}]}");
        QuotaEnforcer.Decision d = q.check("banned", Set.of(), "any-model");
        assertFalse(d.allowed);
        assertEquals("quota_zero", d.reason);
    }

    @Test
    public void userRuleBeatsGroupRuleAndWildcard() {
        QuotaEnforcer q = new QuotaEnforcer();
        q.loadFromJson("{\"rules\":[" +
                "{\"model\":\"*\",\"requestsPerMinute\":1}," +       // default: 1 rpm
                "{\"group\":\"team-a\",\"model\":\"*\",\"requestsPerMinute\":2}," + // group: 2
                "{\"user\":\"alice\",\"model\":\"m\",\"requestsPerMinute\":5}" +   // exact: 5
                "]}");
        // Alice in team-a asking for model m -> should get the most specific (5 rpm).
        for (int i = 0; i < 5; i++) {
            assertTrue(q.check("alice", Set.of("team-a"), "m").allowed, "grant #" + (i + 1));
        }
        assertFalse(q.check("alice", Set.of("team-a"), "m").allowed, "6th exceeds 5 rpm");
    }

    @Test
    public void groupMembersShareBucket() {
        QuotaEnforcer q = new QuotaEnforcer();
        q.loadFromJson("{\"rules\":[{\"group\":\"shared\",\"model\":\"*\",\"requestsPerMinute\":2}]}");
        assertTrue(q.check("alice", Set.of("shared"), "m").allowed);
        assertTrue(q.check("bob", Set.of("shared"), "m").allowed);
        // Third request from anyone in the group is refused — group bucket is drained.
        assertFalse(q.check("carol", Set.of("shared"), "m").allowed);
    }

    @Test
    public void wildcardModelAppliesAcrossModels() {
        QuotaEnforcer q = new QuotaEnforcer();
        q.loadFromJson("{\"rules\":[{\"user\":\"alice\",\"model\":\"*\",\"requestsPerMinute\":2}]}");
        assertTrue(q.check("alice", Set.of(), "m1").allowed);
        assertTrue(q.check("alice", Set.of(), "m2").allowed);
        // Both models share the wildcard bucket key — third request denied.
        assertFalse(q.check("alice", Set.of(), "m3").allowed);
    }

    @Test
    public void setGlobalLimitConfiguresDefault() {
        QuotaEnforcer q = new QuotaEnforcer();
        q.setGlobalLimit(1);
        assertTrue(q.check("anon", Set.of(), "any").allowed);
        assertFalse(q.check("anon", Set.of(), "any").allowed);
    }
}
