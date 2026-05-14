package varga.tarn.yarn.openai;

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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConversationAffinityTest {

    private static ContainerId containerId(String id) {
        ContainerId c = mock(ContainerId.class);
        when(c.toString()).thenReturn(id);
        return c;
    }

    @Test
    public void recordsAndRetrievesAffinity() {
        ConversationAffinity a = new ConversationAffinity(60_000);
        ContainerId c = containerId("c1");
        a.put("conv-1", c);
        assertSame(c, a.get("conv-1"));
        // Unknown conversation id returns null without throwing.
        assertNull(a.get("conv-2"));
        // Null / blank ids are ignored on both put and get.
        a.put("", c);
        a.put(null, c);
        assertNull(a.get(""));
        assertNull(a.get(null));
    }

    @Test
    public void expiresAfterTtl() throws Exception {
        ConversationAffinity a = new ConversationAffinity(50);
        ContainerId c = containerId("c1");
        a.put("conv", c);
        Thread.sleep(80);
        assertNull(a.get("conv"), "entry must expire after the TTL");
    }

    @Test
    public void evictByContainerRemovesAllPinsForThatBackend() {
        ConversationAffinity a = new ConversationAffinity(60_000);
        ContainerId c1 = containerId("c1");
        ContainerId c2 = containerId("c2");
        a.put("conv-1", c1);
        a.put("conv-2", c1);
        a.put("conv-3", c2);
        int removed = a.evictByContainer(c1);
        assertEquals(2, removed);
        assertNull(a.get("conv-1"));
        assertNull(a.get("conv-2"));
        assertSame(c2, a.get("conv-3"));
    }

    @Test
    public void purgeExpiredDropsOnlyStaleEntries() throws Exception {
        ConversationAffinity a = new ConversationAffinity(40);
        a.put("old", containerId("c-old"));
        Thread.sleep(60);
        a.put("fresh", containerId("c-fresh"));
        int removed = a.purgeExpired();
        assertEquals(1, removed);
        assertNotNull(a.get("fresh"));
        assertNull(a.get("old"));
    }
}
