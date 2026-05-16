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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class EmbeddingCacheTest {

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    public void disabledCacheStoresNothing() {
        EmbeddingCache cache = new EmbeddingCache(0);
        assertFalse(cache.isEnabled());
        cache.put("k", b("body"), "application/json");
        assertNull(cache.get("k"));
        assertEquals(0, cache.size());
    }

    @Test
    public void storesAndReturnsEntry() {
        EmbeddingCache cache = new EmbeddingCache(10);
        cache.put("k", b("vector-payload"), "application/json");
        EmbeddingCache.Entry e = cache.get("k");
        assertNotNull(e);
        assertEquals("vector-payload", new String(e.body, StandardCharsets.UTF_8));
        assertEquals("application/json", e.contentType);
        assertNull(cache.get("absent"));
    }

    @Test
    public void keyForIsStableAndDistinct() {
        assertEquals(EmbeddingCache.keyFor(b("{\"model\":\"m\",\"input\":\"hello\"}")),
                EmbeddingCache.keyFor(b("{\"model\":\"m\",\"input\":\"hello\"}")));
        assertNotEquals(EmbeddingCache.keyFor(b("{\"model\":\"m\",\"input\":\"hello\"}")),
                EmbeddingCache.keyFor(b("{\"model\":\"m\",\"input\":\"world\"}")));
    }

    @Test
    public void evictsLeastRecentlyUsedPastCapacity() {
        EmbeddingCache cache = new EmbeddingCache(2);
        cache.put("a", b("A"), "application/json");
        cache.put("b", b("B"), "application/json");
        // Touch "a" so "b" becomes the least-recently-used.
        assertNotNull(cache.get("a"));
        cache.put("c", b("C"), "application/json");

        assertEquals(2, cache.size());
        assertNotNull(cache.get("a"), "recently used entry should survive");
        assertNull(cache.get("b"), "LRU entry should be evicted");
        assertNotNull(cache.get("c"));
    }
}
