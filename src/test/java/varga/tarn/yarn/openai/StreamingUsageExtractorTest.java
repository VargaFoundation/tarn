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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class StreamingUsageExtractorTest {

    private final ObjectMapper om = new ObjectMapper();

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static void feed(StreamingUsageExtractor x, String s) {
        byte[] b = bytes(s);
        x.onChunk(b, 0, b.length);
    }

    @Test
    public void extractsUsageFromCanonicalSseStream() {
        StreamingUsageExtractor x = new StreamingUsageExtractor();
        feed(x, "data: {\"delta\":{\"content\":\"he\"}}\n\n");
        feed(x, "data: {\"delta\":{\"content\":\"llo\"}}\n\n");
        feed(x, "data: {\"id\":\"x\",\"choices\":[],\"usage\":"
                + "{\"prompt_tokens\":7,\"completion_tokens\":11,\"total_tokens\":18}}\n\n");
        feed(x, "data: [DONE]\n\n");

        JsonNode usage = x.findUsage(om);
        assertNotNull(usage, "usage block must be extracted from the SSE tail");
        assertEquals(7L, usage.path("prompt_tokens").asLong());
        assertEquals(11L, usage.path("completion_tokens").asLong());
    }

    @Test
    public void returnsNullWhenUsageMissing() {
        StreamingUsageExtractor x = new StreamingUsageExtractor();
        feed(x, "data: {\"delta\":{\"content\":\"hi\"}}\n\n");
        feed(x, "data: [DONE]\n\n");
        assertNull(x.findUsage(om));
    }

    @Test
    public void slidingWindowKeepsTheTail() {
        StreamingUsageExtractor x = new StreamingUsageExtractor();
        // Spam noise far larger than the 16 KB window — the usage chunk arrives at the end.
        StringBuilder noise = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            noise.append("data: {\"delta\":{\"content\":\"x\"}}\n\n");
        }
        feed(x, noise.toString());
        feed(x, "data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":5}}\n\n");

        JsonNode usage = x.findUsage(om);
        assertNotNull(usage, "the tail-window must preserve the final usage chunk");
        assertEquals(3L, usage.path("prompt_tokens").asLong());
    }
}
