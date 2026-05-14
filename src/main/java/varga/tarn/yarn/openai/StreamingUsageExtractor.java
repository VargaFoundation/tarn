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

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts the {@code usage} block emitted at the tail of an OpenAI SSE stream when the
 * client (or the proxy on its behalf) opts in via {@code stream_options.include_usage=true}.
 *
 * <p>Per the OpenAI streaming protocol, the final non-{@code [DONE]} {@code data:} chunk
 * carries the {@code usage} object — for chat completions that means the chunk just before
 * {@code data: [DONE]\n\n}. The implementation maintains a small sliding window (the last
 * {@code TAIL_BYTES}) of UTF-8 input, then on end-of-stream scans it for the most recent
 * SSE data line containing the {@code "usage"} key and parses it via Jackson.
 *
 * <p>This is deliberately not a full SSE parser. We don't need to reassemble multi-event
 * streams or care about event names — we just want the last usage block before {@code [DONE]}.
 */
public final class StreamingUsageExtractor {

    /** Keep enough bytes to cover several full SSE chunks; usage blocks are small. */
    private static final int TAIL_BYTES = 16 * 1024;
    /** Match an SSE data line whose payload contains a "usage" key (JSON object). */
    private static final Pattern DATA_LINE = Pattern.compile(
            "(?ms)^data:\\s*(\\{[^\\n]*?\"usage\"[^\\n]*\\})\\s*$");

    private final StringBuilder tail = new StringBuilder(TAIL_BYTES);

    /** Append the bytes just relayed downstream. Safe to call across arbitrary chunk sizes. */
    public void onChunk(byte[] data, int offset, int length) {
        if (length <= 0) return;
        // SSE is UTF-8; partial multibyte at the boundary is fine because we eventually parse
        // a full data: line inside the buffer, not the boundary itself.
        tail.append(new String(data, offset, length, StandardCharsets.UTF_8));
        if (tail.length() > TAIL_BYTES) {
            tail.delete(0, tail.length() - TAIL_BYTES);
        }
    }

    /**
     * Locates the most recent SSE {@code data:} chunk that carries {@code "usage"} and
     * returns the parsed JSON node. Returns {@code null} when the buffer holds none —
     * caller should not assume {@code [DONE]} implies a usage block was present (some
     * backends still skip it).
     */
    public JsonNode findUsage(ObjectMapper om) {
        if (tail.length() == 0) return null;
        Matcher m = DATA_LINE.matcher(tail);
        String lastMatch = null;
        while (m.find()) {
            lastMatch = m.group(1);
        }
        if (lastMatch == null) return null;
        try {
            JsonNode root = om.readTree(lastMatch);
            JsonNode usage = root.path("usage");
            return usage.isObject() ? usage : null;
        } catch (Exception e) {
            // Truncated or malformed JSON in the tail — give up silently; metrics are best-effort.
            return null;
        }
    }
}
