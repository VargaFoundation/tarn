package varga.tarn.yarn.logging;

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
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TarnJsonLayoutTest {

    private final TarnJsonLayout layout = new TarnJsonLayout();
    private final ObjectMapper om = new ObjectMapper();

    @AfterEach
    void cleanMdc() {
        MDC.clear();
    }

    @Test
    public void producesParseableJson() throws Exception {
        Category cat = Logger.getLogger("t.c");
        LoggingEvent ev = new LoggingEvent(cat.getClass().getName(), cat, Level.INFO, "hello", null);
        String line = layout.format(ev);
        assertTrue(line.endsWith("\n"));
        JsonNode j = om.readTree(line);
        assertEquals("INFO", j.get("level").asText());
        assertEquals("t.c", j.get("logger").asText());
        assertEquals("hello", j.get("message").asText());
        assertNotNull(j.get("ts"));
        assertNotNull(j.get("thread"));
    }

    @Test
    public void mdcKeysAreFlattenedIntoJson() throws Exception {
        MDC.put("trace_id", "abc123");
        MDC.put("span_id", "def456");
        MDC.put("user", "alice");
        Category cat = Logger.getLogger("t.c");
        LoggingEvent ev = new LoggingEvent(cat.getClass().getName(), cat, Level.INFO, "m", null);
        JsonNode j = om.readTree(layout.format(ev));
        assertEquals("abc123", j.get("trace_id").asText());
        assertEquals("def456", j.get("span_id").asText());
        assertEquals("alice", j.get("user").asText());
    }

    @Test
    public void escapesSpecialCharactersInMessage() throws Exception {
        Category cat = Logger.getLogger("t.c");
        LoggingEvent ev = new LoggingEvent(cat.getClass().getName(), cat, Level.WARN,
                "line1\nline2\"quote\"\\back", null);
        String line = layout.format(ev);
        // If parsing succeeds, escaping is correct.
        JsonNode j = om.readTree(line);
        assertEquals("line1\nline2\"quote\"\\back", j.get("message").asText());
    }

    @Test
    public void includesExceptionAndStack() throws Exception {
        Category cat = Logger.getLogger("t.c");
        Exception e = new RuntimeException("boom");
        LoggingEvent ev = new LoggingEvent(cat.getClass().getName(), cat, Level.ERROR, "fail", e);
        JsonNode j = om.readTree(layout.format(ev));
        assertEquals("java.lang.RuntimeException", j.get("exception").asText());
        assertTrue(j.get("stack").asText().contains("boom"));
    }

    @Test
    public void escapeUtilityHandlesControlChars() {
        assertEquals("\\u0001", TarnJsonLayout.escape("\u0001"));
        assertEquals("\\n", TarnJsonLayout.escape("\n"));
        assertEquals("safe", TarnJsonLayout.escape("safe"));
    }
}
