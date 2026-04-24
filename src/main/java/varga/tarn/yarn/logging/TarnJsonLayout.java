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

import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

/**
 * Log4j 1.x (reload4j) layout that emits newline-delimited JSON suitable for Fluentd,
 * Promtail, Vector, or any log pipeline that ingests JSON lines.
 *
 * <p>Each record includes timestamp (ISO-8601 UTC), level, logger, thread, message, and
 * every MDC key present — so the {@code trace_id} / {@code span_id} pushed by
 * {@link varga.tarn.yarn.openai.TarnTracing} automatically show up for correlation.
 *
 * <p>Why hand-written rather than {@code logstash-log4j-encoder}: keeping zero extra runtime
 * deps on YARN classpath, and avoiding the log4j1 → log4j2 upgrade that would clash with
 * Hadoop's bundled log4j1. This is ~100 lines and does what we need.
 *
 * <p><b>How to activate</b>: set {@code -Dlog4j.configuration=log4j-json.properties} and ship
 * the sample config from {@code src/main/resources/log4j-json.properties}, or add the
 * following to an existing log4j.properties:
 * <pre>
 *   log4j.appender.tarnJson=org.apache.log4j.ConsoleAppender
 *   log4j.appender.tarnJson.layout=varga.tarn.yarn.logging.TarnJsonLayout
 * </pre>
 */
public class TarnJsonLayout extends Layout {

    @Override
    public boolean ignoresThrowable() {
        // We include stack traces ourselves, so tell log4j not to append its own.
        return false;
    }

    @Override
    public void activateOptions() {
        // No options to activate.
    }

    @Override
    public String format(LoggingEvent event) {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        field(sb, "ts", Instant.ofEpochMilli(event.getTimeStamp()).toString()); sb.append(',');
        field(sb, "level", event.getLevel().toString()); sb.append(',');
        field(sb, "logger", event.getLoggerName()); sb.append(',');
        field(sb, "thread", event.getThreadName()); sb.append(',');
        field(sb, "message", String.valueOf(event.getRenderedMessage()));

        // Flatten MDC: trace_id, span_id, user, model, etc.
        @SuppressWarnings("rawtypes")
        Map context = event.getProperties();
        if (context != null && !context.isEmpty()) {
            // Sort for deterministic output (tests, diffing).
            TreeMap<String, String> sorted = new TreeMap<>();
            for (Object entry : context.entrySet()) {
                Map.Entry<?, ?> e = (Map.Entry<?, ?>) entry;
                sorted.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
            }
            for (Map.Entry<String, String> e : sorted.entrySet()) {
                sb.append(',');
                field(sb, e.getKey(), e.getValue());
            }
        }

        ThrowableInformation ti = event.getThrowableInformation();
        if (ti != null) {
            sb.append(',');
            field(sb, "exception", ti.getThrowable().getClass().getName());
            sb.append(',');
            StringBuilder stack = new StringBuilder();
            for (String line : ti.getThrowableStrRep()) {
                stack.append(line).append('\n');
            }
            field(sb, "stack", stack.toString());
        }
        sb.append("}\n");
        return sb.toString();
    }

    private static void field(StringBuilder sb, String key, String value) {
        sb.append('"').append(escape(key)).append("\":\"").append(escape(value)).append('"');
    }

    /**
     * Minimal JSON string escaper — handles the control characters RFC 8259 requires and
     * drops trailing quotes / backslashes. Intentionally tight: a log line is not untrusted
     * input but messages can contain user-provided strings, so escaping is still needed.
     */
    static String escape(String s) {
        if (s == null) return "";
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':  out.append("\\\""); break;
                case '\\': out.append("\\\\"); break;
                case '\b': out.append("\\b"); break;
                case '\f': out.append("\\f"); break;
                case '\n': out.append("\\n"); break;
                case '\r': out.append("\\r"); break;
                case '\t': out.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        return out.toString();
    }
}
