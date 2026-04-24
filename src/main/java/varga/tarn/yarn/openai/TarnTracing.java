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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.MDC;

import java.util.Map;

/**
 * Thin wrapper around the OpenTelemetry API to keep instrumentation idiomatic inside TARN.
 *
 * <p><b>Philosophy:</b> we depend only on {@code opentelemetry-api}. No SDK or exporter is
 * shipped with the uber-JAR. Operators who want distributed traces attach the standard
 * {@code opentelemetry-javaagent.jar} via {@code -javaagent:...} on the AM JVM; the agent
 * auto-configures the global provider and our spans flow to OTLP / Jaeger / Zipkin without
 * any code change here. When no agent is attached, {@code GlobalOpenTelemetry} returns a
 * no-op tracer and the instrumentation is practically free.
 *
 * <p>We also mirror {@code trace_id} / {@code span_id} into the SLF4J MDC so operators get
 * correlated log lines today, even before full structured-logging migration.
 */
public final class TarnTracing {

    public static final AttributeKey<String> ATTR_MODEL = AttributeKey.stringKey("tarn.model");
    public static final AttributeKey<String> ATTR_LORA = AttributeKey.stringKey("tarn.lora");
    public static final AttributeKey<String> ATTR_USER = AttributeKey.stringKey("tarn.user");
    public static final AttributeKey<String> ATTR_CONTAINER = AttributeKey.stringKey("tarn.container_id");
    public static final AttributeKey<Boolean> ATTR_STREAM = AttributeKey.booleanKey("tarn.stream");

    private static final String TRACER_NAME = "varga.tarn";
    private static final String TRACER_VERSION = "0.0.1";

    private TarnTracing() {}

    public static Tracer tracer() {
        return GlobalOpenTelemetry.getTracer(TRACER_NAME, TRACER_VERSION);
    }

    public static TextMapPropagator propagator() {
        return GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
    }

    /**
     * Starts a SERVER span for an incoming proxy request, extracting parent context from
     * W3C trace headers if the upstream gateway (Knox) set them.
     */
    public static Span startIncomingSpan(String opName, Map<String, String> headers) {
        Context parent = propagator().extract(Context.current(), headers, HEADER_GETTER);
        return tracer().spanBuilder(opName).setSpanKind(SpanKind.SERVER).setParent(parent).startSpan();
    }

    /** Start an outbound CLIENT span when the proxy calls Triton upstream. */
    public static Span startUpstreamSpan(String opName) {
        return tracer().spanBuilder(opName).setSpanKind(SpanKind.CLIENT).startSpan();
    }

    /** Inject the current context into a mutable header map for propagation downstream. */
    public static void injectHeaders(Map<String, String> carrier) {
        propagator().inject(Context.current(), carrier, HEADER_SETTER);
    }

    /**
     * Pushes the active span's trace/span ids to SLF4J MDC so that every log line emitted
     * within the {@code try (var ignored = pushMdc(span))} block carries them. Returns an
     * AutoCloseable that cleans MDC on exit.
     */
    public static AutoCloseable pushMdc(Span span) {
        String traceId = span.getSpanContext().getTraceId();
        String spanId = span.getSpanContext().getSpanId();
        MDC.put("trace_id", traceId);
        MDC.put("span_id", spanId);
        return () -> {
            MDC.remove("trace_id");
            MDC.remove("span_id");
        };
    }

    private static final TextMapGetter<Map<String, String>> HEADER_GETTER = new TextMapGetter<>() {
        @Override public Iterable<String> keys(Map<String, String> c) { return c.keySet(); }
        @Override public String get(Map<String, String> c, String key) {
            if (c == null) return null;
            // Header names are case-insensitive.
            String v = c.get(key);
            if (v != null) return v;
            for (Map.Entry<String, String> e : c.entrySet()) {
                if (e.getKey().equalsIgnoreCase(key)) return e.getValue();
            }
            return null;
        }
    };

    private static final TextMapSetter<Map<String, String>> HEADER_SETTER = Map::put;
}
