package varga.tarn.operator;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Tiny Prometheus HTTP API client. Returns the scalar value of an instant query — that's
 * all the canary gate needs (error_rate and p95 are derived expressions evaluated server-
 * side). Failure modes (Prometheus unreachable, malformed response, NaN) all collapse to a
 * sentinel {@link #NO_VALUE} so callers can treat them as "metric unknown".
 *
 * <p>Deliberately not using the official Prometheus Java client: we don't need its
 * sampling/exposition primitives, and pulling it in would inflate the operator JAR by
 * ~3 MB for what amounts to two HTTP requests per CR per reconcile tick.
 */
public final class PrometheusQuerier {

    public static final double NO_VALUE = Double.NaN;

    private static final Logger log = LoggerFactory.getLogger(PrometheusQuerier.class);

    private final String baseUrl;
    private final HttpClient http;
    private final ObjectMapper om = new ObjectMapper();

    public PrometheusQuerier(String baseUrl) {
        if (baseUrl == null || baseUrl.isEmpty()) {
            throw new IllegalArgumentException("baseUrl must be set");
        }
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    // Test-only constructor allowing injection of a controlled HttpClient.
    PrometheusQuerier(String baseUrl, HttpClient http) {
        this.baseUrl = baseUrl;
        this.http = http;
    }

    /**
     * Runs an instant PromQL query and returns the first vector sample's value, or
     * {@link #NO_VALUE} when there's no series, the API is unreachable, or the response
     * isn't shaped like Prometheus' standard {@code data.result[0].value[1]}.
     */
    public double instantScalar(String promQL) {
        try {
            String url = baseUrl + "/api/v1/query?query="
                    + URLEncoder.encode(promQL, StandardCharsets.UTF_8);
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                log.warn("Prometheus {} returned {}", url, resp.statusCode());
                return NO_VALUE;
            }
            JsonNode root = om.readTree(resp.body());
            if (!"success".equals(root.path("status").asText())) return NO_VALUE;
            JsonNode result = root.path("data").path("result");
            if (!result.isArray() || result.isEmpty()) return NO_VALUE;
            JsonNode value = result.get(0).path("value");
            if (!value.isArray() || value.size() < 2) return NO_VALUE;
            String raw = value.get(1).asText();
            if (raw == null || raw.isEmpty() || "NaN".equals(raw)) return NO_VALUE;
            return Double.parseDouble(raw);
        } catch (Exception e) {
            log.debug("Prometheus query failed for `{}`: {}", promQL, e.getMessage());
            return NO_VALUE;
        }
    }
}
