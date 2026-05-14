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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import varga.tarn.yarn.ApplicationMaster;
import varga.tarn.yarn.MetricsCollector;
import varga.tarn.yarn.QuotaEnforcer;
import varga.tarn.yarn.RangerAuthorizer;
import varga.tarn.yarn.TarnConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Reverse-proxy for OpenAI-compatible endpoints ({@code /v1/chat/completions},
 * {@code /v1/completions}, {@code /v1/models}, {@code /v1/embeddings}).
 *
 * <p>Pipeline for every inference request:
 * <ol>
 *   <li>Identify the user (Kerberos via UGI or {@code X-Forwarded-User} / {@code X-TARN-User}
 *       header when behind Knox).</li>
 *   <li>Parse the JSON body, extract {@code model}, split an optional LoRA suffix
 *       (format {@code base#lora}).</li>
 *   <li>Ask Ranger for {@code infer} permission on both the base model and the combined
 *       {@code base#lora} resource — this is the inference-level enforcement that was
 *       missing before P1.1.</li>
 *   <li>Pick the least-loaded container (queue depth descending).</li>
 *   <li>Forward the request. If {@code stream=true} in the body, stream the upstream response
 *       byte-for-byte to the client, recording the full duration in the latency histogram
 *       once the stream completes.</li>
 * </ol>
 */
public class OpenAIProxyHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(OpenAIProxyHandler.class);
    /** Triton's openai_frontend uses {@code #} by default for multi-LoRA routing. */
    public static final String LORA_SEPARATOR = "#";
    /** Upstream timeout: long enough for large LLM generations, but bounded. */
    private static final Duration UPSTREAM_TIMEOUT = Duration.ofMinutes(10);

    private final ApplicationMaster am;
    private final TarnConfig config;
    private final HttpClient upstream;
    private final ObjectMapper om = new ObjectMapper();

    public OpenAIProxyHandler(ApplicationMaster am, TarnConfig config) {
        this.am = am;
        this.config = config;
        // Prefer the AM's shared client (which is TLS-configured when --triton-tls-enabled is set).
        // Fall back to a fresh plain client for unit tests that construct the handler without an AM
        // that owns a Triton client.
        HttpClient shared = am == null ? null : am.getTritonHttpClient();
        this.upstream = shared != null ? shared : HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1) // Triton openai_frontend is HTTP/1.1 today.
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    public void handle(HttpExchange ex) {
        String path = ex.getRequestURI().getPath();
        try {
            // When OAuth is configured, gate every endpoint behind a valid Bearer JWT.
            // The legacy static-token flow continues to work when oauthIssuer is unset.
            if (!authenticate(ex)) return;
            if (path.endsWith("/models") || path.endsWith("/v1/models")) {
                handleListModels(ex);
                return;
            }
            if (path.endsWith("/chat/completions") || path.endsWith("/completions")
                    || path.endsWith("/embeddings")) {
                handleInferenceProxy(ex);
                return;
            }
            if (path.endsWith("/usage") || path.endsWith("/v1/usage")) {
                handleUsageReport(ex);
                return;
            }
            writeJsonError(ex, 404, "not_found", "Unknown endpoint: " + path);
        } catch (Exception e) {
            log.error("Proxy handler failed", e);
            try {
                writeJsonError(ex, 500, "internal_error", e.getMessage() == null ? "unknown" : e.getMessage());
            } catch (IOException io) {
                log.debug("Unable to write error response", io);
            }
        }
    }

    private void handleListModels(HttpExchange ex) throws IOException {
        String user = getUser(ex);
        Set<String> groups = getGroups(ex, user);
        String clientIp = resolveClientIp(ex);
        RangerAuthorizer ra = am.getRangerAuthorizer();

        List<Map<String, Object>> data = new ArrayList<>();
        for (String m : am.getAvailableModels()) {
            if (ra != null && !ra.isAllowed(user, groups, "list", m, clientIp)) continue;
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("id", m);
            entry.put("object", "model");
            entry.put("owned_by", "tarn");
            entry.put("created", System.currentTimeMillis() / 1000L);
            data.add(entry);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("object", "list");
        body.put("data", data);
        writeJson(ex, 200, body);
    }

    private void handleInferenceProxy(HttpExchange ex) throws Exception {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            writeJsonError(ex, 405, "method_not_allowed", "Use POST");
            return;
        }
        // Start a SERVER span rooted on any Knox-propagated trace context; MDC is pushed so
        // every log line inside the handler carries trace_id / span_id.
        Map<String, String> reqHeaders = flattenHeaders(ex);
        Span span = TarnTracing.startIncomingSpan("POST " + ex.getRequestURI().getPath(), reqHeaders);
        try (Scope scope = span.makeCurrent(); AutoCloseable mdc = TarnTracing.pushMdc(span)) {
            handleInferenceProxyTraced(ex, span);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getClass().getSimpleName());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    private void handleInferenceProxyTraced(HttpExchange ex, Span span) throws Exception {
        byte[] body;
        try (InputStream is = ex.getRequestBody()) {
            body = is.readAllBytes();
        }
        Map<String, Object> reqBody;
        try {
            reqBody = om.readValue(body, new TypeReference<Map<String, Object>>() { });
        } catch (Exception e) {
            writeJsonError(ex, 400, "invalid_json", "Request body is not valid JSON");
            return;
        }

        Object modelObj = reqBody.get("model");
        if (!(modelObj instanceof String) || ((String) modelObj).isEmpty()) {
            writeJsonError(ex, 400, "missing_model", "Request must include 'model' field");
            return;
        }
        String requestedModel = (String) modelObj;
        String baseModel = requestedModel;
        String lora = null;
        int sep = requestedModel.indexOf(LORA_SEPARATOR);
        if (sep > 0 && sep < requestedModel.length() - 1) {
            baseModel = requestedModel.substring(0, sep);
            lora = requestedModel.substring(sep + 1);
        }

        String user = getUser(ex);
        Set<String> groups = getGroups(ex, user);
        span.setAttribute(TarnTracing.ATTR_USER, user);
        span.setAttribute(TarnTracing.ATTR_MODEL, baseModel);
        if (lora != null) span.setAttribute(TarnTracing.ATTR_LORA, lora);

        // First gate: process-wide cap on req/s. Sits ahead of every other check so a
        // misconfigured client can't even cost us a Ranger policy lookup. Disabled by
        // default (--global-rate-limit-rps=0).
        varga.tarn.yarn.GlobalRateLimiter globalLimiter = am.getGlobalRateLimiter();
        if (globalLimiter != null && globalLimiter.isEnabled()) {
            long waitMs = globalLimiter.tryAcquire();
            if (waitMs > 0) {
                span.setStatus(StatusCode.ERROR, "global_rate_limit");
                am.getMetricsCollector().recordGlobalRateLimitReject();
                long retryAfterSec = Math.max(1L, (waitMs + 999L) / 1000L);
                ex.getResponseHeaders().set("Retry-After", String.valueOf(retryAfterSec));
                writeJsonError(ex, 429, "global_rate_limit",
                        "Proxy is over its global rate limit ("
                                + globalLimiter.getRequestsPerSecond()
                                + " req/s). Retry after " + retryAfterSec + "s.");
                return;
            }
        }

        // Quotas run BEFORE Ranger: rate-limit cheaply, don't waste a policy-engine call on
        // a request we're going to reject anyway.
        QuotaEnforcer quotas = am.getQuotaEnforcer();
        if (quotas != null) {
            QuotaEnforcer.Decision q = quotas.check(user, groups, baseModel);
            if (!q.allowed) {
                span.setStatus(StatusCode.ERROR, "quota_denied");
                span.setAttribute("tarn.quota.reason", q.reason);
                long retryAfterSec = Math.max(1L, (q.retryAfterMs + 999L) / 1000L);
                ex.getResponseHeaders().set("Retry-After", String.valueOf(retryAfterSec));
                writeJsonError(ex, 429, q.reason,
                        "Request rate limit exceeded (" + q.ruleDesc + "). Retry after "
                                + retryAfterSec + "s.");
                return;
            }
        }

        String clientIp = resolveClientIp(ex);
        RangerAuthorizer ra = am.getRangerAuthorizer();
        if (ra != null) {
            if (!ra.isAllowed(user, groups, "infer", baseModel, clientIp)) {
                log.info("Ranger DENY infer: user={} model={} ip={}", user, baseModel, clientIp);
                span.setStatus(StatusCode.ERROR, "ranger_deny");
                writeJsonError(ex, 403, "permission_denied",
                        "Access to model '" + baseModel + "' is denied by policy");
                return;
            }
            // When a LoRA adapter is requested, the combined name is also a resource so policies
            // can grant base but restrict specific LoRAs.
            if (lora != null && !ra.isAllowed(user, groups, "infer", requestedModel, clientIp)) {
                log.info("Ranger DENY LoRA infer: user={} combined={} ip={}", user, requestedModel, clientIp);
                span.setStatus(StatusCode.ERROR, "ranger_deny_lora");
                writeJsonError(ex, 403, "permission_denied",
                        "Access to LoRA '" + lora + "' on '" + baseModel + "' is denied by policy");
                return;
            }
        }

        Container target = pickLeastLoadedReadyContainer();
        if (target == null) {
            span.setStatus(StatusCode.ERROR, "no_backends");
            writeJsonError(ex, 503, "service_unavailable", "No Triton instances are ready");
            return;
        }
        span.setAttribute(TarnTracing.ATTR_CONTAINER, target.getId().toString());

        String host = target.getNodeId().getHost();
        int upstreamPort = config.tritonPort;
        URI upstreamUri = URI.create(config.tritonScheme() + "://" + host + ":" + upstreamPort
                + ex.getRequestURI().getRawPath());

        // Build a child CLIENT span for the upstream call; inject W3C headers so Triton can join.
        Map<String, String> upstreamHeaders = new HashMap<>();
        upstreamHeaders.put("Content-Type", "application/json");
        upstreamHeaders.put("Accept", "text/event-stream, application/json");
        upstreamHeaders.put("X-Forwarded-User", user);

        boolean streaming = Boolean.TRUE.equals(reqBody.get("stream"));
        span.setAttribute(TarnTracing.ATTR_STREAM, streaming);

        MetricsCollector mc = am.getMetricsCollector();
        long startNs = System.nanoTime();

        // Shadow traffic: asynchronously mirror a sampled fraction of requests to a parallel
        // endpoint for A/B comparison. Responses are discarded, only latency & error counts
        // are recorded against a "shadow" model tag so operators can diff distributions.
        maybeFireShadow(body, ex.getRequestURI().getRawPath(), baseModel);

        Span upstreamSpan = TarnTracing.startUpstreamSpan("triton.upstream");
        upstreamSpan.setAttribute("http.url", upstreamUri.toString());
        upstreamSpan.setAttribute(TarnTracing.ATTR_CONTAINER, target.getId().toString());
        try (Scope ignored = upstreamSpan.makeCurrent()) {
            TarnTracing.injectHeaders(upstreamHeaders);
            HttpRequest.Builder rb = HttpRequest.newBuilder()
                    .uri(upstreamUri)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .timeout(UPSTREAM_TIMEOUT);
            upstreamHeaders.forEach(rb::header);
            HttpRequest forwarded = rb.build();

            if (streaming) {
                HttpResponse<InputStream> resp = upstream.send(forwarded,
                        HttpResponse.BodyHandlers.ofInputStream());
                upstreamSpan.setAttribute("http.status_code", (long) resp.statusCode());
                relayStreamingResponse(resp, ex);
                mc.recordModelRequest(baseModel, resp.statusCode() / 100 == 2);
                // Streaming responses: token usage is in the final "data: {...usage:...}"
                // chunk per OpenAI spec. Parsing that reliably requires buffering the stream,
                // which defeats streaming. Operators who need token accounting for streaming
                // should enable stream_options={"include_usage": true} and point Triton's
                // openai_frontend at our /v1/usage callback (future work).
            } else {
                HttpResponse<byte[]> resp = upstream.send(forwarded,
                        HttpResponse.BodyHandlers.ofByteArray());
                upstreamSpan.setAttribute("http.status_code", (long) resp.statusCode());
                boolean ok = resp.statusCode() / 100 == 2;
                // Record metrics BEFORE flushing the response so tests (and any sync consumer
                // of the counters) observe the update atomically with the visible response.
                mc.recordModelRequest(baseModel, ok);
                if (ok) {
                    recordTokensIfPresent(resp.body(), user, baseModel);
                }
                writeResponse(ex, resp.statusCode(),
                        firstHeader(resp, "Content-Type", "application/json"),
                        resp.body());
            }
        } catch (java.net.http.HttpConnectTimeoutException e) {
            upstreamSpan.setStatus(StatusCode.ERROR, "upstream_timeout");
            mc.recordModelRequest(baseModel, false);
            writeJsonError(ex, 504, "upstream_timeout", "Triton did not respond in time");
        } catch (java.io.IOException e) {
            upstreamSpan.setStatus(StatusCode.ERROR, "upstream_error");
            upstreamSpan.recordException(e);
            mc.recordModelRequest(baseModel, false);
            writeJsonError(ex, 502, "upstream_error", "Upstream Triton error: " + e.getMessage());
        } finally {
            double latencyMs = (System.nanoTime() - startNs) / 1_000_000.0;
            mc.recordInferenceLatency(baseModel, latencyMs);
            upstreamSpan.end();
        }
    }

    /**
     * Out-of-band usage reporter for streaming completions. Streaming responses don't give us
     * a reliable in-band hook to extract the final {@code usage} block (parsing SSE chunks in
     * flight would either block the response or risk dropping bytes). Clients or the Triton
     * {@code openai_frontend} can POST their token counts here after each stream:
     *
     * <pre>{@code
     * POST /v1/usage
     * X-Forwarded-User: alice
     * {
     *   "model": "llama-3-70b",
     *   "prompt_tokens": 42,
     *   "completion_tokens": 128
     * }
     * }</pre>
     */
    private void handleUsageReport(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            writeJsonError(ex, 405, "method_not_allowed", "Use POST");
            return;
        }
        byte[] body;
        try (InputStream is = ex.getRequestBody()) {
            body = is.readNBytes(16 * 1024);
        }
        if (body.length == 0) {
            writeJsonError(ex, 400, "empty_body", "POST body required");
            return;
        }
        com.fasterxml.jackson.databind.JsonNode root;
        try {
            root = om.readTree(body);
        } catch (Exception e) {
            writeJsonError(ex, 400, "invalid_json", "body must be a JSON object");
            return;
        }
        String model = root.path("model").asText(null);
        if (model == null || model.isEmpty()) {
            writeJsonError(ex, 400, "missing_model", "'model' field required");
            return;
        }
        long prompt = root.path("prompt_tokens").asLong(0);
        long completion = root.path("completion_tokens").asLong(0);
        if (prompt < 0 || completion < 0) {
            writeJsonError(ex, 400, "invalid_tokens", "token counts must be non-negative");
            return;
        }
        // Trust the caller's claimed user only if they come in through a proxy that sets it.
        // Otherwise fall back to the authenticated principal.
        String user = getUser(ex);
        am.getMetricsCollector().recordTokens(user, model, prompt, completion);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("recorded", true);
        resp.put("user", user);
        resp.put("model", model);
        resp.put("prompt_tokens", prompt);
        resp.put("completion_tokens", completion);
        writeJson(ex, 200, resp);
    }

    /**
     * Parses the OpenAI {@code usage} field from a successful non-streaming completion and
     * updates per-(user, model) token counters. A malformed or missing usage block is
     * silently ignored — usage is best-effort telemetry, not a contract.
     */
    private void recordTokensIfPresent(byte[] body, String user, String model) {
        try {
            com.fasterxml.jackson.databind.JsonNode root = om.readTree(body);
            com.fasterxml.jackson.databind.JsonNode usage = root.path("usage");
            if (usage.isMissingNode() || !usage.isObject()) return;
            long prompt = usage.path("prompt_tokens").asLong(0);
            long completion = usage.path("completion_tokens").asLong(0);
            if (prompt > 0 || completion > 0) {
                am.getMetricsCollector().recordTokens(user, model, prompt, completion);
            }
        } catch (Exception ignored) {
            // Non-OpenAI shaped response (e.g. image generation): no usage to record.
        }
    }

    /**
     * Best-effort client IP. When Knox or an ingress proxies us, X-Forwarded-For is the
     * source of truth; fall back to the raw peer socket when no trusted proxy is set.
     */
    private static String resolveClientIp(HttpExchange ex) {
        String xff = ex.getRequestHeaders().getFirst("X-Forwarded-For");
        if (xff != null && !xff.isEmpty()) {
            int c = xff.indexOf(',');
            return (c > 0 ? xff.substring(0, c) : xff).trim();
        }
        java.net.InetSocketAddress peer = ex.getRemoteAddress();
        return peer == null ? "unknown" : peer.getAddress().getHostAddress();
    }

    /**
     * Sampling shadow dispatcher. Only fires when both shadowEndpoint and shadowSampleRate
     * are configured; never blocks the primary response path; any failure is logged but
     * swallowed so a degraded shadow never affects the real traffic.
     */
    private void maybeFireShadow(byte[] body, String path, String baseModel) {
        String endpoint = config.shadowEndpoint;
        double rate = config.shadowSampleRate;
        if (endpoint == null || endpoint.isEmpty() || rate <= 0.0) return;
        if (ThreadLocalRandom.current().nextDouble() >= rate) return;
        URI shadowUri;
        try {
            shadowUri = URI.create(endpoint + path);
        } catch (Exception e) {
            log.warn("Invalid shadow endpoint URI: {}", endpoint);
            return;
        }
        HttpRequest shadow = HttpRequest.newBuilder()
                .uri(shadowUri)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .header("Content-Type", "application/json")
                .header("X-TARN-Shadow", "true")
                .timeout(UPSTREAM_TIMEOUT)
                .build();
        long t0 = System.nanoTime();
        upstream.sendAsync(shadow, HttpResponse.BodyHandlers.discarding())
                .whenComplete((resp, err) -> {
                    MetricsCollector mc = am.getMetricsCollector();
                    double latencyMs = (System.nanoTime() - t0) / 1_000_000.0;
                    String shadowModel = "shadow:" + baseModel;
                    mc.recordInferenceLatency(shadowModel, latencyMs);
                    if (err != null) {
                        mc.recordModelRequest(shadowModel, false);
                        log.debug("Shadow request failed for {}: {}", baseModel, err.toString());
                    } else {
                        mc.recordModelRequest(shadowModel, resp.statusCode() / 100 == 2);
                    }
                });
    }

    private static Map<String, String> flattenHeaders(HttpExchange ex) {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, List<String>> e : ex.getRequestHeaders().entrySet()) {
            if (!e.getValue().isEmpty()) out.put(e.getKey(), e.getValue().get(0));
        }
        return out;
    }

    /**
     * Selects the ready container with the smallest reported queue depth. Containers that
     * have not yet passed Triton's {@code /v2/health/ready} (tracked by the AM warmup loop)
     * are skipped — otherwise we would route inference at a backend that still returns
     * connection-refused or 503. Ties are broken alphabetically by host for deterministic
     * behaviour in tests.
     */
    private Container pickLeastLoadedReadyContainer() {
        List<Container> containers = am.getRunningContainers();
        MetricsCollector mc = am.getMetricsCollector();
        Container best = null;
        int bestDepth = Integer.MAX_VALUE;
        synchronized (containers) {
            for (Container c : containers) {
                if (!am.isContainerReady(c.getId())) {
                    continue;
                }
                int depth = mc.getQueueDepth(c.getId().toString());
                if (depth < bestDepth
                        || (depth == bestDepth && best != null
                                && c.getNodeId().getHost().compareTo(best.getNodeId().getHost()) < 0)) {
                    best = c;
                    bestDepth = depth;
                }
            }
        }
        return best;
    }

    /**
     * Pipes bytes from the upstream InputStream to the client OutputStream, flushing after
     * every chunk so SSE events reach the client immediately. Uses chunked transfer
     * (Content-Length=0 in sendResponseHeaders).
     */
    private void relayStreamingResponse(HttpResponse<InputStream> resp, HttpExchange ex) throws IOException {
        int status = resp.statusCode();
        String ct = firstHeader(resp, "Content-Type", "text/event-stream");
        ex.getResponseHeaders().set("Content-Type", ct);
        ex.getResponseHeaders().set("Cache-Control", "no-cache");
        ex.getResponseHeaders().set("X-Accel-Buffering", "no"); // Disable nginx buffering if fronted by one.
        ex.sendResponseHeaders(status, 0); // 0 = chunked
        byte[] buf = new byte[8192];
        try (InputStream in = resp.body(); OutputStream out = ex.getResponseBody()) {
            int n;
            while ((n = in.read(buf)) != -1) {
                out.write(buf, 0, n);
                out.flush();
            }
        }
    }

    /**
     * Enforces auth when OAuth is configured: rejects 401 unless a valid Bearer JWT is
     * presented. With OAuth disabled, returns true so the legacy token / X-User flow keeps
     * working without code duplication at every endpoint.
     */
    private boolean authenticate(HttpExchange ex) throws IOException {
        varga.tarn.yarn.auth.JwtValidator v = am == null ? null : am.getJwtValidator();
        if (v == null) return true; // OAuth not configured.
        String auth = ex.getRequestHeaders().getFirst("Authorization");
        if (auth == null || !auth.startsWith("Bearer ")) {
            ex.getResponseHeaders().set("WWW-Authenticate", "Bearer realm=\"tarn\"");
            writeJsonError(ex, 401, "unauthorized", "Missing Bearer token");
            return false;
        }
        try {
            v.validate(auth.substring("Bearer ".length()).trim());
            return true;
        } catch (Exception e) {
            ex.getResponseHeaders().set("WWW-Authenticate",
                    "Bearer realm=\"tarn\", error=\"invalid_token\"");
            writeJsonError(ex, 401, "unauthorized", "Invalid Bearer token: " + e.getMessage());
            return false;
        }
    }

    /**
     * Resolves the authenticated user. When JWT auth is enabled and a valid Bearer token
     * is presented, identity comes from the {@code sub} claim and X-* headers are ignored
     * (clients can no longer impersonate). Without JWT we fall back to the historical
     * {@code X-Forwarded-User} / {@code X-TARN-User} / UGI chain.
     */
    private String getUser(HttpExchange ex) {
        varga.tarn.yarn.auth.JwtValidator v = am == null ? null : am.getJwtValidator();
        if (v != null) {
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            if (auth != null && auth.startsWith("Bearer ")) {
                try {
                    com.nimbusds.jwt.JWTClaimsSet claims = v.validate(auth.substring("Bearer ".length()).trim());
                    return varga.tarn.yarn.auth.JwtValidator.userOf(claims);
                } catch (Exception e) {
                    // Bad / expired JWT — fall through to the legacy path so the request still
                    // gets a 401 from isAuthorized() upstream. The caller will see "anonymous"
                    // until that gate is updated to reject directly.
                    log.debug("JWT validation failed: {}", e.getMessage());
                }
            }
        }
        String user = ex.getRequestHeaders().getFirst("X-Forwarded-User");
        if (user == null) user = ex.getRequestHeaders().getFirst("X-TARN-User");
        if (user == null || user.isEmpty()) {
            try {
                user = UserGroupInformation.getCurrentUser().getShortUserName();
            } catch (IOException e) {
                user = "anonymous";
            }
        }
        return user;
    }

    private Set<String> getGroups(HttpExchange ex, String user) {
        varga.tarn.yarn.auth.JwtValidator v = am == null ? null : am.getJwtValidator();
        if (v != null) {
            String auth = ex.getRequestHeaders().getFirst("Authorization");
            if (auth != null && auth.startsWith("Bearer ")) {
                try {
                    com.nimbusds.jwt.JWTClaimsSet claims = v.validate(auth.substring("Bearer ".length()).trim());
                    Set<String> claimGroups = v.groupsOf(claims);
                    if (!claimGroups.isEmpty()) return claimGroups;
                } catch (Exception ignored) {
                    // Fall through to UGI; the auth gate will reject upstream.
                }
            }
        }
        return getGroups(user);
    }

    private Set<String> getGroups(String user) {
        Set<String> groups = new HashSet<>();
        try {
            UserGroupInformation ugi = UserGroupInformation.createRemoteUser(user);
            Collections.addAll(groups, ugi.getGroupNames());
        } catch (Exception ignore) {
            // Groups unavailable; Ranger policies must accept user-level grants.
        }
        return groups;
    }

    private static String firstHeader(HttpResponse<?> resp, String name, String def) {
        return resp.headers().firstValue(name).orElse(def);
    }

    private void writeJson(HttpExchange ex, int status, Object body) throws IOException {
        byte[] payload = om.writeValueAsBytes(body);
        writeResponse(ex, status, "application/json", payload);
    }

    private void writeJsonError(HttpExchange ex, int status, String type, String message) throws IOException {
        Map<String, Object> err = new LinkedHashMap<>();
        Map<String, Object> errObj = new LinkedHashMap<>();
        errObj.put("message", message);
        errObj.put("type", type);
        err.put("error", errObj);
        writeJson(ex, status, err);
    }

    private void writeResponse(HttpExchange ex, int status, String contentType, byte[] payload) throws IOException {
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(status, payload.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(payload);
        }
    }

    // Expose for tests — allows swapping the upstream client in-memory.
    HttpClient getUpstreamClient() { return upstream; }

    @SuppressWarnings("unused")
    private static boolean waitFor(boolean condition, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition && System.currentTimeMillis() < deadline) {
            try { TimeUnit.MILLISECONDS.sleep(10); } catch (InterruptedException e) { return false; }
        }
        return condition;
    }
}
