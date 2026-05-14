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
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sanity checks for the OpenAPI spec handler: the bundled JSON parses, the documented
 * operations are present, the Swagger UI page renders. We don't lint the spec against the
 * OpenAPI schema here — that would pull in a new dep — but parsing + topology checks
 * already catch the common breakage (file missing from the JAR, broken JSON).
 */
public class OpenApiSpecHandlerTest {

    private HttpServer server;
    private int port;
    private final ObjectMapper om = new ObjectMapper();
    private final HttpClient client = HttpClient.newHttpClient();

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        OpenApiSpecHandler handler = new OpenApiSpecHandler();
        server.createContext("/v1/openapi.json", handler);
        server.createContext("/docs", handler);
        server.start();
        port = server.getAddress().getPort();
    }

    @AfterEach
    void teardown() {
        if (server != null) server.stop(0);
    }

    private HttpResponse<String> get(String path) throws Exception {
        return client.send(HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    public void specIsValidJsonAndExposesAllExpectedOperations() throws Exception {
        HttpResponse<String> resp = get("/v1/openapi.json");
        assertEquals(200, resp.statusCode());
        assertEquals("application/json", resp.headers().firstValue("Content-Type").orElse(""));
        JsonNode root = om.readTree(resp.body());
        assertEquals("3.0.3", root.path("openapi").asText());
        // The four endpoints we contract on must be present — anything else is fine.
        assertTrue(root.path("paths").has("/v1/models"), "missing /v1/models");
        assertTrue(root.path("paths").has("/v1/chat/completions"), "missing /v1/chat/completions");
        assertTrue(root.path("paths").has("/v1/embeddings"), "missing /v1/embeddings");
        assertTrue(root.path("paths").has("/v1/usage"), "missing /v1/usage");
        // Both auth mechanisms documented.
        assertTrue(root.path("components").path("securitySchemes").has("BearerAuth"));
        assertTrue(root.path("components").path("securitySchemes").has("TarnToken"));
    }

    @Test
    public void docsPageIsServedAsHtml() throws Exception {
        HttpResponse<String> resp = get("/docs");
        assertEquals(200, resp.statusCode());
        assertTrue(resp.headers().firstValue("Content-Type").orElse("").startsWith("text/html"));
        // The page must reference the spec URL or Swagger UI would have nothing to render.
        assertTrue(resp.body().contains("/v1/openapi.json"), "docs page must point at the spec");
        assertTrue(resp.body().contains("SwaggerUIBundle"), "docs page must bootstrap Swagger UI");
    }

    @Test
    public void unknownSubpathReturns404() throws Exception {
        // The handler is registered on /v1/openapi.json and /docs only; anything else under
        // its prefix should 404 rather than echo the spec.
        HttpResponse<String> resp = get("/v1/openapi.yaml");
        assertEquals(404, resp.statusCode());
    }
}
