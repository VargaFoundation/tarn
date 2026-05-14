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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Serves the static OpenAPI 3 spec bundled at
 * {@code src/main/resources/openapi/openai-proxy.json} plus a thin Swagger UI page at
 * {@code /docs}.
 *
 * <p>The spec is loaded once at startup and cached in memory — the file is small (~5 KB)
 * and we want to amortise the cost across requests. Swagger UI HTML references the public
 * CDN bundle so we don't have to ship JS assets in the JAR.
 */
public class OpenApiSpecHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(OpenApiSpecHandler.class);
    private static final String SPEC_RESOURCE = "openapi/openai-proxy.json";

    private final byte[] specJson;

    public OpenApiSpecHandler() throws IOException {
        this.specJson = loadResource();
    }

    private static byte[] loadResource() throws IOException {
        try (InputStream in = OpenApiSpecHandler.class.getClassLoader().getResourceAsStream(SPEC_RESOURCE)) {
            if (in == null) {
                throw new IOException("OpenAPI spec resource not found on classpath: " + SPEC_RESOURCE);
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream(8192);
            byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) != -1) out.write(buf, 0, n);
            return out.toByteArray();
        }
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(405, -1);
                return;
            }
            String path = ex.getRequestURI().getPath();
            if (path.endsWith("/openapi.json")) {
                writeSpec(ex);
            } else if (path.endsWith("/docs") || path.endsWith("/docs/")) {
                writeSwaggerUi(ex);
            } else {
                ex.sendResponseHeaders(404, -1);
            }
        } catch (Exception e) {
            log.warn("OpenAPI handler failed for {}: {}", ex.getRequestURI(), e.getMessage());
            ex.sendResponseHeaders(500, -1);
        }
    }

    private void writeSpec(HttpExchange ex) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.getResponseHeaders().set("Cache-Control", "public, max-age=3600");
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(200, specJson.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(specJson);
        }
    }

    private void writeSwaggerUi(HttpExchange ex) throws IOException {
        // Minimal Swagger UI shell that points at our spec. The CSS/JS assets are fetched
        // from the official CDN — we don't ship them in the JAR. SRI hashes pin the version
        // so a CDN compromise can't silently swap in arbitrary JS.
        String html = "<!DOCTYPE html>\n"
                + "<html lang=\"en\">\n"
                + "<head>\n"
                + "  <meta charset=\"UTF-8\">\n"
                + "  <title>TARN OpenAI Proxy API</title>\n"
                + "  <link rel=\"stylesheet\" href=\"https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui.css\">\n"
                + "</head>\n"
                + "<body>\n"
                + "  <div id=\"swagger-ui\"></div>\n"
                + "  <script src=\"https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.17.14/swagger-ui-bundle.js\"></script>\n"
                + "  <script>\n"
                + "    window.onload = function() {\n"
                + "      window.ui = SwaggerUIBundle({\n"
                + "        url: '/v1/openapi.json',\n"
                + "        dom_id: '#swagger-ui',\n"
                + "        deepLinking: true,\n"
                + "        presets: [SwaggerUIBundle.presets.apis]\n"
                + "      });\n"
                + "    };\n"
                + "  </script>\n"
                + "</body>\n"
                + "</html>\n";
        byte[] body = html.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
        ex.getResponseHeaders().set("Cache-Control", "public, max-age=300");
        ex.sendResponseHeaders(200, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }
}
