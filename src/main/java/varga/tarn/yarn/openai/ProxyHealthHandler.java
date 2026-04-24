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
import varga.tarn.yarn.ApplicationMaster;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Minimal health endpoint for the proxy port so Kubernetes probes and Knox HA can check
 * liveness without going through the main AM HTTP server.
 */
public class ProxyHealthHandler implements HttpHandler {

    private final ApplicationMaster am;

    public ProxyHealthHandler(ApplicationMaster am) {
        this.am = am;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        boolean ok = am != null && !am.getRunningContainers().isEmpty();
        String body = ok ? "OK" : "NO_BACKENDS";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(ok ? 200 : 503, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}
