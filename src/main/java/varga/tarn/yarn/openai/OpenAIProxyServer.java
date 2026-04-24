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

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import varga.tarn.yarn.ApplicationMaster;
import varga.tarn.yarn.TarnConfig;
import varga.tarn.yarn.TlsContextLoader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * OpenAI-compatible HTTP(S) reverse proxy exposed on its own port (default 9000).
 *
 * <p>Why a dedicated server and port:
 * <ul>
 *   <li>Streaming / SSE and long-lived inference requests don't want to share the HttpServer
 *       executor with admin endpoints (a stuck completion would starve /health).</li>
 *   <li>Lets Knox/Ingress route "public" inference traffic to one port and "private" admin
 *       traffic to another — a common on-prem topology.</li>
 *   <li>Enabled only when {@code --openai-proxy-enabled} is set, so installations that don't
 *       need it pay zero cost.</li>
 * </ul>
 */
public class OpenAIProxyServer {

    private static final Logger log = LoggerFactory.getLogger(OpenAIProxyServer.class);

    private final TarnConfig config;
    private final HttpServer server;
    private final ExecutorService pool;

    public OpenAIProxyServer(TarnConfig config, ApplicationMaster am,
                             org.apache.hadoop.conf.Configuration hadoopConf) throws IOException {
        this.config = config;
        InetSocketAddress addr = new InetSocketAddress(config.bindAddress, config.openaiProxyPort);
        if (config.tlsEnabled) {
            try {
                HttpsServer https = HttpsServer.create(addr, 0);
                HttpsConfigurator cfg = TlsContextLoader.buildConfigurator(config,
                        hadoopConf != null ? hadoopConf : new org.apache.hadoop.conf.Configuration());
                https.setHttpsConfigurator(cfg);
                this.server = https;
            } catch (Exception e) {
                throw new IOException("Failed to initialize TLS for OpenAI proxy on port "
                        + config.openaiProxyPort, e);
            }
        } else {
            this.server = HttpServer.create(addr, 0);
        }

        // Fixed-size pool: inference requests are long; unbounded would let a burst exhaust
        // upstream connections. Tuned low by default; operators can override once they benchmark.
        this.pool = Executors.newFixedThreadPool(
                Math.max(8, Runtime.getRuntime().availableProcessors() * 4),
                r -> {
                    Thread t = new Thread(r, "tarn-openai-proxy");
                    t.setDaemon(true);
                    return t;
                });
        this.server.setExecutor(pool);

        OpenAIProxyHandler handler = new OpenAIProxyHandler(am, config);
        this.server.createContext("/v1", handler);
        this.server.createContext("/health", new ProxyHealthHandler(am));
    }

    public void start() {
        server.start();
        log.info("OpenAI proxy listening on {}:{}{}",
                config.bindAddress, config.openaiProxyPort,
                config.tlsEnabled ? " (TLS)" : "");
    }

    public void stop() {
        server.stop(1);
        pool.shutdown();
    }

    public int getPort() {
        return server.getAddress().getPort();
    }
}
