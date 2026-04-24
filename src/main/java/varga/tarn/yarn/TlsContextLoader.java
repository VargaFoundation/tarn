package varga.tarn.yarn;

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

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.io.InputStream;
import java.security.KeyStore;

/**
 * Builds an {@link HttpsConfigurator} from a keystore path (local or HDFS) and a password
 * looked up via Hadoop's credential provider (JCEKS). Centralizes TLS setup so both the
 * admin HTTP server and the OpenAI proxy share the same policy.
 * <p>
 * Policy baseline: TLSv1.2+/TLSv1.3 only, modern cipher suite negotiation left to the JDK.
 */
public final class TlsContextLoader {

    private TlsContextLoader() {}

    public static HttpsConfigurator buildConfigurator(TarnConfig cfg, Configuration hadoopConf) throws Exception {
        SSLContext sslContext = buildSslContext(cfg, hadoopConf);
        return new HttpsConfigurator(sslContext) {
            @Override
            public void configure(HttpsParameters params) {
                SSLParameters defaultParams = sslContext.getDefaultSSLParameters();
                // Restrict to modern TLS. TLSv1.0/1.1 are weak and explicitly disabled.
                defaultParams.setProtocols(new String[]{"TLSv1.3", "TLSv1.2"});
                // Server picks the cipher suite, not the client. Prevents downgrade games.
                defaultParams.setUseCipherSuitesOrder(true);
                params.setSSLParameters(defaultParams);
            }
        };
    }

    public static SSLContext buildSslContext(TarnConfig cfg, Configuration hadoopConf) throws Exception {
        if (cfg.tlsKeystorePath == null || cfg.tlsKeystorePath.isEmpty()) {
            throw new IllegalStateException("TLS enabled but keystore path is missing");
        }
        char[] password = resolveKeystorePassword(cfg, hadoopConf);
        KeyStore keyStore = loadKeystore(cfg, hadoopConf, password);

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, password);

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, null);
        return ctx;
    }

    private static char[] resolveKeystorePassword(TarnConfig cfg, Configuration hadoopConf) throws Exception {
        // Lookup via Hadoop credential providers (JCEKS on HDFS or local) — never pass a
        // plaintext password on the command line.
        char[] pwd = hadoopConf.getPassword(cfg.tlsKeystorePasswordAlias);
        if (pwd == null) {
            throw new IllegalStateException(
                    "No password found for alias " + cfg.tlsKeystorePasswordAlias
                            + ". Configure hadoop.security.credential.provider.path to point at a JCEKS file containing this alias.");
        }
        return pwd;
    }

    private static KeyStore loadKeystore(TarnConfig cfg, Configuration hadoopConf, char[] password) throws Exception {
        KeyStore ks = KeyStore.getInstance(cfg.tlsKeystoreType != null ? cfg.tlsKeystoreType : "JKS");
        Path p = new Path(cfg.tlsKeystorePath);
        FileSystem fs = p.getFileSystem(hadoopConf);
        try (FSDataInputStream in = fs.open(p); InputStream is = in) {
            ks.load(is, password);
        }
        return ks;
    }
}
