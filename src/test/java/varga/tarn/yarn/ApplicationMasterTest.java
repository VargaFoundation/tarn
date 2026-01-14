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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ApplicationMasterTest {

    @Test
    public void testInitialization() {
        ApplicationMaster am = new ApplicationMaster();
        assertNotNull(am);
    }

    @Test
    public void testCliParsing() throws Exception {
        ApplicationMaster am = new ApplicationMaster();
        String[] args = {
                "--port", "9000",
                "--image", "test-image",
                "--model-repository", "hdfs://test",
                "--address", "127.0.0.1",
                "--secrets", "hdfs:///path/to/secrets.jks",
                "--min-instances", "3"
        };
        am.init(args);

        TarnConfig config = am.getConfig();
        assertEquals("127.0.0.1", config.bindAddress);
        assertEquals(9000, config.tritonPort);
        assertEquals("test-image", config.tritonImage);
        assertEquals("hdfs://test", config.modelRepository);
        assertEquals("hdfs:///path/to/secrets.jks", config.secretsPath);
        assertEquals(3, am.getTargetNumContainers());
    }
}
