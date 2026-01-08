package varga.tarn.yarn;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class TarnConfigTest {

    @Test
    public void testDefaultValues() {
        TarnConfig config = new TarnConfig();
        assertEquals("nvcr.io/nvidia/tritonserver:24.09-py3", config.tritonImage);
        assertEquals(8000, config.tritonPort);
        assertEquals(8888, config.amPort);
        assertEquals("host", config.dockerNetwork);
        assertFalse(config.dockerPrivileged);
        assertFalse(config.dockerDelayedRemoval);
        assertNull(config.dockerMounts);
        assertNull(config.dockerPorts);
    }

    @Test
    public void testParseArgs() throws Exception {
        TarnConfig config = new TarnConfig();
        String[] args = {
            "--port", "9000",
            "--image", "my-triton",
            "--am-port", "7777",
            "--token", "secret",
            "--docker-network", "bridge",
            "--docker-privileged",
            "--docker-delayed-removal",
            "--docker-mounts", "/tmp:/tmp",
            "--docker-ports", "8000:8000,8001:8001"
        };
        config.parseArgs(args);
        
        assertEquals(9000, config.tritonPort);
        assertEquals("my-triton", config.tritonImage);
        assertEquals(7777, config.amPort);
        assertEquals("secret", config.apiToken);
        assertEquals("bridge", config.dockerNetwork);
        assertTrue(config.dockerPrivileged);
        assertTrue(config.dockerDelayedRemoval);
        assertEquals("/tmp:/tmp", config.dockerMounts);
        assertEquals("8000:8000,8001:8001", config.dockerPorts);
    }
}
