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
        assertNull(config.zkEnsemble);
        assertEquals("/services/triton/instances", config.zkPath);
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
            "--docker-ports", "8000:8000,8001:8001",
            "--zk-ensemble", "localhost:2181",
            "--zk-path", "/test/path",
            "--scale-up", "0.9",
            "--scale-down", "0.1",
            "--min-instances", "2",
            "--max-instances", "20",
            "--cooldown", "30000",
            "--jar", "target/tarn.jar",
            "--env", "VAR1=VAL1",
            "--env", "VAR2=VAL2",
            "--ranger-service", "triton-ranger",
            "--ranger-app-id", "tarn-app",
            "--ranger-audit"
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
        assertEquals("localhost:2181", config.zkEnsemble);
        assertEquals("/test/path", config.zkPath);
        assertEquals("triton-ranger", config.rangerService);
        assertEquals("tarn-app", config.rangerAppId);
        assertTrue(config.rangerAudit);
        assertEquals("target/tarn.jar", config.jarPath);
        assertEquals("VAL1", config.customEnv.get("VAR1"));
        assertEquals("VAL2", config.customEnv.get("VAR2"));
        assertEquals(0.9, config.scaleUpThreshold);
        assertEquals(0.1, config.scaleDownThreshold);
        assertEquals(2, config.minContainers);
        assertEquals(20, config.maxContainers);
        assertEquals(30000L, config.scaleCooldownMs);
    }
}
