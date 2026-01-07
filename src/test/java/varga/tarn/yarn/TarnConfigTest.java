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
    }

    @Test
    public void testParseArgs() throws Exception {
        TarnConfig config = new TarnConfig();
        String[] args = {
            "--port", "9000",
            "--image", "my-triton",
            "--am-port", "7777",
            "--token", "secret"
        };
        config.parseArgs(args);
        
        assertEquals(9000, config.tritonPort);
        assertEquals("my-triton", config.tritonImage);
        assertEquals(7777, config.amPort);
        assertEquals("secret", config.apiToken);
    }
}
