package varga.tarn.yarn;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

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
