package varga.tarn.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

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
            "--address", "127.0.0.1"
        };
        am.init(args);
        
        assertEquals("127.0.0.1", am.bindAddress);
        // On pourrait aussi vérifier les autres champs si on les rendait accessibles
    }
    
    // In a real scenario, we would use more complex mocks to test the AM logic.
    // Given the constraints, we verify at least the basic properties.
}
