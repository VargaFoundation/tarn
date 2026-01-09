package varga.tarn.yarn;

import org.junit.jupiter.api.Test;
import java.util.Collections;
import static org.junit.jupiter.api.Assertions.*;

public class RangerAuthorizerTest {

    @Test
    public void testRangerDisabled() {
        TarnConfig config = new TarnConfig();
        config.rangerService = null;
        
        RangerAuthorizer authorizer = new RangerAuthorizer(config);
        assertTrue(authorizer.isAllowed("user1", Collections.emptySet(), "list", "model1"));
        authorizer.stop();
    }

    @Test
    public void testRangerEnabledWithAudit() {
        TarnConfig config = new TarnConfig();
        config.rangerService = "triton";
        config.rangerAppId = "tarn-test";
        config.rangerAudit = true;
        
        RangerAuthorizer authorizer = new RangerAuthorizer(config);
        assertNotNull(authorizer);
        authorizer.stop();
    }
}
