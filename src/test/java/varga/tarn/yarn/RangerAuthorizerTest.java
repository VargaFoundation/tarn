/*
 * Copyright © 2008 Varga Foundation (contact@varga.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package varga.tarn.yarn;

import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
    public void testRangerEnabledWithAudit() throws Exception {
        TarnConfig config = new TarnConfig();
        config.rangerService = "triton";
        config.rangerAppId = "tarn-test";
        config.rangerAudit = true;

        RangerBasePlugin mockPlugin = mock(RangerBasePlugin.class);

        RangerAuthorizer authorizer = new RangerAuthorizer(config) {
            @Override
            protected RangerBasePlugin createPlugin(String serviceName, String appId) {
                return mockPlugin;
            }
        };

        assertNotNull(authorizer);
        verify(mockPlugin).init();

        // Vérifier si auditHandler est initialisé via réflexion
        Field auditHandlerField = RangerAuthorizer.class.getDeclaredField("auditHandler");
        auditHandlerField.setAccessible(true);
        Object auditHandler = auditHandlerField.get(authorizer);

        assertNotNull(auditHandler, "auditHandler should be initialized when rangerAudit is true");
        assertTrue(auditHandler instanceof RangerDefaultAuditHandler);

        authorizer.stop();
        verify(mockPlugin).cleanup();
    }

    @Test
    public void testRangerEnabledWithoutAudit() throws Exception {
        TarnConfig config = new TarnConfig();
        config.rangerService = "triton";
        config.rangerAppId = "tarn-test";
        config.rangerAudit = false;

        RangerBasePlugin mockPlugin = mock(RangerBasePlugin.class);

        RangerAuthorizer authorizer = new RangerAuthorizer(config) {
            @Override
            protected RangerBasePlugin createPlugin(String serviceName, String appId) {
                return mockPlugin;
            }
        };

        Field auditHandlerField = RangerAuthorizer.class.getDeclaredField("auditHandler");
        auditHandlerField.setAccessible(true);
        Object auditHandler = auditHandlerField.get(authorizer);

        assertNull(auditHandler, "auditHandler should NOT be initialized when rangerAudit is false");

        authorizer.stop();
    }

    @Test
    public void testIsAllowedCallsPluginWithAuditHandler() throws Exception {
        TarnConfig config = new TarnConfig();
        config.rangerService = "triton";
        config.rangerAudit = true;

        RangerBasePlugin mockPlugin = mock(RangerBasePlugin.class);
        RangerAuthorizer authorizer = new RangerAuthorizer(config) {
            @Override
            protected RangerBasePlugin createPlugin(String serviceName, String appId) {
                return mockPlugin;
            }
        };

        authorizer.isAllowed("alice", Collections.singleton("datascience"), "infer", "resnet50");

        verify(mockPlugin).isAccessAllowed(any(org.apache.ranger.plugin.policyengine.RangerAccessRequest.class), any(org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor.class));
    }
}
