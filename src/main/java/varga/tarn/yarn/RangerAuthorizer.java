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


import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Set;

public class RangerAuthorizer {
    private static final Logger log = LoggerFactory.getLogger(RangerAuthorizer.class);
    private RangerBasePlugin plugin;
    private RangerDefaultAuditHandler auditHandler;
    private final TarnConfig config;

    public RangerAuthorizer(TarnConfig config) {
        this.config = config;
        if (config.rangerService != null && !config.rangerService.isEmpty()) {
            try {
                log.info("Initializing Apache Ranger plugin for service: {}, appId: {}", config.rangerService, config.rangerAppId);
                plugin = createPlugin(config.rangerService, config.rangerAppId);
                plugin.init();

                if (config.rangerAudit) {
                    log.info("Enabling Apache Ranger auditing");
                    auditHandler = new RangerDefaultAuditHandler();
                }
            } catch (Throwable e) {
                log.error("Failed to initialize Apache Ranger plugin. Authorization will be disabled. Error: {}", e.getMessage(), e);
                plugin = null;
            }
        }
    }

    protected RangerBasePlugin createPlugin(String serviceName, String appId) {
        return new RangerBasePlugin("triton", serviceName, appId);
    }

    public boolean isAllowed(String user, Set<String> groups, String action, String model) {
        if (plugin == null) {
            return true; // If Ranger is not configured, allow by default (or we could default to deny if preferred)
        }

        RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
        resource.setValue("model", model);

        RangerAccessRequestImpl request = new RangerAccessRequestImpl();
        request.setResource(resource);
        request.setAccessType(action);
        request.setUser(user);
        request.setUserGroups(groups);
        request.setAccessTime(new Date());
        request.setClientIPAddress("0.0.0.0"); // Could be improved if we pass the real IP

        RangerAccessResult result = plugin.isAccessAllowed(request, auditHandler);

        if (result != null && result.getIsAudited() && auditHandler != null) {
            // Ranger normally handles audit via the auditHandler passed to isAccessAllowed,
            // but we can add additional logging or logic here if needed.
        }

        boolean allowed = result != null && result.getIsAllowed();

        if (!allowed) {
            log.debug("Ranger DENY: user={}, action={}, model={}", user, action, model);
        }

        return allowed;
    }

    public void stop() {
        if (plugin != null) {
            plugin.cleanup();
        }
    }
}
