package varga.tarn.operator;

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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Status subresource for {@link TritonDeployment}. Written only by the reconciler;
 * {@code kubectl edit} on the {@code status} is blocked by the CRD subresource enablement.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TritonDeploymentStatus {

    public static final String PHASE_PENDING = "Pending";
    public static final String PHASE_RECONCILING = "Reconciling";
    public static final String PHASE_READY = "Ready";
    public static final String PHASE_DEGRADED = "Degraded";
    public static final String PHASE_FAILED = "Failed";
    public static final String PHASE_TERMINATING = "Terminating";

    private String phase = PHASE_PENDING;
    private Integer readyReplicas = 0;
    private Integer targetReplicas = 0;
    private Long observedGeneration;
    private List<Condition> conditions = new ArrayList<>();
    private CanaryAnalysisStatus canaryAnalysis;

    public String getPhase() { return phase; }
    public void setPhase(String phase) { this.phase = phase; }
    public Integer getReadyReplicas() { return readyReplicas; }
    public void setReadyReplicas(Integer n) { this.readyReplicas = n; }
    public Integer getTargetReplicas() { return targetReplicas; }
    public void setTargetReplicas(Integer n) { this.targetReplicas = n; }
    public Long getObservedGeneration() { return observedGeneration; }
    public void setObservedGeneration(Long g) { this.observedGeneration = g; }
    public List<Condition> getConditions() { return conditions; }
    public void setConditions(List<Condition> c) { this.conditions = c; }
    public CanaryAnalysisStatus getCanaryAnalysis() { return canaryAnalysis; }
    public void setCanaryAnalysis(CanaryAnalysisStatus c) { this.canaryAnalysis = c; }

    /** Records the in-flight canary gate for kubectl/dashboard visibility. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CanaryAnalysisStatus {
        public static final String RUNNING = "Running";
        public static final String SUCCEEDED = "Succeeded";
        public static final String FAILED = "Failed";
        public static final String UNKNOWN = "Unknown";

        private String state = RUNNING;
        private String variant;
        private String startTime;
        private String lastMessage;

        public CanaryAnalysisStatus() {}
        public CanaryAnalysisStatus(String variant, String state, String message) {
            this.variant = variant;
            this.state = state;
            this.lastMessage = message;
            this.startTime = Instant.now().toString();
        }

        public String getState() { return state; }
        public void setState(String s) { this.state = s; }
        public String getVariant() { return variant; }
        public void setVariant(String v) { this.variant = v; }
        public String getStartTime() { return startTime; }
        public void setStartTime(String t) { this.startTime = t; }
        public String getLastMessage() { return lastMessage; }
        public void setLastMessage(String m) { this.lastMessage = m; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Condition {
        private String type;
        private String status;
        private String reason;
        private String message;
        private String lastTransitionTime;

        public Condition() {}
        public Condition(String type, String status, String reason, String message) {
            this.type = type;
            this.status = status;
            this.reason = reason;
            this.message = message;
            this.lastTransitionTime = Instant.now().toString();
        }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public String getReason() { return reason; }
        public void setReason(String reason) { this.reason = reason; }
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        public String getLastTransitionTime() { return lastTransitionTime; }
        public void setLastTransitionTime(String t) { this.lastTransitionTime = t; }
    }
}
