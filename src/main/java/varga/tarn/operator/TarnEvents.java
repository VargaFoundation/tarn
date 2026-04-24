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

import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.EventSource;
import io.fabric8.kubernetes.api.model.ObjectReference;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Instant;
import java.util.UUID;

/**
 * Emits Kubernetes Events ({@code core/v1/events}) on the CR being reconciled so ops see the
 * reconciler's decisions in {@code kubectl describe td} and {@code kubectl get events}.
 *
 * <p>Scope is kept tight: one event per <em>phase transition</em> or <em>actionable failure</em>,
 * never one per reconcile tick (the watch fires on every status update, which itself creates
 * another event — runaway loop if we're not careful). Event names include a random suffix so
 * they don't collide on reuse.
 */
public class TarnEvents {

    private static final Logger log = LoggerFactory.getLogger(TarnEvents.class);

    private final KubernetesClient client;
    private final String reporterHost;

    public TarnEvents(KubernetesClient client) {
        this.client = client;
        String h;
        try {
            h = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            h = "tarn-operator";
        }
        this.reporterHost = h;
    }

    /** Equivalent of Kubernetes Event "Normal" type — routine state transitions. */
    public void normal(TritonDeployment cr, String reason, String messageFmt, Object... args) {
        emit(cr, "Normal", reason, String.format(messageFmt, args));
    }

    /** Warning events — user-visible in kubectl describe, surface SLO-affecting issues. */
    public void warning(TritonDeployment cr, String reason, String messageFmt, Object... args) {
        emit(cr, "Warning", reason, String.format(messageFmt, args));
    }

    private void emit(TritonDeployment cr, String type, String reason, String message) {
        try {
            String ns = cr.getMetadata().getNamespace();
            String eventName = cr.getMetadata().getName() + "." + shortId();
            ObjectReference ref = new ObjectReferenceBuilder()
                    .withApiVersion(cr.getApiVersion())
                    .withKind(cr.getKind())
                    .withName(cr.getMetadata().getName())
                    .withNamespace(ns)
                    .withUid(cr.getMetadata().getUid())
                    .build();
            var event = new EventBuilder()
                    .withNewMetadata()
                        .withName(eventName)
                        .withNamespace(ns)
                    .endMetadata()
                    .withType(type)
                    .withReason(reason)
                    .withMessage(message)
                    .withInvolvedObject(ref)
                    .withSource(new EventSource("tarn-operator", reporterHost))
                    .withFirstTimestamp(Instant.now().toString())
                    .withLastTimestamp(Instant.now().toString())
                    .withCount(1)
                    .build();
            client.v1().events().inNamespace(ns).resource(event).create();
        } catch (Exception e) {
            // Events are best-effort telemetry; never fail a reconcile because the event
            // write was rejected. Log at debug to avoid alert fatigue.
            log.debug("Failed to emit event ({}/{}): {}", type, reason, e.getMessage());
        }
    }

    private static String shortId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
