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

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * TARN Kubernetes Operator main entrypoint.
 *
 * <p>Watches {@link TritonDeployment} CRs across all namespaces (or a single namespace when
 * {@code WATCH_NAMESPACE} is set) and reconciles each event through
 * {@link TritonDeploymentReconciler}. Updates the CR {@code status} subresource after each
 * reconcile so {@code kubectl get td} shows phase/ready/target.
 *
 * <p>Event handling is intentionally simple: fabric8 Watcher delivers add/modify/delete, and
 * we call the reconciler inline. A more advanced operator would use a work-queue with retry
 * backoff (ala JOSDK), but for the current feature set — CR lifecycle without external
 * dependencies — this is sufficient and keeps the deployment image under 50 MB.
 *
 * <p>Run with:
 * <pre>
 *   docker run -e WATCH_NAMESPACE=tarn ghcr.io/varga-foundation/tarn-operator:latest
 * </pre>
 */
public class TarnOperator {

    private static final Logger log = LoggerFactory.getLogger(TarnOperator.class);

    public static void main(String[] args) {
        String ns = System.getenv("WATCH_NAMESPACE"); // null = all namespaces
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            log.info("TARN operator starting (watchNamespace={}, server={})",
                    ns == null ? "<ALL>" : ns, client.getMasterUrl());
            new TarnOperator().run(client, ns);
        } catch (Exception e) {
            log.error("Operator crashed", e);
            System.exit(1);
        }
    }

    /** Blocks forever watching CRs. Exposed for testing. */
    public void run(KubernetesClient client, String namespace) throws Exception {
        TritonDeploymentReconciler reconciler = new TritonDeploymentReconciler(client);
        MixedOperation<TritonDeployment, io.fabric8.kubernetes.client.dsl.base.ResourceDefinitionContext, Resource<TritonDeployment>> ops;

        // Resolve the typed resource handle for our CR.
        var typedOps = client.resources(TritonDeployment.class);

        var watched = (namespace == null || namespace.isEmpty())
                ? typedOps.inAnyNamespace()
                : typedOps.inNamespace(namespace);

        CountDownLatch stopped = new CountDownLatch(1);

        watched.watch(new Watcher<TritonDeployment>() {
            @Override
            public void eventReceived(Action action, TritonDeployment cr) {
                String id = cr.getMetadata().getNamespace() + "/" + cr.getMetadata().getName();
                switch (action) {
                    case ADDED:
                    case MODIFIED:
                        try {
                            TritonDeploymentStatus newStatus = reconciler.reconcile(cr);
                            cr.setStatus(newStatus);
                            typedOps.inNamespace(cr.getMetadata().getNamespace())
                                    .resource(cr)
                                    .updateStatus();
                        } catch (Exception e) {
                            log.warn("Status update failed for {}: {}", id, e.getMessage());
                        }
                        break;
                    case DELETED:
                        reconciler.cleanup(cr.getMetadata().getNamespace(), cr.getMetadata().getName());
                        break;
                    case ERROR:
                    case BOOKMARK:
                    default:
                        log.debug("Ignoring watch event {} for {}", action, id);
                        break;
                }
            }

            @Override
            public void onClose(WatcherException cause) {
                if (cause == null) {
                    log.info("Watch closed cleanly");
                } else {
                    log.error("Watch terminated with error", cause);
                }
                stopped.countDown();
            }
        });

        log.info("TARN operator watching {} namespace(s); press Ctrl+C to stop",
                namespace == null ? "ALL" : namespace);
        stopped.await();
    }
}
