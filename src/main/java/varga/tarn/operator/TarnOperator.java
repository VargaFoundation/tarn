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
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderCallbacks;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElectionConfigBuilder;
import io.fabric8.kubernetes.client.extended.leaderelection.LeaderElector;
import io.fabric8.kubernetes.client.extended.leaderelection.resourcelock.LeaseLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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
        String leaseNs = System.getenv().getOrDefault("LEADER_ELECTION_NAMESPACE",
                ns == null || ns.isEmpty() ? "default" : ns);
        boolean leaderElection = !"false".equalsIgnoreCase(
                System.getenv().getOrDefault("LEADER_ELECTION_ENABLED", "true"));
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            log.info("TARN operator starting (watchNamespace={}, leaderElection={}, server={})",
                    ns == null ? "<ALL>" : ns, leaderElection, client.getMasterUrl());
            TarnOperator op = new TarnOperator();
            if (leaderElection) {
                op.runWithLeaderElection(client, ns, leaseNs);
            } else {
                op.run(client, ns);
            }
        } catch (Exception e) {
            log.error("Operator crashed", e);
            System.exit(1);
        }
    }

    /**
     * Runs the operator only while holding a Kubernetes Lease. Allows scaling the operator
     * Deployment to multiple replicas for HA — exactly one replica reconciles at a time; the
     * others stand by and take over automatically within {@code leaseDuration} if the leader
     * fails. Uses the {@code coordination.k8s.io/v1/leases} API (GA since 1.14).
     */
    public void runWithLeaderElection(KubernetesClient client, String watchNs, String leaseNs) throws Exception {
        String identity = InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID();
        AtomicReference<Watch> watchRef = new AtomicReference<>();
        CountDownLatch released = new CountDownLatch(1);

        LeaderElector elector = client.leaderElector()
                .withConfig(new LeaderElectionConfigBuilder()
                        .withName("tarn-operator")
                        .withLeaseDuration(Duration.ofSeconds(30))
                        .withRenewDeadline(Duration.ofSeconds(20))
                        .withRetryPeriod(Duration.ofSeconds(4))
                        .withLock(new LeaseLock(leaseNs, "tarn-operator", identity))
                        .withLeaderCallbacks(new LeaderCallbacks(
                                () -> {
                                    log.info("Acquired leadership as {}, starting reconcile loop", identity);
                                    try {
                                        watchRef.set(startWatch(client, watchNs));
                                    } catch (Exception e) {
                                        log.error("Failed to start watch after becoming leader", e);
                                    }
                                },
                                () -> {
                                    log.warn("Lost leadership, stopping reconcile loop");
                                    Watch w = watchRef.getAndSet(null);
                                    if (w != null) w.close();
                                    released.countDown();
                                },
                                newLeader -> log.info("New leader elected: {}", newLeader)))
                        .build())
                .build();
        elector.run();
        // If we ever exit the elector (e.g. JVM shutdown), wait for cleanup.
        released.await();
    }

    /**
     * Start watching CRs; returns the Watch so leader-election callbacks can close it when
     * leadership is lost.
     */
    Watch startWatch(KubernetesClient client, String namespace) {
        TritonDeploymentReconciler reconciler = new TritonDeploymentReconciler(client);
        var typedOps = client.resources(TritonDeployment.class);
        var watched = (namespace == null || namespace.isEmpty())
                ? typedOps.inAnyNamespace()
                : typedOps.inNamespace(namespace);

        return watched.watch(new Watcher<TritonDeployment>() {
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
            }
        });
    }

    /**
     * Legacy single-leader entrypoint — blocks on an internal latch until the watch closes.
     * Used by tests and by deployments with {@code LEADER_ELECTION_ENABLED=false}.
     */
    public void run(KubernetesClient client, String namespace) throws Exception {
        Watch w = startWatch(client, namespace);
        log.info("TARN operator watching {} namespace(s); press Ctrl+C to stop",
                namespace == null ? "ALL" : namespace);
        CountDownLatch stopped = new CountDownLatch(1);
        // Block until interrupted — watch is closed separately via JVM shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { w.close(); } catch (Exception ignored) {}
            stopped.countDown();
        }));
        stopped.await();
    }

    // Unused import silencer — keeping the reference compiling in case we later need it for
    // more sophisticated watchers that plug ops directly.
    @SuppressWarnings("unused")
    private static Object silence(MixedOperation<?, ?, ?> ops, Resource<?> r) {
        return ops != null ? ops : r;
    }
}
