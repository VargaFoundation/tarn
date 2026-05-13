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

import com.sun.net.httpserver.HttpServer;
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    /**
     * Liveness flips to true once the JVM has finished startup; the apiserver client is
     * built and we entered the main loop. Stays true until shutdown.
     */
    private final AtomicBoolean alive = new AtomicBoolean(false);
    /**
     * Readiness mirrors the watch state — true only while a fabric8 Watch is open. In
     * leader-election mode standby replicas report not-ready so the K8s Service (if any)
     * routes probes away from them.
     */
    private final AtomicBoolean ready = new AtomicBoolean(false);

    public static void main(String[] args) {
        String ns = System.getenv("WATCH_NAMESPACE"); // null = all namespaces
        String leaseNs = System.getenv().getOrDefault("LEADER_ELECTION_NAMESPACE",
                ns == null || ns.isEmpty() ? "default" : ns);
        boolean leaderElection = !"false".equalsIgnoreCase(
                System.getenv().getOrDefault("LEADER_ELECTION_ENABLED", "true"));
        int healthPort = Integer.parseInt(
                System.getenv().getOrDefault("HEALTH_PORT", "8080"));
        try (KubernetesClient client = new KubernetesClientBuilder().build()) {
            log.info("TARN operator starting (watchNamespace={}, leaderElection={}, server={})",
                    ns == null ? "<ALL>" : ns, leaderElection, client.getMasterUrl());
            TarnOperator op = new TarnOperator();
            HttpServer health = op.startHealthServer(healthPort);
            try {
                op.alive.set(true);
                if (leaderElection) {
                    op.runWithLeaderElection(client, ns, leaseNs);
                } else {
                    op.run(client, ns);
                }
            } finally {
                op.alive.set(false);
                health.stop(0);
            }
        } catch (Exception e) {
            log.error("Operator crashed", e);
            System.exit(1);
        }
    }

    /**
     * Tiny HTTP server exposing {@code /healthz} (JVM up) and {@code /readyz} (watch open)
     * so Kubernetes probes have something to point at. Bound to all interfaces — RBAC and
     * NetworkPolicy gate access at the cluster level.
     */
    HttpServer startHealthServer(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/healthz", ex -> {
            int code = alive.get() ? 200 : 503;
            byte[] body = (alive.get() ? "ok" : "stopped").getBytes();
            ex.sendResponseHeaders(code, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.createContext("/readyz", ex -> {
            int code = ready.get() ? 200 : 503;
            byte[] body = (ready.get() ? "ready" : "not-ready").getBytes();
            ex.sendResponseHeaders(code, body.length);
            try (var os = ex.getResponseBody()) { os.write(body); }
        });
        server.start();
        log.info("Operator health server listening on :{} (/healthz, /readyz)", port);
        return server;
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
                                        ready.set(true);
                                    } catch (Exception e) {
                                        log.error("Failed to start watch after becoming leader", e);
                                        ready.set(false);
                                    }
                                },
                                () -> {
                                    log.warn("Lost leadership, stopping reconcile loop");
                                    ready.set(false);
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
     * leadership is lost. Failed reconciles are requeued with exponential backoff via the
     * shared {@link #requeueExecutor} so a transient apiserver hiccup doesn't strand the CR
     * in {@code Degraded} until the next manual edit.
     */
    Watch startWatch(KubernetesClient client, String namespace) {
        TritonDeploymentReconciler reconciler = new TritonDeploymentReconciler(client);
        var typedOps = client.resources(TritonDeployment.class);
        var watched = (namespace == null || namespace.isEmpty())
                ? typedOps.inAnyNamespace()
                : typedOps.inNamespace(namespace);

        // Per-CR backoff state. Cleared on a successful reconcile or on DELETED.
        ConcurrentMap<String, Integer> attempts = new ConcurrentHashMap<>();

        return watched.watch(new Watcher<TritonDeployment>() {
            @Override
            public void eventReceived(Action action, TritonDeployment cr) {
                String ns = cr.getMetadata().getNamespace();
                String name = cr.getMetadata().getName();
                String id = ns + "/" + name;
                switch (action) {
                    case ADDED:
                    case MODIFIED:
                        boolean ok = reconcileOnce(reconciler, typedOps, cr, id);
                        if (ok) {
                            attempts.remove(id);
                        } else {
                            int attempt = attempts.merge(id, 1, Integer::sum);
                            scheduleRequeue(client, reconciler, typedOps, ns, name, id, attempt, attempts);
                        }
                        break;
                    case DELETED:
                        // The reconciler's finalizer drives cleanup on the deletion path —
                        // by the time this event fires the dependent resources are gone. Run
                        // cleanup() anyway as a safety net for legacy CRs that never got the
                        // finalizer attached (idempotent: delete-by-name is a no-op when absent).
                        reconciler.cleanup(ns, name);
                        attempts.remove(id);
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
     * Runs one reconcile + status write. Returns true on success, false on any thrown
     * exception (caller decides whether to requeue). The status-update failure path is
     * treated as a soft failure — the next tick will try again.
     */
    private boolean reconcileOnce(TritonDeploymentReconciler reconciler,
                                  io.fabric8.kubernetes.client.dsl.MixedOperation<
                                          TritonDeployment, ?, ?> typedOps,
                                  TritonDeployment cr, String id) {
        try {
            TritonDeploymentStatus newStatus = reconciler.reconcile(cr);
            cr.setStatus(newStatus);
            typedOps.inNamespace(cr.getMetadata().getNamespace())
                    .resource(cr)
                    .updateStatus();
            return !TritonDeploymentStatus.PHASE_DEGRADED.equals(newStatus.getPhase());
        } catch (Exception e) {
            log.warn("Reconcile failed for {}: {}", id, e.getMessage());
            return false;
        }
    }

    /**
     * Re-fetches the CR and runs another reconcile after an exponentially backed-off delay.
     * Without this requeue path, a single transient error (apiserver 5xx, network blip)
     * strands the CR in Degraded until a human edits it.
     */
    private void scheduleRequeue(KubernetesClient client, TritonDeploymentReconciler reconciler,
                                 io.fabric8.kubernetes.client.dsl.MixedOperation<
                                         TritonDeployment, ?, ?> typedOps,
                                 String ns, String name, String id, int attempt,
                                 ConcurrentMap<String, Integer> attempts) {
        if (requeueExecutor == null) return; // Operator shutting down.
        long delaySec = Math.min(60L, (long) Math.pow(2, Math.min(attempt, 6)));
        log.info("Requeue {} attempt {} in {}s", id, attempt, delaySec);
        requeueExecutor.schedule(() -> {
            TritonDeployment fresh = client.resources(TritonDeployment.class)
                    .inNamespace(ns).withName(name).get();
            if (fresh == null) {
                // CR removed in the meantime; cleanup() in the DELETED branch handles it.
                attempts.remove(id);
                return;
            }
            boolean ok = reconcileOnce(reconciler, typedOps, fresh, id);
            if (ok) {
                attempts.remove(id);
            } else if (attempt < 6) {
                int next = attempts.merge(id, 1, Integer::sum);
                scheduleRequeue(client, reconciler, typedOps, ns, name, id, next, attempts);
            } else {
                log.error("Gave up requeuing {} after {} attempts — manual intervention needed",
                        id, attempt);
                attempts.remove(id);
            }
        }, delaySec, TimeUnit.SECONDS);
    }

    /**
     * Shared executor for requeues — single-threaded to keep the order deterministic. Lazy
     * init so tests that call {@link #startWatch} directly don't need a tear-down.
     */
    private volatile ScheduledExecutorService requeueExecutor =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "tarn-operator-requeue");
                t.setDaemon(true);
                return t;
            });

    /**
     * Legacy single-leader entrypoint — blocks on an internal latch until the watch closes.
     * Used by tests and by deployments with {@code LEADER_ELECTION_ENABLED=false}.
     */
    public void run(KubernetesClient client, String namespace) throws Exception {
        Watch w = startWatch(client, namespace);
        ready.set(true);
        log.info("TARN operator watching {} namespace(s); press Ctrl+C to stop",
                namespace == null ? "ALL" : namespace);
        CountDownLatch stopped = new CountDownLatch(1);
        // Block until interrupted — watch is closed separately via JVM shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ready.set(false);
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
