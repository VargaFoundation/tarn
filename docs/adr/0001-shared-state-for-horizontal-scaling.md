# ADR-0001: Shared state for horizontal scaling

- Status: Accepted
- Date: 2026-05-28

## Context

The OpenAI proxy enforces several limits in process-local memory: per-(user,model) request quotas
(`QuotaEnforcer`), the process-wide rate limit (`GlobalRateLimiter`), conversation→container
affinity for KV-cache reuse (`ConversationAffinity`), and (added alongside this work) per-user daily
token budgets (`TokenBudgetEnforcer`).

This is correct for a single process — the common YARN case (one Application Master) and the Helm
default (`replicaCount: 1`). But the Helm chart ships soft pod anti-affinity, a PodDisruptionBudget
and a `replicaCount` knob, and the admin endpoint already advertises that quota *rules* "propagate
to all AM replicas". With more than one replica the **enforcement** state is not shared, so:

- the global rate limit and quotas are enforced independently per replica (≈ N× too loose);
- conversation affinity breaks when a follow-up turn lands on a different replica, and is lost on
  restart.

`QuotaEnforcer`'s own javadoc anticipated this: "swap the implementation behind `#check` for a
shared store" once horizontally scaled.

## Decision

Introduce a small `varga.tarn.yarn.shared.SharedState` abstraction with two backends:

- `local` (default): returns no stores; the components keep their in-memory state. Behaviour is
  byte-for-byte unchanged, so single-replica/YARN deployments are unaffected.
- `zk`: reuses the existing Curator/ZooKeeper client. Live-replica count comes from ephemeral
  membership znodes; rate limits, quotas and budgets are enforced fair-share by that count;
  conversation affinity is shared via a `CuratorCache` mirror and survives restarts.

Selected with `--shared-state=local|zk` (Helm `config.sharedState`). The three components and the
budget enforcer accept an optional store/replica-count supplier; `null`/`1` preserves the legacy
path.

## Consequences

- Correct multi-replica enforcement without touching the single-replica default.
- Reuses ZooKeeper (already a hard dependency) — no new required infrastructure.
- Fair-share is approximate (see [ADR-0003](0003-fair-share-over-precise-counters.md)); a precise
  shared counter is a documented follow-up.
- Cross-replica KV-cache reuse via shared affinity is bounded by the proxy routing to its local
  container pool today; the immediate win is restart-survival and fair-share correctness.
