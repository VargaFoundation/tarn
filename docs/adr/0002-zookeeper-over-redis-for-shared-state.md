# ADR-0002: ZooKeeper (not Redis) as the first shared-state backend

- Status: Accepted (the "Redis as the precise backend" option is superseded by
  [ADR-0004](0004-hbase-not-redis-for-precise-backend.md) — the precise backend is HBase)
- Date: 2026-05-28

## Context

[ADR-0001](0001-shared-state-for-horizontal-scaling.md) introduces a shared-state abstraction for
multi-replica enforcement. It needs a backend providing: live-replica membership, a way to enforce
limits across replicas, and a shared conversation→container map. Candidate backends: the ZooKeeper
ensemble TARN already depends on, or Redis.

## Decision

Implement the first shared backend on **ZooKeeper**, reusing the existing `CuratorFramework`. Keep
Redis as an optional future backend behind the same interfaces (`RateLimitStore`, `AffinityStore`,
`SharedState`).

- Membership: Curator `PersistentNode` (ephemeral, auto-recreated on reconnect) + `CuratorCache`.
- Affinity: persistent znodes mirrored by a `CuratorCache` for O(1) local reads, with throttled
  writes and idempotent (leaderless) expiry deletes.
- `curator-recipes` is already on the classpath; only `NodeCache` was used before.

## Decision drivers

- **No new required infrastructure.** ZooKeeper is already mandatory for Knox service discovery and
  quota hot-reload. Redis would add a new operational dependency, image, and failure mode.
- **Reuses proven wiring.** The existing connection-state listener, retry policy and config-watch
  pattern carry over directly.
- **Fit.** Ephemeral-znode membership and a watched config tree are exactly what ZooKeeper is good
  at.

## Consequences

- Zero new deployment surface for the common case.
- ZooKeeper is a coordination store, not a high-throughput counter — so the rate/quota/budget design
  must avoid per-request ZK I/O (it does; see [ADR-0003](0003-fair-share-over-precise-counters.md)),
  and high-cardinality affinity writes are throttled.
- A *precise* counter backend (atomic increment + native TTL) is still wanted; we implemented it on
  **HBase** rather than Redis to stay Hadoop-native — see
  [ADR-0004](0004-hbase-not-redis-for-precise-backend.md).
