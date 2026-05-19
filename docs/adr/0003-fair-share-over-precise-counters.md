# ADR-0003: Fair-share over precise distributed counters

- Status: Accepted
- Date: 2026-05-28

## Context

With a ZooKeeper shared backend ([ADR-0002](0002-zookeeper-over-redis-for-shared-state.md)), the
proxy must enforce cluster-wide ceilings — global rate limit (req/s), per-(user,model) quotas (rpm)
and per-user token budgets (tokens/day) — across N replicas. Two strategies:

1. **Precise distributed counter** — every request does an atomic increment against a shared counter
   (ZK `DistributedAtomicLong` or Redis `INCR`). Exact, but adds a synchronous round-trip to every
   inference request and concentrates contention on hot keys.
2. **Fair-share** — each replica enforces `limit / liveReplicas` locally using its existing in-memory
   bucket/window; the live count comes from membership and is read without per-request I/O.

## Decision

Use **fair-share by default**. Each replica computes its slice from the membership-derived live
count (recomputed only on a scale event, not per request) and enforces it with the same local
token-bucket / window code used in single-replica mode. The remainder of an uneven division is
handed to the lowest-indexed replicas so per-replica shares sum exactly to the configured ceiling
(rate/quota); budgets use floor division (never overspend). Expose `--rate-limit-strategy` as the
seam for a future `precise` mode (ZK/Redis counters).

## Decision drivers

- **No hot-path I/O.** These are protective ceilings, not billing; a few-millisecond ZK round-trip
  per inference is a poor trade. Fair-share adds zero network calls on the request path.
- **Identity in the common case.** With one replica the slice is the full limit, so the default
  (`local`) and single-replica `zk` behave exactly as before.
- **Safe approximation.** Behind a round-robin Service/Knox, load is roughly even, so the slices sum
  to the ceiling. Skew/lumpiness errs toward throttling early (rate/quota) or under-allowing
  (budgets) — never toward exceeding the global limit.

## Consequences

- Under skewed load or `limit < replicas`, a replica may throttle before the global limit is truly
  reached. Acceptable for ceilings; documented.
- Exact cluster-wide accounting (e.g. strict cost budgets) needs the precise/Redis backend — a
  documented follow-up reachable via `--rate-limit-strategy=precise` without changing call sites.
