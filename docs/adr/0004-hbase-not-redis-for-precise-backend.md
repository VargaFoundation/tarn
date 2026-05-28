# ADR-0004: HBase (not Redis) for the precise shared-state backend

- Status: Accepted
- Date: 2026-05-28
- Supersedes the "Redis as the precise backend" option in
  [ADR-0002](0002-zookeeper-over-redis-for-shared-state.md).

## Context

[ADR-0003](0003-fair-share-over-precise-counters.md) keeps fair-share (ZooKeeper) as the default
but leaves room for an opt-in *precise* backend giving exact cross-replica counters and native TTL.
An earlier draft named Redis for that role. On review the question was raised: why add Redis when
TARN already runs on Hadoop, which offers equivalent technologies?

It's a fair objection. Redis on a Hadoop cluster is a new external dependency — its own deployment,
HA, security and monitoring — for a capability the stack already provides.

## Decision

Implement the precise backend on **HBase** (`--shared-state=hbase`), not Redis.

What the precise path needs maps directly onto HBase primitives:

| Need | Redis | HBase (chosen) |
| --- | --- | --- |
| Atomic counter | `INCR` | server-side `Increment` (no read-modify-write race) |
| Self-expiring windows / affinity | `EXPIRE` / `SETEX` | per-cell TTL (`Mutation.setTTL`) |
| High write throughput on the hot path | yes | yes (HBase is built for it; ZooKeeper is not) |

A single auto-created table holds windowed counters (rate limit, quotas, token/cost budgets) and
TTL'd affinity in one column family. The HBase client is a `provided` dependency (the cluster
supplies it, like Hadoop), with ZooKeeper/Curator/JUnit transitives excluded so it doesn't perturb
the app's own ZK/Curator on the compile/test classpath.

## Decision drivers

- **Hadoop-native, no new infrastructure.** HBase is part of the ecosystem TARN already targets; no
  extra service to run, secure or monitor.
- **Right primitives.** Atomic `Increment` + per-cell TTL is exactly the Redis `INCR`/`EXPIRE`
  pattern, server-side and exact.
- **Throughput.** Unlike ZooKeeper (a coordination service), HBase handles a counter write per
  request; this is the cost ADR-0003 flagged for the precise path, and HBase absorbs it.
- **Testability.** The enforcement *logic* (`CountingRateLimitStore`, budget accounting) is written
  against a `WindowedCounter` interface and unit-tested with an in-memory fake; the HBase wire is a
  thin adapter validated against a real HBase.

## Consequences

- Operators wanting exact cluster-wide limits/budgets need an HBase instance reachable via
  `hbase-site.xml`; those who don't can stay on fair-share `zk` or single-replica `local`.
- One counter round-trip per request in `hbase` mode (the precise-vs-latency trade from ADR-0003);
  the counter fails open on a transient HBase error so inference is not hard-failed.
- The in-JVM `HBaseTestingUtility` mini-cluster cannot run in our build because the app's Jetty 11
  conflicts with the Hadoop mini-cluster's Jetty 9.4; the HBase wire is therefore integration-tested
  in a CI job with a real (or containerised) HBase, while the logic is fully covered by unit tests.
- Redis is explicitly not pursued: it would add an external dependency for no capability gain.
