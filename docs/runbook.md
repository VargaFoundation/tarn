# TARN operations runbook

Operational reference for running TARN (Triton on YARN) in production. Pairs with the
[README](../README.md) (features) and [SECURITY.md](../SECURITY.md) (hardening). For the rationale
behind the bigger design choices, see the [ADRs](adr/).

## Health & readiness

- **Admin** (`--am-port`, default 8888): `GET /health` returns `200 OK` once at least one Triton
  backend is ready, `503 NO_INSTANCES_READY` otherwise, and `503 RANGER_UNAVAILABLE` if Ranger is
  strict and degraded. Use it as the K8s readiness probe.
- **OpenAI proxy** (`--openai-proxy-port`, default 9000): `GET /health` on the proxy port.
- **Operator** (K8s): `/healthz` (liveness) and `/readyz` (leader elected / watch active).
- **Metrics**: `GET /metrics` (admin port) — Prometheus text. Scrape with the bundled
  `ServiceMonitor` (`serviceMonitor.enabled=true`).

Key series to alert on: `tarn_running_containers`, `tarn_inference_requests_total{status="error"}`,
`tarn_inference_latency_seconds` (histogram), `tarn_global_rate_limit_rejected_total`,
`tarn_token_budget_exceeded_total`, `tarn_live_replicas`, `tarn_shared_state_mode`.

## Scaling

- Autoscaling is driven by `--scale-mode` (`gpu_util` | `queue_depth` | `composite`) between
  `--min-instances` and `--max-instances`, evaluated every `--monitor-interval-ms`.
- Damp flapping with `--scale-stability-window 2|3` (consecutive ticks past a threshold before
  acting).
- Tune `--queue-capacity-per-container` to (or slightly under) the backend's batch width so
  queue-normalized load is meaningful.
- Scale-down is graceful: the container is deregistered from ZooKeeper first (Knox/HAProxy stop
  routing), then TARN waits up to `--drain-timeout-ms` (default 30s) for its queue to drain, then
  issues SIGTERM. In-flight requests are not dropped. A `drain_timeout` alert fires if the queue
  was non-empty at the deadline.

## Horizontal scaling & shared state

The OpenAI proxy's **enforcement** state (quotas, global rate limit, conversation affinity, token
budgets) is per-process by default. Running more than one replica requires a shared backend or the
limits are enforced per-replica (≈ N× too loose) and affinity/KV-cache locality is lost.

- **Single replica / single YARN AM**: leave `--shared-state=local` (default). Behaviour is exactly
  as before shared state existed.
- **Multiple replicas** (`replicaCount > 1`, Helm): set `--shared-state=zk` (Helm
  `config.sharedState=zk`). It reuses the configured ZooKeeper ensemble. Quotas, the global rate
  limit and token budgets are then enforced **fair-share** (`ceil(limit / liveReplicas)` per
  replica) and conversation affinity is shared and survives a restart. Confirm with
  `tarn_shared_state_mode{mode="zk"} 1` and `tarn_live_replicas`.
- ZK layout: membership at `<zk-parent>/shared/members/*` (ephemeral), affinity at
  `<zk-parent>/shared/affinity/*` (persistent, leader-purged).
- **ZK outage**: rate limiting keeps enforcing with the last-known replica count (fail-functional);
  affinity reads continue from the local cache mirror; writes resume on reconnect. The membership
  ephemeral is auto-recreated (Curator `PersistentNode`). Watch for `zk_connection_lost` /
  `zk_reconnected` alerts.
- Fair-share is approximate (lumpy/skewed load); for exact cluster-wide limits use a single replica
  until the precise/Redis backend lands (see [ADR-0003](adr/0003-fair-share-over-precise-counters.md)).

## Quotas & token budgets (hot-reload)

Both live in one JSON document (`--quotas hdfs:///tarn/quotas.json`):

```json
{
  "rules":   [{"user": "alice", "model": "llama-3-70b", "requestsPerMinute": 60}],
  "budgets": [{"group": "free-tier", "tokensPerDay": 50000}]
}
```

- **Inspect** live rules: `GET /admin/quotas` (admin token).
- **Update** without restart: `POST /admin/quotas` with the new JSON. TARN validates the JSON, then
  writes it to the ZK config znode (`<zk-parent>/config/quotas`); every replica reloads within one
  Curator event. The response states whether enforcement is cluster-wide (shared-state=zk) or
  per-replica.
- Quotas → `429` + `Retry-After` with reason `rate_limited`. Budgets → `429` reason
  `budget_exceeded` once the daily window is exhausted; the window rolls 24h after first use.

## Embedding cache

- Opt-in: `--embedding-cache-size N` (Helm `config.embeddingCacheSize`), 0 disables. Per-process
  bounded LRU; safe because embeddings are deterministic.
- Lookups run after auth/quota/budget/Ranger, so policy still applies; a hit costs no tokens/budget.
- Watch `tarn_embedding_cache_hits_total` / `tarn_embedding_cache_misses_total`; hit ratio is the
  GPU work avoided. Raise the size for RAG workloads that re-embed the same corpus.

## Tracing (OpenTelemetry)

TARN emits spans via the OTel API but ships no SDK/agent (keeps the JAR lean). To export:

1. Mount the agent (`opentelemetry-javaagent.jar`) into the pod/container.
2. Set `extraJavaOpts` (Helm) or `JAVA_TOOL_OPTIONS`:
   `-javaagent:/opt/otel/opentelemetry-javaagent.jar -Dotel.service.name=tarn -Dotel.exporter.otlp.endpoint=http://otel-collector:4317`
3. `config.otelEndpoint` also sets `OTEL_EXPORTER_OTLP_ENDPOINT`.

A SERVER span is created per proxy request and a CLIENT span for the upstream Triton call; W3C
`traceparent` is propagated and `trace_id`/`span_id` are pushed to the log MDC (enable JSON logs
with `-Dlog4j.configuration=log4j-json.properties`).

## Common alerts → first action

| Alert / symptom | Likely cause | First action |
| --- | --- | --- |
| `NO_INSTANCES_READY` | all backends warming or failed | check `tarn_running_containers`, container logs, model localization |
| `RANGER_UNAVAILABLE` | Ranger plugin degraded + strict | check Ranger service reachability; review `ranger_degraded` alert |
| `zk_connection_lost` | ZK ensemble unreachable | check ZK health; enforcement is fail-functional meanwhile |
| spike in `tarn_global_rate_limit_rejected_total` | runaway client / cap too low | raise `--global-rate-limit-rps` or throttle the client |
| spike in `tarn_token_budget_exceeded_total` | user hit daily budget | expected; raise the budget rule if legitimate |
| limits look ~N× too loose | multi-replica without shared state | set `--shared-state=zk` |
