# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versions follow [Semantic Versioning](https://semver.org/). Dates are in
UTC.

## [Unreleased]

### Fixed

- `TritonCommandBuilder` no longer passes `--allow-gpu-metrics=false`, which
  was silently zeroing the `tarn_gpu_utilization` / `tarn_gpu_memory_used`
  Prometheus metrics in spite of the README advertising them.
- The OpenAI proxy now filters target containers on the warmup-confirmed
  ready state (tracked by the AM after `/v2/health/ready` succeeds), so
  requests stop landing on Triton instances still loading their models.
- The K8s operator now uses a finalizer (`tarn.varga.io/operator-cleanup`)
  so `kubectl delete tritondeployment` waits for cleanup before the CR is
  removed — previous behaviour orphaned dependent Deployments if the
  operator pod crashed mid-delete.

### Added

- `ScalingPolicy` accepts a stability-window parameter (`--scale-stability-window`
  / `SCALE_STABILITY_WINDOW`). Default `1` keeps the historical immediate-fire
  behaviour; values of 2–3 dampen oscillation when load grazes a threshold for
  a single tick.
- `RangerAuthorizer.getMode()` exposes the effective access-control mode
  (`DISABLED`, `ENFORCING`, `DEGRADED_DENY`, `DEGRADED_PERMIT`) — surfaced in
  the dashboard and the AM startup log.
- Operator HTTP endpoints `/healthz` and `/readyz`, wired into the Helm
  template as liveness/readiness probes.
- Operator now requeues failed reconciles with exponential backoff and
  retries 409 conflicts on K8s resource updates.
- `MetricsCollector.purgeMissingModels` drops per-model state for models the
  AM no longer advertises, bounding memory on long-running deployments that
  rotate models.
- OWASP DependencyCheck runs in CI with a cached NVD database; report
  uploaded as a build artifact.
- Dockerfiles run as UID 10000 to match the Helm `securityContext`.
- `CONTRIBUTING.md` and `SECURITY.md` documenting contribution flow and
  vulnerability reporting.
- `TritonDeployment` CRD accepts `spec.accelerator.profile` (NVIDIA MIG, e.g.
  `2g.10gb`) and `spec.accelerator.sliceSize` for time-slicing on backends that
  expose fractional resources. The reconciler maps `profile` to
  `nvidia.com/mig-<profile>` for the K8s resource request.
- OpenAPI 3 spec served at `/v1/openapi.json` with a Swagger UI shell at
  `/docs` — clients can now generate SDKs straight from the proxy.
- `--global-rate-limit-rps` puts a process-wide cap on the OpenAI proxy. Refused
  requests get HTTP 429 + `Retry-After`; rejects are counted as
  `tarn_global_rate_limit_rejected_total`.
- South-side TLS to Triton via `--triton-tls-*` flags (truststore + optional
  mTLS client keystore). `MetricsCollector` and the OpenAI proxy switch their
  outbound HttpClient and URI scheme accordingly.
- OAuth2 / OIDC / JWT authentication backed by Nimbus JOSE+JWT. Setting
  `--oauth-issuer` / `--oauth-audience` / `--oauth-jwks-url` enforces
  `Authorization: Bearer` on every proxy endpoint; the legacy static token
  remains active when OAuth is not configured.
- Token usage for **streamed** completions: the proxy now forces
  `stream_options.include_usage=true` and parses the final SSE chunk, so
  chargeback works for `stream:true` requests too.
- Sticky routing by `X-Conversation-Id` (`--sticky-routing-enabled`) — the
  proxy pins follow-ups of a conversation to the same Triton container so the
  KV cache stays warm. Affinity entries expire after a configurable TTL and
  are dropped automatically when their container is reaped.
- Canary auto-promotion MVP in the K8s operator: setting
  `spec.traffic[].analysis` on a variant starts an SLO gate; after
  `observationWindowSec` the operator queries Prometheus for error rate and
  p95 latency vs. the baseline variant, and on success promotes the canary
  to 100% by patching `spec.traffic` weights.
- Cross-replica enforcement via `--shared-state=zk` (`SHARED_STATE`). Reusing the
  existing Curator/ZooKeeper client, the OpenAI proxy's global rate limit and
  per-user quotas are enforced fair-share across replicas — each replica gets
  `ceil(limit / liveReplicas)`, and the shares sum to the configured ceiling, so
  the limit holds cluster-wide instead of being enforced ~N× too loosely. Live
  membership comes from ephemeral znodes (`PersistentNode`, auto-recreated on
  reconnect); conversation affinity is shared via a `CuratorCache` mirror with
  throttled writes, so a follow-up turn resolves the same container and the
  mapping survives an AM restart. The default `local` keeps the historical
  per-process behaviour, so single-replica / single-AM deployments are unchanged.
  Mode and live count are exposed as `tarn_shared_state_mode{mode=...}` and
  `tarn_live_replicas` and on the dashboard. (A precise/Redis backend behind the
  same interfaces is a planned follow-up.)
- Per-user **daily token budgets** (`TokenBudgetEnforcer`). A `budgets` array in the
  same quotas JSON (`{"budgets":[{"user":"alice","tokensPerDay":2000000}, ...]}`)
  caps how many tokens a user may consume per 24h; once exhausted, requests get
  `429 budget_exceeded` until the window rolls. This closes the loop on the token
  metering TARN already does. Rules match by user/group (group = per-member budget),
  first-match by specificity; enforcement is fair-shared across replicas like the
  rate limit. Refusals counted as `tarn_token_budget_exceeded_total`. (Per-model and
  cost-based budgets, and a precise shared counter, are planned follow-ups.)
- Optional **embedding response cache** (`--embedding-cache-size`, `EMBEDDING_CACHE_SIZE`,
  0 disables). A bounded per-process LRU keyed on the request-body hash serves repeat
  `/v1/embeddings` requests without an upstream call — embeddings are deterministic, so
  hits are always correct. The lookup runs after auth/quota/budget/Ranger (policy still
  enforced) and a hit consumes no tokens/budget. Hit/miss exposed as
  `tarn_embedding_cache_hits_total` / `tarn_embedding_cache_misses_total`.
- Enforced **JaCoCo line-coverage floor** (`jacoco:check`, 50%, runs on `mvn test`) and a **Trivy
  gate** that fails the build on fixable CRITICAL image CVEs (HIGH still reported via SARIF).
- Operations [runbook](docs/runbook.md) and design [ADRs](docs/adr/) (shared state for horizontal
  scaling; ZooKeeper over Redis; fair-share over precise counters); OpenTelemetry agent wiring
  documented in `values.yaml` / the runbook.
