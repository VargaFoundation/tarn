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
