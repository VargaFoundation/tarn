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
