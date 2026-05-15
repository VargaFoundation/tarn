# Security Policy

## Supported versions

TARN is in active development. Until we cut a `1.0.0`, only the `main` branch
receives security fixes. Once we tag stable releases, this section will list
which release lines remain supported.

## Reporting a vulnerability

**Do not open a public GitHub issue.**

Email the maintainers privately and include enough detail to reproduce:

- Affected commit / version (`git rev-parse HEAD` or the image tag).
- A short reproduction (config, request, behaviour observed).
- Your assessment of impact (information disclosure, RCE, DoS, privilege
  escalation, etc.).

Contact: **security@varga-foundation.example** (replace with the real
mailing address before publishing the project — until then, open a private
GitHub Security Advisory via the repository's "Security" tab).

### Response expectations

| Stage | Target |
| --- | --- |
| Acknowledgement of report | 3 business days |
| Triage + reproduction confirmation | 10 business days |
| Patch or mitigation merged | 30 days for High/Critical |
| Public disclosure | After a patched release is available |

We will credit reporters in the release notes unless asked otherwise.

## Scope

In scope:

- The TARN application (YARN Application Master, OpenAI proxy, dashboard,
  Kubernetes operator).
- Helm chart manifests and the default values they ship with.
- Docker images published from this repository.

Out of scope:

- Vulnerabilities in upstream Hadoop, Ranger, Knox, NVIDIA Triton, fabric8,
  or other dependencies — please report those upstream. TARN tracks dependency
  CVEs via OWASP DependencyCheck in CI and bumps versions as fixes land.
- Misconfiguration in user-supplied cluster setup (Kerberos, RBAC, network
  policies). The README documents the recommended baseline; deviations are the
  operator's responsibility.

## Hardening already in place

For context on what is *already* enforced (so you can focus on what isn't):

- OIDC / OAuth2 bearer-JWT authentication (Nimbus JOSE+JWT): when
  `--oauth-issuer` / `--oauth-audience` / `--oauth-jwks-url` are set, every
  proxy endpoint requires a signature-, issuer-, audience- and expiry-validated
  `Authorization: Bearer` token, and identity comes from the JWT `sub` claim
  (client-supplied `X-*` user headers are then ignored, so callers cannot
  impersonate).
- Constant-time API-token comparison for the legacy static token; token only
  accepted via the `X-TARN-Token` header (never as a query parameter).
- Shell-metacharacter rejection on all user-supplied paths
  (`TritonCommandBuilder.requireSafePath`).
- SSRF guard on outbound metric scrapes (loopback / link-local / metadata
  endpoints refused).
- Mandatory TLSv1.2+ for admin HTTPS / OpenAI proxy when TLS is enabled.
- South-side TLS to Triton (`--triton-tls-*`): the AM/proxy can verify Triton's
  certificate against a truststore and present a client keystore for mTLS so
  Triton can authenticate TARN in return.
- Ranger fail-closed (`--ranger-strict`) when a Ranger service is configured.
- OpenAI proxy enforces Ranger + per-user quotas *before* forwarding to
  Triton.
- Token chargeback covers streaming completions: the proxy forces
  `stream_options.include_usage=true` and parses the final SSE chunk, so
  `stream: true` requests are accounted just like non-streaming ones.
- JCEKS-backed secret resolution — passwords never on the command line.
- Helm pod runs as UID 10000 with `readOnlyRootFilesystem`, dropped
  capabilities, RuntimeDefault seccomp profile.
- Network policy restricts ingress to the release namespace.

## Known gaps

These are tracked in the repository's issue list; patches are very welcome —
see [CONTRIBUTING.md](CONTRIBUTING.md):

- **Per-replica enforcement.** Quotas and the global rate limit are enforced in
  process-local memory, and conversation affinity (KV-cache sticky routing) is
  held per process. Running more than one replica therefore multiplies the
  effective limits (each replica enforces its own copy), and affinity is neither
  shared across replicas nor preserved across a restart. Shared-state backends
  (ZooKeeper / Redis) that make these correct under horizontal scaling are in
  progress; until then, enforce limits with a single replica or an upstream
  gateway.
- **No response or embedding cache.** Identical requests are always forwarded
  upstream; only Triton's in-process KV cache is reused (via sticky routing).
- **Request-rate quotas only.** Quotas cap requests per minute; there is no
  token- or cost-based budget enforcement (token usage is metered but not
  capped).
