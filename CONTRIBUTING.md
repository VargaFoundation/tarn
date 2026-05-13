# Contributing to TARN

Thanks for your interest. TARN is small enough that this guide stays short on
purpose — please read it once before opening your first PR.

## Local setup

```bash
git clone https://github.com/varga-foundation/tarn.git
cd tarn
./mvnw clean package
```

Java 17 (Temurin) is required. The build uses the bundled `mvnw`, so a
system Maven is not needed.

Run the full local check suite before pushing:

```bash
./mvnw clean verify   # tests + jacoco
./mvnw spotbugs:check # static analysis (warnings are tolerated, errors are not)
helm lint helm/       # if you changed the chart
```

## Issue first, code second

For anything beyond a typo or a tiny bug fix, open an issue first so the
direction can be agreed on. Drive-by refactors and architecture changes that
land in a PR without prior discussion will usually be sent back for an issue.

## PR checklist

- [ ] One concern per PR. Don't bundle unrelated changes.
- [ ] Tests for any new behaviour. We aim to keep coverage at or above 50 %.
  The CI uploads a JaCoCo report — check it for the area you touched.
- [ ] Run `./mvnw test` locally; do not rely on CI as your test runner.
- [ ] If you changed the Helm chart, run `helm template` with at least one
  optional feature flag flipped on to catch template typos.
- [ ] Update `README.md` / docs when you change user-visible behaviour
  (command-line flags, env vars, CRD fields).
- [ ] Update `CHANGELOG.md` under the `## [Unreleased]` heading.

## Commit & PR style

- Short, imperative commit subjects, no scope prefix: `fix scaling oscillation`,
  not `chore(scaling): fix oscillation`.
- One logical change per commit when reasonable; squash later if review asks.
- Reference the issue with `Fixes #N` in the PR body, not the subject.
- Sign off your commits (`git commit -s`) — the project follows DCO. No CLA
  beyond that.

## Code style

- Java 17 features welcome; don't go beyond what the build targets.
- Two new dependencies = a conversation. Prefer the JDK + what's already in
  `pom.xml`. Anything brought in via `dependency-reduced-pom.xml` (Hadoop,
  Ranger, Curator) is fair game without discussion.
- Don't add `@Author` tags, don't add multi-paragraph Javadoc on trivial
  methods. Comment the *why*, never the *what*.

## Reporting security issues

See [SECURITY.md](SECURITY.md). **Do not** open a public GitHub issue for
suspected vulnerabilities.

## Releasing (maintainers)

1. Bump the version in `pom.xml` and `helm/Chart.yaml`.
2. Update `CHANGELOG.md` — move `[Unreleased]` entries under a new dated
   heading.
3. Tag the commit (`git tag -s v0.x.y && git push --tags`).
4. The release workflow pushes the Docker image and Helm chart to GHCR.
