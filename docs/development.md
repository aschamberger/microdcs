# Development

This page describes the development workflow of MicroDCS: toolchain, commands, the local dev
stack, tests, CI, docs, and the Claude Code setup for local and cloud sessions. The pages below it
in the navigation are the reference per component. The archived Python implementation and its
workflow stay on the
[`python-archive`](https://github.com/aschamberger/microdcs/tree/python-archive) branch.

## Toolchain

| Tool | Version | Notes |
|---|---|---|
| .NET SDK | 11.0.100 (pinned in `global.json`, `rollForward: patch`: CI installs exactly this version, the Docker SDK image is pinned by digest to the same one; a patch upgrade changes `global.json`, the lock files and the image digest in one PR, D-59) | `dotnet-install.sh` or a package manager |
| Native AOT prerequisites (Linux) | `clang`, `zlib1g-dev` | Needed for a host AOT publish (`scripts/aot-publish`); the container build brings its own |
| Docker with Compose v2 | recent | Dev stack and the integration test harness (D-15) |
| Python 3 | any 3.x | Only for the docs site (zensical) |
| GitHub CLI | 2.90.0 or later with `gh-stack` | Stacked PRs ([D-22](porting/decisions.md#d-22)) |

Check the toolchain:

```bash
dotnet --info            # SDK 11.0.100 listed
docker compose version
clang --version
```

## Common Commands

```bash
dotnet build                                  # whole solution (MicroDCS.slnx), warnings are errors
dotnet test                                   # Microsoft.Testing.Platform runner (global.json)
dotnet format --verify-no-changes             # formatting and analyzer style check
CI=true dotnet restore                        # locked-mode restore, as in CI
scripts/aot-publish src/MicroDCS.App linux-x64 out/   # Native AOT publish, D-14 warning allowlist
out/microdcs smoke                            # CloudEvents AOT smoke check
docker build -t microdcs . && docker run --rm microdcs   # same check in the runtime image
```

`scripts/aot-publish` fails on every trim or AOT warning except the per-assembly `IL2104`/`IL3053`
summaries of `CloudNative.CloudEvents*`, which is not annotated for trimming yet
([D-14](porting/decisions.md#d-14)). The runtime image is `runtime-deps` chiseled: non-root
(uid 1654), no shell. `src/` projects restore for `linux-x64` and `linux-arm64`, so the lock files
stay valid for a RID publish in locked mode.

Platforms are linux-x64 and linux-arm64 ([D-32](porting/decisions.md#d-32)). Native AOT links for
the build machine's architecture, so CI builds each image on a native runner. On an x64 machine an
arm64 build needs QEMU (`docker buildx build --platform linux/arm64 .`, slow) or is left to CI.

Packages are managed centrally in `Directory.Packages.props`; every project has a
`packages.lock.json`. After changing a package version run `dotnet restore` and commit the updated
lock files. `nuget.config` maps every package to nuget.org; NuGet audit runs on every restore.

## Local Dev Stack

`compose.yaml` starts Mosquitto, NATS with JetStream, Valkey (`valkey-bundle`,
[D-26](porting/decisions.md#d-26)) and the Aspire dashboard, unauthenticated and plaintext, for
local use only:

```bash
docker compose up -d --wait     # Mosquitto :1883, NATS :4222, Valkey :6379, Aspire dashboard :18888
docker compose down -v          # stop and wipe the Valkey volume
```

Images are pinned by digest; Dependabot proposes updates.

## Run the Application

`microdcs serve` (project `src/MicroDCS.App`) runs the processors whose bindings are configured, on
the transport each binding names (`mqtt`, `nats` or `memory`). Configuration comes from environment
variables with the prefix `APP_` ([Operations](operations.md#configuration-reference)). The
greetings demo on the dev broker:

```bash
APP_MicroDcs__CloudEventSource=urn:microdcs:dev \
APP_MicroDcs__Bindings__greetings__Prefix=dev \
APP_MicroDcs__Bindings__greetings__ScopeLevels=2 \
dotnet run --project src/MicroDCS.App -- serve
```

Add `APP_MicroDcs__Valkey__ConnectionString=localhost:6379` for Valkey-backed dedupe, and the
`machinery_jobs` binding (`APP_MicroDcs__Bindings__machinery_jobs__Prefix=…`) for the Machinery
Jobs processor, the SFC host and the job order publisher, which need Valkey. The management
listener with the dev MCP server is described in [Inspection and MCP](inspection.md#enable-it);
`docker compose --profile app up -d --build --wait` runs the image with it.

## Integration Tests

`compose.test.yaml` is the integration harness ([D-15](porting/decisions.md#d-15)). The `tests`
service builds the `tests` target of the `Dockerfile` and runs `tests/MicroDCS.IntegrationTests`
against Valkey, Mosquitto and NATS on the compose network, all in containers:

```bash
docker compose -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from tests
docker compose -f compose.test.yaml ps -a tests     # must show "exited (0)"
docker compose -f compose.test.yaml cp tests:/results TestResults/compose   # TRX report
docker compose -f compose.test.yaml down -v
```

Compose exits 0 when the tests container never started (for example a dependency failed), so
check the container's own exit code too. Every service in the file must stay up until the tests
end; a one-shot service would trigger `--abort-on-container-exit` early.

The tests read their endpoints from `MICRODCS_TEST_VALKEY`, `MICRODCS_TEST_MQTT`, `MICRODCS_TEST_NATS`
and `MICRODCS_TEST_VALKEY_ACL` (`host:port`). Without them a plain `dotnet test` on the host skips
them; `--fail-skips on` (inside compose, and in CI) turns a skip into a failure. `valkey-acl` is a second Valkey
that requires authentication: with it configured, the Valkey tests run the production code as an
island ACL user with the rule from [Deployment](deployment.md#tenancy), so a command the rule does
not grant fails a test, and the password rotation test runs there. For fast iteration on the host:

```bash
docker compose -f compose.test.yaml up -d valkey valkey-acl mosquitto nats
MICRODCS_TEST_VALKEY=localhost:6380 MICRODCS_TEST_VALKEY_ACL=localhost:6381 \
  MICRODCS_TEST_MQTT=localhost:1884 MICRODCS_TEST_NATS=localhost:4223 \
  dotnet test --project tests/MicroDCS.IntegrationTests
```

The test ports (6380, 6381, 1884, 4223) are offset from the dev stack, so both can run at the same time.

CI runs the suite this way, on the host against the compose services, in the `dotnet` job together with the unit
tests, so the integration coverage reaches SonarQube Cloud (`--filter-not-trait Category=Image --fail-skips on`).
The `integration` job runs `scripts/image-tests`, which also builds the `tests` image; the `tests` entrypoint itself runs
only locally (D-15 revised in S22a).

## Dependency Updates

Dependabot opens the pull requests; some cannot pass as opened (D-59). An SDK or runtime patch (the Docker images,
`global.json`) changes the implicit `Microsoft.NET.ILLink.Tasks` in every lock file, and a NuGet group can leave a related
package behind, so the locked restore fails. Such updates go into one hand-made PR: bump the versions together, then
`dotnet restore --force-evaluate` regenerates every lock file, and the usual checks run (build, tests, AOT publish, image
tests, `scripts/bench`). Close the superseded Dependabot PRs with a link to it. Majors of `Microsoft.Extensions.*` (a new
.NET release) are ignored in `.github/dependabot.yml`; moving to a new .NET release is a decision of its own.
`Microsoft.CodeAnalysis.CSharp` (the generator's Roslyn) must stay at or below the Roslyn of the SDK in `global.json`,
or the compiler skips the generator; our own build fails first.

## Image Tests

`scripts/image-tests` runs the tests with the trait `Category=Image` against the application image (the `image` profile
of `compose.test.yaml`):

* **SFC replicas** (D-53): two instances, killed from outside through the Docker socket.
* **Acceptance** (S23, Q-TEST-2): the image with every role and the greetings and Machinery Jobs bindings (service
  `app`, `MICRODCS_TEST_APP` names its topic prefix), driven only over MQTT as an MES and the equipment would. These are
  the Python integration scenarios: greetings, the job commands, a recipe answered by the equipment, the retained topics
  an MES reads and clears. Each test uses its own scope and ids, because job ids and dedupe claims are global.

To run the acceptance tests against a local build instead of the image, start `dotnet run --project src/MicroDCS.App --
serve` with the `app` service's settings (broker and Valkey at the host ports above) and run the class
`AppAcceptanceTests` with `MICRODCS_TEST_APP=acc MICRODCS_TEST_MQTT=localhost:1884`.

## Schemas and Benchmarks

The JSON Schemas that payloads name in `dataschema` live in `docs/schemas/<name>/v<version>.json`; the docs site serves
them at `https://aschamberger.github.io/microdcs/schemas/` (D-58). They are generated from the models and compared in the
tests of the owning project (golden files of the wire format); `UPDATE_SCHEMAS=1 dotnet test` rewrites them. A
non-additive change gets a new version and a new file; old versions stay.

`scripts/bench` runs `bench/MicroDCS.Benchmarks` (BenchmarkDotNet ShortRun) and compares the allocated bytes per operation
with `bench/baseline.json`: more than 10 % (at least 64 B) above or below it, or a benchmark without a result, fails (below: rewrite the baseline so it stays tight). Mean times are
reported only, shared runners are too noisy. The benchmarks run in process (`--inProcess`) until BenchmarkDotNet knows .NET 11 (D-59). After an intended change, `scripts/bench --update` rewrites the baseline.
A test that waits for a timeout it does not assert costs the whole run that time: stop a host in a test with a short
grace period, because shutdown waits for the expiry timers of open commands until the grace period ends (D-37).

## CI

`ci.yml` and `security.yml` decide per pull request what has to run (`scripts/ci-changes`, [D-56](porting/decisions.md#d-56)):

| Change | Runs on the pull request |
|---|---|
| Documentation only (`docs/`, `overrides/`, `.claude/`, `zensical.toml`, top-level `*.md`, `LICENSE`) | `dotnet` builds and runs the SonarQube Cloud analysis (its check is required); everything else reports a skip |
| Code (including `docs/schemas/`, golden files) | plus tests, format, NuGet audit, `container (linux-x64)`, `integration` (image tests), `bench`, `trivy-image` |
| Image inputs (`Dockerfile`, `global.json`, `Directory.*.props`, `nuget.config`, `ci.yml`, the publish scripts) | plus `container (linux-arm64)` and `kubernetes` |
| Manifest or what the kind test exercises (`deploy/`, `tests/k8s/`, `scripts/k8s-test`, `src/MicroDCS.App`, `src/MicroDCS.Inspection`, `*Options*.cs`, `*ServiceCollectionExtensions.cs`) | plus `kubernetes` |

Pushes to `main`, tags and `workflow_dispatch` run everything; a daily schedule runs both container builds and
`kubernetes`. `scripts/ci-changes pull_request <base> <head>` prints the decision locally.

## Kubernetes Test

`scripts/k8s-test` applies `deploy/` with the overlay in `tests/k8s` to a kind cluster and checks
the probes, Pod Security `restricted`, the NetworkPolicies, the management token and the Valkey ACL
([Deployment](deployment.md#kind-test)). It needs Docker, `kind` and `kubectl`;
`scripts/k8s-test install ~/.local/bin` installs the pinned versions with checksum checks.
`KEEP_CLUSTER=1` keeps the cluster for debugging. CI runs it in the `kubernetes` job ([CI](#ci)).

## Security Scanning

Gitleaks (secrets) and Trivy (vulnerabilities, misconfiguration, SBOM) run as digest-pinned
containers from `compose.scan.yaml`, through `scripts/scan`, locally and in CI alike
([D-20](porting/decisions.md#d-20)). Results go to `scan-results/` (git-ignored); the Trivy DB is
cached in `.cache/trivy`.

```bash
scripts/scan                  # Gitleaks over the working tree, then Trivy over the repository
scripts/scan history          # Gitleaks over the full history of all refs
scripts/scan range main..HEAD # Gitleaks over a commit range
scripts/scan image            # build the runtime image and scan it with Trivy
scripts/scan workflows        # actionlint and zizmor over .github/workflows
scripts/scan help             # all modes
```

| Scanner | What it checks | Fails on |
|---|---|---|
| Gitleaks (`.gitleaks.toml`) | Default rules plus a canary rule, in files and history | Any finding |
| actionlint | Workflow syntax, expressions, action inputs, shellcheck over `run:` blocks | Any error |
| zizmor (`.github/zizmor.yml` if needed) | Workflow security: template injection, permissions, pinning, cache poisoning; with `GH_TOKEN` set also impostor commits behind pinned SHAs and known-vulnerable actions | Any finding at the default persona |
| Trivy fs (`trivy.yaml`) | NuGet lock files, `Directory.Packages.props`, `docs/requirements.txt`; `Dockerfile` and `deploy/` misconfiguration | HIGH or CRITICAL with a fix available |
| Trivy image | OS packages of the runtime image | Same |

Trivy does not check compose files or GitHub workflows; actionlint and zizmor cover the workflows. The Native AOT binary carries no
`.deps.json`, so the image scan sees only OS packages; the NuGet packages are covered by the lock
files. Each Trivy run also writes a CycloneDX SBOM (`sbom-trivy-fs.cdx.json` for the source,
`sbom-trivy-image.cdx.json` for the image).

Waivers go to `.trivyignore.yaml` (with a statement and an expiry date) or, for test fixtures
only, to a narrowly scoped `.gitleaks.toml` allowlist. They are listed in
[Security](security.md#scanner-waivers).

Optional pre-commit hook (Gitleaks over the staged changes): `git config core.hooksPath .githooks`.

CI: `security.yml` runs `gitleaks` (commit range on PRs, full history on `main` and daily),
`workflows` (zizmor online with the job token, SARIF to code scanning), `trivy-fs` and `trivy-image` (SARIF to code scanning, SBOMs as artifacts) on every PR, on `main`
and daily. The `dotnet` job in `ci.yml` runs the SonarQube Cloud analysis
(`dotnet sonarscanner begin`/`end` around the build and the tests with coverage from
`Microsoft.Testing.Extensions.CodeCoverage`). The quality gate is the `SonarCloud Code Analysis`
check of the SonarQube Cloud app. Sonar's C# rules also run in every local build
(`SonarAnalyzer.CSharp` as a global package reference); like all warnings they fail the build.
Coverage locally:

```bash
dotnet test --coverage --coverage-output-format xml --results-directory TestResults
```

### SonarQube MCP server (optional, per developer)

Agents can query SonarQube Cloud (PR issues, quality gate, hotspots, coverage) and analyse single
files through SonarSource's MCP server. It needs a personal user token, so it is configured in the
user scope of Claude Code, never in the repository's `.mcp.json`. Read-only, with telemetry off and
a pinned version (D-20); the workspace mount lets the server read files by path instead of taking
their content through the agent's context:

```bash
claude mcp add --scope user sonarqube \
  --env SONARQUBE_TOKEN=<your user token> --env SONARQUBE_ORG=aschamberger \
  --env SONARQUBE_TOOLSETS=analysis,issues,quality-gates \
  --env SONARQUBE_READ_ONLY=true --env TELEMETRY_DISABLED=true \
  -- docker run --init -i --rm -e SONARQUBE_TOKEN -e SONARQUBE_ORG \
     -e SONARQUBE_TOOLSETS -e SONARQUBE_READ_ONLY -e TELEMETRY_DISABLED \
     -v "$PWD":/app/mcp-workspace:ro \
     sonarsource/sonarqube-mcp:1.31.0.4891
```

Run it from the repository root (`$PWD` is resolved when the command is added). Local file analysis
without SonarQube for IDE uses `analyze_code_snippet`, which SonarSource has deprecated; with the
SonarQube for VS Code extension in connected mode, set `SONARQUBE_IDE_PORT` for the
`analyze_file_list` tool. Read-only mode is deliberate: accepting an issue or marking a false
positive is a waiver and needs a reason in [Security](security.md#scanner-waivers).

## Documentation

The site is built with Zensical, pinned with hashes in `docs/requirements.txt`:

```bash
python3 -m venv .venv-docs
.venv-docs/bin/pip install --require-hashes -r docs/requirements.txt
.venv-docs/bin/zensical serve
```

Regenerate the pins after changing `docs/requirements.in` with
`pip-compile --generate-hashes --strip-extras docs/requirements.in`. The site deploys from `main`
through GitHub Actions (`.github/workflows/docs.yml`).

## Claude Code

The port runs in Claude Code sessions ([D-23](porting/decisions.md#d-23)). The repository carries:

* `CLAUDE.md`: project instructions and the session protocol.
* `.claude/rules/*.md`: path-scoped rules, loaded when matching files are read or edited.
* `.claude/skills/session-start` and `.claude/skills/session-end`: run as `/session-start` and
  `/session-end`.
* `.claude/settings.json`: shared permission allowlist and the `dotnet format` hook. Personal
  overrides go into `.claude/settings.local.json` and `CLAUDE.local.md` (both git-ignored).

### Cloud sessions (Claude Code on the web)

Cloud sessions have no .NET SDK preinstalled and read no repository file for machine setup. Paste
this script into the **setup script** of the claude.ai/code environment for this repository. It
must stay under about 5 minutes so the environment can be cached:

```bash
#!/bin/bash
set -euo pipefail

# .NET SDK, same version as global.json
curl -fsSL https://dot.net/v1/dotnet-install.sh -o /tmp/dotnet-install.sh
bash /tmp/dotnet-install.sh --version 11.0.100 --install-dir "$HOME/.dotnet"
{
  echo 'export DOTNET_ROOT="$HOME/.dotnet"'
  echo 'export PATH="$HOME/.dotnet:$HOME/.dotnet/tools:$PATH"'
  echo 'export DOTNET_CLI_TELEMETRY_OPTOUT=1'
  echo 'export DOTNET_NOLOGO=1'
} >> "$HOME/.bashrc"

# Native AOT prerequisites; Docker for the compose harness if the image lacks it
SUDO=$(command -v sudo || true)
$SUDO apt-get update -qq
$SUDO apt-get install -y -qq clang zlib1g-dev
command -v docker >/dev/null || $SUDO apt-get install -y -qq docker.io docker-compose-v2
```

The network allowlist of the environment must reach `dot.net`, `builds.dotnet.microsoft.com`
(SDK download), `api.nuget.org` and `globalcdn.nuget.org` (packages), and Docker Hub and
`mcr.microsoft.com` (images). Verify once in a fresh cloud session: `dotnet --info`,
`dotnet restore`, `docker compose pull`.
