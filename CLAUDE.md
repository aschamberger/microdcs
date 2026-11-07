# MicroDCS

MicroDCS is a framework for distributed sequence control (DCS) applications in OT and manufacturing.
It builds on open standards: CloudEvents over MQTT v5 (later NATS, AMQP 1.0, Zenoh), OPC UA companion
specifications as payload models (ISA-95 Job Control, Machinery Jobs), Valkey for state, and
OpenTelemetry.

**Status: port from Python to .NET in progress.** `main` is the .NET rewrite and is not a working
product until the parity gate (S23). The Python implementation is frozen on the `python-archive`
branch; read it with `git show python-archive:<path>` (for example
`git show python-archive:src/microdcs/core.py`). Python tests there are the behavioural
specification, but the wire format and key schema are a clean break (D-2).

## Session protocol (mandatory)

The port runs in numbered sessions. The plan lives in `docs/porting/`:

* `docs/porting/index.md`: rationale, libraries, decisions summary, open questions, session table, working rules.
* `docs/porting/sessions.md`: one block per session with tasks and exit criteria.
* `docs/porting/decisions.md`: every decision in full (D-n).
* `docs/porting/log.md`: session log, newest last.

At the start of a session run `/session-start` (or read: `index.md`, the own block in
`sessions.md`, the last `log.md` entry, and the research files and decisions the block links to).
Do not re-research settled decisions; if one is wrong, record a dated revision in `decisions.md`
instead of deviating silently.

At the end run `/session-end`: definition of done, `log.md` entry, status in the session table,
decisions and open questions updated, a "Next session pickup" that needs no chat history, and the
stacked-PR steps. The handoff is part of the session's own PR.

## Tech stack

* .NET 10 (SDK pinned in `global.json`), C# latest, Native AOT for the application.
* Central package management (`Directory.Packages.props`), lock files, NuGet audit, `nuget.config` maps everything to nuget.org.
* Tests: xUnit v3 on Microsoft.Testing.Platform, Shouldly, NSubstitute (D-31).
* Planned libraries (see the library table in `docs/porting/index.md`): StackExchange.Redis (Valkey), MQTTnet 5, NATS.Net, AMQPNetLite, a Zenoh binding, OPC UA UA-.NETStandard 2.x (models only, D-4), CloudNative.CloudEvents (D-14), ModelContextProtocol.AspNetCore.
* Docs: Zensical (`zensical.toml`, `docs/`), pinned in `docs/requirements.txt`; published from `main` via GitHub Actions.

## Structure

```
src/MicroDCS.Core/          core library (contracts arrive in S06)
src/MicroDCS.App/           AOT application (`microdcs`); until S08 only the CloudEvents AOT smoke check
tests/MicroDCS.Core.Tests/  unit tests
tests/MicroDCS.IntegrationTests/  integration tests, run inside compose.test.yaml (skipped on the host)
scripts/aot-publish         AOT publish with the D-14 warning allowlist (CI and Dockerfile use it)
scripts/scan                Gitleaks and Trivy through compose.scan.yaml (D-20), locally and in CI
compose.scan.yaml           digest-pinned scanner containers; configs in .gitleaks.toml, trivy.yaml, .trivyignore.yaml
.github/workflows/          ci.yml (build, tests, Sonar, AOT image, integration), security.yml (scanners), docs.yml
Dockerfile                  AOT publish into chiseled runtime-deps (non-root, no shell; linux-x64 and linux-arm64, D-32); `tests` target for compose
compose.test.yaml           integration harness (D-15): Valkey, Mosquitto, tests container
docs/                       docs site; docs/porting/ is the port plan
schemas/                    JSON Schemas (greetings, machinery jobs, SFC recipe, IJT)
deploy/                     Kubernetes manifest (Python era, ported in S22)
compose.yaml                local dev stack: Mosquitto, Valkey (valkey-bundle), Aspire dashboard
overrides/                  Zensical template overrides (WIP announcement bar)
.claude/                    rules, skills, hooks and shared settings for Claude Code
```

## Commands

```bash
dotnet build                          # whole solution (MicroDCS.slnx); warnings are errors
dotnet test                           # all tests (Microsoft.Testing.Platform)
dotnet format --verify-no-changes     # style check as in CI; `dotnet format` fixes
CI=true dotnet restore                # locked-mode restore; commit packages.lock.json changes
docker compose up -d --wait           # dev stack; `docker compose down -v` wipes Valkey
scripts/scan                          # Gitleaks (working tree) and Trivy fs; `scripts/scan help` for all modes
```

Integration tests run inside compose (D-15):
`docker compose -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from tests`,
then `docker compose -f compose.test.yaml down -v`. Compose exits 0 if the tests container never ran,
so also check `docker compose -f compose.test.yaml ps -a tests` (CI does).
AOT publish: `scripts/aot-publish src/MicroDCS.App linux-x64 <out-dir>` (fails on every trim/AOT
warning except the allowlisted `CloudNative.CloudEvents*` ones), then `<out-dir>/microdcs smoke`.
Container: `docker build -t microdcs . && docker run --rm microdcs`.

## Coding rules

* **Zero warnings.** `TreatWarningsAsErrors` is on; fix the cause, do not suppress. A suppression needs a justification comment and is the exception.
* **Native AOT and trimming.** No reflection-based discovery, no `dynamic`, no `Activator.CreateInstance`, no `MakeGenericType`/`MakeGenericMethod`, no `Reflection.Emit`, no `Expression.Compile`. System.Text.Json only with source-generated `JsonSerializerContext`. Logging only with `[LoggerMessage]`. DI registrations explicit. Libraries under `src/` are `IsAotCompatible`, so the trim and AOT analyzers run on every build; `IL2xxx`/`IL3xxx` warnings fail the build.
* **Async.** `CancellationToken` on every async API, `ConfigureAwait(false)` in library code, `TimeProvider` instead of `DateTime.UtcNow` or `Task.Delay` without one.
* **Style.** File-scoped namespaces, `_camelCase` private fields, `required` and records where useful. `.editorconfig` is authoritative.
* **Public API** has XML docs (`GenerateDocumentationFile` is on for `src/`).
* **No secrets or payloads in logs.** Log identifiers and sizes, not message contents.
* **Docs follow code.** Configuration fields and their docs change in the same PR.

## Design decisions that are intentional

Do not flag these in reviews; the full text is in `docs/porting/decisions.md`.

* **Fail-fast startup** (D-27): a component that cannot start ends the process; no degraded mode.
* **Explicit error on queue backpressure** (D-28): a full bounded queue fails the enqueue instead of blocking silently.
* **No size limit on `custommetadata`** (D-29): transport limits bound it.
* **Comma-separated list config values** (D-30): split without escaping, values cannot contain commas.
* **Push-command response routing is instance-affine** (D-9): a response that reaches another instance is dropped; recovery re-dispatches.
* **Dedupe-key trust comes from transport access control** (D-13).
* **Each transport uses its native features** (D-10): no lowest-common-denominator emulation.
* **No blocking Valkey commands** (D-7): stream consumption polls.
* **Clean break for wire format and key schema** (D-2): no compatibility with the Python format.
* **CloudEvents upstream SDK, AOT-ready, fork late** (D-14).

## Git and pull requests

* One session is one PR on a branch named after it (for example `s04-library-spike`), with the session id in the PR title, assigned to its GitHub milestone (M0 to M3).
* Each milestone is one stacked-PR chain managed with `gh stack` (D-22); squash merge bottom-up as soon as a session is green. Details in the `/session-end` skill.
* `main` is protected: pull requests only, squash merge, linear history.

## Tools

* Prefer the project's own tools when they are available in a session: code-analysis and quality tools (SonarQube Cloud project `aschamberger_microdcs`; use a SonarQube analysis tool on changed files when one is configured) and the MicroDCS dev MCP server (inspection tools, from S11, configured in `.mcp.json`).
* Read Python sources from the archive branch instead of guessing their behaviour.
