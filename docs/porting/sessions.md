# Sessions

One block per session. Dependencies and status are in [the plan](index.md#sessions); working rules and the definition of done are in [the plan](index.md#working-rules-for-every-session). Each block names the research files and decisions to read before starting.

## M0 Setup

### S01 Repository reset, scaffolding, agent setup

Read: [branching](research/branching.md), D-16, D-20.

* **First commit: remove Python from `main`.** `python-archive` exists (2026-11-07), so Python stays readable via `git show python-archive:path`.

  | Action | Files |
  |---|---|
  | Keep | `docs/` (including `docs/porting/`), `schemas/`, `deploy/`, `review.md`, `zensical.toml`, `SECURITY.md`, `README.md` (with banner; licence line becomes MIT) |
  | Replace | `LICENSE` (MIT text, D-24); `Dockerfile` (S02); `.vscode/tasks.json` and `.vscode/settings.json` (.NET tasks); `.gitignore` (add `CLAUDE.local.md`, `.claude/settings.local.json`); `.github/copilot-instructions.md` (becomes `CLAUDE.md`, see below); `.github/dependabot.yml` (`uv` entry becomes `nuget`, keep `docker` and `github-actions`, add `pip` for `/docs`); `compose.yaml` (Valkey instead of Redis, drop the Python `app` profile, pin Aspire `:latest` and Mosquitto `:2` by digest) |
  | Remove | `src/microdcs`, `tests`, `app`, `pyproject.toml`, `uv.lock`, Python CI (`ci.yml` content) |

* **Docs build keeps its pin.** `docs.yml` installs zensical unpinned today; the pin came from the `uv` dev group, which goes away. Add `docs/requirements.txt` with zensical pinned and hashed, let `docs.yml` install from it (`--require-hashes`), and cover it with the Dependabot `pip` entry.
* **Pages from Actions, no `gh-pages` branch.** Today Pages is in legacy mode (source: branch `gh-pages`), filled by `peaceiris/actions-gh-pages` with `contents: write`. Rewrite `docs.yml`: workflow default `permissions: contents: read`; a `build` job (pinned zensical, `zensical build`, `actions/upload-pages-artifact` with `./site`); a `deploy` job with `pages: write` and `id-token: write`, `environment: github-pages`, `actions/deploy-pages`; triggers `push` to `main` and `workflow_dispatch`; `concurrency: pages`. Drop the git credential step and the third-party action. Order: switch the Pages source right before merging (`gh api -X PUT repos/aschamberger/microdcs/pages -f build_type=workflow`), check that the `github-pages` environment allows deployments from `main` only, merge, confirm the site, then delete the `gh-pages` branch. Actions get pinned by SHA in S03.
* **Docs site as live WIP:** WIP notice, "not yet ported" admonitions, nav entry "Port to .NET (WIP)" for `docs/porting/` in `zensical.toml` (details in [branching](research/branching.md) item 5). Check that tables and anchors render. Update `docs/overall-design.md` "Southbound Connectivity" for D-11.
* **Dependabot security updates:** turn them on once the `nuget` entry replaces `uv` (left off on 2026-11-07 to avoid security PRs for the Python dependencies).
* **Dependabot:** the open `uv` PRs were closed on 2026-11-07 (#31 to #35). New `uv` PRs may appear until the `uv` entry is removed from `dependabot.yml`; close them too.
* **Branch protection on `main`:** pull requests required, no required approvals (single maintainer), squash merge only (repository merge settings), delete head branches after merge. Required checks are added in S02 and S03.
* **Stacked PRs (D-22):** check the current status of GitHub stacked PRs; install GitHub CLI 2.90.0 or later and the `gh-stack` extension (ask before installing); create the GitHub milestones M0 to M3. S01 itself is the bottom PR of the M0 stack. Write the workflow (branch naming, stack commands, squash merge, log entry in the same PR) into the session-end instructions.
* **Toolchain check:** `dotnet --info` (SDK 10), Docker and compose, the Linux AOT prerequisites (`clang`, `zlib1g-dev`). Ask before installing anything. Optional `.devcontainer/` for local use. Cloud sessions (Claude Code on the web) have no .NET SDK preinstalled and read no repository file for machine setup: document the setup script for the claude.ai/code environment (SDK 10, `clang`, `zlib1g-dev`, Docker; under about 5 minutes so it is cached) in `docs/development.md`, and check that the network allowlist reaches NuGet.
* **Repository scaffolding:** `global.json` (pinned SDK), `Directory.Build.props` (`PackageLicenseExpression` `MIT`, nullable, `TreatWarningsAsErrors`, `IsAotCompatible`, analyzers, deterministic build, XML docs, `AnalysisLevel`), `Directory.Packages.props` (central package management), `nuget.config` (package source mapping, NuGet audit, lock files), `.editorconfig`, `.gitattributes`, `.gitignore`, solution file (`.slnx`), empty projects `src/MicroDCS.Core` and `tests/MicroDCS.Core.Tests`.
* **Decisions to take** (defaults in brackets): root namespace and package prefix [`MicroDCS`]; test framework [xUnit v3 with `Microsoft.Testing.Platform`]; assertion and mocking libraries [plain asserts or Shouldly, NSubstitute; no reflection-heavy mocking in AOT paths]; logging [`Microsoft.Extensions.Logging` with the `LoggerMessage` source generator]; DI [`Microsoft.Extensions.DependencyInjection`, no runtime-reflection registration in AOT paths]; style [file-scoped namespaces, `required`/records where useful, `TimeProvider` for time, `CancellationToken` everywhere].
* **Carried-over design decisions.** Sort the "Design Decisions" list of the Python `.github/copilot-instructions.md`. Language-independent items become short decisions (D-27 onward, "carried over from Python"): fail-fast startup, explicit error on queue backpressure, `custommetadata` without an extra size limit (R-12), comma-separated config values if the config format keeps lists. Python-specific items are dropped.
* **Agent setup for Claude Code (D-23):**
  * `CLAUDE.md` at the root, under about 200 lines: project description, tech stack, structure, commands (`dotnet build/test/publish`, compose test command), coding rules (AOT: no reflection, no `dynamic`, source-generated System.Text.Json and logging, no `Activator`/`MakeGenericType`; zero warnings), the design decisions that must not be flagged, and the **session protocol** (read `docs/porting/index.md` and the own session block, definition of done, mandatory handoff). Reference the plan by path; do not `@`-import it, because imports load into every session.
  * `.claude/rules/*.md` with `paths:` frontmatter (only loaded when a matching file is read or edited): `csharp.md` (`**/*.cs`), `tests.md` (`tests/**`), `compose-ci.md` (`compose*.yaml`, `.github/workflows/**`, `**/Dockerfile`), `docs.md` (`docs/**`; keep docs in sync with configuration fields).
  * Skills `.claude/skills/session-start/SKILL.md` and `.claude/skills/session-end/SKILL.md` with `disable-model-invocation: true` (run as `/session-start` and `/session-end`): the start reading list from the working rules, and the end checklist (log entry, status, decisions, pickup, definition of done, stacked-PR steps from D-22).
  * `.claude/settings.json` (committed): permission allowlist for the routine commands (`dotnet build/test/format/publish`, `docker compose` on the repository's compose files, read-only `git` and `gh`); optional `PostToolUse` hook that runs `dotnet format` on edited `*.cs` files (the script filters by extension, because the file path arrives as JSON). Personal overrides stay in `.claude/settings.local.json`.
  * `.mcp.json` at the root for project MCP servers: empty or absent until S11 adds the MicroDCS dev MCP server. Claude Code asks once before using a project server.
  * Remove `.github/copilot-instructions.md`. No `AGENTS.md` unless a second agent tool is used again; then `AGENTS.md` holds the shared text and `CLAUDE.md` imports it with `@AGENTS.md`.
  * Describe tool capabilities (for example "SonarQube analysis tools", the MCP tools from S11) instead of naming tool ids.
* Exit: `dotnet build` and `dotnet test` run on an empty solution, the docs site builds with the pinned zensical and shows `docs/porting/`, `CLAUDE.md`, rules, skills and settings in place, D-27 onward recorded.

### S02 CI, container, compose harness, CloudEvents AOT measurement

Read: [test setup](research/test-setup.md), [CloudEvents](research/cloudevents.md), D-14, D-15.

* **CI** (`.github/workflows/ci.yml` for .NET): restore in locked mode, build, unit tests, `dotnet format --verify-no-changes`, NuGet vulnerability audit, AOT publish smoke (warnings as errors, with `TrimmerSingleWarn` and an allowlist limited to the single assembly-level `IL2104`/`IL3053` warnings of `CloudNative.CloudEvents*`, D-14), compose integration job (below).
* **Workflow names before checks become required:** today `docs.yml` is named `ci` and `ci.yml` is named `tests`. Rename them (for example `docs` and `ci`), then add the required checks in branch protection.
* **Dockerfile:** multi-stage AOT publish to a minimal runtime-deps image (non-root, no shell), pinned by digest.
* **Compose harness (D-15):** `compose.test.yaml` with Valkey (`valkey-bundle` pinned by digest, D-26), Mosquitto and the `tests` service (runs inside compose), healthchecks, `--abort-on-container-exit --exit-code-from tests`, log upload on failure, a trivial integration test proving the path. `compose.yaml` stays the dev stack.
* **CloudEvents (D-14):** check PR #381 and the latest upstream release. Reference `CloudNative.CloudEvents` and `.SystemTextJson` from NuGet; add an AOT publish smoke test that serializes and parses an event with `Data` as `JsonElement` and `byte[]` inside the container. Record whether it works at runtime. If it fails and cannot be handled, apply the "fork late" rule and record why. The other libraries' AOT checks belong to S04.
* Exit: CI green on `main`, an AOT smoke binary runs in the container, the CloudEvents AOT result and the PR #381 status recorded in the log.

### S03 Security and quality scanning (D-20)

Read: [CI scanning](research/ci-scanning.md), D-20.

* **Manual prerequisites first** (listed in the research file): SonarQube Cloud project and `SONAR_TOKEN` (Actions and Dependabot secret), code scanning. GitHub secret scanning and push protection are already on (2026-11-07); optionally add non-provider patterns and validity checks. If the token does not exist yet, the Sonar job skips with a clear message and the rest of the session proceeds.
* **Pin the existing workflows:** every `uses:` to a full commit SHA with a version comment, default `permissions: contents: read`, `persist-credentials: false`; confirm the Dependabot `github-actions`, `docker`, `nuget` and `pip` ecosystems cover the new files (grouped updates).
* **`compose.scan.yaml`** with `gitleaks` and `trivy` services (images by digest), `.gitleaks.toml` (canary rule, narrowly scoped test-fixture allowlist with comments), `trivy.yaml` (`scanners: vuln,misconfig`, severity HIGH/CRITICAL, `ignore-unfixed`), `.trivyignore.yaml` (empty, waiver rules in a comment), `scripts/scan`, optional `pre-commit` hook (`core.hooksPath`).
* **`security.yml`:** the three jobs from the layout table (PR, push, daily), SARIF upload for Trivy (job-level `security-events: write`), Gitleaks SARIF as artifact, SBOM (CycloneDX) artifact, Trivy DB cache. The first `push` run of `gitleaks` scans the full history, including the Python history shared with `python-archive`; assess any finding there.
* **Sonar in `ci.yml`:** `dotnet-sonarscanner` pinned in `.config/dotnet-tools.json`, `begin`/`build --no-incremental`/test with coverage/`end`, exclusions (generated code, `spikes/**`, `docs/**`), quality gate wait, coverage format decided, skip on fork PRs. README badges.
* **Prove the gates can fail** (self-test from the research file) on a throwaway branch, then delete it; make the checks required.
* **Agent setup:** security and quality rules in `CLAUDE.md` (or a rule file scoped to workflows, Docker and configuration) and the `session-end` skill (run `scripts/scan` before commits touching workflows, Docker, configuration; use SonarQube analysis tools when available; no suppression without reason and expiry); scan outputs, `.sonarqube/`, coverage files and the maintenance dump directory in `.gitignore`.
* Exit: the three scanners run on a PR and on `main`, required checks set, first SBOM artifact, canary self-test proven, CI timings and the coverage decision in the log, D-20 confirmed or revised with the reason.

## M1 Foundation

### S04 Spike: library stack, AOT and Valkey behaviour

Read: library table and Q-ARCH-1, Q-DATA-1, Q-DATA-2 in [the plan](index.md); [Valkey findings](research/valkey-findings.md); D-7, D-18.

In a throwaway project (`spikes/`, deleted or documented at the end), one AOT-published console app against the compose stack. Temporary compose profiles for NATS (JetStream) and zenohd, because the S02 harness has neither.

* **StackExchange.Redis:** Lua with string scripts; Streams consumer groups and `XAUTOCLAIM` (including the deleted-ids part of the reply); reconnect behaviour. **Stream consumption without blocking reads:** measure latency and Valkey load for plain polling and for polling plus a pub/sub nudge (D-7). **Documents:** raw `JSON.SET/GET` plus `FT.SEARCH` with the flat-array reply parser versus plain-string documents in a hash `{v, doc}` with set/sorted-set indexes (Q-DATA-1). **Time:** confirm that the stream entry id can serve as event time (D-18), and that scripts may call `TIME` and then write.
* **MQTTnet 5:** user properties, response topic, correlation, expiry, retained, shared subscription, manual ack.
* **NATS.Net:** JetStream, KV, request/reply.
* **Kestrel minimal APIs** and **`ModelContextProtocol.AspNetCore`** with explicit, source-generated registration (D-19).
* **AMQPNetLite** publish-only: warnings, trimmer annotations needed.
* **Zenoh:** `Zenoh-CS` ([`sanri/zenoh-csharp`](https://github.com/sanri/zenoh-csharp)) publish-only: AOT and trim warnings (classic `DllImport` with blittable signatures is usually AOT-safe, `MarshalAs` and delegate marshalling are not always), put/subscribe and get/queryable with attachments through the managed API, zenoh-c 1.10.0 built with `UNSTABLE_API` and `SHARED_MEMORY` in the runtime image, for linux-x64 and linux-arm64 (D-32). Spot-check the P/Invoke declarations the session uses against the zenoh-c headers. If the result is not clean: try ZenohDotNet, then decide between contributing upstream and a fork.
* Output: findings table in `research/library-spike.md`; decisions on Q-ARCH-1 (which libraries are AOT-clean, whether AMQP is isolated in an optional package); measurements as input for S07 (Q-DATA-1, Q-DATA-2). The Zenoh library choice may wait until before S25, but the findings go in now.

### S05 Spike: OPC UA ISA-95 for models only (D-4)

Read: D-4, D-21; ISA-95 and Work Master rows in the open questions.

Use `Opc.Ua.ISA95*` without a server: can the Common Model and Job Control V1/V2 types be used as plain models and serialized (OPC UA JSON versus System.Text.Json, AOT)? Is the job state machine usable standalone or is an own one needed (Q-ARCH-4)? Prototype the Work Master extension as NodeSet v2 plus generated models, and try the schema generation tooling for `dataschema` and the recipe schema. Check target frameworks against net10. Record the licence metadata of the packages in use and of the NodeSets (D-21). Output: job model strategy (decision), Q-ARCH-2 and Q-ARCH-4 answered.

### S06 Design: core contracts

Read: [abstraction mapping](research/abstraction.md), [inspection and MCP](research/inspection-mcp.md), [review triage](research/review-triage.md), D-3, D-9, D-10, D-14, D-19.

Little code: decisions and compiling interface stubs in `MicroDCS.Core`.

* The `CloudEvent` usage behind one envelope-codec seam (D-14: typed `mdcs*` extension attributes, transport context, no `object Data` in own types).
* **First: the processor authoring API** (D-25, Q-STRAT-4). Write two or three example processors as they should read (greetings, a job-order handler, a request/response command) before any interface; then attributes plus source generators for handler registration, analyzers and their diagnostics, typed payloads, the test harness experience. The pipeline contracts below follow from it.
* Hosting and configuration (Q-ARCH-3: `Microsoft.Extensions.Hosting`, `IConfiguration` binding with source generation; final configuration names).
* The logical address model and renderers (A-3), the typed transport context (A-2), the inbound pipeline and transport interface (A-1, A-5, A-6, A-7, A-8), the state-topic port (A-4), delivery semantics per transport (Q-ARCH-7), the level-0 scope rule (R-6).
* Error model, shutdown and backpressure semantics.
* Inspection abstractions, redaction policy and management listener layout (D-19 with Q-OPS-1).
* CloudEvents checkpoint (D-14).

### S07 Design: data layer and SFC rules

Read: [SFC core](research/sfc-core.md) (including "Open points from the plan review"), [Valkey findings](research/valkey-findings.md), `research/library-spike.md` from S04, D-7, D-12, D-17, D-18.

Little code: decisions and compiling stubs in `MicroDCS.Valkey` and `MicroDCS.Sfc`.

* **Key schema:** documents and secondary indexes (Q-DATA-1), job identity `(scope, id)` versus no hash tags now, persistent scope registry, stream trimming rule (no `MAXLEN` on work and outbox streams).
* **Stream consumption:** polling strategy from the S04 measurements (Q-DATA-2), one poller component that also runs the due-index sweep.
* **SFC core rules (D-17):** `IRecipeInterpreter`, work identity, schema direction and payload versioning, recipe acceptance capabilities. Late-response rule and outcome-based conditions are decided; design the outcome mapping per `type_id`, the build-time validation, and whether `timeout`/`error` become routable outcomes.
* **Time model (D-18):** event time from the stream entry id, sweep interval and outage threshold (the grace rule itself is decided).
* **Retry policy:** `max_attempts` and its default (the deadline comes from the recipe's `timeout_seconds`, D-18).
* Move D-17 and D-18 from proposal to decided (or revise them).

### S08 Core

Envelope and extension attributes, processor base and handler registration (source generated), hosting and configuration with validation, in-memory transport, processor test harness (R-15, `MicroDCS.Testing`), the greetings processor as the demo and first end-to-end test. Start `docs/` pages for the .NET API as the code lands. CloudEvents checkpoint (D-14).

### S09 Valkey layer (parallel to S08)

Read: the S07 key-schema decisions, [Valkey findings](research/valkey-findings.md), R-4.

Key schema (D-2, S07), connection setup and options, DAO base with idempotent writes under unknown outcome (R-4), dedupe lease DAO with the Python semantics (claim, done, lease remaining), Streams and Lua helpers with the trimming rule (trim only acknowledged entries; log and count the deleted ids `XAUTOCLAIM` returns), the poller component from S07, secondary indexes as decided (or the Valkey-specific `FT.SEARCH` parser with an integration test), health check. Tests against compose Valkey, including reconnect during an operation.

### S10 Pipeline (A-1)

Transport-neutral inbound pipeline: decode, dedupe claim, expiry handling, scope check (R-6 rule from S06), dispatch to processors, response publishing, `mark done`, ack/nack, retry. Tracing once (ActivitySource, W3C context), structured logging without payloads (R-10), security log category (R-11, first part). Envelope benchmark with BenchmarkDotNet (R-18). Tests on the in-memory transport, including crash-between-process-and-ack cases.

### S11 Inspection layer and MCP read tools

Read: [inspection and MCP](research/inspection-mcp.md), D-19.

`MicroDCS.Inspection` abstractions, inspector registration per component, redaction policy, management listener (default off), then `MicroDCS.Mcp` with the S11 read tools from the tool sketch and `inject_event` in the write group, pipeline decision ring buffer, bearer-token auth for non-loopback, audit logging, size and rate limits. Tests: tool contract tests against the in-memory host, redaction tests, "disabled means no listener". Compose profile that enables it, the server entry in `.mcp.json`, the `CLAUDE.md` section on using it. Verify AOT with the smoke test.

### S12 Maintenance tools with snapshot and restore

Read: "Maintenance tools" in [inspection and MCP](research/inspection-mcp.md), D-19.

`MicroDCS.Maintenance` (`IMaintenanceOperations`), plan/apply with tokens, environment guard, item cap, audit, architecture test that keeps it out of the REST adapter and `MicroDCS.Inspection`; first tools `maint_dedupe_release`, `maint_purge_scope`; snapshot before apply, `maint_restore`, `maint_snapshots_list`; dump directory git-ignored. Tests: "refuses outside Development/Test", plan/apply version mismatch, snapshot and restore round trips, retention pruning. Decide the open defaults (item cap, retention, size cap, marker key).

### S13 MQTT transport

MQTTnet 5 binding: binary-mode mapping with MQTT 5 properties, request/response with per-instance reply topic (D-9), retained publish and delete with TTL, shared subscriptions, topic rendering and scope check, TLS, username/password, client certificates, password files, reconnect. Compose e2e: greetings over MQTT, plus the "no unacknowledged message after dedupe" check from the unverified list.

### S14 NATS transport (abstraction check)

NATS.Net: subject rendering with an escaping rule, headers mapping, JetStream consumers with ack, request/reply, KV or last-per-subject state topics (A-4), native TTL where available (D-10), NATS accounts for tenancy (R-20). Goal is to find MQTT leaks: any change to the core interfaces is recorded as a decision and applied to MQTT. Compose profile `nats`.

### S15 SFC core library (parallel to S08 to S14)

Read: [SFC core](research/sfc-core.md), the S07 decisions, D-17, D-18.

The pure core (findings 1 to 11): compiled graph with build-time validation (including "a cycle must wait" and the propagation limit), versioned recipe and state payloads with checked-in schema and a wire-name test, step/transition/branch/qualifier semantics, `StartWork`/`CancelWork` commands, continuation, `Tick` and `Resume` events, deadlines and `max_attempts`, exhaustive unit tests and no I/O. The in-memory host with virtual clock and transcript, and the architecture test that keeps the project dependency-free. Port the Python `test_sfc_engine.py` and `test_sfc_recipe.py` scenarios (from `python-archive`) as simulations. Read-only `sfc_explain` and `sfc_dryrun` become possible once S11 exists.

## M2 Parity gate

### S16 Job models and state machine

Implement the S05 strategy: ISA-95 types, job state machine with persistence, Work Master extension (NodeSet v2 and generated models), the recipe-independent job order types, serialization with source generation. Port the Python state machine tests as the specification.

### S17 Machinery Jobs processor and job DAOs

Commands (store, start, abort, ...), job order and response DAOs with the S07 index design, station configuration delivery (`ConfigWorkMaster`), change streams for the publisher, inspectors and MCP tools from the sketch. Compose e2e over MQTT.

### S18 Job order publisher and state topics

`JobOrderPublisher` on the state-topic port (A-4): MQTT retained (state index, order, result, delete), NATS equivalent, cursors and sequence in Valkey, polling per S07, singleton behaviour and deployment modes (Q-ARCH-9).

### S19 SFC Valkey host

Read: [SFC core](research/sfc-core.md), the S07 decisions, D-9, D-18.

The thin host around the S15 core: work stream consumption through the poller, CAS script that stores state, due-index entry and outbox entries atomically, scope registry, push_command dispatch with the persisted deadline (D-9), due-index sweep, trimming rule. Inspectors and MCP tools from the sketch. Recipe execution for push actions in compose.

### S20 SFC pull events, recovery, multi-instance

Selector-based pull-event matching with persisted waits and build-time rejection of ambiguous recipes (R-1), `XAUTOCLAIM` recovery, the `Resume`-based recovery scan (finding 6), late-response absorption per the S07 rules. Tests with two replicas in compose: kill one mid-job; start a job through replica A, kill A, and check that replica B picks up its work and deadlines (scope registry); stop all pods longer than a deadline and check the outage grace rule. Measure sweep latency.

### S21 HTTP surface and observability

Q-OPS-1: liveness and readiness, metrics (OTLP and/or Prometheus), job state query API (R-19) as the stable REST adapter over the inspection services (D-19), using the `(scope, id)` identity if S07 chose it; authn/z for the HTTP endpoints (R-8); OpenTelemetry completeness (traces over all transports, metrics for the pipeline and the SFC engine).

### S22 Container, Kubernetes, supply chain, security review

Image pinned by digest, one multi-arch image for linux-x64 and linux-arm64 from the two native CI builds (D-32), probes using the HTTP surface, hardened manifest with egress NetworkPolicy, SBOM attestation, signing and verification in CI (R-9; the SBOM artifact exists since S03), review of scanner waivers (D-20), the open SonarQube Cloud findings on `deploy/k8s.yaml` (storage requests and limits, rules S6870/S6897, left for the manifest port in S03), `docs/security.md` update including the trust model (D-13), operational limits per transport (R-12), tenancy for the remaining transports (R-20), support model and zones/conduits (R-21), a review of the "Done in the session" list against the new code, and the unverified items that need a cluster (document what is still unverified).

### S23 Docs port, parity acceptance, release

Port `docs/` (concepts, operations with all configuration equivalents, persistence, sfc_engine, security, your-first-processor, development), run the parity scenarios (Q-TEST-2) and the acceptance checklist ([branching](research/branching.md) item 8), benchmark baseline (R-18), review the maintenance tool group (D-19: keep as dev-profile tool or remove, per tool), update the README, remove the WIP notice. Turn `docs/porting/` into an archived design record or remove it.

## M3 Extensions (after the parity gate)

### S24 AMQP transport

AMQPNetLite binding with the SDK AMQP mapping for CloudEvents, settlement semantics, reply-to, broker target decision. Own optional package, not `IsAotCompatible`, with the AMQPNetLite trim/AOT warnings (all in `Amqp.Serialization`) suppressed in that project only and the `Data` body only ([D-33](decisions.md#d-33)); an AOT-published integration check against an AMQP 1.0 broker (Artemis worked in S04).

### S25 Zenoh transport

First verify the S04 finding ([library spike](research/library-spike.md#zenoh-cs-050-and-zenoh-c-1100)): `Session.Get` against a queryable crashes with `free(): invalid pointer`; suspected stale `ZGetOptions` (no `cancellation_token`). Compare every struct the transport uses with the zenoh-c headers. Then record the binding choice (`Zenoh-CS` with an upstream fix, a fork, ZenohDotNet or an own binding), including how the zenoh-c version is pinned together with the package. Then the binding and the CloudEvents-over-Zenoh mapping (attachment or structured), query/reply for commands (D-10), no-redelivery behaviour (A-5), router versus peer mode, native library in the image for both platforms (D-32).

### S26 Packaging and release

Needs a released upstream CloudEvents package (D-14). NuGet package split, semver tooling, changelog, API reference generation (R-13, R-17), and a `dotnet new` template package (`MicroDCS.Templates`) replacing the Python `init` scaffolder (Dockerfile, Kubernetes manifest, `CLAUDE.md` and `.claude/` starter files, VS Code tasks and settings).
