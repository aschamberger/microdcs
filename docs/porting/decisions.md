# Decisions

One entry per decision. The summary table is in [the plan](index.md#decisions). Status: **decided**, **proposal** (direction agreed, details open for the named session), **revised** (decided; replaced an earlier version, which is summarised under "Revised").

Entry template:

```text
## D-n Title
Status: decided | proposal | revised (date)
Decision: what was decided.
Consequences: what follows from it.
Open: what a named session still has to settle (proposals only).
Background: link to research/ or docs.
```

## D-1 Big-bang rewrite {#d-1}

Status: decided. Answers Q-STRAT-1 and Q-STRAT-2.

Decision: rewrite in .NET in one go; the Python implementation is frozen after the port starts.

Consequences: no side-by-side operation; the interop test (Q-TEST-2) becomes optional.

## D-2 Clean break for wire format and key schema {#d-2}

Status: decided. Answers Q-ARCH-2 (compatibility part).

Decision: no compatibility with the Python wire format or Valkey key schema.

Consequences: free to redesign payload encoding, key schema and DAOs; no migration of existing data.

## D-3 Port the existing protocol abstraction {#d-3}

Status: decided. Answers Q-STRAT-3.

Decision: port `ProtocolHandler`/`ProtocolBinding`/`ProcessorBinding` instead of designing a new abstraction.

Consequences: holds for the processor-facing model. The transport-neutral pipeline lives inside `mqtt.py` and must be extracted (A-1).

Background: [abstraction mapping](research/abstraction.md).

## D-4 OPC UA for models only {#d-4}

Status: decided. Answers Q-ARCH-5.

Decision: no OPC UA server or transport.

Consequences: only the model and state-machine parts of `Opc.Ua.ISA95*` are needed; server hosting parts are irrelevant.

## D-5 Valkey compatibility test, no Python changes {#d-5}

Status: revised (the Python switch to Valkey was dropped). Answers Q-DATA-3. Done on 2026-11-07.

Decision: the Python code is archived as is (D-16). The existing Python integration tests ran once against a throwaway `valkey/valkey-bundle` container (nothing committed) to validate `JSON.*`, Lua, Streams, `FT.*` and ACLs before the .NET DAO design.

Consequences: one incompatibility found (the RESP3 reply shape of `FT.SEARCH`); it feeds Q-DATA-1.

Background: [Valkey findings](research/valkey-findings.md).

## D-6 `review.md` routing findings are redesigned in .NET {#d-6}

Status: decided.

Decision: the pull-event selector and push-command response routing are .NET design inputs, not fixed in Python.

Consequences: the Python code keeps its documented limitations.

Background: [review.md triage](research/review-triage.md).

## D-7 StackExchange.Redis is the Valkey client {#d-7}

Status: decided. Answers Q-DATA-2.

Decision: StackExchange.Redis; GLIDE is off the table.

Consequences:

* JSON documents and search go through raw `Execute` calls or are redesigned (Q-DATA-1).
* **No blocking commands.** The library excludes every `BLOCKING` command on purpose ("a command that parks the connection stalls every other caller sharing the multiplexer"); `StreamReadGroupAsync` has no block parameter. SFC work consumption and the publisher, which use `XREADGROUP ... BLOCK` and `XREAD ... BLOCK` in Python, must poll. Options: (a) plain polling (about 100 to 250 ms, reading in `count` batches); (b) polling plus a pub/sub nudge (the script that runs `XADD` also `PUBLISH`es; pub/sub can lose messages, so the poll stays as the fallback); (c) raw `Execute` with `BLOCK` on a dedicated multiplexer, parsing replies by hand. S04 measures latency and Valkey load for (a) and (b); S07 decides. One poller component can also run the D-18 sweep.
* Do not port the Python loop shape: it sleeps `stream_block_ms` after every iteration, even after a read that returned work, and calls `ensure_consumer_group` for every known scope on every iteration, so every hop in a chain of steps waits at least one interval.

## D-8 MicroDCS is not in production {#d-8}

Status: decided.

Decision: no backward compatibility, migration or deprecation concerns, in either direction.

Consequences: applies to wire formats, key schema, public API and config names. Python is only a behavioural reference.

## D-9 Push-command response routing stays instance-affine {#d-9}

Status: decided. Answers R-2.

Decision: the instance that sent the command waits for the response on its own reply address and hands control back to the DCS. The message TTL makes a late response stale anyway.

Consequences: the pending-command map and the per-instance reply address are kept. Only the crash case needs a fix: persist the dispatch deadline so another instance can re-dispatch after expiry (realised by D-18).

## D-10 Each protocol's native features {#d-10}

Status: decided. Answers A-4 to A-6.

Decision: no lowest-common-denominator emulation.

Consequences: bindings declare capabilities. Native request/response, expiry, ack, last value, shared consumption and CloudEvents bindings per transport. A capability a transport lacks is "not supported", not emulated.

## D-11 Zenoh is a full CloudEvents transport binding {#d-11}

Status: decided. Answers Q-ARCH-10.

Decision: same processor model as MQTT, NATS and AMQP.

Consequences: needs a CloudEvents-over-Zenoh mapping of our own (no spec binding). `docs/overall-design.md` "Southbound Connectivity" must be updated (S01). The library choice is separate (see the Zenoh row in the library table).

## D-12 HA is a deployment concern, later {#d-12}

Status: decided. Answers Q-DATA-4 and R-4.

Decision: code targets a single Valkey endpoint; no Sentinel or cluster work in the first iteration.

Consequences: every write path stays idempotent under unknown outcome, because StackExchange.Redis reconnects on its own (S09).

Revised 2026-11-08 (S07): Valkey **cluster mode is out of scope for good**, not only deferred. Resilience comes from islands: a plant is split into several independent deployments (separate Kubernetes instances, each with its own Valkey) per area, not one large deployment per site. Consequences: no hash tags, no `CROSSSLOT` concerns, no `(scope, id)` identity (D-42); replication or Sentinel failover needs no code changes. Cross-island aggregation is a northbound concern.

## D-13 Dedupe-key trust comes from the transport {#d-13}

Status: decided. Answers R-7.

Decision: the publisher identity is guaranteed by the transport (authn, ACLs), not by the framework.

Consequences: no extra publisher-authentication layer. The security docs state that dedupe is only as strong as the transport's access control.

## D-14 CloudEvents: upstream SDK, AOT-ready, fork late {#d-14}

Status: revised (replaces "thin fork now").

Decision: no own CloudEvents model from scratch and no fork now. Build everything AOT-ready and let upstream catch up; fork late if it has not.

* **Default:** depend on upstream `CloudNative.CloudEvents` from NuGet (2.9.0 for now, a pre-release or later version when the AOT change ships). Use only the projects needed (`.SystemTextJson`, probably `.Amqp`).
* **If PR #381 is merged but not released:** the release cadence is slow (2.8.0 in 2024-08, 2.9.0 in 2026-07), so use upstream `main` as a pinned git submodule (source dependency, no fork) until the release.
* **Fork only if blocked:** if the PR is stuck or rejected, or the AOT measurement (S02) shows a runtime failure that warning suppression cannot hide. The patch already exists (the PR branch), so forking late costs about an afternoon. Keep names, assembly names and public API identical to upstream so it stays a one-line switch back.
* **Seam:** the SDK is used only behind one internal boundary in `MicroDCS.Core` (the envelope codec). Designed in S06.
* **CI handling of the known SDK warnings:** AOT publish uses `TrimmerSingleWarn` and fails on every warning except the single assembly-level `IL2104`/`IL3053` of the `CloudNative.CloudEvents*` assemblies. If the runtime check in S02 fails, the "fork only if blocked" rule applies immediately.
* **Checkpoints:** check PR #381 and the release notes at the start of S02, S06, S08 and S26 and record the status in the log. S26 (NuGet release) needs a released upstream package without a source dependency; if a fork is in use by then, drop it first.
* **New bindings live in MicroDCS** (extension methods like the upstream ones): MQTT on MQTTnet 5 with binary mode and MQTT 5 properties, NATS (compare with the third-party package first), Zenoh. They can be offered upstream later.
* **Breaking core-model changes** (typed attributes, no `object Data`, `ReadOnlyMemory<byte>` data) stay out of scope until benchmarks justify them (R-18); then upstream first.

Consequences: no fork maintenance and no submodule in the first sessions; no publishing restriction unless a fork is needed at S26. The AOT smoke test needs the warning allowlist, and a runtime AOT failure may show up in S02, early enough to fork then.

Open: whether the 2.x line stays the only line after the AOT change (a major bump would change the referenced API).

Measured (S02, 2026-11-07): the AOT binary works at runtime. The allowlist is implemented in `scripts/aot-publish`; today only `CloudNative.CloudEvents.SystemTextJson` triggers it. Rule for the codec: construct `JsonEventFormatter` with `JsonSerializerOptions` whose `TypeInfoResolver` is source generated. The default constructor fails under `PublishAot`. Details in the research file.

Background: [CloudEvents SDK check](research/cloudevents.md).

## D-15 Docker Compose integration tests {#d-15}

Status: decided.

Decision: one compose-based setup that runs identically locally and in CI; the `tests` service runs inside compose by default.

Consequences: `MicroDCS.Testing` provides the in-memory transport, fake clock and processor harness; containers come from compose; Testcontainers stays optional for consumers (R-15).

Revised 2026-11-11 (S22a, maintainer's choice): CI runs `tests/MicroDCS.IntegrationTests` on the host in the `dotnet` job against the `compose.test.yaml` services (`valkey`, `valkey-acl`, `mosquitto`, `nats`), with `--fail-skips on` and coverage for SonarQube Cloud; the run inside the `tests` container is no longer repeated in CI (it was the same suite a second time, about 5 min). Services, healthchecks and test code stay one setup; the compose `tests` service stays the local all-in-containers path, and CI still builds the `tests` image for the replica tests (`integration` job). [CI runtime](research/ci-runtime.md).

Background: [integration test setup](research/test-setup.md).

## D-16 Python on `python-archive`, port on `main` {#d-16}

Status: revised (replaces the long-lived `dotnet` branch and swap).

Decision: the Python implementation is preserved on the `python-archive` branch (created 2026-11-07 at `12c1415`, no tag); the port continues directly on `main`.

Consequences: no swap procedure. `main` is not a working product until the parity gate (S23); acceptable because of D-8. The docs site keeps deploying from `main` as the live work-in-progress, including this plan. Security fixes for Python go through pull requests into `python-archive`.

Background: [porting approach](research/branching.md).

## D-17 SFC semantics as a pure core library {#d-17}

Status: decided (2026-11-08, S07; open points settled in [D-44](#d-44)).

Decision: `(graph, state, snapshot, event) -> (next state, continuation, commands)`, in a separate dependency-free project in this repository (not a separate product). Fixed already:

* commands are inert data applied in order (`StartWork`, `CancelWork`);
* job-level results are the continuation (`Complete`, `Defer`, `Fault`);
* faults are verdicts, policy is host-side.

Valkey, work stream, CAS and transports stay in a thin host.

Consequences: replaces "port `sfc_engine.py` 1:1". BPMN is not adopted now; the core sits behind `IRecipeInterpreter` so a BPMN interpreter can be added later. The core is built in S15, the host in S19.

Decided on 2026-11-07:

* **Late responses:** completion matches on the action key. A response from an earlier attempt completes the action if it is still open; any further response for a closed action is absorbed as a duplicate. Late responses for aborted or cancelled jobs are absorbed.
* **Transition conditions are outcome-based** (option 2 of the condition analysis). An action completion carries a named outcome; `SfcTransition.condition` names the outcome it reacts to, or `"true"`; `priority` orders the transitions of a selection, and the lowest-priority transition with `"true"` is the default path. A transition fires when its source step's actions are complete and the condition matches. Build-time validation: every selection divergence has a default and unique priorities; named outcomes must exist for the action types involved. This is a restricted form of IEC 61131-3 transition conditions (named Boolean transitions, priority-ordered selection), not a departure from them; arbitrary expressions are not supported. Python never evaluated `condition`, so existing recipes keep working where they use one path per step.

Open for S07: work identity details, schema direction and payload versioning, recipe acceptance capabilities, how a response maps to an outcome per `type_id` (CloudEvent type or one response field), whether exhausted retries report reserved outcomes `timeout` and `error` that a recipe may route (abort only if no transition matches), the R-1 selector on the same mechanism, and the review points (scope discovery, retry limit, stream trimming, `(scope, id)` identity).

Background: [SFC core library](research/sfc-core.md).

## D-18 SFC time model {#d-18}

Status: decided (2026-11-08, S07; open points settled in [D-44](#d-44)). Hash tags are not needed (D-12, D-42).

Decision: the core reads no clock. Events carry timestamps from Valkey server time (proposal: taken from the work item's stream entry id); the state holds absolute deadlines and exposes `NextDeadline`; the host keeps one due-index entry per job; all replicas sweep it without a leader.

Consequences: closes the "no working job-level timeout" gap of the Python engine (R-2).

Decided on 2026-11-07:

* **Outage grace:** a platform outage (all pods or Valkey down) must not abort running jobs. The host keeps a heartbeat written by the sweeps; when the gap since the last sweep exceeds a threshold, the tick is marked `afterOutage` and the core extends open deadlines by the outage duration instead of failing them.
* **Deadline source:** the action deadline comes from the recipe's `SfcActionAssociation.timeout_seconds`; the wire expiry of the command is derived from it (deadline minus a configurable grace).

Open for S07: sweep interval, outage threshold, hash-tag key naming for later cluster mode, `max_attempts` (recipe field with a host default; the default value), which due-index keys a pod sweeps (scope registry).

Background: [SFC core library, time model](research/sfc-core.md#time-model-and-the-multi-instance-model).

## D-19 Inspection layer plus optional dev MCP server {#d-19}

Status: decided (2026-11-08, S11, first slice built; maintenance group follows in S12).

Decision: in-process inspection services are the contract; REST and MCP are thin adapters on a separate management listener. MCP is an optional package, default off, read-only, loopback only, payloads redacted, with a separate write group. A separate maintenance group (cleanup, delete, repair) is MCP-only and never exposed over REST: dev-only project, environment guard, plan/apply, snapshot and restore.

Consequences: first slice in S11, maintenance tools in S12; every stateful component ships its inspector afterwards (definition of done). MCP calls the services, not the REST API.

Open: listener layout with Q-OPS-1 (S06), redaction policy (S06), snapshot retention and size defaults (S12, settled in [D-45](#d-45)). Configuration names are placeholders until Q-ARCH-3.

Confirmed 2026-11-08 (S11): built as decided, with these refinements. The contract types (`IInspector`, `Sensitivity`, `Redaction`) stay in `MicroDCS.Core` because the pipeline and its inspectors live there; `MicroDCS.Inspection` holds the query service (`InspectionService`: result cap, redaction, rate limit, audit) and the management listener, `MicroDCS.Mcp` the adapter. The listener has its own small web host with `IManagementModule` as the extension point, so the framework core does not depend on ASP.NET. Flags: `Management:Enabled` (default off, opens no port and keeps no buffer), `McpEnabled`, `WriteEnabled`, `IncludePayloads`; combinations without `Enabled` fail at start. The REST adapter is not built in S11 (the listener serves health and MCP); it comes with the HTTP surface in S21. MCP tools are static methods registered with the generic `WithTools<T>(JsonSerializerOptions)` overload; this works in the AOT binary (Kestrel plus MCP, no trim warnings). [Findings](research/inspection-mcp.md#s11-implementation-findings-2026-11-08).

Background: [inspection layer and MCP server](research/inspection-mcp.md).

Reviewed 2026-11-12 (S23): every maintenance tool is kept as a dev-profile tool, `maint_sfc_rebuild_due_index_plan` included as a manual trigger beside the recovery scan of D-53 ([parity acceptance](research/parity-acceptance.md#maintenance-tool-review-d-19)).

## D-20 Security and quality scanning in CI {#d-20}

Status: decided (containers).

Decision: Gitleaks, Trivy and SonarQube Cloud. Gitleaks and Trivy run as digest-pinned containers from one `compose.scan.yaml`, identical locally and in CI. All actions pinned by commit SHA, least-privilege permissions. Sonar via SonarScanner for .NET with coverage.

Consequences: motivated by the March 2026 compromise of the Trivy action and binaries. Gates: any secret, fixable HIGH/CRITICAL, the Sonar quality gate. Built in S03; the first full-history Gitleaks run there also covers the Python history (no separate scan before the freeze, because the project never used credentials).

Confirmed 2026-11-07 (S03): built as decided, no revision. Gitleaks and Trivy run through `scripts/scan` and `compose.scan.yaml` in `security.yml`; Sonar runs as SonarScanner for .NET in the `ci.yml` `dotnet` job with coverage from `Microsoft.Testing.Extensions.CodeCoverage` (VS coverage XML). Corrections to the background: Trivy does not check compose files or workflows, and the image scan sees only OS packages (the SBOM therefore comes in a source and an image part). Required checks: `gitleaks`, `trivy-fs`, `trivy-image` and the SonarQube Cloud app check `SonarCloud Code Analysis` (the CI job does not wait for the gate). [Findings](research/ci-scanning.md#implementation-findings-s03-2026-11-07).

Background: [CI security and quality scanning](research/ci-scanning.md).

## D-21 OPC UA .NET stack licence {#d-21}

Status: decided (2026-11-07).

Decision: UA-.NETStandard is MIT licensed since v2, so driver 1 adds no licence constraint. The earlier concern (a dual RCL/GPL-2.0 licence) applied to older versions.

Consequences: no fallback model generation is needed for licence reasons. S05 records the licence metadata of the exact `Opc.Ua.*` packages in use and the licence of the NodeSets it consumes, as a short check.

## D-22 One stack per milestone, one PR per session {#d-22}

Status: decided (2026-11-07).

Decision: each milestone (M0 to M3) is one GitHub stacked-PR chain, managed with the `gh stack` CLI extension; each session is one PR in it. PRs are squash-merged bottom-up as soon as the session is done and green.

Consequences:

* `main` gets one commit per session; the next session can start on top of an unmerged one without waiting.
* Branch protection, required checks and the scanners apply to every PR, because GitHub enforces the rules of the final target branch (`main`) on each PR in a stack. After a merge, the rest of the stack is rebased onto the new base automatically.
* Merging early keeps stacks short, so rebases stay cheap. A milestone is not merged as one block.
* Needs GitHub CLI 2.90.0 or later with the `gh-stack` extension. Stacked PRs were in public preview in mid-2026; check the status in S01. Fallback without the feature: plain PRs to `main`, one per session, same squash rule.
* GitHub milestones M0 to M3 group the PRs for the progress view.

Revised (2026-11-07, S02): no follow-up PR for the post-merge state. The log entry lists "Post-merge checks", and the next session's `/session-start` verifies them and records the result in its own entry.

Not chosen: one PR per milestone (reviews of 10k lines or more, CI feedback only at the end, a long-lived branch like the one D-16 rejected).

## D-23 Claude Code is the agent tool {#d-23}

Status: decided (2026-11-07).

Decision: the sessions run in Claude Code. The Copilot instruction setup is replaced, not kept in parallel.

Consequences (built in S01):

| Copilot | Claude Code |
|---|---|
| `.github/copilot-instructions.md` | `CLAUDE.md` at the root (plain Markdown, under about 200 lines) |
| `.github/instructions/*.instructions.md` with `applyTo:` | `.claude/rules/*.md` with `paths:` frontmatter, loaded only when a matching file is read or edited |
| `.github/prompts/*.prompt.md` | Skills `.claude/skills/<name>/SKILL.md` with `disable-model-invocation: true`, run as `/<name>` |
| `.vscode/mcp.json` | `.mcp.json` at the root (`mcpServers`), approved once per user |
| `.github/workflows/copilot-setup-steps.yml` | No repository file: a setup script in the claude.ai/code environment for cloud sessions (the .NET SDK is not preinstalled); project steps both locally and in the cloud (for example `dotnet restore`) as a `SessionStart` hook |
| none | `.claude/settings.json` for the shared permission allowlist and hooks; `CLAUDE.local.md` and `.claude/settings.local.json` stay personal and git-ignored |

`AGENTS.md` is only added if a second agent tool comes back; then `CLAUDE.md` imports it (`@AGENTS.md`). Optional, not decided: Claude Code PR review (managed Code Review or `anthropics/claude-code-action`) on the session PRs.

## D-24 MicroDCS is MIT licensed {#d-24}

Status: decided (2026-11-07).

Decision: the .NET implementation is published under the MIT licence. The archived Python implementation on `python-archive` stays under LGPL-2.1.

Context: LGPL-2.1 requires that users can relink an application with a modified library. Native AOT links the library into a single binary, so every application built on MicroDCS would have to deal with that. The .NET ecosystem and the dependencies (OPC UA stack MIT, D-21; CloudEvents, NATS.Net, AMQPNetLite Apache-2.0) are permissive. All commits so far are by the maintainer, so the change needs no other consent.

Consequences: S01 replaces `LICENSE`, the licence line in `README.md` and sets `PackageLicenseExpression` to `MIT` in `Directory.Build.props`. Contributions from then on are MIT.

## D-25 The processor authoring experience is a primary goal {#d-25}

Status: decided (2026-11-07). Answers the goal part of Q-STRAT-4.

Decision: writing a processor must feel polished; it is what MicroDCS is for. The bar is at least the Python experience (decorated handlers on a class, typed payloads, little wiring).

Consequences: S06 designs the authoring API first and derives the pipeline contracts from it, not the other way round. Expected shape, to be confirmed in S06: attributes plus source generators for handler registration (AOT-safe, no reflection), analyzers with clear diagnostics for wrong handler signatures, the in-memory test harness (R-15) as part of the experience, `dotnet new` templates (S26), and a "your first processor" page that is kept working (S23). Explicit registration in code stays possible underneath.

## D-26 `valkey-bundle` is the Valkey image {#d-26}

Status: decided (2026-11-07).

Decision: the compose stacks and the deployment use `valkey/valkey-bundle` (pinned by digest), which includes valkey-json and valkey-search.

Consequences: the modules are available, so S04 and S07 can choose freely how documents and indexes are stored (Q-DATA-1). Choosing plain-string documents for some data does not change the image.

## D-27 Fail-fast startup {#d-27}

Status: decided (2026-11-07, carried over from Python).

Decision: if a component cannot start (configuration invalid, processor initialisation fails, transport cannot connect within its startup budget), the process exits with an error instead of running degraded.

Consequences: no catch-all around host startup. Kubernetes restart policies and readiness probes handle recovery. Reconnects after a successful start are a transport concern (S13 and later), not covered by this rule.

Background: [Python design decisions](research/python-design-decisions.md).

## D-28 Explicit error on queue backpressure {#d-28}

Status: decided (2026-11-07, carried over from Python).

Decision: when a bounded internal queue is full, enqueueing fails with an explicit error (exception or failed result) instead of blocking silently.

Consequences: overload becomes visible to the caller and in logs and metrics. S06 defines the error type and where it surfaces in the pipeline; bounded `System.Threading.Channels` with `BoundedChannelFullMode.Wait` are not used for these queues without a timeout that turns into the same error.

Revised 2026-11-08 (M1 review, RV-7): an event that goes to several bindings is enqueued **all or nothing**. The capacity of every target queue is checked under one lock before anything is added; when one is full, none gets the event and `OutboundQueueFullException` names that binding. Without this a retry after the error duplicated the event under a new id on the healthy bindings.

Background: [Python design decisions](research/python-design-decisions.md).

## D-29 No extra size limit on `custommetadata` {#d-29}

Status: decided (2026-11-07, carried over from Python, R-12).

Decision: the `custommetadata` extension attribute gets no size check of its own. The transport limits bound it (MQTT maximum packet size, NATS `max_payload`, AMQP maximum message size, Kestrel request limits).

Consequences: S22 documents the per-transport limits (R-12). Reviews do not flag the missing check.

Background: [Python design decisions](research/python-design-decisions.md).

## D-30 Comma-separated list values in configuration {#d-30}

Status: decided (2026-11-07, carried over from Python). Applies only if the configuration format keeps list values in single strings (Q-ARCH-3, S06).

Decision: list values in a single configuration string (for example environment variables) are split on commas without escaping.

Consequences: safe because the values are topic names, subjects or `name:value` pairs where a comma is not valid (MQTT topics cannot contain commas). If S06 moves to `IConfiguration` arrays (`APP__X__0`, `APP__X__1`), this decision is obsolete and is marked so.

Background: [Python design decisions](research/python-design-decisions.md).

## D-31 .NET conventions {#d-31}

Status: decided (2026-11-07, S01 defaults).

Decision:

* Root namespace and package prefix `MicroDCS` (`MicroDCS.Core`, `MicroDCS.Mqtt`, ...).
* Tests: xUnit v3 on `Microsoft.Testing.Platform` (package `xunit.v3.mtp-v2`, runner set in `global.json`), Shouldly for assertions, NSubstitute for test doubles (with its analyzers). No reflection-heavy mocking in AOT paths; test projects are not AOT-published.
* Logging: `Microsoft.Extensions.Logging` with the `LoggerMessage` source generator; no string interpolation in log calls.
* Dependency injection: `Microsoft.Extensions.DependencyInjection`, explicit registrations; no assembly scanning or runtime-reflection registration in AOT paths.
* Style: file-scoped namespaces, `required` members and records where useful, `TimeProvider` for time, `CancellationToken` on every async API, `ConfigureAwait(false)` in library code. Formatting and analyzer rules are in `.editorconfig`; warnings are errors.

Consequences: `Directory.Build.props` and `.editorconfig` enforce the build-level parts; `CLAUDE.md` and `.claude/rules/` carry the rest.

## D-32 Platforms: linux-x64 and linux-arm64, built natively {#d-32}

Status: decided (2026-11-07, S02 follow-up).

Context: Native AOT links for the build machine's architecture. On an x64 host the arm64 link fails without an arm64 cross toolchain and sysroot (seen in S02). All base images (`sdk:10.0-noble-aot`, `runtime-deps:10.0-noble-chiseled`, `valkey-bundle`, `eclipse-mosquitto`) are multi-arch.

Decision: MicroDCS supports `linux-x64` and `linux-arm64`. CI builds and smoke-tests the AOT image natively on both: the `container` job is a matrix on `ubuntu-24.04` and `ubuntu-24.04-arm` (free for public repositories). No QEMU emulation (slow AOT compile) and no cross-compilation toolchain in the Dockerfile.

Consequences:

* `src/` projects restore both RIDs (`Directory.Build.props`), so lock files stay valid; the Dockerfile maps `TARGETARCH` to the RID.
* Required checks are `container (linux-x64)` and `container (linux-arm64)`; a new matrix entry changes the protection.
* Native dependencies must exist for both: zenoh-c (S04 checks the arm64 build, S25 ships it), anything else that brings a native library.
* S22 publishes one multi-arch image (manifest list from the two native builds, `docker buildx imagetools create`), signed and attested per platform.
* Local arm64 builds on an x64 machine need QEMU or are skipped; CI is the reference.
* Unit and integration tests run on x64 only until a platform-specific code path appears; then add the arm64 runner to that job.

Not chosen: x64 only (excludes arm64 edge devices and Graviton/Ampere hosts); QEMU in one job (slow, and emulation hides nothing the native runner would not show); cross-compilation (sysroot maintenance in the Dockerfile).

Revised 2026-11-11 (S22a, maintainer's choice): `container (linux-arm64)` builds on pull requests only when the image inputs change (Dockerfile, SDK, packages, `ci.yml`, the publish scripts; D-56), and always on pushes to `main`, tags and the daily schedule. The check stays required; on other PRs its steps are skipped. A platform-specific break from a code change shows on `main` before any image is published, because `publish` needs both container builds.

## D-33 Library stack: AOT result per library, AMQP via AMQPNetLite.Core, Zenoh open {#d-33}

Status: decided (2026-11-07, S04), AMQP part revised 2026-11-07 (see the "Revised" line). Answers Q-ARCH-1. Evidence in [library spike](research/library-spike.md).

Context: the plan assumed one AOT-published application with all chosen libraries. S04 published each library in its own Native AOT project, then together, with `IL2026`/`IL3050` as errors.

Decision:

* **Baseline stack:** StackExchange.Redis 3.4.x, MQTTnet 5.2.x, NATS.Net 3.3.x, Kestrel minimal APIs and `ModelContextProtocol.AspNetCore` 2.4.x publish without trim or AOT warnings and work at runtime in the AOT binary; all together 24.3 MB (linux-x64). They go into the product as planned.
* **AMQP on `AMQPNetLite.Core`, no isolation needed.** The S04 spike first referenced the `AMQPNetLite` metapackage, which includes the object serializer `Amqp.Serialization` (13 warnings: 6 `IL2070`, 7 `IL3050`). `AMQPNetLite.Core` 2.5.5 alone publishes with no IL warnings and sends and receives at runtime (Artemis). The transport depends on `AMQPNetLite.Core` only and uses the `Data` body and message properties, never `AmqpSerializer`.
* **Zenoh library: no choice yet.** Zenoh-CS 0.5.0 publishes clean, but that proves little (the assembly is not annotated), and publish/subscribe with attachments works, while `Session.Get` against a queryable crashes the process with an invalid free. Suspected cause: `ZGetOptions` lacks the trailing `cancellation_token` field of zenoh-c 1.10.0 (not verified). The choice among a fix upstream, a fork, ZenohDotNet or an own binding is made before S25; S25's first task is to verify the cause and the struct layouts used by the transport against the zenoh-c headers. The Zenoh transport stays in M3, so the parity gate is not affected.
* **Valkey streams:** consumers poll; the preferred shape for S07 is a pub/sub nudge from the script that runs `XADD` plus a slow fallback poll (D-7 option b). Plain polling costs about half the interval in median latency per hop.
* Q-DATA-1 is not decided here; the measurements (no performance reason to drop valkey-json and valkey-search) are in the research file for S07.

Consequences: S24 references `AMQPNetLite.Core`, not the metapackage, and checks with an AOT publish that no `Amqp.Serialization` assembly is pulled in; S25 starts with the Zenoh verification; S22 sizes the image with a native `libzenohc.so` per architecture only when S25 lands; S07 decides Q-DATA-1 and the stream consumption shape with these numbers.

Not chosen: dropping AMQP (the transport list in the plan stays); suppressing the serializer warnings (unnecessary with `AMQPNetLite.Core`); writing an AMQP codec of our own (cost without need).

Revised 2026-11-07 (S04 follow-up): the first version isolated AMQP in a non-AOT-compatible optional package because of warnings that came from the metapackage's serializer, not from the protocol library. A question about `RabbitMQ.AMQP.Client` (which depends on `AMQPNetLite.Core`) led to the check. Whether the transport is its own NuGet package is now only a dependency question for S26. `RabbitMQ.AMQP.Client` stays an option for S24 if a RabbitMQ-specific transport is wanted (RabbitMQ 4 topology and confirms); it is not tested yet.

## D-34 Job models: generated ISA-95 V1 and V2 types, version-neutral model, NodeSet2 extension, own pure state machine {#d-34}

Status: decided (2026-11-07, S05; direction set by the maintainer: clean architecture over a fast win). Answers Q-ARCH-2 (job models) and Q-ARCH-4. Evidence in [OPC UA spike](research/opcua-spike.md).

Context: D-4 allows OPC UA for models only. S05 tested the 2.0.1 packages as plain models without a server in Native AOT projects, the NodeSet2 path for an own model extension, Job Control V1, and the state logic of the stack.

Decision:

* **Types:** the generated `Opc.Ua.ISA95` (Common Model), `Opc.Ua.ISA95.JobControl.V1` and `.V2` types from `OPCFoundation.NetStandard.Opc.Ua.ISA95`. The server and client packages are not referenced. Both Job Control versions are supported.
* **Version-neutral model (the architecture to adopt):** the application never handles generated types in its logic. A small set of neutral records (job order, job response, work master, parameter, property, material, resource requirement), a canonical state enum and an operation enum sit in the middle; conversions to and from V1 and V2 sit at the boundary. Property to test: a same-version round trip is lossless (all standard fields, nested parameters and properties, engineering units, V2 `EncodingMask` bits); a cross-version view carries only the fields the target version can represent. The stack's `Opc.Ua.ISA95.Server` follows exactly this design, but all of it is `internal`. We write our own, using the upstream code as the reference (copying with the MIT notice kept is allowed, record the origin in the file header), and ask upstream whether the neutral model and conversions can be published as a package of their own (maintainer action, tracked in the log). V1 and V2 differ: V1 has a flat state enum with `Waiting/Ready/Loaded/Running/Held/Suspended/Completed/Aborted/Closed/Error`, commands `Store/StoreAndStart/Start/Update/Stop/Cancel/Clear`, string IDs `ID`, no optional-field masks; V2 has hierarchical state arrays (`ISA95StateDataType`), `Pause/Resume/Abort/RevokeStart`, `JobOrderID`, localized text and optional-field masks.
* **Wire format for job models:** OPC UA JSON, **verbose**, written with `JsonEncoder` and read with `JsonDecoder` (AOT clean, `Utf8JsonWriter` based) behind a thin MicroDCS codec that hides three traps: encode the root with `value.Encode(encoder)`, set `EncodingMask` bits on hand-built V2 objects, and treat the decoder as lenient (unknown fields ignored, nothing required), so required-field validation is ours or schema based. System.Text.Json source generation is not used on the generated types.
* **Schemas:** `Opc.Ua.Core.Schema` generates JSON Schema at runtime for generated and own types; it can replace `dataclassgen` for the job models.
* **Work Master extension as a NodeSet2 model that extends the standard one:** an own information model `urn:microdcs:jobcontrol` (`MicroDcs.JobControl.NodeSet2.xml`), requiring Job Control V2, with `ISA95WorkMasterExtDataType` as a subtype of `ISA95WorkMasterDataType` (`DataSchema`, `Data`). The SDK source generator (`OPCFoundation.NetStandard.Opc.Ua.SourceGeneration`, NodeSet2 as `AdditionalFiles`) generates the class, activator and `AddMicroDcsJobControl()` registration at build time; AOT clean. The NodeSet is publishable and can be offered upstream later. **A new CloudEvent type identifies the extended Work Master** (for example `...config.workmaster.ext.v1`, next to the standard one; names fixed in S06/S16), so the receiver decodes the subtype directly and the missing type discriminator in the JSON does not matter. Job orders carry Work Master IDs only; a subtype embedded in a job order would decode as the base type (verified), which is not used.
* **State machine: transition table generated from the NodeSet, own pure transition function.** The rules are data in the OPC Foundation NodeSet2 of Job Control V2 (`ISA95-JOBCONTROL`, `UA-Nodeset`, MIT, version 2.0.0): the receiver machine has 13 transitions with their causes (methods) and effects (`ISA95JobOrderStatusEventType`), plus the sub-machines Interrupted (Held and Suspended), Ended (Completed to Closed) and Prepare (Waiting, Ready, Loaded). The Python implementation generated its table from that file (`dataclassgen`) and added by hand only what the NodeSet does not hold: Store and StoreAndStart from an initial state, Cancel, Clear, and the initial sub-states. We do the same: a small tool reads the vendored NodeSet2 and writes a C# table (checked in, with a drift test that regenerates and compares in CI); the few method-driven additions are a separate hand-written supplement with a reference to the spec text. A pure function `(canonical state, operation) -> new state or rejection` uses the table: no I/O, no locks, no store, so the Valkey compare-and-set (D-17) can call it. Not used: the stack's engine (rules embedded in its 1.3k-line lock-based `InMemoryIsa95JobControlProvider`, no Prepare machine, nothing persisted, `internal`), its `Isa95V2StateMachine` (only the state-array codec), and the generated `*State` classes (the transitions exist there only as address-space node factories, not as data). Upstream tests and the upstream rules serve as cross-check vectors. **V1 is not a restriction of the V2 table.** Its NodeSet has no state machine (state and command enums and three methods only), and the stack's guide lists V1 behaviour that differs: `Stop` removes the stored order, `RequestJobResponse` needs exactly one selector (job order ID or state), and `Loaded` and `Error` exist as response states. V1 rules are written from the V1 specification text with their own tests, on the same canonical states. **Execution-system transitions** are part of the model besides the method-driven operations: BeginExecution (AllowedToStart to Running), Hold (Running to Interrupted, sub-state Held; Pause lands in Suspended), Complete (to Ended, sub-state Completed) and Close (Completed to Closed). The table gives the transition, the operation chooses the sub-state. Cancel and Clear are removals, not transitions, and produce a catalog change instead of a status event.
* **Operation results use the Annex B `ReturnStatus` bitmap** (UInt64, several bits at once): Success bit 0, UnknownJobOrderId 1, InvalidCommand 2 (V1), InvalidStatus 3, UnableToAccept 4, InvalidRequest 32. A rejected transition is a business result with these bits, not an exception. Our CloudEvent responses carry the bitmap; the Python `MethodReturnStatus` lacks bit 2.
* **Vendoring discipline for the NodeSets:** the NodeSet2 files used by the table generator and the extension are vendored with their upstream commit, version, publication date and node count, pinned by a test (the stack does the same: Common 390, V1 91, V2 258 nodes). The V2 file is taken from the stack repository design folder, because the generated types come from it; it is identical to the `UA-Nodeset` file apart from whitespace in the header comment.
* **Dependency weight accepted:** the ISA95 package pulls the full `Opc.Ua.Core` stack; AOT trimming leaves 9 MB for the round trip, 12 MB with schema generation or the generated extension.

Consequences: S16 builds the neutral model, conversions, codec, state machine and the NodeSet2 extension project on these packages; the `Directory.Packages.props` entries (ISA95, Core.Schema, SourceGeneration 2.0.1) arrive with S16. Job order payloads are OPC UA JSON verbose (S06 records this in the envelope contract). Open for S16: V1/V2 field mapping where the models diverge (V1 single-string description, `Loaded` and `Error` states), the persisted job document format (S07), and the wire names against `schemas/machinery_jobs.schema.json` (D-2 allows a break).

Revised 2026-11-08 (S16, maintainer): the V1 state mapping left open here follows the upstream provider: V1 `Waiting` is NotAllowedToStart, `Ready` is AllowedToStart, `Loaded` and `Error` are V1-only and show as Running and Aborted in a V2 view. The V1 specification gives no mapping and its text for Ready differs; details in [job models](research/job-models.md). Prepare sub-states exist for V2 only. CloudEvent types: `com.github.aschamberger.ISA95-JOBCONTROL_V2.config.workmaster.v1` and `.config.workmaster.ext.v1`.

Not chosen: System.Text.Json on the generated types (unusable output); hand-written POCOs mirroring the spec (loses spec-conformant types and schemas); a hand-coded transition table (the spec data is machine readable and the Python port already generated it); using the server package's engine (internal, entangled with locks and subscriptions, no persistence); a `[DataType]` attribute class for the Work Master extension (works, but not publishable as an OPC UA model); V2 only (V1 is cheap with the neutral model).

## D-35 Processor authoring API: attributes, a source generator, explicit registration underneath {#d-35}

Status: decided (2026-11-08, S06; checkpoint with the maintainer on the example processors). Answers Q-STRAT-4 and refines D-25.

Decision: a processor is a `partial` class marked `[Processor(binding, Id = ...)]` with constructor injection and no base class. Handlers are methods: `[Incoming]` (payload type is the first parameter), `[Outgoing(typeof(T))]` (type-id keyed payload factory, P-13), `[OnResponse]`, `[OnExpired]`, `[Fallback]`, and lifecycle methods `[OnInitialize]`, `[OnStarted(Singleton = true)]`, `[OnShutdown]`. Payloads are records with `[CloudEventType(id, Schema, Codec)]`; properties marked `[CarriedAs(name)]` travel as CloudEvent attributes and are restored on the response (E-4). Parameters are bound at build time: the payload, `EventContext`, `[Scope]`, `[SubjectPart(i)]`, `CancellationToken`, services. Return values: a payload, enumerables, `Replies`, `Task`/`ValueTask` of those, `void`, `null`. A source generator (S08) emits the `ProcessorDescriptor` and the payload descriptors; building the descriptor by hand stays possible. Analyzers report wrong signatures with `MDCS0xx` diagnostics (class not partial, no payload parameter, duplicate type id, unsupported parameter or return type, payload without `[CloudEventType]`).

* `[Incoming]` and `[Outgoing]` name the direction; the message intent (data, event, command, meta) is a separate dimension. `[Command]` was rejected because incoming handlers also receive commands.
* `[Outgoing]` stays because the SFC dispatches push commands by type id and free-form parameters (`sfc_engine.py`), without a compile-time payload type. The factory receives a `CommandRequest` (scope, job, action key, command id, correlation id, parameters) and returns the payload; `IOutgoingCommands.DispatchAsync(typeId, request)` replaces the engine's processor and payload-class lookup. Application-initiated events use `IEventPublisher`. `trigger_outgoing_event` is dropped.
* The Python pre/post callback hooks become `IProcessorFilter`, registered explicitly. The SFC hooks (`register_*_handler`) become `IActionSink`. Python's `response()`/`takeover` is replaced by constructors/`with` and the pipeline's copy of carried attributes.
* The test harness (`MicroDCS.Testing`, S08) drives a processor without broker or Valkey.

Consequences: stubs in `MicroDCS.Core/Authoring`; the generator and analyzers are S08. Python parity rows P-1 to P-19 in [core contracts](research/core-contracts.md). Open for S08: the exact `MDCS` diagnostic list and the generator's incremental design.

Revised 2026-11-08 (S08): (1) A Roslyn generator cannot read another generator's output, so it cannot add the `[JsonSerializable]` entry to a `JsonSerializerContext`. The application declares one `JsonSerializerContext` for its payloads; the generator uses its `.Default.<Type>` property (MDCS009 reports a payload that is missing). (2) `[CarriedAs]` needs `[JsonIgnore]` on the same property, otherwise the value also lands in the JSON body (found by a test); MDCS007 enforces it. Names are lowercase letters and digits, at most 20 characters (CloudEvents extension attribute rule), so `x-hidden-str` became `hiddenstr` (allowed by D-2). (3) Registration without reflection: the generator implements `IDescribedProcessor` (static abstract `Describe()`) on the processor, so `AddProcessor<T>()` needs no scanning. Handlers may be `static`. (4) Only the `Json` codec gets a generated descriptor; `Binary` and `OpcUaJson` payloads use handwritten descriptors (MDCS010). (5) A `[Fallback]` handler may return a prebuilt `EventEnvelope` (the raw echo, P-10). (6) The diagnostics are reported by the generator itself (MDCS001 to MDCS011, list in [Processors in .NET](../processors-dotnet.md)), not by a separate analyzer.

Revised 2026-11-09 (S19): `[Outgoing(typeof(T), Outcomes = [...])]` declares the outcomes of a command type, and an `[OnResponse]` handler may return `ActionOutcome` (alone or in `Replies`), which is taken out of the replies and reported to `IActionSink`. A response handler that returns none reports `done`; a `[Fallback]` handler reports nothing, so a response of an unknown type does not complete an action.

Revised 2026-11-08 (S17, D-50): `OpcUaJson` payloads get a generated descriptor when the type is a generated OPC UA structure with a public constructor; MDCS010 stays for other types and for `Binary`. Point (4) above is superseded.

## D-36 Envelope and payload codec seam {#d-36}

Status: decided (2026-11-08, S06). Applies D-14, D-34.

Decision: `EventEnvelope` is a plain record (typed attributes, `ReadOnlyMemory<byte>` data, string extensions, `EnvelopeError`). The CloudEvents SDK is used only behind `IEnvelopeCodec` (carrier of string attributes + content type + data, binary content mode) and later structured mode; each transport builds its carrier. The codec owns attribute names, parsing, the collision guard (extensions named like standard attributes are dropped, E-3) and the `mdcserrorcontext` text form (`k=v,k=v`, D-30). Payloads are described by `IPayloadDescriptor` (type id, schema, content type, decode, encode with carried attributes). Codecs: `Json` (System.Text.Json with generated `JsonTypeInfo`), `OpcUaJson` verbose for job models (D-34), `Binary`. MessagePack is gone. Event ids are free strings (no UUID requirement, A-6). Decode errors name fields, never values (`PayloadException`).

Consequences: `IEnvelopeCodec` and `IPayloadDescriptor` in `MicroDCS.Core/Messaging`; implementations in S08 (codec on upstream CloudEvents) and S16 (OPC UA codec). Wire attribute names may differ from Python (D-2).

Revised 2026-11-08 (S08): `IPayloadDescriptor` gained `CarriedNames`, so the pipeline can copy a request's carried attributes to a response whose type declares the same name (E-5). `CloudEventsEnvelopeCodec` uses the SDK for the standard attributes (id, source, type, time, subject, data content type, data schema) and handles the MicroDCS attributes as string extensions; `expiryinterval` is whole seconds.

## D-37 Pipeline, transport contract, delivery semantics and error model {#d-37}

Status: decided (2026-11-08, S06). Answers A-1, A-2, A-5, A-6, Q-ARCH-7.

Decision:

* **One pipeline** (A-1) owns: claim, expiry-timer cancel, scope check, dispatch, reply publishing, expiry timers, mark done, ack outcome, the single `ActivitySource` (W3C context from `traceparent`/`tracestate`), and the inspection ring buffer. Order as Python (T-1): claim (waiting for a lease holder), cancel expiry timer, scope check, dispatch, publish replies, `mark_done`, then ack.
* **A transport** (`ITransport`) supplies: connection and reconnect, subscriptions from `IAddressRenderer`, decoding to `InboundMessage` (binding id, request or response kind, envelope, parsed address, `ReplyAddress`, redelivered flag), `PublishAsync(OutboundMessage)`, and a declared `DeliveryProfile`. It calls `IInboundHandler.HandleAsync` and acts on the `DeliveryOutcome`. Metadata is typed (`ReplyAddress`, `Retain`, parsed address); no `dict[str, Any]` and no transport name reaches processors (A-2).
* **Dispatch by subscription:** a subscription belongs to one binding, the transport reports it, the pipeline calls that binding's processor only (P-16).
* **Delivery (Q-ARCH-7):** the dedupe claim (`IDedupeStore`) applies when the profile is `AtLeastOnce` (MQTT QoS 1, JetStream, AMQP); not for `AtMostOnce` (Zenoh, NATS core, MQTT QoS 0). Commands over an at-most-once path rely on SFC re-dispatch with the stable `mdcsactionkey` (D-9) and equipment that deduplicates on it. Duplicates are acknowledged.
* **Request/response (A-6):** the model carries `correlationid`, `causationid`, `mdcsactionkey`; the reply address is a transport-native `ReplyAddress`, per instance (D-9). The local expiry timer (key: causation id or id) lives in the pipeline, is cancelled by the response, and calls `[OnExpired]`.
* **Error model:** `DeliveryOutcome.Ack` (processed or duplicate), `Drop` (permanent: undecodable, unknown type without fallback, scope rejected; acknowledged so it is not redelivered; logged with ids and sizes, counted, visible in inspection; a request with a reply address gets an error event with `mdcserrorkind`), `Fault` (the handler threw: no `mark_done`, no ack; the lease lapses and the message is redelivered). A fault stops the host unless the transport can nack and redeliver (D-27; MQTT cannot redeliver in-session, so the process ends and the session redelivers on reconnect).
* **Publishing:** bounded queue per binding (size from configuration, capped by the maximum); a full queue throws `OutboundQueueFullException` (D-28). Publishing waits while the transport reconnects (until shutdown); readiness is false meanwhile.
* **Shutdown contract:** (1) `StopReceivingAsync` on every transport, (2) in-flight messages finish (not cancelled), (3) outgoing queues drain, (4) transports stop, (5) expiry timers are awaited until the grace period ends, then cancelled, (6) processors' `[OnShutdown]`, (7) shared infrastructure closes. The grace period is the host shutdown timeout.

Consequences: contracts in `MicroDCS.Core/Transport`. S08 builds the pipeline against an in-memory transport; S10 completes it with the Valkey dedupe store; S13 and S14 implement `ITransport`. Open: dedupe lease versus slowest handler (configuration, validated at start).

Revised 2026-11-08 (S08): `MinimalPipeline` implements the order claim, scope check, decode, filters and handler, replies, `mark_done` with the in-memory dedupe store; outgoing queues, expiry timers, the activity source and inspection are S10. Replies go to the request's `ReplyTo`; without one they go to the request's address with the `events` intent (revisit in S10 with the publisher design). A handler exception returns `Fault` and is logged; stopping the host on a fault (D-27) is S10.

Revised 2026-11-08 (S10): `MessagePipeline` replaces `MinimalPipeline` and also implements `IEventPublisher`. Order of steps: claim, expiry-timer cancel, scope check and dispatch, replies, `mark done` (a dropped message is claimed, answered and marked done, as in Python). `DeliveryProfile.CanRedeliver` (default false) decides whether a fault stops the host; `ITransport.GetReplyAddress(bindingId, instanceId)` supplies the per-instance reply address, set on commands only. Dropped requests with a reply address get an error event of type `io.microdcs.error.v1` (`EnvelopeError`, fixed text, no payload). The expiry timer key is the command id, the timer starts before the publish, and the events the `[OnExpired]` handler returns are published (Python discarded them); during shutdown they are not, because the queues are closed before the timers end. `PublishOptions.BindingId` selects one binding; without it an event goes to every binding that declares the intent and whose type filter accepts it. Reply without a reply address keeps going to the `events` address of the request. The activity source and the meter are named `MicroDCS`; security rejections log in the category `MicroDCS.Security`.

Revised 2026-11-14 (S23a): a fourth outcome `DeliveryOutcome.Retry` for a failing dedupe store (claim or `mark done`; a Valkey outage). Before, the exception escaped the pipeline: MQTT left the message unacknowledged while the host kept running, so it waited for the next reconnect. Now the pipeline gives the claim back, records the decision as `Unavailable` (apart from handler faults) and stops the host when the transport cannot redeliver, as for a fault (D-27). NATS keeps a `Retry` message in its worker and runs it again every `NakDelay` with "in progress" acknowledgements, so no delivery is used and an outage does not turn messages into poison (a nak would count against the consumer's `MaxDeliver` on the server, which the compose test showed); MQTT treats it like `Fault`. A handler that fails on Valkey stays a `Fault`.

## D-38 Address model, scope rule, state-topic port {#d-38}

Status: decided (2026-11-08, S06). Answers A-3, A-4, A-7, R-6.

Decision:

* **Logical address:** `Prefix`, `ScopePath` (segments), optional `Discriminator`, `Intent`. One renderer per transport (MQTT and Zenoh `/`, NATS `.`, AMQP broker address). Subscriptions are rendered per scope depth from 0 to `ScopeLevels` as in Python, with the shared group rendered natively.
* **Segment rule for all transports:** a segment is non-empty and uses only `A-Z a-z 0-9 _ -`. This excludes every separator and wildcard of the four protocols, so no escaping exists. The scope path comes from the scope part of the subject (`scope[/resource]`, scope dotted); the resource part never forms address levels (T-24). An invalid scope segment makes the message unpublishable or, on receipt, rejected.
* **Scope check (A-7):** `ScopeCheck.Enforce` (default) compares the subject's scope path with the address scope path; mismatch is a drop. **Level-0 rule (R-6):** a subject on an address without scope path is rejected by default (`Level0Subjects.Reject`); a binding can declare `Accept` and then relies on transport ACLs, documented per transport (S22). This is stricter than Python, which accepted (D-8 allows it).
* **State-topic port (A-4):** `IStateTopicPort.PublishAsync/DeleteAsync` implemented natively (MQTT retained with expiry and zero-byte delete, NATS KV or last-per-subject stream, Zenoh storage). AMQP reports `StateTopics = false`. The job order publisher (S18) depends on the port.

Consequences: `LogicalAddress`, `AddressPattern`, `IAddressRenderer`, `ScopePolicy`, `IStateTopicPort` in `MicroDCS.Core/Transport`, with unit tests for the pure rules. Open for S13: the exact MQTT renderer including `$share/`.

## D-39 Hosting and configuration {#d-39}

Status: decided (2026-11-08, S06). Answers Q-ARCH-3.

Decision: `Microsoft.Extensions.Hosting` (Generic Host) replaces `MicroDCS`, `SystemEventTaskGroup` and `AdditionalTask`; the host shutdown timeout is the grace period. Registration is explicit: `AddMicroDcs()`, `AddProcessor<T>()`, `AddMqttTransport()` and so on, with the generated descriptors. Configuration binds the `MicroDcs` section with source-generated binding (AOT); environment variables use the prefix `APP_` and `__` as section separator (`APP_MicroDcs__Processing__...`), a deliberate break from `APP_SECTION_FIELD` (D-2). Lists stay comma separated strings (D-30). Bindings are `MicroDcs:Bindings:{id}` sections (prefix, scope levels, discriminator, reply base, shared group, queue size, type filter, scope policy); a processor names its id in `[Processor(Id = ...)]`. Instance roles are flags (Processor, Publisher; default both); the instance id is `POD_ID` or generated. Options are validated at start and every error ends the process (D-27): missing prefix, duplicate prefix + discriminator, unknown transport, lease shorter than handler budget, missing files. Hosted services are the lifecycle: transports, publishers and the expiry tracker are `BackgroundService` or `IHostedService`; the `isinstance(MQTTPublisher)` role check disappears. Payload logging has no switch (CLAUDE.md).

Consequences: `MicroDcsOptions`, `BindingOptions`, `ManagementOptions`, `InstanceRoles` in `MicroDCS.Core/Hosting`; the host wiring is S08. Q-ARCH-9 (deployment split) stays with S18.

Revised 2026-11-08 (S08): `ProcessorDescriptor` carries `ReplyPayloads` (descriptors of the payload types handlers return). Options are validated by `MicroDcsOptionsValidator` when the hosted service starts (`IOptions.Value` throws), not with `ValidateOnStart`. The singleton start hook goes through `IStartLease` (always granted in memory; the Valkey layer replaces it).

## D-40 Inspection contracts and management listener {#d-40}

Status: decided (2026-11-08, S06). Details for D-19 (decided in S11). Answers the S06 part of Q-OPS-1.

Decision: `IInspector` (name, `InspectAsync(InspectionQuery)` returning a capped page with cursor) is the contract; entries are `Kind`, `Key` and fields whose values carry a `Sensitivity` (`Public`, `Identifier`, `Payload`) set at the source. `Redaction.Apply` drops `Payload` values unless `include_payloads` is on, and adapters call it before output. Inspectors use fixed-size buffers and cost nothing when inspection is off. **One management listener** (Kestrel), loopback `127.0.0.1:8081` by default, serving `/health/live`, `/health/ready`, `/metrics`, the versioned REST API under `/api/v1` and, when the optional package is referenced and enabled, MCP under `/mcp`. Non-loopback binds need bearer authentication (R-8); Kubernetes uses the pod address with a NetworkPolicy and probes on the same port. Read, write and maintenance tool groups, plan/apply and snapshot design are as in D-19.

Consequences: `MicroDCS.Core/Inspection` holds the abstractions; S11 builds the adapters and the first inspectors. Open: probes on a separate port if a deployment needs them; S21 refines the REST contract.

Revised 2026-11-10 (S21): **no `/metrics` endpoint**; metrics leave by OTLP only and a Prometheus server scrapes an OpenTelemetry Collector (the ASP.NET Core Prometheus exporter is a prerelease package; maintainer's choice). The REST contract is refined in [D-54](#d-54): read-only `/api/v1/jobs`, off by default (`ApiEnabled`). Readiness now asks the checks registered in the application (it looked in the listener's own host before, so it always answered ready).

## D-41 Documents are versioned hashes with explicit indexes {#d-41}

Status: decided (2026-11-08, S07). Answers Q-DATA-1. Background: [data layer](research/data-layer.md).

Decision: every document (job order, job response, work master, SFC state) is a Valkey hash with the fields `v` (integer version), `scope`, `doc` (the serialized document as a string) and the indexed attributes as own fields (for example `state`, `order`). A write is one Lua script per document type that checks the expected version, stores the hash, maintains the secondary indexes (sets, sorted sets, id keys) and appends the change or work entry, atomically. Secondary indexes are derived data and can be rebuilt by a scan. valkey-json and valkey-search are not used: no `JSON.*`, no `FT.*`, no reply parser. The Python TAG queries become `GET jobresponse:byorder:{orderId}` and `SMEMBERS`/`SSCAN` on `jobresponse:bystate:{scope}:{state}`.

Consequences:

* No hard dependency on modules; the compose image stays `valkey-bundle` (D-26), the code runs on plain Valkey.
* Index and document are consistent after every write (no asynchronous indexing), so read-after-write holds.
* We own index consistency: one `put` script per document type, an index-rebuild routine, and a test that compares each index with a full scan.
* Free-form queries are not available; a new query is a new index in the script plus a rebuild.
* Scripts read fields from the hash, not from JSON, so `cjson` is only needed where a script must look inside `doc`.
* Revisit if operators need ad hoc queries across scopes.

## D-42 Key schema, job identity and write idempotency {#d-42}

Status: decided (2026-11-08, S07). Answers the key-schema part of S07, R-4 and the `(scope, id)` review point.

Decision:

* **Prefix and names:** `{KeyPrefix}:` (configuration, default `microdcs`) in front of the names in [data layer](research/data-layer.md#key-schema). One generic `ref:{kind}:{scope}` set replaces the five reference-list key families of Python. All keys come from one `KeySchema` class and every script receives every key it touches in `KEYS`.
* **Identity:** a job is identified by its id alone, as in Python. There are **no hash tags** and no `(scope, id)` pairs in APIs. REST, MCP and MES lookups take the id.
* **Scope registry:** a persistent set `scopes`, written by the script that stores the first document of a scope; replaces the global change stream of Python as the discovery mechanism for the publisher and the SFC host.
* **Write idempotency (R-4):** every write is a full overwrite guarded by the version. A retry after an unknown outcome reads the hash: if `v` has advanced and the field `op` equals the operation id of the retry, the write already happened and counts as success; otherwise the CAS failed and the caller re-evaluates.
* **Streams:** work and outbox streams have no `MAXLEN`; entries are trimmed only after acknowledgement (`XTRIM MINID` below the lowest pending id of the group). Ids that `XAUTOCLAIM` reports as deleted are logged and counted. Change streams read by cursor keep `MAXLEN ~ 5000`; the publisher republishes the state index at start.
* **Dedupe keys** keep the Python semantics (lease, token, `done`, `ttl`), with a 16-byte SHA-256 prefix of `source:id` instead of Blake2b (not in the BCL).

Consequences: `MicroDCS.Valkey` (S09) holds `KeySchema`, the scripts and the DAOs. Key names and the wire format are a clean break (D-2). Dropped from Python: `TransactionDedupeDAO` and `CounterDAO` (unused, already in D-37 parity list), the global change stream.

## D-43 Stream consumption: nudge plus slow fallback poll {#d-43}

Status: decided (2026-11-08, S07). Answers Q-DATA-2.

Decision: one poller component per instance reads all work and change streams. A script that appends to a stream also `PUBLISH`es `scope` on the channel `{KeyPrefix}:nudge:{kind}`; the poller subscribes once per instance and reads the stream named by the nudge at once. Pub/sub can lose messages, so the poller also reads every stream on a fallback interval (default 1 s, `MicroDcs:Valkey:PollInterval`). No blocking commands (D-7). The same poller runs the due-index sweep (S07 time model). After a read that returned entries it reads again at once; it sleeps only when a read returns nothing. Consumer groups are created when a scope is discovered (scope registry), not on every iteration.

Consequences: S04 measured p50 about 1 ms with nudge against 125 ms for a 250 ms poll, with 503 server commands per 100 messages (including 100 `PUBLISH`) against 238 for the 250 ms poll and 362 for the 50 ms poll. Idle cost is one read per stream per second per consumer. Python's loop shape (sleep after every iteration, group creation every iteration) is dropped.

## D-44 SFC core rules, time and retry values {#d-44}

Status: decided (2026-11-08, S07). Settles the open points of [D-17](#d-17) and [D-18](#d-18), which become decided. Background: [SFC rules](research/sfc-rules.md).

Decision:

* **Project and interface:** `MicroDCS.Sfc`, no reference to any other MicroDCS project or external package (architecture test in S15). `IRecipeInterpreter` has `DataSchema`, `Build(recipe, capabilities)` returning a graph or build errors, and `Step(graph, state, snapshot, event)` returning `(state, continuation, commands)`. The SFC interpreter is the first implementation.
* **Work identity:** the action key `{jobId}:{actionName}` is the work token id. Each attempt has its own command event id; a response is matched by the action key, never by the instance-local command id. A response whose action key or type contradicts the open action is rejected, not matched to a sibling. Late responses follow D-17.
* **Schema direction:** the C# model is the source. The JSON Schema for recipe and state is generated at build time, checked in, and a test regenerates and compares it; every serialized property has an explicit wire name. Recipe wire names stay as in the Python schema (`timeout_seconds`, `type_id`, ...), so existing recipes stay valid. The recipe `dataschema` URI carries the version; the state document carries a `format` number (additive changes bump the minor).
* **Recipe pinning:** a job stores the recipe by content hash; the recipe text is written once to `sfc:recipe:{hash}` when a job starts. A changed work master does not change running jobs (Python reloaded the work master at every step).
* **Capabilities:** the station supplies flags (`PushCommand`, `PullEvent`, `SimultaneousBranch`, `SelectionBranch`, qualifier `N`) and, per `type_id`, the outcome names its processor declares. `Build` refuses a recipe that needs more; the same check is the job acceptability check.
* **Qualifiers:** only `N` is accepted. P, S, R, L and D are rejected at build with a clear error. Python never read the qualifier (parity row S-23); they can be added later as capabilities without a format change.
* **Outcomes:** an action completes with a named outcome. The processor declares the outcomes of a response type and returns one from its typed `[OnResponse]` handler; the recipe only names them. `SfcTransition.condition` names an outcome or `"true"` (D-17). Reserved outcomes `timeout` (attempts exhausted) and `error` (the response handler reports failure) are routable; if no transition of the step matches, the continuation is `Fault` and the host aborts the job.
* **Build-time validation:** exactly one initial step; unknown steps in transitions, actions or branches; branch without paths; selection divergence needs a `"true"` default and unique priorities; outcome names must exist for the action types involved (plus the reserved ones); two pull-event selectors that cannot be told apart (R-1); a cycle with no wait; unreachable steps; duplicate names. Hard limit of 10,000 dispatches per evaluation returns `Fault`.
* **Time:** the core reads no clock. `At` of an event is the millisecond part of the work item's stream entry id (ticks included); events that do not pass through the stream (push responses handled by the sending instance) use one Valkey `TIME` call. Action deadlines are `At + timeout_seconds` (the recipe field, required); the wire expiry is the deadline minus `Sfc:ExpiryGrace` (default 2 s).
* **Sweep:** every instance sweeps the due-index of every known scope each `Sfc:SweepInterval` (default 1 s, start jitter up to 250 ms), in the poller of D-43. The sweep writes `sfc:heartbeat`. If the gap since the last heartbeat exceeds `Sfc:OutageThreshold` (default 30 s) the tick is marked `afterOutage` and the core extends open deadlines by the gap.
* **Retry:** `max_attempts` is an optional recipe field per action association with a host default `Sfc:MaxAttempts` (default 3). An expired deadline re-dispatches with the same action key until the attempts are used, then the outcome is `timeout`. No backoff in the first version. `error` is not retried.
* **Scope discovery:** the `scopes` registry (D-42) is read at start and every `Sfc:ScopeRefreshInterval` (default 5 s) by every instance, which then creates the consumer group and sweeps that scope. A scope is never removed automatically (scopes are few); the maintenance tools (S12) can remove one.
* **Poison work items:** a handler exception leaves the item pending; after `Sfc:MaxDeliveries` (default 5) deliveries it moves to `sfc:dead:{scope}` with the reason and is acknowledged (Python acknowledged every failed item, S-6).
* **Atomic step:** one generic script compares the state version, stores the new state, appends the commands as work-stream entries, updates the due-index (`NextDeadline`) and the scope's active-job set. A host crash between state and execution is covered by redelivery of the entries (execution is idempotent by action key) and by the `Resume` event.

Revised 2026-11-08 (S15, built as decided, with these refinements): (1) recipe wire names are the PascalCase names of the Python schema (`TimeoutSeconds`, `TypeId`, `MaxAttempts` for the new field), not snake case; (2) `IRecipeInterpreter.Step` is named `Evolve` (IEC 61131-3 speaks of the evolution of the chart; `Step` is an SFC element and a reserved word, CA1716; `Evaluate` says too little) with the event parameter `input`; (3) the continuation `SfcCancelled` is added for `SfcCancel`, additive to `Complete`, `Defer` and `Fault`; (4) the default transition `true` does not match when an action ended with a reserved outcome, so a failure never takes the default path; (5) a state is a JSON document with `format` 1, command ids are `{actionKey}#{sequence}`; (6) existing recipes with `Condition: "always"` must use `true`; (7) every `Evolve` result carries a disposition `Applied`, `Absorbed` or `Rejected` (the late-response and contradiction rules of D-17 and D-44). (8) C# names follow the standard: divergence and sequence instead of branch and path (recipe wire names unchanged), the full qualifier enum `SfcActionQualifier`, `SfcActionBlock` instead of `SfcActionAssociation`, features `SimultaneousDivergence` and `SelectionDivergence`. (9) after review: each step entry has a `Round` (carried by `StartWork`, optionally echoed in responses) so that loops do not take answers of an earlier pass; the last expiry emits `CancelWork` with reason `timeout`; a selection divergence ends on the outcomes of the sequence taken; recipes are capped at 20,000 elements; action names must not contain `:`; `Parameters` are not a selector until S20. Details in [SFC rules](research/sfc-rules.md#s15-design-details).

Revised 2026-11-09 (S19, built as decided, with these refinements): (10) the `At` of a `start` is one read of the server clock, not the entry id: an order can wait in the stream longer than its first timeout, and that wait must not cost an attempt. A repeated start is absorbed, so nothing replays the value. (11) `sfc:heartbeat` is a hash per scope and is written by the sweep script itself, so exactly one sweep sees a gap; that sweep takes every entry of the index and passes the gap on its ticks. (12) outcomes are declared on the `[Outgoing]` factory (the key the recipe knows) and returned from `[OnResponse]`; `done` is every type's default. (13) the state has `startedAt` (format 1.1). (14) the wire expiry is the time left minus `Sfc:ExpiryGrace` where the time allows it; otherwise the full time left, so a timeout below the grace still reaches the equipment. (15) `XAUTOCLAIM` and the dead stream are built in S19 instead of S20, because a failed entry depends on them.

Revised 2026-11-09 (S20, D-53): (16) `Parameters` are not a selector; the optional `Selector` field is. SFC011 accepts two waits of one type when their selectors set one field to different literals. (17) a pull event's outcomes come from `[Incoming(Outcomes = ...)]`.

Consequences: S15 builds the core and the in-memory host against these rules; S19 and S20 build the Valkey host. The outage test and the two-replica test in S20 are acceptance tests.

## D-45 Maintenance operations: plan/apply, snapshots, guards {#d-45}

Status: decided (2026-11-08, S12). Settles the open points of the maintenance design in [inspection and MCP](research/inspection-mcp.md#maintenance-tools-cleanup-delete-repair-mcp-only) under [D-19](#d-19).

Decision:

* **Project:** `MicroDCS.Maintenance` (`IMaintenanceOperations`, Valkey implementation) references Core and Valkey, not Inspection and not ASP.NET. `MicroDCS.Mcp` is the one adapter (`MaintenanceTools`); architecture tests fail if `MicroDCS.Inspection`, Core or Valkey reference it. The future REST adapter must not.
* **Plan and apply:** `Plan…` returns targets, warnings, a 128-bit random single-use token (lifetime 5 min, 50 plans at most, in memory) and a digest over the stored state of every target. Apply recaptures the targets and refuses on a different digest, writes the snapshot, then runs one Lua script that deletes the keys, set members and hash fields; hash documents are deleted only when their `v` still equals the planned version (compare-and-delete). A plan token is spent even when the apply fails.
* **Targets come from the key schema**, never from patterns: `purge_scope` follows the scope's lists and sets to the documents, the response indexes, the SFC state of active and due jobs, both streams, acceptance limit, reference lists, publisher cursor and registry entry. Shared recipe texts stay.
* **Defaults:** item cap 100 (`allow_large` lifts it), snapshot retention 20, snapshot size cap 5 MiB, plan lifetime 5 min, marker key `{prefix}:env` kept as an optional second guard (`RequireEnvironmentMarker`, default off; compose profile `app` turns it on and writes it).
* **Environment guard:** `Enabled` plus the host environment `Development` or `Test`. The options validator ends the start otherwise (D-27); every operation checks again, so direct API use outside these environments is refused too.
* **Snapshots:** one JSON file per apply that deletes or overwrites, written and flushed (temp file, flush to disk, move) before the change; no snapshot, no change. Content is the stored text as written plus type, time to live and version. Restore writes the images back with their versions and index structures instead of going through the DAOs, because the snapshot already holds the derived structures; it refuses when a target exists with different content unless planned with `overwrite` (then the replaced content is snapshotted). Stream entries come back with new ids; consumer groups and pending entries are not restored. Broker state is out of scope; the publisher reset (S18) is the broker-side repair.
* **Audit:** plan, apply and refusal are logged in the security category with caller, tool, arguments, counts, digest and snapshot id, no payloads.

Consequences: later sessions add their tools as further operations on `IMaintenanceOperations` with a `Plan…` method, a case in the apply, and an integration round-trip test (S17/S18 `maint_job_delete`, `maint_retained_clear`, `maint_publisher_reset`; S19/S20 `maint_sfc_*`, `maint_stream_*`). The dev compose volume for snapshots is not used: the chiseled image runs non-root, so profile `app` writes to `/tmp`. S23 decides per tool whether it stays.

Open: whether `maint_purge_scope` should also clear retained topics (needs the transport client; revisit with S18).

## D-46 MQTT transport: connection, QoS, session and acknowledgement {#d-46}

Status: decided (2026-11-08, S13). Details for D-9, D-10, D-37 and D-38. Findings and the parity list: [MQTT transport](research/mqtt-transport.md).

Decision:

* **One connection per instance** carries the subscriptions of all MQTT bindings, the reply subscription and the retained publishes (Python used two clients). Client id `microdcs-{instance id}` unless `MicroDcs:Mqtt:ClientId` is set; two instances must not share one.
* **QoS 1 end to end:** subscriptions and publishes use QoS 1 (`PublishQos` may be 0), so the delivery profile is `AtLeastOnce` and the dedupe claim applies (D-37). Python subscribed and published events at QoS 0.
* **Persistent session** (`CleanStart` false) with `SessionExpiry` 1 hour by default (Python: never). A generated instance id changes on restart, so a never-expiring session would leak and, in a shared group, keep queueing messages for a consumer that is gone. A deployment with stable ids (StatefulSet, `POD_ID`) raises it.
* **Manual acknowledgement after the pipeline:** a worker takes a message from a bounded inbox (`MessageWorkers` workers), calls the pipeline once per matching binding and acknowledges when none returned `Fault`. `Drop` and `Ack` acknowledge, an undecodable message or a topic without matching subscription is dropped and acknowledged by the transport (counted, `MQTT dropped`), a fault leaves it unacknowledged and the pipeline ends the host (`CanRedeliver` false, D-27); the session redelivers after the restart.
* **Reply topic:** `{ReplyBase}/{instance id}`, subscribed unshared; commands carry it as the MQTT response topic and the id of the command (or the causation id of a response) as correlation data (UTF-8). A binding that publishes commands over MQTT needs `ReplyBase` (checked at start).
* **Addresses:** `MqttAddressRenderer` renders `prefix/scope/discriminator/intent`, `+` per scope level and `$share/{group}/` natively. A received topic is parsed with the binding's discriminator; a topic with an invalid scope segment is dropped so the scope check cannot be skipped.
* **State topics** (`IStateTopicPort`): retained, QoS 1, message expiry = time to live, topic = address + `/` + name (segments follow the segment rule); delete is a zero-byte retained message.
* **Reconnect:** a lost connection starts a loop on the `TimeProvider` (delay doubles from 1 s to 30 s); publishing waits meanwhile and sends again after the reconnect; subscriptions are made again. A broker that is not reachable at start ends the process (D-27).
* **Configuration:** section `MicroDcs:Mqtt` (host, port, client id, user, password or password file, SAT token file, CA file, require TLS, client certificate and key, session expiry, keep-alive, operation timeout, reconnect delays, publish QoS), validated at start. Transport name `mqtt` in `MicroDcs:Bindings:{id}:Transport`.

Consequences: `MicroDCS.Transport.Mqtt` (MQTTnet 5.2, AOT clean) with `AddMqttTransport`; the `transport.mqtt` inspector; compose profile `app` runs the greetings demo over Mosquitto. The NATS session (S14) checks `ITransport` against this implementation for MQTT leaks.

Open: duplicate handling while the first delivery is still in flight waits for the full lease (T-20); a poll cap is a candidate for S19.

Revised 2026-11-08 (M1 review): (1) a failed re-subscribe after a reconnect disconnects before the next attempt, and the loop checks the connection again after it ends, so a drop in between is not lost (RV-11, RV-12); (2) a message is sent at most `MaxPublishAttempts` (5) times, waiting for a reconnect does not count; after that the publish fails and the pump ends the host (RV-16, D-27); (3) the client options, including the SAT token and the password file, are built again for every reconnect attempt (RV-17); (4) a response topic from a peer goes through the reply-address policy (D-48). The T-20 wait is solved in the pipeline: a duplicate polls at most every second, and a faulted message releases its claim (L-7).

## D-47 NATS transport: JetStream requests, inbox replies, KV state topics {#d-47}

Status: decided (2026-11-08, S14). Details for D-9, D-10, D-37 and D-38. Findings and the leak list: [NATS transport](research/nats-transport.md).

Decision:

* **Requests through JetStream:** a stream per binding prefix (`{StreamPrefix}_{prefix}`, subjects `{prefix}.>`, `AllowMsgTTL`), a durable pull consumer per binding with explicit acknowledgement after the pipeline, `DeliverPolicy.New`, `MaxDeliver` 5, `AckWait` 30 s, `InactiveThreshold` 1 h. The delivery profile is `AtLeastOnce` with `CanRedeliver` true, so the dedupe claim applies and a fault does not end the host.
* **Shared group = shared consumer:** with `SharedGroup` the consumer is named `{binding}-{group}` and the instances compete on it; without a group each instance has its own consumer and sees every message.
* **Responses are plain messages** on `{ReplyBase}.{instance token}` (core subscription, no queue group). A lost response is recovered by re-dispatch (D-9). The reply address of a command travels in the header `Mdcs-Reply-To`.
* **Mapping:** binary mode, attributes as headers under their own names, `Content-Type`, `Nats-TTL` from the expiry interval, `Nats-Msg-Id` for publisher-side duplicate detection.
* **Outcome mapping:** `Ack` to ack, `Drop` to terminate, `Fault` to nak with `NakDelay`; from the `MaxDeliver`-th failed delivery on the message is terminated, logged and counted (`microdcs.nats.poisoned`) instead of vanishing in the server.
* **State topics** are keys of a KV bucket (`StateBucket`); a put is a stream publish with `Nats-TTL` (native time to live), a delete is a KV delete.
* **Reconnect** is the client library's; the transport restarts the consume loop after each reconnect. A server that is not reachable at start ends the process (D-27).
* **Addresses:** `prefix.scope.scope.discriminator.intent`, `*` per scope level; `/` in a configured prefix or reply base is rendered as `.`. The instance id is mapped to a subject token.
* **Tenancy (R-20):** one NATS account per tenant, chosen by the connection credentials (creds file, NKey, user and password, token); no tenant logic in the code.
* **`Retain` is a hint (core, applies to MQTT too):** `OutboundMessage.Retain` and `PublishOptions.Retain` map to the MQTT retain flag only; other transports ignore it with a warning. Last values that must outlive the publisher use `IStateTopicPort`. This changes the XML docs only.
* **Configuration:** section `MicroDcs:Nats` (URL list, client name, credentials and TLS as files, JetStream domain, stream prefix and max age, replicas, consumer inactive threshold, ack wait, max deliver, nak delay, state bucket, operation timeout, reconnect delays), validated at start together with the NATS bindings. Transport name `nats`.

Consequences: `MicroDCS.Transport.Nats` (NATS.Net 3.3.2, AOT clean) with `AddNatsTransport`; the `transport.nats` inspector; compose service `nats` in the dev stack and the test harness (digest-pinned `nats:2.15-alpine`, server 2.11 or later needed). The core interfaces `ITransport`, `IStateTopicPort`, `InboundMessage` and `OutboundMessage` needed no change.

Open: a faulted message is redelivered while its dedupe claim is still held, so the redelivery waits for the lease (30 s). S19 releases the claim on a fault and caps the wait (finding L-7).

Revised 2026-11-08 (M1 review): (1) **shared consumer name** is `{binding}-{group}-{hash}`, the hash taken over the filter subjects. Instances that filter differently (a rollout that changes intents, scope levels or the discriminator) get their own consumer instead of overwriting the shared one, where old instances would terminate types they do not know. During such a rollout both consumers receive the messages they filter; the dedupe store suppresses the second processing when Valkey is shared, and the old consumer ends through `InactiveThreshold` (RV-19). `AckWait` and `MaxDeliver` are not part of the hash: the last start sets them for the group, as before. (2) Consume recovery recreates the consumer with its start configuration (RV-14). (3) Each consumer pulls one message at a time, and every queued or running message gets `AckProgress` every `AckWait / 3` until it is settled, so waiting for a worker does not count as a delivery (RV-20). (4) A publish is repeated when the attempt saw a disconnect or reconnect (epoch of opened connections), also when the client is back by the time the timeout arrives (RV-15). (5) Credentials are read from their files in the auth callback on every connect (RV-17). (6) `Mdcs-Reply-To` goes through the reply-address policy (D-48). L-7 is done in the pipeline (release on fault, poll at most every second).

## D-48 Reply addresses from peers are checked before the instance publishes to them {#d-48}

Status: decided (2026-11-08, M1 review, RV-18).

Context: a request carries the address its answer goes to (MQTT response topic, NATS `Mdcs-Reply-To`). The instance publishes the reply there with its own credentials, which include rights a peer may not have: the JetStream API and the state bucket on NATS, the retained state topics on MQTT. A peer that may only send commands could otherwise make the instance delete a stream (`$JS.API.STREAM.DELETE.*`) or overwrite a state key (a confused deputy). D-13 trusts transport access control for the identity of the sender, not for where the instance writes.

Decision: `ReplyAddressPolicy` in Core, applied by every transport when a message arrives, rejects a reply address that

* starts with `$` (broker and server namespaces: `$JS`, `$KV`, `$SYS`),
* contains a wildcard of the transport (`+ #` for MQTT, `* >` for NATS), white space, control characters or an empty segment,
* lies below a binding prefix of this instance (its own data, event, command and state addresses), unless it lies below one of the instance's reply bases (instance-to-instance replies stay allowed),
* or, when the binding sets `AllowedReplyPrefixes` (comma separated, D-30), does not start with one of them.

A message with a rejected reply address is dropped before the pipeline (acknowledged or terminated, counted as dropped) and logged in `MicroDCS.Security` with the binding and the reason, never the address. Restricting replies to `ReplyBase` only was rejected: MES clients bring their own reply topics.

Consequences: the AMQP and Zenoh transports (S24, S25) apply the same policy with their separators and wildcards. Broker ACLs stay the first line of defence; the policy covers the gap between what a peer may send and what the instance may write.

## D-49 Credential files are read on every connect, and a rejected credential does not end the retries {#d-49}

Status: decided (2026-11-08, M1 review, RV-17). Applies to every transport.

Context: mounted secrets rotate in place (Kubernetes projected service account tokens expire after about an hour, operators replace password and token files). A transport that reads them once at start can no longer reconnect after a rotation, and the process stays up without doing any work. The file can also be out of step for a moment during a rotation, so a rejected credential is not final.

Decision:

* A transport reads credential files on **every connect attempt**, through its native mechanism (D-10): MQTT builds the client options per attempt, NATS uses the auth callback. Password and token files go through `SecretFile.Read`.
* A credential the server rejects does **not** end the reconnect attempts; they go on with the usual backoff. On NATS this needs `IgnoreAuthErrorAbort`, because NATS.Net by default stops for good after the same authentication error twice.
* The start stays fail-fast (D-27): a credential that is rejected at start ends the process.
* Every transport has the same compose test (`RV17_a_rotated_password_file_is_read_on_every_reconnect_attempt`): the host connects, the file changes to a wrong password, the connection is cut, the transport must stay disconnected; the file is corrected and the transport must reconnect. A cached credential fails the first check, giving up fails the second.

Consequences: S24 and S25 implement this and carry the same test. The harness Mosquitto has a password file with the user `rotate`; anonymous clients stay allowed. TLS client certificates are out of the rule's test: MQTT reloads them with the options, on NATS loading them is the library's matter and is not verified.

Revised 2026-11-11 (S22): **Valkey follows the same rule.** `MicroDcs:Valkey` gained `User`, `PasswordFile`, `TlsCaFile` and `RequireTls` (Python parity, `RedisConfig`). StackExchange.Redis has no credential callback, so `ValkeyConnection` reads the file in `BeforeSocketConnect`, which runs before every socket connect on the options instance the multiplexer keeps. The rotation test runs against `valkey-acl`, a harness Valkey that requires authentication: on a server whose `default` user needs no password a rejected AUTH silently falls back to `default` and the test proves nothing.


## D-50 Machinery Jobs processor: V2 only, wire shapes in the extension NodeSet, atomic recipe start {#d-50}

Status: decided (2026-11-08, S17; maintainer chose V2 only and the recipe start rule). Analysis and the Python parity list: [Machinery Jobs](research/machinery-jobs.md).

Decision:

* **V2 commands only.** The processor serves Job Control V2, as Python did. Stored documents are V2 OPC UA JSON (`ISA95JobOrderAndStateDataType`). V1 stays in the S16 models and has no command handlers.
* **Wire shapes in `urn:microdcs:jobcontrol`.** OPC UA methods have no structure for their arguments. The call, result and status event shapes are data types in the extension NodeSet (model 1.1.0), generated by the SDK. One small `[CloudEventType]` subclass per CloudEvent type sits on top. No hand-written encoders.
* **A response has its own type id:** the call id with `.response` before the version (`...Store.v2`, `...Store.response.v2`). Python shared one id, but the registry maps one id to one payload type (D-2 allows the break). `ReturnStatus` is a JSON string, as OPC UA JSON writes every UInt64.
* **Generator:** `[CloudEventType(Codec = OpcUaJson)]` gets a generated `OpcUaJsonPayloadDescriptor<T>` when the type is a generated OPC UA structure with a public constructor. Otherwise MDCS010 stays. `Binary` stays rejected. This revises D-35 (4).
* **Recipe start:** every transition into AllowedToStart writes the `start` work entry, in the same script as the job order. Python started a recipe on StoreAndStart only and wrote the entry separately, so a failure left a job that never ran.
* **Removal:** Cancel and Clear delete the order in a version-guarded script that also writes the change entry (D-34). The job response stays.
* **Identity:** a command naming an order of another scope gets `UnknownJobOrderId`; a Store of such an id gets `UnableToAccept`. The other scope learns nothing about the order (D-42).
* **Write outcome:** each command writes with its event id as the operation. A redelivered command whose write was the last one on the order reports success and emits nothing (R-4); a redelivered removal finds its operation in the change entry. A command that another command wrote over in between is applied again and judged by the state machine: recognising it would need an operation history on every order, for a window of one fault between the write and the dedupe marker. A version conflict re-reads and re-applies, four attempts in all. A Valkey failure is a fault and the transport redelivers, because the outcome is unknown; Python returned a definite failure.
* **Acceptance limit:** a scope limit always applies, also 0 (accept nothing). The configured default 0 means no limit. An `Update` does not count, because it adds no order. The count and the write are not one atomic step: two replicas can exceed a limit by one, as in Python.

Consequences: S18 reads `Cancel` and `Clear` change entries (Python's publisher handled only `Clear`, so a cancelled order kept its retained topics). S19 consumes the `start` work entries and must re-read the order and skip unless it is still in AllowedToStart, because RevokeStart and Cancel do not withdraw an entry and Start, RevokeStart, Start leaves two. Until S19 no consumer exists and entries stay in `sfc:work:{scope}` (no `MAXLEN`, D-42).

Not chosen: hand-written `IEncodeable` classes (about 30 lines each, and the extension model already exists); a hand-built `ProcessorDescriptor` (loses the generated parameter binding, D-25); an atomic limit in the script (accepting a small overshoot keeps the script generic).

## D-51 Job order publisher: split by role, lease as a guard, topics {#d-51}

Status: decided (2026-11-09, S18). Answers Q-ARCH-9. Background: [job order publisher](research/job-publisher.md).

Context: the publisher serves the MES, not the control path. It should be prompt, but its failures and load must not reach the control path. The command handler only writes a change entry in its script, so the two are already decoupled logically; what remains is the shared poller, connection, process and CPU. Python used a separate Deployment with one replica and a flag.

Decision:

* **Deployment modes:** production runs two Deployments, processors with `Roles=Processor` and one publisher with `Roles=Publisher`, `replicas: 1` (manifest in S22). A pod restart is enough failover, because the MES reads retained last values meanwhile. A separate deployment can also name another broker through its own environment. `Roles=All` stays the default for development and small sites.
* **Hand-off:** `PollAsync` only marks the scope as dirty; a task of its own reads and publishes. D-43 stays: one poller per instance, which now cannot be held up by a broker. Dirty marks also batch the work.
* **Lease as a guard:** a renewed key `publisher:lease` with the instance id, TTL 15 s, renewed every 5 s in its own loop so a publish that waits for a broker cannot lose it. Only the holder publishes, and it gives the lease up on stop. It covers `Roles=All` with several replicas and the overlap of a rolling update: without it a slower publisher can overwrite a retained topic with an older value that stays until the next change. Python needed no leader election because it trusted the deployment.
* **Publisher-only instance:** transports connect in every role except `None` and subscribe, and on NATS create streams, only under `Processor`. Processor hooks do not run.
* **Topics:** `prefix/{scope levels}/{discriminator}/data/` plus `state-index`, `order/{id}`, `result/{id}`, on the transport the `machinery_jobs` binding names. A transport without state topics ends the start (D-27). Payloads are OPC UA JSON: `JobOrderStateIndexDataType` (extension NodeSet 1.2.0), `ISA95JobOrderDataType` without state, `ISA95JobResponseDataType`. `Sequence` is an Int64, so it is a JSON string as OPC UA JSON writes it.
* **State has one home:** `order/{id}` carries the order alone and changes on Store, StoreAndStart and Update; `state-index` carries the state.
* **Job order ids** must fit the segment rule (D-38), because they become topic levels. Store, StoreAndStart and Update refuse others with `InvalidRequest`. The publisher still skips and counts one that arrives some other way, so it cannot block its scope.
* **Results:** `result/{id}` is written on any change of a job in Completed, Closed or Aborted with a response. Python checked on `ResultUpdate` only and lost a result stored before the end. `HasResult` in the index is true only when that topic exists.
* **Rebuild:** a scope is rebuilt from the documents on its first visit under a lease, when it has no cursor, when the stream was trimmed past the cursor (MAXLEN, D-42), and at half the time to live so that idle orders keep their topics. Removals during a gap cannot be seen; those topics run out by their time to live.
* **Delivery:** at least once, the cursor moves after the topics are written. A failed round repeats on the next poll.
* **Maintenance:** `maint_publisher_reset` removes the cursor of a scope, and the running publisher rebuilds it. `maint_retained_clear` removes state topics through `IStateTopicCatalog` in Core; broker state has no snapshot. The sequence is never reset, so an MES can keep comparing.

* **MQTT session of a publisher-only instance:** clean start and session expiry 0, so it cannot resume subscriptions of an earlier run under another role.
* **NATS delete markers** carry a time to live of 1 h. A delete would otherwise leave a marker per removed key for ever, and job ids are unique.

Consequences: `MicroDCS.MachineryJobs/Publishing`, options in `MicroDcs:JobPublisher`, inspector `publisher`, MCP `publisher_status`, `maint_retained_clear_plan`, `maint_publisher_reset_plan`. `InMemoryTransport` implements the port. S19 must run the SFC host under `Processor` only. S22 writes the two Deployments.

Not chosen: role flag alone (silent double publishing with the default roles and during rollouts); a lease with hot standby as the default (failover in seconds is not needed for this consumer, though `replicas: 2` works); a publisher poller of its own (a second nudge subscription for what a dirty mark achieves); encoding unsuitable ids into topics (the MES could no longer derive the topic from the id); publishing the order on every change (full documents on every transition for a field the state index already has).

## D-52 SFC host: project, job transitions through the work stream, cancel on Abort and Stop {#d-52}

Status: decided (2026-11-09, S19; maintainer chose cancel on Abort and Stop and the full tool set in S19). Background: [SFC host](research/sfc-host.md).

Decision:

* **Project:** `MicroDCS.Sfc.Hosting` references the core, Valkey and Machinery Jobs. `MicroDCS.Sfc` stays dependency-free. It runs under the `Processor` role only (D-51); `MicroDcs:Sfc:Enabled` turns it off for a station without recipes.
* **Job order transitions are work entries.** A step writes one `job` entry with the transitions it needs, in the compare-and-set of its state. `JobExecution` in Machinery Jobs applies them through the state machine, version-guarded and idempotent by operation id, and writes the job response on `Complete`. A `Complete` applies `BeginExecution` first, because the entries of one step can reach two consumers.
* **Abort and Stop end the recipe.** The MES command writes a `cancel` work entry in the same script as the order. Python left the recipe running and commanding equipment, and refused its final `End`. **Pause and Resume hold the recipe, from S20a.** Until then they only change the job order, as in Python: the recipe keeps sending commands and can complete a job the MES has paused (`Complete` is accepted from Interrupted). In S20a, Pause writes a `hold` entry and Resume a `release` entry in the same script as the order. While held no transition fires and nothing new is dispatched, including re-sends; responses to commands already out are still recorded; open deadlines do not expire and on release move by the length of the hold. The recipe stops at the next step boundary. Telling the equipment to stop is S27: recipe-declared hold and restart actions need a recipe format extension and a command contract that do not exist yet.
* **A `BeginExecution` that is refused** ends the recipe unless the order is already executing, so a recipe that started against a revoked or aborted order does not run on.
* **Work entries:** `start`, `cancel`, `tick`, `resume`, `dispatch:{action}`, `job`. A push command travels in its `dispatch` entry as JSON. The consumer sends it only while that attempt is the open one.
* **Response path:** the routing map (D-9) supplies the action key and the round; a responder that echoes `mdcsactionkey` is matched without it. A step that cannot be done with what is stored is logged instead of faulting the pipeline.
* **A full outgoing queue** is not a poison entry: it is counted and the deadline dispatches again (D-28).

Consequences: `IActionSink` is `CompleteAsync(ActionResponse)` and `EventReceivedAsync`; `FailAsync` and `ScopeDiscoveredAsync` are gone (local expiry decides nothing, D-18; scope registry, D-42). `MessagePipeline` implements `IOutgoingCommands`. S20 adds pull events, the recovery scan and the capability flag `PullEvent`. S20a adds hold and release.

Revised 2026-11-09 (S20): pull events and the recovery scan are built, see [D-53](#d-53).

Revised 2026-11-10 (S20a): hold and release are built. The order script writes `hold` when the order enters Interrupted (Held or Suspended) and `release` when it goes from Interrupted to Running. Both entries align the recipe with the order as it is when the entry runs, because two consumers can take them in either order. The hold is core state (`heldAt`, state format 1.2): while held, `NextDeadline` is empty, ticks and resumes are absorbed, and responses, failures and events for work already out are recorded (the maintainer chose to record events; the wait stays in the waits index). The release moves open deadlines by the length of the hold, issues every open push attempt again with its command id and attempt, then fires what became possible. After the command is queued, the dispatch records `sent:{action} = {startedAt}:{commandId}` in the state hash and skips a command id it finds there, so a command sent before the hold, or a redelivered dispatch entry, does not go out again; a crash between the queue and the marker can send an id twice, never lose it. The alignment reads the order again after its write and repeats until both agree, and measures the hold with the server clock. Background: [SFC host](research/sfc-host.md#hold-and-release-s20a).

Revised 2026-11-12 (S27): telling the equipment to stop is built as recipe-declared hold and restart commands, see [D-57](#d-57). A recipe without them keeps the behaviour above.

Not chosen: applying the job transition directly after the compare-and-set (a crash between them would leave a finished recipe on a running job); a dedicated job-transition stream (a second consumer for entries that only ever follow a step); cancelling equipment work on the wire (no protocol for it exists, as in Python).

## D-53 SFC pull events, recovery scan, replica tests {#d-53}

Status: decided (2026-11-09, S20; maintainer chose the `Selector` field and real containers). Background: [SFC host](research/sfc-host.md).

Decision:

* **Selector:** an optional `Selector` on a `pull_event` action with `Subject` and `CorrelationId`, exact match, literal or the placeholder `{jobId}` (R-1). A recipe is shared by many jobs, so only the placeholder tells jobs apart. No selector means every event of the type. Build error `SFC011` remains for waits a selector does not separate.
* **Choosing the job:** the jobs waiting for a type are kept in `sfc:waits:{scope}:{typeId}`, ordered by wait start, written in the compare-and-set of the state. The first job the core accepts takes the event, so one event completes one action. Every incoming event reads this index first, and only an event someone waits for enters the work stream. An entry can be stale (a deleted job); the consumer removes it when it finds one.
* **One event, one job:** the write that applies an event also sets `sfc:event:{scope}:{source|id}` for one hour, and the script refuses a write whose event is already claimed. The key is the CloudEvent identity, not the stream entry, because a broker redelivery appends a new entry. A wait that begins in a step goes to the end of the queue, even when the job already waited for the type in the step before.
* **Early events are dropped**, as in Python: no buffering. The action ends with `timeout`.
* **Recovery scan:** an instance queues a `resume` with the scan mark for each running job when it first sees a scope. Every action with a deadline counts as live, because the sweep owns it. A resend on every restart would use up `MaxAttempts`, and after an outage it would race the tick that moves the deadlines (D-9, D-18). The scan re-issues only work without a deadline and repairs lost due and wait entries. A repaired due entry is not due before `OutageThreshold`, so a tick already under way goes first. A job that cannot be evaluated is logged and left alone.
* **Replica tests:** two replicas of the application image, killed with SIGKILL through the Docker Engine API from a test container. A separate `replicas` compose profile and `scripts/replica-tests`, because a container that exits would end `--abort-on-container-exit`. Only this profile mounts the Docker socket.

Consequences: `IActionSink.EventReceivedAsync` takes the handler's `ActionOutcome`. `SfcCapabilities` has `EventOutcomes`. A station needs an `[Incoming]` handler for a pull type (`SFC014`). Inspector kind `sfc.waits`, MCP tool `sfc_waits`. Measured sweep latency with the default 1 s interval and two replicas, 20 deadlines spread over 1.4 s, two runs: p50 662 and 730 ms, max 1009 and 1018 ms from deadline to the command arriving.

Not chosen: `Parameters` as the selector (two meanings for one field); scanning the active set per event (every incoming event would read every running job); resending on restart as in Python; in-process hosts for the kill tests (no real process death).

## D-54 HTTP surface and telemetry {#d-54}

Status: decided (2026-11-10, S21; maintainer chose OTLP only and a read-only jobs API). Answers Q-OPS-1 and the OpenTelemetry coverage question. Background: [HTTP surface and observability](research/http-observability.md).

Decision:

* **OTLP only.** `MicroDCS.Telemetry` (`AddMicroDcsTelemetry`) adds the OpenTelemetry SDK with the OTLP exporter for the signals that have an endpoint (`OTEL_EXPORTER_OTLP_ENDPOINT` or a signal endpoint); without one the SDK is not added. All other settings are the standard `OTEL_*` variables. Sources and meters by the pattern `MicroDCS*`, plus `NATS.Net`, the .NET runtime and ASP.NET Core meters. No instrumentation packages. Console logging stays next to the OTLP log exporter.
* **Spans:** the pipeline's consumer span gets `messaging.system` and `messaging.destination.subscription.name` (`InboundMessage.Subscription`, set by MQTT and NATS). A producer span `send {type}` is current while `PublishAsync` and command dispatch build the envelope, so `traceparent` always names a span. The SFC host has one span per work entry (`MicroDCS.Sfc`), and a dispatched command is its child. Health probes are not recorded. **No Valkey spans:** StackExchange.Redis has no instrumentation and the contrib package is not stable.
* **Readiness:** `transports` (every transport connected; ready without a role) from `AddMicroDcs`, `valkey` (`PING`) from `AddMicroDcsValkey`. `503` names the failing checks, never their error text. Liveness stays "the process answers" (D-27).
* **REST:** `GET /api/v1/jobs?scope=&state=&limit=`, `GET /api/v1/jobs/{id}` (order, response state, recipe state), `GET /api/v1/openapi.json`; read-only, off by default (`Management:ApiEnabled`), through `InspectionService` (audit, cap, redaction), stable DTOs with source-generated JSON, RFC 9457 problems, 429 with `Retry-After`. Rate-limit windows per caller, so REST polling does not starve MCP. MES commands stay on the message bus. The OpenAPI document is checked in and embedded; a test compares its paths with the routes.

Consequences: `AddHealthChecks` adds a hosted service (`HealthCheckPublisherHostedService`), so code that took the single hosted service now picks `MicroDcsHostedService` by type. The compose profile `app` sends telemetry to the Aspire dashboard and enables the API. Probes and the manifest are S22.

Not chosen: the Prometheus exporter (prerelease) or an own Prometheus text writer (maintainer chose OTLP only); per-command Valkey spans through the StackExchange.Redis profiling API (after-the-fact sessions, not live spans); REST write routes for MES commands (a second command path next to the bus, needs token scopes).

## D-55 Image publishing, signing and the Kubernetes manifest {#d-55}

Status: decided (2026-11-11, S22; maintainer chose GHCR, cosign keyless, a kind test in CI). Answers Q-OPS-2 and R-9. Background: [deployment and security review](research/deployment-security.md).

Decision:

* **Publishing:** pushes to `main` and `v*` tags publish `ghcr.io/aschamberger/microdcs` from `ci.yml` after `dotnet`, `container`, `integration` and `kubernetes` passed; pull requests never publish. Each platform is built on its native runner (D-32) and pushed by digest; `publish-manifest` creates the multi-arch index. Tags: `main` and `sha-<12>` on `main`; `<version>` on a tag, plus `<major>.<minor>` and `latest` for a final release (a pre-release such as `v1.0.0-rc.1` moves neither). Release (tag) builds use no build caches.
* **Signing and SBOMs:** cosign keyless (Sigstore, the job's OIDC token). Each platform digest is signed and carries two CycloneDX attestations: the app's NuGet closure from its lock file (the AOT binary hides it from an image scan) and the image's OS packages, both made by `scripts/scan sbom` with the Trivy gate. The index is signed too. The publish job verifies every signature and attestation against the identity `https://github.com/aschamberger/microdcs/.github/workflows/ci.yml@refs/(heads/main|tags/v.+)`. No buildx provenance or SBOM manifests.
* **Manifest:** `deploy/k8s.yaml` (ServiceAccount, ConfigMap, processor and publisher Deployments by role, PodDisruptionBudget, three NetworkPolicies) plus `deploy/kustomization.yaml`, which pins the image. Secrets are mounted files; their paths sit in the Deployments. TLS to MQTT and Valkey is required in the example. Probes use `/health/live` and `/health/ready` on a pod-address listener with a bearer token.
* **Verification:** `scripts/k8s-test` runs `deploy/` in kind with the overlay `tests/k8s` (Pod Security `restricted`, Valkey with the island ACL user and `default` off, NetworkPolicy probes, the management token), locally and in the CI job `kubernetes` (required check).
* **Valkey island user:** the documented ACL rule (prefix keys and channels, data, stream, scripting and pub/sub categories, `+time`) is what the Valkey integration tests run under when `valkey-acl` is configured. `ValkeyConnection` turns off the client's tiebreaker, configuration channel and `INFO`/`CONFIG`/`CLUSTER` (one endpoint, D-12).

Consequences: a new required check `kubernetes`; the GHCR package visibility is set once after the first publish; S23 pins the release digest in `deploy/kustomization.yaml`. What still needs a real cluster is listed in [Deployment](../deployment.md#not-verified).

Not chosen: GitHub artifact attestations (cosign is what more admission controllers check); pushing from the `container` job (it also runs on pull requests); buildx provenance (it wraps each platform push in an index with an `unknown/unknown` entry); a Helm chart (an example to adapt is enough before the release; S26 decides on packaging).

## D-56 CI path rules, shared image and schedule {#d-56}

Status: decided (2026-11-11, S22a; maintainer chose the host-only integration run, linux-arm64 and kind on `main` and the schedule, and protection changes). Background: [CI runtime](research/ci-runtime.md).

Context: the PR critical path was 8 to 11 min. The integration suite ran twice (host for coverage, compose container), each about 5 min, of which 4.5 min were nine SFC host tests waiting out a 30 s command expiry in the shutdown phase of D-37; the runtime image was built five times per PR.

Decision:

* **Change detection:** `scripts/ci-changes` (shell, no marketplace action) classifies a PR's diff against its merge base into `code`, `image` and `k8s`; every other event runs everything. Documentation-only paths are a short allowlist (`docs/`, `overrides/`, `.claude/`, `zensical.toml`, top-level `*.md`, `LICENSE`); a path missing there only costs a full run.
* **Skips keep the required checks reportable:** non-matrix jobs skip with a job-level `if` (reported as success), matrix jobs (`container`) skip their steps, `trivy-image` gates its steps itself. The `changes` job is a required check, because a failed `changes` would skip its dependants and a skip counts as passed; `scripts/ci-changes` fails when `git diff` fails instead of reporting no change.
* **Documentation-only PRs** still build and run the SonarQube Cloud analysis in `dotnet` (only an analysis posts the required `SonarCloud Code Analysis` check); tests, format, audit, images and `trivy-image` are skipped. Gitleaks, workflows and trivy-fs always run.
* **Main and schedule:** `container (linux-arm64)` and `kubernetes` run on PRs only for image inputs or the manifest and what the kind test exercises (`deploy/`, `tests/k8s/`, `scripts/k8s-test`, `src/MicroDCS.App`, `src/MicroDCS.Inspection`, option classes and service registrations); always on pushes, tags and a daily `ci` schedule (container builds and kind only).
* **One integration run** on the host (D-15 revised); `integration` runs the replica tests, which also build the `tests` image.
* **No shared image build:** `kubernetes`, `integration` and `trivy-image` keep their own builds (from the `container` job's buildx cache where one exists). Taking the image of `container` would wait for both matrix legs (linux-arm64 included) and put two builds in a row on the critical path; an artifact hand-off also breaks re-running one job after the artifact expired. Since `kubernetes` and linux-arm64 now run on few PRs, most PRs build the runtime image three times instead of five.
* **Caches unchanged:** NuGet through `setup-dotnet`, buildx `type=gha` per platform, the daily Trivy DB; release (tag) builds still use none.

Consequences: required checks are `changes` plus the previous ten. Test rigs must not wait out timeouts they do not assert (the SFC rig stops with a 1 s grace period). A new path that affects the image or the kind test belongs in `scripts/ci-changes`.

Not chosen: `paths`/`paths-ignore` on the workflow trigger (a workflow that does not start never reports its required checks); dropping arm64 from the required checks (a PR that changes the image could merge with a broken arm64 build); a single image build feeding `integration` and `kubernetes` (tried in S22a: +2 to 3 min on the critical path, waits for the arm64 leg); moving `trivy-image` into `ci.yml` (it belongs to the daily security scan).


## D-57 SFC equipment hold: hold and restart commands, contract with the equipment {#d-57}

Status: decided (2026-11-12, S27; the maintainer chose per-action declaration, completion by the restart, fallback on a failed hold, and a restart with its own timeout that uses no attempt). Background: [SFC host](research/sfc-host.md#equipment-hold-s27), [SFC core](../sfc-core.md#equipment-hold).

Context: since S20a a MES Pause holds the recipe at the next step boundary, but a command that is out runs to its end (D-52). ISA-88 phase logic also has holding and restarting states, in which the equipment stops its work and continues it later.

Decision:

* **Recipe format v1.1.0.** A push-command action may declare `Hold` and `Restart` (both or neither), each with `TypeId`, `TimeoutSeconds` and `Parameters`. The `dataschema` becomes `.../sfc-recipe/v1.1.0/`. The interpreter still accepts `v1.0.0/` (`IRecipeInterpreter.Accepts`), because the fields are additive. A v1.0.0 work master that declares them is refused. Build errors: `SFC019` (shape: on a pull event, one without the other, no type or timeout) and `SFC020` (the hold type does not declare `held`). `SFC014` and `SFC015` cover the hold and restart types and the new feature `SfcFeature.EquipmentHold`, which the host offers unless `MicroDcs:Sfc:EquipmentHold` is `false`.
* **Hold command.** On `SfcHold`, every dispatched push action with hold logic gets a `StartWork` of kind `HoldCommand`. It has the same action key, the command id `{actionKey}#{n}:hold`, and the deadline `At + Hold.TimeoutSeconds`. It goes out while the job is held, once, and its deadline counts during the hold (`NextDeadline` while held is the earliest hold deadline). Answers are matched by command id, which the host takes from the response's causation id (`SfcActionCompleted/Failed.CommandId`).
* **`held`** ends the attempt: `CancelWork` with reason `held` withdraws its command, and a later answer to it is absorbed. On `SfcRelease`, or at once if the release came first, a `RestartCommand` is sent with a new command id and the deadline `At + Restart.TimeoutSeconds`. It uses no attempt and is sent once. Its response completes the action, and transitions may name outcomes of the restart type. If it expires, the outcome is the reserved `timeout`. A second pause holds the restarted work the same way.
* **Could not hold:** any other outcome, an error, or the hold's deadline (a `held` that arrives after the deadline still restarts the work). The action keeps the S20a behaviour: its answer is recorded, and on release its command is issued again with its id. No fault: a pause does not end a job. If the work answers first, the hold is withdrawn (`superseded`).
* **Host.** The hold has its own sent marker `sent:{action}:hold`. Before sending a hold, the host checks the marker of the work. Work that never went out (its dispatch reached a held job) is answered as not held by the host, so the release sends it. Abort and Stop withdraw hold and restart commands like other open work, without telling the equipment (D-52).
* **State format 1.3:** per action `hold` (`Holding`, `Held`, `NotHeld`), `holdCommandId`, `holdDeadline`, `restarted`.

Consequences: a recipe without hold logic behaves as in S20a. Equipment that offers hold logic answers hold and restart commands with the causation id of the command, and identifies the work by `mdcsactionkey`. `sfc_job` and `sfc_explain` show the hold state.

Revised 2026-11-12 (S23, D-58): the recipe `dataschema` values are now `https://aschamberger.github.io/microdcs/schemas/sfc-recipe/v1.1.0.json` and `…/v1.0.0.json`; the rule is unchanged.

Not chosen: a hold per step or per recipe (the equipment would have to work out which work is meant); completion by the original command's response after a restart (the equipment would have to keep the request open across the hold, and two commands would answer for one action); faulting the job when a hold fails (a pause would end a job), or a per-recipe choice (more format surface); retrying hold and restart up to `MaxAttempts` (equipment that does not answer a hold will not answer it twice; a second restart could start the work twice).

## D-58 Parity acceptance: schema documents as golden files, acceptance tests against the image, benchmark gate {#d-58}

Status: decided (2026-11-12, S23; the maintainer chose to serve the schemas from the docs site). Background: [parity acceptance](research/parity-acceptance.md).

Context: the parity gate needs the Python integration scenarios to pass against the application (Q-TEST-2), golden files for the wire format after the clean break of D-2 (Q-TEST-1), and a benchmark baseline in CI (R-18). The `dataschema` URLs of the payloads pointed to `aschamberger.github.io/schemas/…` (and `aschamberger.github.com` for the job payloads), which nothing served, and the checked-in greetings and Machinery Jobs schemas were the Python ones.

Decision:

* **Schema documents.** `dataschema` is the URL of a document under the docs site, `https://aschamberger.github.io/microdcs/schemas/<name>/v<version>.json`, plus `#/$defs/<Name>` when the document holds several types. The files live in `docs/schemas/`, so the docs build publishes them with no extra step. Documents: `greetings/v1.0.0` (`Hello`, `Bye`, `Raw`), `sfc-recipe/v1.1.0` and the frozen `v1.0.0`, `sfc-state/v1.0.0`, `microdcs-jobcontrol/v1.2.0` (the extension NodeSet namespace with its model version: method calls and results, status event, state index, Work Master extension), `isa95-jobcontrol-v2/v2.0.0` (station configuration types), `machinery-jobs/v1.0.0` (the plain JSON payloads). The OPC UA documents come from `JobControlSchemas.GenerateNamespaceJson<T>()` with `$defs` sorted; the others from `JsonSchemaExporter` over the source-generated contexts. The IJT schema is dropped: no .NET payload uses it, and it stays on `python-archive`.
* **Golden files (Q-TEST-1)** are these documents: the owning test project regenerates and compares them (`UPDATE_SCHEMAS=1` rewrites), and a test checks that every Machinery Jobs payload names a definition its document has. A non-additive change gets a new version and file; old versions stay served. A PR that touches `docs/schemas/` counts as code in `scripts/ci-changes`. Wire text written by hand is pinned once more in the acceptance test for a Store in OPC UA JSON. No separate payload fixture files.
* **Acceptance tests (Q-TEST-2)** run against the AOT image (service `app` of the `image` profile, every role) over MQTT only. They share the trait `Category=Image` and `scripts/image-tests` (renamed from `replica-tests`) with the SFC replica tests. Each test uses its own scope and ids.
* **Benchmark gate (R-18).** `scripts/bench` runs the BenchmarkDotNet ShortRun and fails when the allocated bytes per operation differ from `bench/baseline.json` by more than 10 % (at least 64 B) either way (a drop means rewriting the baseline, so it stays tight), or when a benchmark has no result. Mean times are reported in the job summary, not gated. Job `bench` in `ci.yml` follows the `code` path rule and is not a required check.

Consequences: the `dataschema` values and the SFC recipe format markers change (D-8: nothing stored needs migrating; a work master with an old URL is refused like an unknown format). The URLs resolve once the docs site is published from `main`. A model change that alters a schema fails the tests until the document is regenerated.

Not chosen: a separate repository for a user site at `aschamberger.github.io/schemas/` (one more repository to keep in step); validating payloads against the schemas at runtime or in the acceptance tests (a JSON Schema library dependency for a check the compare test already covers at the source); gating on mean times (shared runners vary by more than the regressions worth catching).

## D-59 .NET 11 as the target {#d-59}

Status: decided (2026-11-13, maintainer). Background: the open Dependabot PRs #76 to #84.

Context: .NET 11 (STS) was released on 2026-11-10; .NET 10 (LTS) and .NET 11 are both supported until November 2028. Dependabot proposed the 11.0.0 `Microsoft.Extensions.*` packages as separate PRs, which cannot pass the locked restore on their own.

Decision:

* **Target `net11.0`**, SDK 11.0.100 in `global.json`, all `Microsoft.Extensions.*` at 11.0.0. Reasons: lower memory use, async and Native AOT improvements, C# 15, at the same support end date as .NET 10.
* **Images on Ubuntu 26.04** (`sdk:11.0-resolute-aot`, `runtime-deps:11.0-resolute-chiseled`): .NET 11 publishes no `noble` variants.
* **Package pruning:** `Microsoft.Extensions.*.Abstractions`, `Options` and `Configuration.Abstractions` are part of the .NET 11 framework (NU1510), so the projects no longer reference them.
* **Roslyn 5.9** for `MicroDCS.Generators` (Dependabot #79): the compiler loads a generator only if its Roslyn is not newer than its own, and every SDK that builds `net11.0` has at least 5.12. The rule: at or below the Roslyn of the SDK in `global.json`; our own build fails first if a bump goes above it.
* **Dependabot** ignores majors of `Microsoft.Extensions.*`. SDK and runtime patches, and NuGet groups that leave related packages behind, go into one hand-made PR with `dotnet restore --force-evaluate` (`docs/development.md`, Dependency Updates).

Consequences: the next move (.NET 12, LTS, November 2027) is again a decision and one PR. The AOT binary grew by 0.9 MB to 39.0 MB. BenchmarkDotNet 0.15.8 does not know net11.0 for its out-of-process toolchain, so `scripts/bench` runs in process (`--inProcess`) until a release does; the allocations are unchanged except `HandleHello` (2216 to 2208 B), and the baseline was rewritten.

Not chosen: staying on .NET 10 LTS (no extra support time gained, and the benefits above wait a year).

