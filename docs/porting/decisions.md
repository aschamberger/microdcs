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

Background: [integration test setup](research/test-setup.md).

## D-16 Python on `python-archive`, port on `main` {#d-16}

Status: revised (replaces the long-lived `dotnet` branch and swap).

Decision: the Python implementation is preserved on the `python-archive` branch (created 2026-11-07 at `12c1415`, no tag); the port continues directly on `main`.

Consequences: no swap procedure. `main` is not a working product until the parity gate (S23); acceptable because of D-8. The docs site keeps deploying from `main` as the live work-in-progress, including this plan. Security fixes for Python go through pull requests into `python-archive`.

Background: [porting approach](research/branching.md).

## D-17 SFC semantics as a pure core library {#d-17}

Status: proposal. Details settled in S07.

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

Status: proposal. Details settled in S07.

Decision: the core reads no clock. Events carry timestamps from Valkey server time (proposal: taken from the work item's stream entry id); the state holds absolute deadlines and exposes `NextDeadline`; the host keeps one due-index entry per job; all replicas sweep it without a leader.

Consequences: closes the "no working job-level timeout" gap of the Python engine (R-2).

Decided on 2026-11-07:

* **Outage grace:** a platform outage (all pods or Valkey down) must not abort running jobs. The host keeps a heartbeat written by the sweeps; when the gap since the last sweep exceeds a threshold, the tick is marked `afterOutage` and the core extends open deadlines by the outage duration instead of failing them.
* **Deadline source:** the action deadline comes from the recipe's `SfcActionAssociation.timeout_seconds`; the wire expiry of the command is derived from it (deadline minus a configurable grace).

Open for S07: sweep interval, outage threshold, hash-tag key naming for later cluster mode, `max_attempts` (recipe field with a host default; the default value), which due-index keys a pod sweeps (scope registry).

Background: [SFC core library, time model](research/sfc-core.md#time-model-and-the-multi-instance-model).

## D-19 Inspection layer plus optional dev MCP server {#d-19}

Status: proposal.

Decision: in-process inspection services are the contract; REST and MCP are thin adapters on a separate management listener. MCP is an optional package, default off, read-only, loopback only, payloads redacted, with a separate write group. A separate maintenance group (cleanup, delete, repair) is MCP-only and never exposed over REST: dev-only project, environment guard, plan/apply, snapshot and restore.

Consequences: first slice in S11, maintenance tools in S12; every stateful component ships its inspector afterwards (definition of done). MCP calls the services, not the REST API.

Open: listener layout with Q-OPS-1 (S06), redaction policy (S06), snapshot retention and size defaults (S12). Configuration names are placeholders until Q-ARCH-3.

Background: [inspection layer and MCP server](research/inspection-mcp.md).

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
