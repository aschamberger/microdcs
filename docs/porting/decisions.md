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

Not chosen: System.Text.Json on the generated types (unusable output); hand-written POCOs mirroring the spec (loses spec-conformant types and schemas); a hand-coded transition table (the spec data is machine readable and the Python port already generated it); using the server package's engine (internal, entangled with locks and subscriptions, no persistence); a `[DataType]` attribute class for the Work Master extension (works, but not publishable as an OPC UA model); V2 only (V1 is cheap with the neutral model).

## D-35 Processor authoring API: attributes, a source generator, explicit registration underneath {#d-35}

Status: decided (2026-11-08, S06; checkpoint with the maintainer on the example processors). Answers Q-STRAT-4 and refines D-25.

Decision: a processor is a `partial` class marked `[Processor(binding, Id = ...)]` with constructor injection and no base class. Handlers are methods: `[Incoming]` (payload type is the first parameter), `[Outgoing(typeof(T))]` (type-id keyed payload factory, P-13), `[OnResponse]`, `[OnExpired]`, `[Fallback]`, and lifecycle methods `[OnInitialize]`, `[OnStarted(Singleton = true)]`, `[OnShutdown]`. Payloads are records with `[CloudEventType(id, Schema, Codec)]`; properties marked `[CarriedAs(name)]` travel as CloudEvent attributes and are restored on the response (E-4). Parameters are bound at build time: the payload, `EventContext`, `[Scope]`, `[SubjectPart(i)]`, `CancellationToken`, services. Return values: a payload, enumerables, `Replies`, `Task`/`ValueTask` of those, `void`, `null`. A source generator (S08) emits the `ProcessorDescriptor` and the payload descriptors; building the descriptor by hand stays possible. Analyzers report wrong signatures with `MDCS0xx` diagnostics (class not partial, no payload parameter, duplicate type id, unsupported parameter or return type, payload without `[CloudEventType]`).

* `[Incoming]` and `[Outgoing]` name the direction; the message intent (data, event, command, meta) is a separate dimension. `[Command]` was rejected because incoming handlers also receive commands.
* `[Outgoing]` stays because the SFC dispatches push commands by type id and free-form parameters (`sfc_engine.py`), without a compile-time payload type. The factory receives a `CommandRequest` (scope, job, action key, command id, correlation id, parameters) and returns the payload; `IOutgoingCommands.DispatchAsync(typeId, request)` replaces the engine's processor and payload-class lookup. Application-initiated events use `IEventPublisher`. `trigger_outgoing_event` is dropped.
* The Python pre/post callback hooks become `IProcessorFilter`, registered explicitly. The SFC hooks (`register_*_handler`) become `IActionSink`. Python's `response()`/`takeover` is replaced by constructors/`with` and the pipeline's copy of carried attributes.
* The test harness (`MicroDCS.Testing`, S08) drives a processor without broker or Valkey.

Consequences: stubs in `MicroDCS.Core/Authoring`; the generator and analyzers are S08. Python parity rows P-1 to P-19 in [core contracts](research/core-contracts.md). Open for S08: the exact `MDCS` diagnostic list and the generator's incremental design.

## D-36 Envelope and payload codec seam {#d-36}

Status: decided (2026-11-08, S06). Applies D-14, D-34.

Decision: `EventEnvelope` is a plain record (typed attributes, `ReadOnlyMemory<byte>` data, string extensions, `EnvelopeError`). The CloudEvents SDK is used only behind `IEnvelopeCodec` (carrier of string attributes + content type + data, binary content mode) and later structured mode; each transport builds its carrier. The codec owns attribute names, parsing, the collision guard (extensions named like standard attributes are dropped, E-3) and the `mdcserrorcontext` text form (`k=v,k=v`, D-30). Payloads are described by `IPayloadDescriptor` (type id, schema, content type, decode, encode with carried attributes). Codecs: `Json` (System.Text.Json with generated `JsonTypeInfo`), `OpcUaJson` verbose for job models (D-34), `Binary`. MessagePack is gone. Event ids are free strings (no UUID requirement, A-6). Decode errors name fields, never values (`PayloadException`).

Consequences: `IEnvelopeCodec` and `IPayloadDescriptor` in `MicroDCS.Core/Messaging`; implementations in S08 (codec on upstream CloudEvents) and S16 (OPC UA codec). Wire attribute names may differ from Python (D-2).

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

## D-40 Inspection contracts and management listener {#d-40}

Status: decided (2026-11-08, S06). Details for D-19 (which stays a proposal until S11 delivers the first slice). Answers the S06 part of Q-OPS-1.

Decision: `IInspector` (name, `InspectAsync(InspectionQuery)` returning a capped page with cursor) is the contract; entries are `Kind`, `Key` and fields whose values carry a `Sensitivity` (`Public`, `Identifier`, `Payload`) set at the source. `Redaction.Apply` drops `Payload` values unless `include_payloads` is on, and adapters call it before output. Inspectors use fixed-size buffers and cost nothing when inspection is off. **One management listener** (Kestrel), loopback `127.0.0.1:8081` by default, serving `/health/live`, `/health/ready`, `/metrics`, the versioned REST API under `/api/v1` and, when the optional package is referenced and enabled, MCP under `/mcp`. Non-loopback binds need bearer authentication (R-8); Kubernetes uses the pod address with a NetworkPolicy and probes on the same port. Read, write and maintenance tool groups, plan/apply and snapshot design are as in D-19.

Consequences: `MicroDCS.Core/Inspection` holds the abstractions; S11 builds the adapters and the first inspectors. Open: probes on a separate port if a deployment needs them; S21 refines the REST contract.

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

Consequences: S15 builds the core and the in-memory host against these rules; S19 and S20 build the Valkey host. The outage test and the two-replica test in S20 are acceptance tests.
