# Port to .NET

Status: draft v2, restructured on 2026-11-07. The Python implementation is frozen on the `python-archive` branch. Next session: **S02**.

## How to use these files

| File | Content | Read when |
|---|---|---|
| `index.md` (this file) | Rationale, libraries, decision summary, open questions, session overview, working rules | Every session |
| [`sessions.md`](sessions.md) | One block per session with tasks and exit criteria | Every session: the own block |
| [`decisions.md`](decisions.md) | Every decision in full (context, decision, consequences, open points) | When the own block or a question refers to a decision |
| [`log.md`](log.md) | Session log, newest last | Every session: the last entry |
| `research/` | Background and findings behind the decisions | Only the files the own session block links to |

## Rationale

Main drivers for doing this now, before investing more in the Python implementation:

1. **OPC UA .NET stack 2.0** ([What's new](https://github.com/OPCFoundation/UA-.NETStandard/blob/master/docs/WhatsNewIn2.0.md)). Direct model generation for standard companion specs (plus the generator itself), and the ISA-95 libraries abstract Job Control V1/V2 ([ISA95 guide](https://github.com/OPCFoundation/UA-.NETStandard/blob/master/docs/ISA95.md)). V1 has been ignored so far. MIT licensed since v2 (D-21).
2. **MQTT library risk.** `aiomqtt` is maintained by a single developer, development looks stalled, and the OpenTelemetry instrumentation for it would have to be maintained here (today a git-pinned fork, see `review.md`).
3. **More protocols than MQTT.** Northbound: NATS and AMQP. Southbound: Zenoh.
4. **Drop MessagePack-RPC** (transport, sidecar protocol and the `msgpack` serialization path).
5. **Performance and strong typing.** .NET Native AOT, span-based/low-allocation code paths where they matter.
6. **Fully open-source components.** Replace Redis with Valkey. The Linux Foundation maintains the `valkey-bundle` image, which pre-packages valkey-json, valkey-search and valkey-bloom.
7. **HTTP endpoints in-process** (health, metrics, query API) instead of the sidecar approach that works around Python shortcomings.

## Planned libraries

| Purpose | Library | Open point |
|---|---|---|
| Valkey/Redis client | StackExchange.Redis (**decided**, D-7) | **No blocking commands at all** (`XREADGROUP ... BLOCK`, `XREAD ... BLOCK` are excluded on purpose), so stream consumption polls (S04, S07). No JSON or search API (`JSON.*`, `FT.*` are used today), see Q-DATA-1. AOT: v2.7+, scripts as plain strings with `RedisKey[]`/`RedisValue[]` (no `LuaScript.Prepare` with anonymous-type parameters) |
| MQTT | MQTTnet | Conditional AOT: byte payloads or JSON source generators only. MQTT 5 features relied on: user properties, shared subscriptions, manual ack, message expiry, retained |
| NATS | NATS.Net | AOT friendly. JetStream vs. core, CloudEvents binding |
| AMQP | AMQPNetLite | AOT needs manual trimmer annotations (`[DynamicDependency]`). AMQP 1.0 only (RabbitMQ 4+, Artemis, Azure Service Bus; not RabbitMQ 0-9-1) |
| Zenoh | **No official binding** (`eclipse-zenoh/zenoh-csharp` is a placeholder, checked 2026-11-06). **Preferred candidate: [`sanri/zenoh-csharp`](https://github.com/sanri/zenoh-csharp)** (NuGet `Zenoh-CS` 0.5.0, a fork of the Eclipse repository): wraps zenoh-c 1.10.0 with a version mapping per release; EPL-2.0 OR Apache-2.0; covers session, publisher, subscriber, queryable, querier, liveliness and shared memory, with attachments in the C declarations; `netstandard2.0`, classic `DllImport`; native zenoh-c not bundled (installed on the host, built with `UNSTABLE_API` and `SHARED_MEMORY`); one maintainer, 19 stars, a commit gap from 2025-12 to 2026-08. Fallbacks: ZenohDotNet (third-party, MIT, one author, bundles the native library), or a thin binding of our own over zenoh-c | S04: publish-only test of `Zenoh-CS` (AOT warnings, whether managed attachments work, zenoh-c in the image); if the warnings are not clean, contributing upstream (or a fork) beats writing our own binding. Choice recorded as a decision before S25 |
| OPC UA | OPC Foundation UA-.NETStandard 2.x (`Opc.Ua.ISA95*`), MIT since v2 (D-21) | AOT supported via source-generated models ([Native AOT](https://github.com/OPCFoundation/UA-.NETStandard/blob/master/docs/NativeAoT.md#publish-an-application)); **do not use** the Reflection.Emit complex-type builder ([Complex Types](https://github.com/OPCFoundation/UA-.NETStandard/blob/master/docs/ComplexTypes.md#type-builders)). Open: version stability, NodeSet v2 tooling for the Work Master extension (S05) |
| CloudEvents | `CloudNative.CloudEvents` 2.9.0 (July 2026) | **Not AOT compatible today** (open issue and PR #381), no MQTTnet 5 / NATS / Zenoh binding. D-14: use upstream, stay AOT-ready, fork late and only if blocked |
| MCP | `ModelContextProtocol.AspNetCore` 2.x | Marked AOT compatible; attribute-based tool discovery not verified under AOT (S04) |
| ~~MessagePack-CSharp~~ | not needed | MessagePack is dropped (driver 4) |

## Porting baseline

Size of the Python implementation on `python-archive` (about 11.4k lines of Python in `src/microdcs`):

| Area | Python source | LOC |
|---|---|---|
| Core, config, app wiring | `__init__.py`, `core.py`, `app/__main__.py` | ~850 |
| CloudEvent and processor framework | `common.py`, `dataclass.py` | ~1400 |
| MQTT transport (with OTEL variants) | `mqtt.py` | ~900 |
| MessagePack-RPC transport | `msgpack.py` | ~750 (**dropped**) |
| Redis DAOs, dedupe, Lua scripts, streams | `redis.py` | ~1600 |
| SFC engine | `sfc_engine.py` | ~1450 |
| Machinery Jobs processor and publisher | `processors/machinery_jobs.py`, `publishers/` | ~1300 |
| Demo processor | `processors/greetings.py` | ~100 |
| Models (generated plus ext/mixins) | `models/` | ~2400 (mostly generated) |
| Code generator | `scripts/dataclassgen.py`, template | ~400 (replaced by the OPC UA generator?) |
| App scaffolder (`init`) | `scripts/init.py`, `scripts/init/` templates | ~175 plus ~720 template lines (replaced by `dotnet new` templates, S26) |

## Decisions

Full text in [decisions.md](decisions.md). Status: **decided**, **proposal** (direction agreed, details open), **revised** (decided, replaced an earlier version).

| Id | Status | Decision |
|---|---|---|
| [D-1](decisions.md#d-1) | decided | Big-bang rewrite; Python is frozen |
| [D-2](decisions.md#d-2) | decided | Clean break for wire format and Valkey key schema |
| [D-3](decisions.md#d-3) | decided | Port the existing protocol abstraction (processor-facing model); extract the pipeline |
| [D-4](decisions.md#d-4) | decided | OPC UA for models only, no OPC UA server or transport |
| [D-5](decisions.md#d-5) | revised | Valkey compatibility test only, no Python changes (done, see research) |
| [D-6](decisions.md#d-6) | decided | `review.md` routing findings are redesigned in .NET |
| [D-7](decisions.md#d-7) | decided | StackExchange.Redis is the Valkey client |
| [D-8](decisions.md#d-8) | decided | Not in production: no compatibility, migration or deprecation concerns |
| [D-9](decisions.md#d-9) | decided | Push-command response routing stays instance-affine; persist the dispatch deadline |
| [D-10](decisions.md#d-10) | decided | Each protocol's native features, no lowest-common-denominator emulation |
| [D-11](decisions.md#d-11) | decided | Zenoh is a full CloudEvents transport binding |
| [D-12](decisions.md#d-12) | decided | HA is a deployment concern, later |
| [D-13](decisions.md#d-13) | decided | Dedupe-key trust comes from transport access control |
| [D-14](decisions.md#d-14) | revised | CloudEvents: upstream SDK, AOT-ready, fork late only if blocked |
| [D-15](decisions.md#d-15) | decided | Docker Compose integration tests, identical locally and in CI |
| [D-16](decisions.md#d-16) | revised | Python preserved on `python-archive`; the port continues on `main` |
| [D-17](decisions.md#d-17) | proposal | SFC semantics as a pure, dependency-free core library; late-response rule and outcome-based transition conditions decided |
| [D-18](decisions.md#d-18) | proposal | SFC time model: timestamped events, absolute deadlines, shared due-index; outage grace and recipe timeout decided |
| [D-19](decisions.md#d-19) | proposal | Inspection layer plus optional dev MCP server, built early |
| [D-20](decisions.md#d-20) | decided | Gitleaks, Trivy and SonarQube Cloud in CI; scanners as pinned containers |
| [D-21](decisions.md#d-21) | decided | OPC UA .NET stack 2.x is MIT licensed; no licence constraint from driver 1 |
| [D-22](decisions.md#d-22) | decided | One GitHub stack per milestone, one squash-merged PR per session |
| [D-23](decisions.md#d-23) | decided | Claude Code is the agent tool; Copilot files are replaced |
| [D-24](decisions.md#d-24) | decided | MicroDCS is MIT licensed (Python archive stays LGPL-2.1) |
| [D-25](decisions.md#d-25) | decided | The processor authoring experience is a primary goal |
| [D-26](decisions.md#d-26) | decided | `valkey-bundle` is the Valkey image |
| [D-27](decisions.md#d-27) | decided | Fail-fast startup (carried over from Python) |
| [D-28](decisions.md#d-28) | decided | Explicit error on queue backpressure (carried over from Python) |
| [D-29](decisions.md#d-29) | decided | No extra size limit on `custommetadata` (carried over from Python) |
| [D-30](decisions.md#d-30) | decided | Comma-separated list values in configuration, if lists stay strings (carried over from Python) |
| [D-31](decisions.md#d-31) | decided | .NET conventions: `MicroDCS` prefix, xUnit v3/MTP, Shouldly, NSubstitute, M.E.Logging and DI, style |

Next free id: **D-32**.

## Open questions

Each question ends in a decision or a finding recorded by its owner session. Answered questions are kept struck through with the decision id.

| Id | Question | Owner |
|---|---|---|
| ~~Q-STRAT-1~~ Migration style | → D-1 | — |
| ~~Q-STRAT-2~~ Python's future | → D-1, D-5, D-16: archived on `python-archive`, security fixes only | — |
| ~~Q-STRAT-3~~ Port goal | → D-3 | — |
| Q-STRAT-4 Public API | Goal decided: a polished authoring experience (D-25). Open: the concrete .NET shape of `@incoming`/`@outgoing` handlers and dataclasses (attributes plus source generators, analyzers) | S06 |
| Q-STRAT-5 Packaging and release | NuGet package split (core, MQTT, NATS, AMQP, Zenoh, OPC UA models, SFC), semver, `dotnet new` templates replacing the Python scaffolder. Publishing needs a released upstream CloudEvents package (D-14). Licence: MIT (D-24), OPC UA dependency fine (D-21) | S26 |
| Q-ARCH-1 Native AOT | One AOT-published app with all chosen libraries (StackExchange.Redis, MQTTnet, NATS.Net, AMQPNetLite, Zenoh candidate, MCP SDK, Kestrel minimal APIs), `IL2026`/`IL3050` as pass criterion. If AMQPNetLite cannot be made clean, AMQP goes into its own optional package | S04 |
| Q-ARCH-2 Serialization | System.Text.Json source generation vs. OPC UA JSON encoders; wire format for job orders (OPC UA JSON vs. plain JSON from the generated schema) | S05 |
| Q-ARCH-3 Hosting and configuration | `Microsoft.Extensions.Hosting` as replacement for `MicroDCS`/`SystemEventTaskGroup`/`AdditionalTask`; keep `APP_*` names or move to `IConfiguration` conventions (`APP__MQTT__HOSTNAME`) | S06 |
| Q-ARCH-4 State machine | Stateless, the OPC UA ISA-95 job state machine, or source generated? Must persist state and support the OPC UA transitions/methods | S05 |
| ~~Q-ARCH-5~~ OPC UA server | → D-4 | — |
| Q-ARCH-6 Abstraction fit | Extension points exist, the transport-neutral pipeline does not. Decide A-1 to A-8 ([abstraction mapping](research/abstraction.md)) | S06 |
| Q-ARCH-7 Delivery semantics | Which transports need the Valkey dedupe claim (everything with redelivery), and what the Zenoh command path relies on (SFC re-dispatch, D-9) | S06 |
| Q-ARCH-8 SFC multi-instance model | How the CAS scripts relate to the pure transition; scope discovery, retry limit, stream trimming, key identity ([SFC core](research/sfc-core.md)) | S07 |
| Q-ARCH-9 Deployment modes | Keep the processor/publisher split and single-container mode? Effect of the in-process HTTP surface | S18 |
| ~~Q-ARCH-10~~ Southbound | → D-11. Remaining: CloudEvents-over-Zenoh mapping (attachment vs. structured), Zenoh query/reply for commands, router vs. peer mode | S25 |
| Q-DATA-1 Documents and secondary indexes | Today: valkey-json for documents (`JSON.SET` whole-document writes from Python; partial path updates only inside the four SFC Lua CAS scripts) and valkey-search (`FT.CREATE ... ON JSON` with TAG fields `$.JobOrderID`, `$._normalized_state`, `$._scope`; `FT.INFO`; two `FT.SEARCH` queries). Call sites: Lua 9 `JSON.SET`, 4 `JSON.GET`, 1 `JSON.TYPE`; Python 4 whole-document sets, 7 reads. Options: (a) plain strings in a hash `{v, doc}` with explicit secondary indexes (sets/sorted sets per scope and state) written in the same atomic script, no modules, plain `valkey/valkey` image; (b) keep valkey-json and valkey-search through raw `Execute`, with the Valkey-specific `FT.SEARCH` reply parser ([Valkey findings](research/valkey-findings.md)). The image is `valkey-bundle` either way (D-26). With D-17 (whole versioned state document, CAS on the version) partial path updates are not needed, and Lua can decode JSON with the built-in `cjson` where a script must read a field | S04 measures, S07 decides |
| ~~Q-DATA-2~~ Client library | → D-7. Remaining: no blocking reads (poll, poll plus pub/sub nudge, or raw `BLOCK` on a dedicated multiplexer); consumer groups, `XAUTOCLAIM`, Lua `EVAL`, reconnect behaviour | S04 measures, S07 decides |
| ~~Q-DATA-3~~ Valkey before the port | → D-5 | — |
| ~~Q-DATA-4~~ High availability | → D-12. Remaining: every write path idempotent under unknown outcome (R-4) | S09 |
| Q-OPS-1 HTTP surface | Liveness/readiness, metrics, job state query API (R-19), OpenAPI; listener layout shared with inspection and MCP (D-19) | S06 layout, S21 |
| Q-OPS-2 Container and Kubernetes | Chiseled/distroless AOT image, manifest, Zenoh native library in the image, hardening from `docs/security.md` | S22 |
| Q-OPS-3 Security | Authn/z per transport (NATS accounts/NKeys, AMQP SASL, Zenoh ACLs), the level-0 scope rule (R-6) | S10, S22 |
| Q-TEST-1 Test strategy | Compose setup decided (D-15). Python tests are the specification for S16, S15 and S19. Open: golden-file payload fixtures with D-2 | S02, S23 |
| Q-TEST-2 Behavioural parity | Parity scenarios of the Python integration tests as wire-level e2e tests in compose, re-specified (D-2) | S23 |
| ~~Q-TEST-3~~ Documentation | → D-16: no side-by-side docs; pages are ported and the "not yet ported" notices removed | S23 |
| OpenTelemetry coverage | What .NET, MQTTnet, NATS.Net and StackExchange.Redis give out of the box; which custom handler variants disappear | S04, S10 |
| ISA-95 via OPC UA, Work Master extension, JSON Schema generation | Plain typed models without a server? Work Master as NodeSet v2 plus generated models? Can [SchemaGeneration](https://github.com/OPCFoundation/UA-.NETStandard/blob/master/docs/SchemaGeneration.md) replace `dataclassgen`? | S05 |
| R-12 Operational limits | Document per-transport message size limits and Kestrel request limits | S22 |
| R-20 Multi-tenancy | NATS accounts in S14, the other transports and the key-prefix convention in S22 | S14, S22 |
| R-21 Support model, zones/conduits | Language-independent documentation | S22 |
| ~~D-11 docs update~~ | Done in S01 | — |
| CloudEvents benchmark (R-18) | BenchmarkDotNet on the envelope once the pipeline hot path exists | S10, baseline in S23 |

## Sessions

Each session fits one Claude Code session. Sessions marked "parallel" can run in any order after their prerequisites. Details in [sessions.md](sessions.md).

| Session | Goal | Depends on | Status |
|---|---|---|---|
| **M0 Setup** | | | |
| S01 | Repository reset: remove Python, scaffolding, Claude Code setup, WIP docs site | none | done |
| S02 | CI, container, compose harness, CloudEvents AOT measurement | S01 | open |
| S03 | Security and quality scanning (D-20) | S02 | open |
| **M1 Foundation** | | | |
| S04 | Spike: library stack, AOT and Valkey behaviour | S02 | open |
| S05 | Spike: OPC UA ISA-95 for models only | S02 (parallel to S04) | open |
| S06 | Design: core contracts (envelope, processor API, hosting, pipeline, transports, inspection) | S04 | open |
| S07 | Design: data layer and SFC rules (key schema, stream consumption, SFC core, time, retries) | S04, S06 | open |
| S08 | Core: envelope, processor API, hosting, in-memory transport, test harness | S06 | open |
| S09 | Valkey layer | S07 (parallel to S08) | open |
| S10 | Pipeline (A-1) | S08, S09 | open |
| S11 | Inspection layer and MCP read tools | S09, S10 | open |
| S12 | Maintenance tools with snapshot and restore | S11 | open |
| S13 | MQTT transport | S10 | open |
| S14 | NATS transport (abstraction check) | S13 | open |
| S15 | SFC core library (pure), in-memory host | S07 (parallel to S08 to S14) | open |
| **M2 Parity gate** | | | |
| S16 | Job models and state machine | S05, S08 | open |
| S17 | Machinery Jobs processor and job DAOs | S10, S16 | open |
| S18 | Job order publisher and state topics | S13, S17 | open |
| S19 | SFC Valkey host: work stream, outbox, due-index, push commands | S09, S15, S17 | open |
| S20 | SFC pull events, recovery, multi-instance | S19 | open |
| S21 | HTTP surface and observability | S17 | open |
| S22 | Container, Kubernetes, supply chain, security review | S21 | open |
| S23 | Docs port, parity acceptance, release | all of M2 | open |
| **M3 Extensions** | | | |
| S24 | AMQP transport | S14 | open |
| S25 | Zenoh transport and CloudEvents-over-Zenoh mapping | S14 | open |
| S26 | Packaging, NuGet release, `dotnet new` templates | released upstream CloudEvents package | open |
| Event | PR #381 merged or AOT-clean CloudEvents released | upstream | Check at S02, S06, S08, S26 (D-14) |

Session ids were renumbered on 2026-11-07; the mapping from the old ids is in the [log](log.md).

## Working rules for every session

* **Size rule:** one coherent area, roughly up to 1.5k lines of new code including tests, or a spike whose output is only findings. If a session turns out too large, stop at a clean boundary and write the remainder into the handoff.
* **Branches and PRs (D-22):** one milestone is one GitHub stack (`gh stack`); each session is one PR in that stack, on a branch named after the session (for example `s04-library-spike`), with the session id in the PR title. A PR targets the branch of the PR below it; the bottom PR targets `main`. Merge bottom-up with **squash**, as soon as a session is done and its checks are green, so `main` gets one commit per session and the stack stays short. Parallel sessions go into the stack in the order they are started. A session that needs a second PR adds it to the stack on top of its first one. The session's `log.md` entry and status change are part of its own PR. Each PR is also assigned to the GitHub milestone (M0 to M3) for the progress view.
* **Start:** read this file, the own block in `sessions.md`, the last `log.md` entry, and the research files and decisions the block links to. `CLAUDE.md` and the path rules load by themselves. Do not re-research settled decisions; if one is wrong, record the change as a revision in `decisions.md` instead of silently deviating.
* **Definition of done (code sessions):** build with zero warnings (warnings as errors), unit tests green, AOT publish without `IL2xxx`/`IL3xxx` warnings for the touched projects, compose integration tests green for touched transports, public API has XML docs, no secrets or payloads in logs; once S03 exists the scanners are green (Gitleaks clean, Trivy without fixable HIGH/CRITICAL, Sonar quality gate on new code; waivers only with reason and expiry, D-20); once S11 exists every new stateful component ships its read-only inspector (D-19).
* **End (mandatory, last task of every session):** append a `log.md` entry, set the session's status in the table above, update the affected decisions and open questions, and write the "Next session pickup" so that a fresh session needs nothing from chat history.
* **Decisions:** new ones take the next free id (above) and get a full entry in `decisions.md` plus a row here. A revision keeps the id and adds a dated "Revised" line.
* **Keep the plan small:** log entries stay short (about 15 lines); findings and analysis go to `research/` or the regular docs.

## Out of scope for porting / later

* **Job spec client:** southbound implementation of the Job Control spec. Use the OPC UA library's server example as the integration-test partner.
* **Tightening client** (IJT).
