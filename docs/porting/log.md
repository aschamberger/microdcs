# Session log

Newest entry last. Keep each entry to about 15 lines; findings that others depend on go to `research/` and are linked.

Template:

```text
## S<nn> <title> (<date>, <branch/PR links>)
Status: done | partial (what remains)
Done: <3-6 bullets>
Findings: <facts learned that others depend on, with evidence: command, output summary, link>
Decisions: <new or changed D-ids>
Changed in the plan: <files and sections updated>
Open questions: <new ones>
Post-merge checks: <what must hold after the squash merge; the next session verifies and records it>
Next session pickup: <session id, exact first steps, anything to prepare (manual steps for the maintainer)>
```

## Pre-port: Valkey compatibility run (2026-11-07)

Status: done.
Done: Python integration tests run against a throwaway `valkey/valkey-bundle` container, nothing committed (D-5).
Findings: `JSON.*`, Lua, Streams, `FT.CREATE`/`FT.INFO` and ACLs work; `FT.SEARCH` returns a flat array even on RESP3 and breaks the Python code. Details in [Valkey findings](research/valkey-findings.md).
Decisions: D-5 done.

## Freeze and plan restructure (2026-11-07)

Status: done.
Done:

* `python-archive` created from `main` at `12c1415` unchanged, no tag. Stale local refs removed (`feature/aiomqtt-v3`, already deleted on GitHub and fully merged; an orphaned `remote/main` ref). Five Dependabot `uv` branches remain open on GitHub (closed in S01).
* `porting.md` and `porting-review.md` replaced by `docs/porting/` (plan, decisions, sessions, log, research). The review findings were applied to their target sections; the review file was not kept.
* No Gitleaks history scan before the freeze: the project never used credentials; the first full-history run in S03 covers the shared history.

Decisions: D-21 (OPC UA stack is MIT since v2). D-7, D-17, D-18 gained the review points.
Session ids renumbered (old → new): S0a removed (done above); S0b → S01, S0c → S02, S0d → S03, S1 → S04, S2 → S05, S3 → S06 (core contracts) and S07 (data layer, SFC rules), S4 → S08, S5 → S09, S6 → S10, S6b → S11 (inspection, MCP read tools) and S12 (maintenance), S7 → S13, S8 → S14, S12 → S15 (pure core) and S19 (Valkey host), S9 → S16, S10 → S17, S11 → S18, S13 → S20, S14 → S21, S15 → S22, S16 → S23, S17 → S24, S18 → S25, S19 → S26.
Next session pickup: S01 (`python-archive` is on GitHub; the plan itself was merged through its own PR before S01). The first S01 commit removes Python.

## Plan updates (2026-11-07, later)

Status: done.
Decisions: D-22 (one GitHub stack per milestone, one squash-merged PR per session), D-23 (Claude Code replaces the Copilot setup). Carried-over Python decisions now start at D-24.
Findings: `sanri/zenoh-csharp` (NuGet `Zenoh-CS` 0.5.0) wraps zenoh-c 1.10.0 and is the preferred Zenoh candidate; checked in S04.
Changed in the plan: library table, S01, S03, S04, S11, S25, S26, working rules; research files on CI scanning and MCP.
Done: GitHub CLI 2.103.3 installed in `~/.local/bin` (checksum verified), logged in, `gh auth setup-git`; `python-archive` is on GitHub at `12c1415`; Dependabot `uv` PRs #31 to #35 closed with a comment.
Changed in the plan: S01 switches GitHub Pages from the `gh-pages` branch (legacy mode, `peaceiris/actions-gh-pages`) to deployment through Actions.

## Maintainer decisions (2026-11-07)

Decisions: D-24 (MIT licence), D-25 (authoring experience is a primary goal), D-26 (`valkey-bundle` image); D-18 revised (outage grace, deadline from the recipe's `timeout_seconds`). Carried-over Python decisions now start at D-27.
Done: .NET SDK 10.0.403 installed in `~/.dotnet` with `dotnet-install.sh`; `clang` and `zlib1g-dev` installed by the maintainer. `~/.dotnet` still has to be added to `PATH` (maintainer).
D-17 revised: late-response rule (finding 4) and outcome-based transition conditions (IEC 61131-3 compatible: named Boolean transitions, priority order, `"true"` as default) decided.
Open for S07: `max_attempts` default, outcome mapping per `type_id`, routable `timeout`/`error` outcomes.
Done: GitHub secret scanning and push protection enabled (2026-11-07); no alerts at the time of the check.

## S01 Repository reset, scaffolding, agent setup (2026-11-07, [#38](https://github.com/aschamberger/microdcs/pull/38), follow-up `s01-handoff`)

Status: done.
Done:

* Python removed from `main` (first commit); MIT `LICENSE`, README banner, `SECURITY.md` without the `pyproject.toml` reference. `compose.yaml` on `valkey-bundle`, images pinned by digest, Python `app` profile dropped. Dependabot: `nuget` replaces `uv`, `pip` for `/docs`, plus `docker-compose` for the digest pins.
* Scaffolding: `global.json` (10.0.403, MTP runner), `Directory.Build.props`, central packages, `nuget.config` with source mapping, `.editorconfig`, `MicroDCS.slnx`, `MicroDCS.Core` and `MicroDCS.Core.Tests` (one test). Build, test, `format --verify-no-changes` and locked restore pass with zero warnings.
* Docs: zensical 0.1.0 pinned with hashes (`docs/requirements.txt`); `docs.yml` builds and deploys through Actions. WIP announcement bar (`overrides/main.html`), "Not yet ported" on every Python page, nav "Port to .NET (WIP)"; tables, anchors and mermaid checked in a local build. D-11 in "Southbound Connectivity"; `development.md` has the .NET toolchain and the cloud setup script.
* Claude Code: `CLAUDE.md` (102 lines), four path rules, `/session-start` and `/session-end` (stack workflow), `.claude/settings.json` with allowlist and hooks (restore at session start, whitespace format of edited `*.cs`). `.mcp.json` left absent until S11; no `.devcontainer`.
* GitHub: squash merge only, delete head branches, `main` protected (PR required, 0 approvals, admins included, linear history, conversation resolution), milestones M0 to M3, `gh-stack` v0.3.0 installed (official extension; stacked PRs usable with gh 2.103.3).

Findings: `clang` was missing despite the earlier log; the maintainer installed it (clang 18.1.3), and a throwaway AOT console publish ran. Dependabot needs the `docker-compose` ecosystem for digests in compose files. The format hook uses `dotnet format whitespace --folder` (about 1.4 s) instead of a full `dotnet format` (about 4 s).
Decisions: D-27 to D-30 carried over from Python ([sorted list](research/python-design-decisions.md)), D-31 .NET conventions (S01 defaults taken).
Changed in the plan: index (D-27 to D-31, session status), decisions, research file added.
Open questions: none new. Not verified: the cloud setup script and NuGet reachability from a claude.ai/code environment (needs a cloud session).
Post-merge (done): Pages source switched to Actions, `github-pages` environment allows `main` only, #38 squash-merged (`8748d43`), docs workflow deployed (build and deploy green), live site shows the banner and "Port to .NET (WIP)", `gh-pages` deleted, Dependabot alerts and security updates on (alerts were off and are a prerequisite). `gh stack merge` refuses a single PR ("not a stacked pull request": a GitHub stack needs two or more PRs); `gh pr merge --squash` used instead, now in `/session-end`. Three stale `dependabot/uv/*` branches without PRs remain on GitHub (harmless).
Next session pickup: S02. Run `/session-start S02`. All S01 PRs are merged, so start a new M0 stack from `main`: `git switch main && git pull --ff-only && gh stack init s02-ci-container`. Workflow names change in S02 (`docs.yml` is already named `docs`; the new .NET `ci.yml` should be named `ci`) before required checks are added to the `main` protection. Maintainer, optional: paste the setup script from `docs/development.md` into the claude.ai/code environment and run `dotnet restore` in a cloud session to verify NuGet reachability.

## S02 CI, container, compose harness, CloudEvents AOT measurement (2026-11-07, [#40](https://github.com/aschamberger/microdcs/pull/40), follow-up [#41](https://github.com/aschamberger/microdcs/pull/41))

Status: done.
Done:

* `ci.yml` (workflow `ci`) has three jobs. `dotnet`: locked restore with audit, Release build, tests, format, `dotnet package list --vulnerable`. `container`: AOT image, `microdcs smoke` in it, non-root and no-shell check. `integration`: compose harness plus the tests container's own exit code. All three ran green on #40.
* `src/MicroDCS.App` (`microdcs smoke`); `scripts/aot-publish` with the D-14 allowlist; `Dockerfile` from `sdk:10.0-noble-aot` to `runtime-deps:10.0-noble-chiseled`, digest-pinned, uid 1654, no shell, 25.5 MB.
* `compose.test.yaml` (Valkey and Mosquitto on ports 6380/1884, ephemeral) plus `tests/MicroDCS.IntegrationTests` (raw Valkey `PING`, MQTT 5 CONNACK). They skip on the host without `MICRODCS_TEST_*`; in compose `--fail-skips on`.
* Fixed an S01 bug in `Directory.Build.props`: the `src/` and `tests/` groups matched `Contains('/src/')` on the absolute path, so every project matched in a `/src` container build. The match now uses the repo-relative path. `src/` projects restore `linux-x64;linux-arm64`, so a RID publish works in locked mode.

Findings: the CloudEvents 2.9.0 AOT binary works at runtime, but only with source-generated `JsonSerializerOptions` passed to the formatter ([measurement](research/cloudevents.md#aot-measurement-s02-2026-11-07)). PR #381 is open and 2.9.0 is the latest release. `compose up --exit-code-from tests` exits 0 when `tests` never started, and a one-shot service aborts the run early; both are handled in `ci.yml` and documented in `docs/development.md`. The arm64 AOT link needs an arm64 host or a cross toolchain; follow-up #41 adds native arm64 CI (D-32).
Decisions: D-14 gained the measured result and the codec rule (no revision). Follow-up #41: D-32 (linux-x64 and linux-arm64, native CI builds), D-22 revised (no post-merge follow-up PR; the next session verifies "Post-merge checks").
Changed in the plan: index (status, library table, Q-TEST-1 owner S23, event row), D-14, research/cloudevents.md; `CLAUDE.md`, `docs/development.md`, `/session-end` DoD.
Open questions: none new.
Post-merge (done for #40): squash-merged (`729e8a8`), `ci` and docs green on `main`.
Follow-up #41: `container` became a matrix with `container (linux-x64)` on `ubuntu-24.04` and `container (linux-arm64)` on `ubuntu-24.04-arm`, plus an image architecture check. The session skills now use "Post-merge checks" instead of follow-up PRs.
Post-merge checks (for #41): `ci` green on `main` with all four jobs; required checks on `main` are `dotnet`, `container (linux-x64)`, `container (linux-arm64)`, `integration` (GitHub Actions app id 15368, `strict`), set before the merge.
Next session pickup: S03. Run `/session-start S03`. All S02 PRs are merged, so start a new M0 stack from `main`: `git switch main && git pull --ff-only && gh stack init s03-scanning`. S03 adds jobs to `ci.yml` and a `security.yml`; add their job names to the required checks (`gh api -X PATCH .../branches/main/protection/required_status_checks` with the full `checks` list, app id 15368). Maintainer before S03: SonarQube Cloud project and `SONAR_TOKEN` secret (Actions and Dependabot), see [CI scanning](research/ci-scanning.md).

## S03 Security and quality scanning (2026-11-07, [#42](https://github.com/aschamberger/microdcs/pull/42), self-test [#43](https://github.com/aschamberger/microdcs/pull/43) closed)

Status: done. Previous session post-merge: verified (`ci` and `docs` green on `main`, the four required checks set).
Done:

* Every `uses:` pinned by commit SHA; docs pip install wheel-only (Sonar S8541). `compose.scan.yaml` (Gitleaks 8.30.1, Trivy 0.76.2 by digest; Trivy cosign-verified), `scripts/scan`, `.gitleaks.toml` (canary rule), `trivy.yaml`, `.trivyignore.yaml`, opt-in `.githooks/pre-commit`.
* `security.yml`: `gitleaks` (PR range; full history on `main` and daily), `trivy-fs`, `trivy-image` (SARIF to code scanning, CycloneDX SBOMs `sbom-source` and `sbom-image`). The `dotnet` job in `ci.yml` runs SonarScanner for .NET with coverage (`Microsoft.Testing.Extensions.CodeCoverage`, VS coverage XML).
* `SonarAnalyzer.CSharp` as a global package reference: Sonar C# rules fail local and CI builds alike (maintainer decision). Agent rule `.claude/rules/security.md`, docs (development, security waiver table), README badges.

Findings ([details](research/ci-scanning.md#implementation-findings-s03-2026-11-07)): Sonar ran as Automatic Analysis before S03 and is now CI-based. Trivy does not check compose files or workflows. The full git history (`python-archive` included) has no Gitleaks findings. Self-test on #43: `gitleaks`, `trivy-fs` and the Sonar gate went red; a Sonar C# bug fails the build itself. Timings: gitleaks 7 s, trivy-fs 30 s, trivy-image 1.5 min, Sonar adds ~40 s to `dotnet`.
Decisions: D-20 confirmed (no revision), with corrections in the research file.
Changed in the plan: index (status, D-20 row), D-20, research/ci-scanning.md, sessions.md (S22: k8s Sonar findings); `CLAUDE.md`, `.claude/rules/`.
Open questions: none new. Optional, not added: zizmor/actionlint in CI, SonarQube MCP server for agents.
Post-merge checks: `ci` and `security` green on `main` (gitleaks scans the full history there); SonarQube Cloud `main` analysis shows C# and coverage; required checks on `main`: `dotnet`, `integration`, `container (linux-x64)`, `container (linux-arm64)`, `gitleaks`, `trivy-fs`, `trivy-image` (app 15368) and `SonarCloud Code Analysis` (app 12526), strict, set before the merge; Dependabot (Insights → Dependency graph → Dependabot) lists `compose.scan.yaml` and `.config/dotnet-tools.json`.
Post-merge fix ([#44](https://github.com/aschamberger/microdcs/pull/44)): `docs` failed on `main` because `python-version: 3.x` resolved to CPython 3.15, for which `pyyaml` 6.0.3 has no wheel, and the S03 wheel-only install refuses to build it; the docs Python is now pinned to 3.14. `ci`, `security`, the Sonar gate on `main` and Dependabot coverage of `compose.scan.yaml` and `.config/dotnet-tools.json` were verified after the merge.
Follow-up ([#45](https://github.com/aschamberger/microdcs/pull/45)): actionlint and zizmor in `scripts/scan workflows` and the required `security.yml` job `workflows`; `/session-end` unstacks merged single-PR stacks locally; optional SonarQube MCP setup documented.
Next session pickup: S04. Run `/session-start S04`. M0 is complete, so start the M1 stack from `main`: `git switch main && git pull --ff-only && gh stack init s04-library-spike`. New packages now also pass the Sonar C# rules in the build; spike code under `spikes/` is excluded from the Sonar analysis but not from the build analyzers. Run `scripts/scan` before commits touching workflows, Docker or configuration. No maintainer preparation needed.

## S04 Spike: library stack, AOT and Valkey behaviour (2026-11-07, branch `s04-library-spike`)

Status: done (Zenoh `Get` crash unresolved, handed to S25). Previous session post-merge: verified (`ci`, `security`, `docs` green on `main`; the nine required checks are set).
Done:

* One AOT console project per library plus a combined one (`spikes/`, deleted at the end), run against a temporary compose stack with NATS, zenohd and Artemis.
* Valkey: Lua string scripts, `TIME` then write, `XAUTOCLAIM` with deleted ids, stream id as event time, polling versus nudge latency, valkey-json/search versus hash plus sets, reconnect.
* MQTTnet v5 features, NATS.Net (JetStream, KV, request/reply), Kestrel with MCP over HTTP, AMQPNetLite against Artemis, Zenoh-CS against zenohd.

Findings ([library spike](research/library-spike.md)): Redis, MQTTnet, NATS.Net, Kestrel and MCP are AOT clean and work at runtime (24.3 MB together). AMQPNetLite has 13 warnings, all in `Amqp.Serialization`. Zenoh-CS: pub/sub with attachments works, `Session.Get` against a queryable crashes (`free(): invalid pointer`, AOT and JIT); suspected stale `ZGetOptions` struct, not verified because the sandbox denied building the cloned source. Nudge gives p50 1 ms against half the poll interval; no performance reason to drop valkey-json and valkey-search.
Decisions: D-33 (answers Q-ARCH-1: AMQP in an optional package, Zenoh library open).
Changed in the plan: index (status, library table, Q-ARCH-1, Q-DATA-1, Q-DATA-2, D-33), decisions, sessions (S24, S25), research/library-spike.md.
Open questions: none new. Not covered: arm64 runs (CI is the reference), ZenohDotNet.
Post-merge checks: `ci`, `security` and `docs` green on `main` (docs-only change).
Next session pickup: S05 (OPC UA ISA-95 spike, no dependency on S04) or S06 (design, depends on S04). Run `/session-start S05` or `S06`. Start from `main` after the S04 merge: `git switch main && git pull --ff-only && gh stack init <branch>`. Nothing to prepare for the maintainer; for S25 later, decide whether the Zenoh-CS fix goes upstream.

Follow-up (S04, correction): AMQPNetLite's 13 AOT warnings came from the metapackage's serializer. `AMQPNetLite.Core` alone is AOT clean and works at runtime, so D-33 was revised (no optional package for AOT reasons) and S24, the index and the library spike were corrected. The same PR carries the SonarLint connected-mode setting in `.vscode/settings.json`. Post-merge checks: `ci`, `security` and `docs` green on `main`.

## S05 Spike: OPC UA ISA-95 for models only (2026-11-07, branch `s05-opcua-isa95-spike`)

Status: done. Previous session post-merge: verified (`ci`, `security` and `docs` green on `main` for #46 and the follow-up #47; nine required checks set).
Done:

* Throwaway AOT projects (not committed) with `Opc.Ua.ISA95` 2.0.1 as plain models: V1 and V2 objects, OPC UA JSON encode and decode, schema generation, publish AOT; an own NodeSet2 model with a Work Master subtype through the SDK source generator; the stack's server package read for its state logic and V1/V2 design.
* Direction from the maintainer after the first result: NodeSet path, V1 and V2, check whether the stack's state machine would be the right thing to use; then: the transition rules come from the NodeSet (as in Python), and the extended Work Master gets its own CloudEvent type. All tried or recorded; D-34 rewritten twice.

Findings ([OPC UA spike](research/opcua-spike.md)): packages target net10.0, MIT. Models work without a server and publish AOT clean (9 to 12 MB). `JsonEncoder`/`JsonDecoder` give usable verbose JSON, System.Text.Json on the generated types does not; three codec traps. NodeSet2 plus the source generator creates a Work Master subtype that resolves the V2 model from the referenced assembly, AOT clean; as a subtype inside a job order it decodes as the base type. V1 works with the same codec. The stack has a version-neutral core and V1/V2 conversions (internal), the right architecture; its engine is lock/dictionary based and entangled with notifications. The transition rules are data in the V2 NodeSet (34 references, causes and effects; MIT, `UA-Nodeset`), including a Prepare machine the engine lacks; V1 has no state machine in its NodeSet and its behaviour differs (Stop removes the order), per the upstream guide `docs/ISA95.md`, which was read in full: neutral store contract, execution-system transitions, Annex B `ReturnStatus`, NodeSet pinning. S16 generates the table from the NodeSet.
Decisions: D-34 (answers Q-ARCH-2 for job models and Q-ARCH-4).
Changed in the plan: index (status, OPC UA row, Q-ARCH-2, Q-ARCH-4, ISA-95 row, D-34, session table), decisions, sessions (S16), research/opcua-spike.md, zensical.toml nav.
Open questions: none new. Maintainer action: ask upstream (UA-.NETStandard) whether the neutral job model, conversions and rules can be public, and whether the Work Master NodeSet2 is welcome. Not covered: binary/XML encodings, arm64, line-by-line rules comparison (S16), V1 to V2 field conversions, schema validation of incoming payloads.
Post-merge checks: `ci`, `security` and `docs` green on `main` (docs-only change).
Next session pickup: S06 (design: core contracts, depends on S04, done). Run `/session-start S06`. Start from `main` after the S05 merge: `git switch main && git pull --ff-only && gh stack init <branch>`. S06 records OPC UA JSON verbose as the job order payload in the envelope contract (D-34). The upstream question is the maintainer's; it does not block S06.

## S06 Design: core contracts (2026-11-08, branch `s06-core-contracts`)

Status: done. Previous session post-merge: verified (`ci`, `security`, `docs` green on `main` for #48; nine required checks set). CloudEvents checkpoint (D-14): PR #381 still open, 2.9.0 latest.
Done:

* Python cross-check made part of the workflow (maintainer request): `/session-start` and `/session-end` and the working rules require a parity list; S06 has about 75 rows, none open ([core contracts](research/core-contracts.md)).
* Authoring API drafted as three example processors and confirmed at a checkpoint with the maintainer (D-35); `[Outgoing]` kept after reading `sfc_engine.py` (the SFC dispatches push commands by type id).
* Compiling stubs in `MicroDCS.Core` (`Authoring`, `Messaging`, `Transport`, `Inspection`, `Hosting`), 14 unit tests for the pure rules; build, format, `dotnet test`, locked restore and AOT publish with smoke run are clean.
Findings: hidden payload fields (`_x` travel as CloudEvent attributes), the misnamed pre/post hooks, ack of duplicates, singleton `post_start` lease, five-phase shutdown and the SFC observer hooks were in no plan document; all have a disposition. `TransactionDedupeDAO` and `CounterDAO` are unused in Python and dropped.
Decisions: D-35 authoring API, D-36 envelope and payload codec seam, D-37 pipeline, transport contract, delivery and error model, D-38 address model, scope rule (level-0 rejected by default, R-6), state-topic port, D-39 hosting and configuration (`APP_` + `__`), D-40 inspection contracts and management listener.
Changed in the plan: index (decisions, Q-STRAT-4, Q-ARCH-2/3/6/7, Q-OPS-1, session table, working rule), sessions (S06 reading list, S08 block), decisions, research/core-contracts.md, zensical.toml nav, both session skills.
Open questions: none new. Not designed here on purpose: generator internals and the diagnostic list (S08), the publisher (S18), per-transport renderers (S13, S14), the OPC UA payload codec (S16).
Post-merge checks: `ci`, `security` and `docs` green on `main` (the docs build covers the new nav entry and the table fix in the decisions summary).
Next session pickup: S07 (design: data layer and SFC rules, depends on S04 and S06, both done). Run `/session-start S07`. Start from `main` after the S06 merge: `git switch main && git pull --ff-only && gh stack init <branch>`. Read the Python cross-check rule first: S07 needs a parity list for `redis.py` (key schema, Lua scripts, DAOs) and `sfc_engine.py` before any decision; S06's list covers only dedupe, the post-start lock and the push-command dispatch. `IDedupeStore` and `IStateTopicPort` (D-37, D-38) are the contracts the Valkey layer (S09) implements. S08 and S07 can run in parallel.

## S07 Design: data layer and SFC rules (2026-11-08, branch `s07-data-sfc-rules`)

Status: done. Previous session post-merge: verified (`ci`, `security`, `docs` green on `main` for #49).
Done:

* Python cross-check of `redis.py`, `sfc_engine.py`, the recipe model and the publisher's stream use: 30 plus 40 parity rows, none open ([data layer](research/data-layer.md), [SFC rules](research/sfc-rules.md)).
* Data layer decided with a written comparison: hash documents with explicit indexes, no valkey-json or valkey-search; key schema, id-only identity, scope registry, version-guarded idempotent writes, stream trimming rule, nudge plus fallback poll.
* Maintainer decision: Valkey cluster mode is out of scope for good; resilience through independent island deployments (D-12 revised), so no hash tags.
* SFC open points of D-17 and D-18 settled in D-44; both are now decided.
* Compiling stubs: `MicroDCS.Sfc` (`ActionKey`, `SfcOutcome`, `SfcCapabilities`, `IRecipeInterpreter`) and `MicroDCS.Valkey` (`KeySchema`) with two test projects.
Findings: the Python engine never reads `qualifier`, `timeout_seconds`, `condition` or `recovery_idle_threshold_s`; it reloads the work master at every step (a changed work master changes running jobs); it acks failed work items; the finish script breaks JSON on a quote in the error text.
Decisions: D-41 to D-44; D-12 revised; D-17, D-18 decided.
Changed in the plan: index (decisions, Q-DATA-1/2/4, Q-ARCH-8, session table), sessions (S09 block), decisions, two new research pages, zensical nav.
Open questions: none new.
Post-merge checks: `ci`, `security` and `docs` green on `main`; the nine required checks still match the workflow job names (new test projects are covered by `dotnet test`).
Next session pickup: S08 (core, depends on S06), S09 (Valkey layer, depends on S07) and S15 (SFC core, depends on S07) can start in any order. Run `/session-start <id>`. Start from `main` after the S07 merge: `git switch main && git pull --ff-only && gh stack init <branch>`. S09 starts from `KeySchema` and the key table in the data layer page; S15 from `MicroDCS.Sfc` and D-44 (architecture test: no references); read the parity rows named in each block.

## S08 Core: envelope, processor API, hosting, in-memory transport, test harness (2026-11-08, branch `s08-core`)

Status: done. Previous session post-merge: verified (`docs` green on `main` for #50; `ci` and `security` green at the time of the check; nine required checks set). CloudEvents checkpoint (D-14): PR #381 still open, 2.9.0 latest.
Done:

* `CloudEventsEnvelopeCodec` on upstream CloudEvents, JSON and binary payload descriptors, `[CarriedAs]` carry and restore.
* New `MicroDCS.Generators` (Roslyn): payload descriptors, `IDescribedProcessor.Describe()`, all handler kinds and lifecycle hooks, MDCS001 to MDCS011.
* Hosting: `AddMicroDcs`/`AddProcessor`/`AddInMemoryTransport`, source-generated options binding, validator, hosted service with the shutdown phases, `MinimalPipeline`, in-memory transport, dedupe store, state topics.
* New `MicroDCS.Testing` (`ProcessorHarness` with a fake clock); greetings demo processor in `MicroDCS.App`; `microdcs smoke` now also runs the core end to end under Native AOT.
* Docs: [Processors in .NET](../processors-dotnet.md) with the option table and diagnostics. About 3k lines including tests, above the size rule; kept in one PR at the maintainer's request.
Findings: a generator cannot add `[JsonSerializable]` entries (generators do not see each other's output); a carried property also needs `[JsonIgnore]`; CloudEvents extension names cannot contain hyphens; `cond ? memory : null` turns into an empty `ReadOnlyMemory` through `byte[]` (caught by a test). Acceptance rows and their tests: [core contracts](research/core-contracts.md#s08-acceptance-tests).
Decisions: revisions of D-35, D-36, D-37, D-39 (no new id).
Changed in the plan: decisions, index (session table), core-contracts research, `zensical.toml` nav, new docs page.
Open questions: none new. Reply routing without a reply address (events intent) is revisited in S10.
Post-merge checks: `ci`, `security` and `docs` green on `main`; the nine required checks still match the job names (four new test projects are covered by `dotnet test`).
Next session pickup: S09 (Valkey layer, depends on S07), S10 (pipeline, depends on S08 and S09), S15 (SFC core) and S16 (job models, depends on S05 and S08) can start; recommended next: S09, then S10. S10 replaces `MinimalPipeline` (keep the order of steps and `HostLifecycleTests`/`GreetingsTests` as regression tests) and takes over the outgoing queue, expiry timers, activity source and fault handling. `IDedupeStore` and `IStartLease` are the seams S09 implements. Start from `main` after the merge: `git switch main && git pull --ff-only && gh stack init <branch>`.

## S09 Valkey layer (2026-11-08, branch `s09-valkey-layer`, stacked on `s08-core`)

Status: done. Previous session post-merge: S08 (#51) is still open and stacked below; S07 verified (`ci`, `security`, `docs` green on `main` for #50).
Done:

* `MicroDCS.Valkey`: options (`MicroDcs:Valkey`) with validator, fail-fast connection, `ValkeyContext` with the unknown-outcome write protocol (R-4, operation id in the hash), health check, meter.
* DAOs on hash documents with explicit indexes (D-41): job orders, job responses (by-order, by-state), work masters, reference lists, job acceptance. One put script per type; version guard, scope check, change entry, scope registry and nudge in one step.
* `ValkeyDedupeStore` and `ValkeyStartLease` (replace the in-memory defaults in either registration order).
* `ValkeyStreams` (no `MAXLEN`, acknowledged-only trim, autoclaim with lost-id log and counter), `ValkeyPoller` (nudge plus fallback poll, scope registry, `IStreamConsumer`, `IPollerTask`).
* `SfcStateDao` with the generic compare-and-set script (D-44), due-index, active set, heartbeat, recipe store; `ValkeyIndexMaintenance` (verify against a scan, rebuild).
* 22 integration tests against compose Valkey (including a killed connection, lost replies and nudge versus fallback) and unit tests for options and registration. Docs: [Valkey layer in .NET](../valkey-layer.md). Parity rows R-1 to R-30: [data layer](research/data-layer.md#s09-implementation-status-2026-11-08), none open.
Findings: a script must get every key in `KEYS`, so the by-state key is built from a state read before the script and checked inside it; `RedisValue.TryParse` succeeds for a missing value; `CLIENT KILL` is a deterministic reconnect test. The Valkey library is not referenced by the App yet, so the AOT publish does not cover it; the AOT analyzers run on its build (`IsAotCompatible`), and the App gets the wiring in S10.
Decisions: none new.
Changed in the plan: index (session table), data-layer research (S09 status), new docs page and nav entry.
Open questions: none new. Q-DATA-4 / R-4 is answered by the operation-id protocol.
Post-merge checks: `ci`, `security` and `docs` green on `main`; the nine required checks still match the job names; compose `tests` container exited with 0 including the new Valkey tests.
Next session pickup: S10 (pipeline, depends on S08 and S09) once #51 and this PR are merged: `git switch main && git pull --ff-only && gh stack init s10-pipeline`. S10 replaces `MinimalPipeline` and wires `AddMicroDcsValkey` in the App. S15 (SFC core) and S16 (job models) are independent and can start in any order; S19 builds the SFC host on `SfcStateDao` and `IStreamConsumer`.


## S10 Pipeline (2026-11-08, branch `s10-pipeline`, stacked on `s09-valkey-layer`)

Status: done. Previous session post-merge: S08 (#51) and S09 (#52) are still open and stacked below; S07 verified (`ci`, `security`, `docs` green on `main` for #50, nine required checks set).
Done:

* `MessagePipeline` replaces `MinimalPipeline`: claim, expiry cancel, scope check, dispatch, replies, mark done; outcomes `Ack`/`Drop`/`Fault`; error event to the reply address on a drop; a fault stops the host unless `DeliveryProfile.CanRedeliver`.
* Outgoing events: `IEventPublisher` on the pipeline, bounded queue and pump per binding (D-28), type filter, commands carry the instance reply address (`ITransport.GetReplyAddress`).
* Expiry timers (P-12) with `[OnExpired]`, shutdown phases 3 and 5, one `ActivitySource` and `Meter` named `MicroDCS` with W3C context, `MicroDCS.Security` log category.
* App references `MicroDCS.Valkey`; `microdcs smoke` has `valkey-registration`; AOT publish clean with StackExchange.Redis.
* `bench/MicroDCS.Benchmarks` (R-18); 25 pipeline tests, App smoke tests; docs: pipeline section in [Processors in .NET](../processors-dotnet.md). Parity list S10: [core contracts](research/core-contracts.md#s10-pipeline-python-parity-and-tests), none open.
Findings: Python keys the expiry timer by the command's `causationid` when set, so a response cannot cancel it (changed, keyed by command id). A timer whose clock delay is created after a yield misses a fake-clock advance (fixed: create it synchronously). An `ActivityListener` is process wide, so tests filter by trace id. A subject with an invalid scope segment silently became an address without scope when publishing (fixed, D-38 says unpublishable); the options validator already rejects a queue size above the maximum, so no capping exists. The generator throws `IndexOutOfRangeException` instead of a diagnostic when the `[Processor]` argument does not resolve (not fixed, see pickup).
Decisions: revision of D-37 (no new id).
Changed in the plan: decisions (D-37), index (status, OpenTelemetry and R-18 rows), core-contracts research, docs page, `.editorconfig` (`bench/`), Dockerfile (restore inputs for `bench/`), solution.
Open questions: none new. Dedupe lease versus slowest handler stays a configuration check for S13.
Verified (CI fixes included: Sonar rules in `dotnet format` and new-code coverage 76 % to above 80 %, `bench/` excluded from coverage): build 0 warnings, tests green (22 skipped on the host), format clean, `CI=true dotnet restore`, AOT publish and `docker run` smoke, compose integration tests (`tests` exited 0), `scripts/scan` clean. The Sonar quality gate runs on the PR.
Post-merge checks: `ci`, `security` and `docs` green on `main`; the nine required checks still match the job names; the Docker image builds with the `bench/` restore inputs; expiry tests stay stable in CI (no sleeps, fake clock).
Next session pickup: S11 (inspection, depends on S09 and S10) or S13 (MQTT transport, depends on S10). Merge #51, #52 and this PR first (`gh stack merge`), then `git switch main && git pull --ff-only && gh stack init <branch>`. S13 implements `ITransport` including `GetReplyAddress`, `CanRedeliver` (false: MQTT cannot nack in-session), message workers, subscription names and the `$share/` renderer. Small fix to take along: the generator crash on an unresolved `[Processor]` argument should report a diagnostic (add a test in `MicroDCS.Generators.Tests`).

## S11 Inspection layer and MCP read tools (2026-11-08, branch `s11-inspection`)

Status: done. Previous session post-merge: verified for #54 (`ci`, `security`, `docs` green on `main`; the nine required checks match the job names). Image build with the `bench/` restore inputs is exercised by this session's Docker build.
Done:

* `MicroDCS.Inspection`: `InspectionService` (result cap, redaction, rate limit, audit line), the management listener (own Kestrel host, off by default, loopback `Host` check, bearer token off loopback, `/health/*`), `IManagementModule`, `EventInjector` (write group through the pipeline).
* Core: `PipelineDecisionLog` ring buffer (zero size while off), inspectors `app`, `processors`, `pipeline`, `Management:*` options with start-time validation. `ValkeyInspector` in `MicroDCS.Valkey`.
* `MicroDCS.Mcp`: ten read tools and `inject_event`, generic `WithTools<T>` registration; AOT clean. `microdcs serve`, smoke check `inspection-mcp`, compose profile `app` (token secret), `.mcp.json`, `CLAUDE.md` section, [Inspection and MCP](../inspection.md).
* Generator: unresolved `[Processor]` argument reports MDCS012 (pickup item from S10).
* 25 new tests (service, listener, MCP contract against the in-memory host, options, ring buffer, Valkey inspector in compose). No Python behaviour replaced; `log_payloads` stays dropped (C-5).
Findings: [S11 implementation findings](research/inspection-mcp.md#s11-implementation-findings-2026-11-08) (AOT result, `RequestDelegate` overload in libraries, optional MCP arguments need `= null`, `aot-publish` rewrites the Generators lock file).
Decisions: D-19 decided (refinements in its entry); D-40 note.
Changed in the plan: decisions, index, inspection research, docs (new page, nav, diagnostics table), `CLAUDE.md`, compose.
Open questions: REST adapter moved to S21 (Q-OPS-1).
Verified: build 0 warnings, 206 tests green (24 integration skipped on host), format clean, `CI=true dotnet restore`, AOT publish (allowlist only) with `smoke` and `docker run`, compose integration tests (24 passed, `tests` exited 0), `scripts/scan` clean, `zensical build`. The Sonar quality gate runs on the PR.
Post-merge checks: `ci`, `security`, `docs` green on `main`; required checks unchanged; `docker compose --profile app up --wait` still starts with `MICRODCS_MCP_TOKEN` set.
Next session pickup: S12 (maintenance tools, depends on S11) or S13 (MQTT transport). Run `git switch main && git pull --ff-only && gh stack init <branch>`. S12 adds `MicroDCS.Maintenance` as an `IManagementModule`-style dev-only package (plan/apply tokens, environment guard, snapshots; an architecture test keeps it out of `MicroDCS.Inspection`). S13 implements `ITransport` (`GetReplyAddress`, `CanRedeliver` false) and registers a transport inspector (`transport.mqtt`). After `scripts/aot-publish`, run `git checkout src/MicroDCS.Generators/packages.lock.json`.


## S12 Maintenance tools with snapshot and restore (2026-11-08, branch `s12-maintenance`, stacked on `s11-inspection`)

Status: done. Previous session post-merge: not verifiable yet, #55 (S11) was still open with green checks when S12 started; the next session checks `ci`, `security` and `docs` on `main` for both.
Done:

* `MicroDCS.Maintenance`: `IMaintenanceOperations` (plan dedupe release, plan scope purge, plan restore, apply, list snapshots), `ValkeyMaintenance`, `KeyImage` (exact stored text, digest), `SnapshotStore` (flushed JSON files, retention, size cap), `PlanStore` (single-use tokens), options with the environment guard at start, optional `{prefix}:env` marker (`KeySchema.EnvironmentMarker`).
* Apply: digest recheck, snapshot first (no snapshot, no change), one Lua script with compare-and-delete on document versions; restore as one transaction, refuses on differing content unless `overwrite`.
* `MicroDCS.Mcp`: `maint_dedupe_release_plan`, `maint_purge_scope_plan`, `maint_restore_plan`, `maint_apply`, `maint_snapshots_list`, registered only with `MicroDcs:Maintenance:Enabled`. Compose profile `app`: Development, marker service, snapshots in `/tmp`. `.microdcs/` git- and docker-ignored.
* Tests: 34 unit (guard refusal outside Development/Test, options, plan store, snapshot write/list/retention/size cap/unwritable/path ids, digest, architecture), 3 MCP contract, 8 compose integration (round trips with exact state equality, version mismatch, conflict and overwrite, cap, unwritable directory, marker).
Findings: [S12 implementation findings](research/inspection-mcp.md#s12-implementation-findings-2026-11-08) (`bool` tool arguments need the JSON context entry; `HostingEnvironment` is internal). Python parity: none, Python has no maintenance tooling.
Decisions: D-45 (new). Open: broker-side purge (S18).
Changed in the plan: decisions, index, inspection research, `docs/inspection.md` (maintenance section and settings table), `CLAUDE.md`, compose.
Verified: build 0 warnings, 251 tests (32 integration skipped on host), compose integration 32 passed 0 skipped, format clean, `CI=true dotnet restore`, AOT publish (allowlist only) with `smoke`, image with profile `app` run end to end (plan, apply, snapshot list over MCP), `scripts/scan` clean, `zensical build`. Sonar quality gate runs on the PR.
Post-merge checks: `ci`, `security`, `docs` green on `main`; required checks unchanged; `docker compose --profile app up --wait` starts with `MICRODCS_MCP_TOKEN` set (the `valkey-marker` service must exit 0).
Next session pickup: S13 (MQTT transport, depends on S10) or S15/S16. Run `git switch main && git pull --ff-only && gh stack init <branch>` once S11 and S12 are merged; otherwise `gh stack checkout s12-maintenance` and `gh stack add <branch>`. S13 implements `ITransport` (`GetReplyAddress`, `CanRedeliver` false) and registers a transport inspector (`transport.mqtt`). Later sessions add their `maint_*` tools as operations on `IMaintenanceOperations` with an integration round trip (D-45). After `scripts/aot-publish`, run `git checkout src/MicroDCS.Generators/packages.lock.json`.

## S15 SFC core library, in-memory host and explain/dry-run tools (2026-11-08, branch `s15-sfc-core`)

Status: done. Previous session post-merge: verified (`ci`, `security`, `docs` green on `main` for #56). Size: about 1.8k lines in `MicroDCS.Sfc`, 0.7k in the simulation and tool, 1.5k of tests; larger than the size rule because the tools were asked for in the same PR.
Done:

* `MicroDCS.Sfc` (no references, architecture test): recipe model with the Python PascalCase wire names and the IEC 61131-3 names in C# (`SfcDivergence` with `Sequences`, `SfcActionBlock`, `SfcActionQualifier` with all 11 qualifiers), `SfcGraph.Build` with 16 validation codes (`SFC000` to `SFC016`), `SfcInterpreter.Evolve` over `SfcState` and the events `Start`, `ActionCompleted`, `ActionFailed`, `EventReceived`, `Tick` (with outage extension), `Resume`, `Cancel`; commands `StartWork`, `CancelWork`; continuations `Defer`, `Complete`, `Fault`, `Cancelled`; dispositions `Applied`, `Absorbed`, `Rejected`.
* JSON: source-generated context, explicit wire names, schemas generated from the model into `schemas/sfc_recipe.schema.json` (replaces the Python one) and `schemas/sfc_state.schema.json`; tests regenerate and compare (`UPDATE_SCHEMAS=1` rewrites).
* `MicroDCS.Sfc.Simulation`: `SfcSimulation` (virtual clock, scripted equipment, transcript, evaluation limit), `SfcScript` and its text parser, `SfcExplainer`, `BuildPermissive`.
* `MicroDCS.Mcp`: `sfc_explain` and `sfc_dryrun`, read-only and pure, always registered; checked in the AOT binary over `/mcp`.
* Tests: 131 in `MicroDCS.Sfc.Tests` (build codes, step semantics, Python scenarios, simulation, schemas, architecture), 2 MCP contract tests added.
Findings: [S15 test parity and design details](research/sfc-rules.md#s15-test-parity-test_sfc_enginepy-57-and-test_sfc_recipepy-19); Python parity: no row open. Decision refinements are recorded as a dated revision of D-44 (PascalCase wire names, `Evolve`, standard names for divergence, sequence and qualifier, `Cancelled`, failure rule for the default transition, `always` becomes `true`).
Changed in the plan: decisions, index, sessions, sfc-rules research, `docs/sfc-core.md` (new, nav entry), `docs/inspection.md`, `CLAUDE.md`.
Verified: build 0 warnings, 362 tests (32 integration skipped on host), format clean, `CI=true dotnet restore`, AOT publish (allowlist only) with `smoke`, `docker build` and `docker run` of the image, compose integration 32 passed 0 skipped (tests container exited 0), `scripts/scan` clean, `sfc_dryrun` called on the AOT binary over `/mcp`, `zensical build`. The Sonar quality gate runs on the PR.
Open questions: recipes in the docs use `Condition: "always"`, which is now `true` (S23 docs port); recipe lookup by hash for the two tools (S19).
Post-merge checks: `ci`, `security`, `docs` green on `main`; required checks unchanged.
Next session pickup: S13 (MQTT transport) or S16 (job models); S19 needs S17 as well. Run `git switch main && git pull --ff-only && gh stack init <branch>` once S15 is merged, otherwise `gh stack checkout s15-sfc-core` and `gh stack add <branch>`. For S19: the host loads `SfcState` and the graph by `RecipeHash` (`sfc:recipe:{hash}`), calls `SfcInterpreter.Evolve` with `At` from the stream entry id, and writes `result.State`, `result.Commands` and `state.NextDeadline` through the generic script; map `SfcFault` and `SfcComplete` to the job triggers, and feed `SfcSnapshot.LiveWork` from the outbox. `sfc_explain` can take the `valkey_document` kind `sfcstate` once S19 stores states in the core format; a recipe lookup by hash is the follow-up for both tools. After `scripts/aot-publish`, run `git checkout src/MicroDCS.Generators/packages.lock.json`.
