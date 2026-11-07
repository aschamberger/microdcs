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
Next session pickup: S04. Run `/session-start S04`. M0 is complete, so start the M1 stack from `main`: `git switch main && git pull --ff-only && gh stack init s04-library-spike`. New packages now also pass the Sonar C# rules in the build; spike code under `spikes/` is excluded from the Sonar analysis but not from the build analyzers. Run `scripts/scan` before commits touching workflows, Docker or configuration. No maintainer preparation needed.
