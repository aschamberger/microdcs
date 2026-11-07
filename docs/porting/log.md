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
