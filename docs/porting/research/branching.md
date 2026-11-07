# Porting approach (branching)

Background for [D-16](../decisions.md#d-16). The Python implementation is preserved on a `python-archive` branch, and the port continues directly on `main`. No long-lived feature branch and no swap.

**Why it is cleaner.** One default branch from the start: Dependabot (config and security updates), branch protection, required checks, issues, README and the repository name need no changes later. There is no rename procedure and no drift between two long-lived branches. The cost is that `main` is not a working product for the duration of the port; this is acceptable because MicroDCS is not in production (D-8) and `python-archive` is the usable version in the meantime.

## Procedure and consequences

1. **Freeze point (done 2026-11-07).** `python-archive` was created from `main` at `12c1415` unchanged: no Python changes, no Valkey switch (D-5), no tag. Rule: security fixes only, through pull requests into `python-archive`. The Valkey compatibility run was done beforehand ([Valkey findings](valkey-findings.md)). No history secret scan was run at the freeze: the project never used credentials, and the first full-history Gitleaks run in S03 covers the shared history anyway.
2. **First commit on `main` after the freeze (S01):** remove the Python code. The keep/replace/remove table is in the S01 block. Python stays readable via the branch (`git show python-archive:path`).
3. **Walking skeleton before features.** First PRs: `global.json`, `Directory.Build.props`, solution, CI with build, unit tests and AOT publish smoke, the compose test harness (D-15) with one trivial end-to-end test. Everything after that has CI from the first commit.
4. **Workflows on the archive branch need no change.** `ci.yml` triggers on `push` to `main`, on every `pull_request` (no branch filter) and on a weekly `schedule`. A security-fix PR into `python-archive` therefore still runs the Python tests. Pushes to `python-archive` trigger nothing, and the weekly run stops once `main` carries the .NET `ci.yml` (scheduled runs use the default branch); both are acceptable for a frozen branch. `docs.yml` triggers only on pushes to `main`, so the archive branch cannot deploy the site; from S01 the `github-pages` environment also allows deployments from `main` only. `python-archive` receives no Dependabot updates (config is read from the default branch only).
5. **Docs site is the live work-in-progress (decided).** `docs.yml` stays on `main` and keeps deploying on every push, so the published site always shows the current .NET state (readable on a phone, including this plan). It builds with `zensical build` and, from S01, deploys through GitHub Actions (`upload-pages-artifact` and `deploy-pages`) instead of pushing to a `gh-pages` branch, which is then deleted. The Python docs remain readable in the archive branch's `docs/` folder. Pages that are not ported yet still describe the Python version, so mark them:
   * A WIP notice on the site (announcement bar if zensical supports it, otherwise an admonition on the home page): "Port to .NET in progress. Pages marked Python describe the archived implementation (`python-archive` branch)."
   * An admonition at the top of each not yet ported page, removed when the page is ported (S23 checks that none is left).
   * The plan is published on the site: `docs/porting/` with a nav entry "Port to .NET (WIP)" (the nav is in `zensical.toml`). Rendering check: tables and mermaid.
6. **Visibility.** README on `main`: port to .NET in progress, the Python implementation is on `python-archive`.
7. **Branch protection** on `main`: from S01 pull requests required (no required approvals, single maintainer); required checks added in S02 and S03. Small vertical PRs.
8. **Parity gate (S23, replaces the swap).** Acceptance checklist: parity scenarios pass in compose, AOT image builds and runs, docs ported (no "not yet ported" notice left, WIP notice removed), security behaviours reproduced. Then release.

## Alternatives

| Option | Assessment |
|---|---|
| `python-archive` branch, port on `main` (chosen) | Cleanest and cheapest: no swap, default branch features work from day one. `main` is not usable until parity |
| Long-lived `dotnet` branch, swap with `main` at the end (earlier D-16) | Keeps `main` usable, but needs the rename procedure, Dependabot `target-branch` entries, recreated branch protection and invisible progress |
| .NET in a `dotnet/` folder on `main` | No swap, but mixed toolchains and path-filtered CI for months |
| New repository | Loses issues, stars and doc URLs; not needed |
