# CI security and quality scanning

Background for [D-20](../decisions.md#d-20).

Request: set up CI with SonarQube Cloud (code quality and security), Trivy (vulnerabilities, misconfiguration, SBOM) and Gitleaks (secrets). Checked 2026-11-06: the GitHub repository (public, default branch `main`), the Gitleaks action README, the Trivy action README and the Trivy security advisories and release lists, the SonarQube scan action README and the SonarQube Cloud .NET coverage docs. Not run: nothing is installed or configured yet (no secrets, no accounts).

## What each tool covers (and does not)

| Tool | Covers | Does not cover | Placement |
|---|---|---|---|
| **Gitleaks** | Secrets in the working tree and in the **whole git history** (generic and provider rules) | Vulnerable dependencies, code issues | PR, push to `main`, daily |
| **Trivy** | (1) `fs`: known vulnerabilities in NuGet dependencies from `packages.lock.json` (needs locked restore, S01), misconfiguration in `Dockerfile`, compose files and the Kubernetes manifests in `deploy/`; (2) `image`: OS packages of the runtime base image; (3) SBOM (CycloneDX) | A Native AOT binary carries no `.deps.json`, so an **image scan does not see the NuGet packages inside the binary**; the `fs` scan of the lock files is what covers them. Trivy's own secret scanner duplicates Gitleaks: turn it off (`scanners: vuln,misconfig`) | PR, push, daily (new CVEs appear without a code change) |
| **SonarQube Cloud** | C# bugs, vulnerabilities, security hotspots, code smells, duplication, coverage on new code, quality gate with PR decoration | Dependency vulnerabilities (its SCA is part of a paid product; Trivy covers it), image contents, secrets in history | PR and push to `main` |

GitHub's own features are complementary and free for public repositories: **secret scanning with push protection** (blocks provider-pattern secrets at `git push`, before CI) and the dependency graph with Dependabot alerts. Not asked for, worth a decision later: CodeQL (overlaps Sonar for C#).

## Decision D-20 (decided: containers): scanners run as pinned containers through one compose file, used identically locally and in CI

**Why not simply the marketplace actions.** On 2026-03-19 an attacker with stolen credentials published a malicious Trivy v0.69.4, force-pushed **76 of 77 version tags of `aquasecurity/trivy-action`** to credential-stealing code and replaced all tags of `aquasecurity/setup-trivy` (advisory GHSA-69fq-xp46-6x23, critical; DockerHub images v0.69.5 and v0.69.6 were affected on 2026-03-22; the advisory lists images referenced by digest and builds from source as not affected). A security scanner has access to the checkout and often the tokens of the job, so it is a high-value target. Lessons applied here:

1. **Pin everything by immutable reference.** Container images by digest, GitHub Actions by full commit SHA (with the version in a comment), tool versions explicit (never `latest`). Dependabot (`docker` and `github-actions` ecosystems, already configured) updates digests and SHAs. Today's workflows use mutable tags (`@v7`); the .NET workflows start pinned.
2. **Do not run the marketplace wrapper where a pinned container does the same job.** `compose.scan.yaml` defines `gitleaks` and `trivy` services (images by digest, read-only mounts, no network except what the tool needs). CI runs `docker compose -f compose.scan.yaml run --rm <service>`; a developer runs the same command (D-15 principle). Gitleaks' marketplace action also needs a licence for organization-owned repositories (free for personal accounts; this repository is under a personal account); the CLI container avoids that constraint if the repository ever moves to an organization.
3. **Versions to avoid, versions to use** (from the advisory and the release lists read on 2026-11-06): never Trivy v0.69.4 to v0.69.6, never `trivy-action` tags before v0.35.0 (the release list shows v0.35.0 and v0.36.0, 2026-04-22, after the incident); the latest Trivy is v0.76.2 (2026-11-05). The latest Gitleaks is v8.30.1 (2026-03-21) and the latest `sonarqube-scan-action` v8.4.1 (2026-11-03). Re-check at implementation time.
4. **Least privilege for jobs.** `permissions: contents: read` by default; `security-events: write` only on the job that uploads SARIF; `persist-credentials: false` on checkout; no `pull_request_target`; secrets only where needed (the Sonar token only in the Sonar step).
5. **Verify downloads.** Where a binary is downloaded instead of a container (the .NET Sonar scanner is a NuGet tool), pin the version in a tool manifest with a restore from the lock; for Trivy checksums and signatures are published (cosign verification is optional).

Rejected alternative: marketplace actions pinned by SHA. Same gates, but a second mechanism that differs from the local run and carries the supply-chain exposure above. Revisit only if the container route proves impractical in S03 (for example Trivy DB rate limits or image-scan access to the Docker daemon in CI); record the reason if so.

## Pipeline layout

| Workflow / job | Trigger | What it does | Blocks merge |
|---|---|---|---|
| `ci.yml` / `build-test-sonar` | PR, push `main` | `setup-dotnet` (from `global.json`), `restore --locked-mode`, SonarScanner `begin`, `build --no-incremental`, tests with coverage, SonarScanner `end`; quality gate result back to the PR | Yes (quality gate) |
| `ci.yml` / `aot-smoke`, `compose-e2e` | PR, push `main` | S02 content | Yes |
| `security.yml` / `gitleaks` | PR (commit range), push `main` (full history), daily | `compose.scan.yaml` gitleaks with `.gitleaks.toml`; SARIF as artifact | Yes |
| `security.yml` / `trivy-fs` | PR, push `main`, daily | vuln (lock files) and misconfig (`Dockerfile`, compose, `deploy/`, workflows); SARIF upload to code scanning | Yes (HIGH and CRITICAL, fixable only: `ignore-unfixed`) |
| `security.yml` / `trivy-image` | PR, push `main`, daily | scan of the image from the build (`buildx` with GHA cache; tarball, no registry push); SBOM (CycloneDX) as artifact | Yes (same rule) |

Other settings: `concurrency` groups cancel superseded PR runs; the daily schedule rebuilds the base image to catch new OS CVEs; scheduled failures notify through GitHub (the user who last edited the cron); NuGet package restore uses the central lock files, and `dotnet list package --vulnerable` stays as a cheap first check (S02).

**Coverage and the Sonar scan.** The SonarQube Cloud docs (read 2026-11-06) support, for C#: `dotnet-coverage collect "dotnet test" -f xml` with `sonar.cs.vscoveragexml.reportsPaths`, or `-f cobertura` with `sonar.cs.cobertura.reportsPaths`, plus Coverlet, OpenCover and dotCover. Decide in S03 which tool produces the file with xUnit v3 on Microsoft.Testing.Platform (the `Microsoft.Testing.Extensions.CodeCoverage` extension, same format, is a likely replacement for the extra tool; not verified). The action's README states that .NET solutions must use SonarScanner for .NET rather than the generic action, so the Sonar step is `dotnet sonarscanner begin/end` (tool pinned in `.config/dotnet-tools.json`; verify that Dependabot's NuGet ecosystem updates tool manifests, otherwise a manual bump task). Properties (project key, organization, exclusions, coverage paths) go on `begin` or in the scanner's analysis XML, not in `sonar-project.properties`.

**Scope settings for Sonar:** exclude generated code (`**/*.g.cs`, source-generator output, generated models and schemas if they are checked in), `spikes/**`, `docs/**`; keep tests in analysis but out of coverage and duplication metrics. Start with the default quality gate ("Sonar way": no new bugs or vulnerabilities, all new security hotspots reviewed, coverage on new code at 80 percent, duplication below 3 percent). Because the .NET code is new, everything counts as new code at first, so the gate is strict from day one, which is the intent. Revisit thresholds only with a written reason.

## Gating, waivers, noise

* **Fail on:** any Gitleaks finding; Trivy HIGH or CRITICAL with a fix available (unfixed ones are reported, not blocking); the Sonar quality gate.
* **Waivers are code-reviewed and expire.** `.trivyignore.yaml` entries need a statement and an `expired_at` date (VEX-style); `.gitleaks.toml` allowlists need a comment saying why (test fixtures only, by path and regex, never a blanket); Sonar exceptions (false positives, accepted hotspots) are marked in Sonar with a comment and listed in `docs/security.md`. Reviewed at S22 and S23.
* **Self-test, so the gates cannot rot silently.** At setup (S03) prove each gate can fail: on a throwaway branch add a fake secret matching a custom canary rule, a package with a known HIGH CVE in a throwaway project, and a deliberate Sonar bug; confirm red; delete the branch. Optional later: a scheduled job that scans a checked-in canary fixture with the allowlists disabled and must fail.
* **Noise control:** pin DB caches (`actions/cache` for the Trivy DB; the DB comes from `ghcr.io` and can hit rate limits), run Sonar on `main` and PRs only, and avoid duplicate findings (Trivy secret scanning off).

## Local use and agent sessions

* **Same tools locally:** `docker compose -f compose.scan.yaml run --rm gitleaks` (working tree) and `... trivy-fs`; a `scripts/scan` wrapper; an optional git `pre-commit` hook running the Gitleaks container on staged changes (`core.hooksPath`).
* **SonarQube for IDE connected mode** (VS Code): SonarQube for IDE, or the SonarQube MCP server for Claude Code, exposes analysis tools to agents (analyze a file, list potential security issues, exclude files, set up connected mode); which route works with Claude Code is checked in S03. Once the Cloud project exists and the IDE is connected with a token, agents can analyze changed files against the project's quality profile before finishing a session. The instruction files (S03) describe this capability, not tool ids, and tell agents to use it when available and to run the local scan wrapper before commits that touch workflows, Dockerfiles or configuration.
* **Dump directories hold data:** the MCP maintenance snapshots (D-19) contain payloads; the dump directory is git-ignored (S01), and Gitleaks is the backstop if a snapshot is ever staged.

## Manual steps (maintainer; cannot be automated)

1. SonarQube Cloud: sign in with GitHub, create or choose an organization, import `aschamberger/microdcs`, choose **CI-based analysis** (turn off Automatic Analysis, it does not do .NET with coverage), note the organization and project keys, create a token, store it as the repository secret `SONAR_TOKEN` **and** as a Dependabot secret of the same name (Dependabot-triggered runs do not receive normal Actions secrets, so without it the Sonar step is skipped for Dependabot PRs). Fork PRs get no secrets either: the Sonar step is skipped for them and the job says so.
2. GitHub repository settings: enable secret scanning and **push protection**, Dependabot alerts and security updates, code scanning (SARIF upload target); set branch protection on `main` with the required checks listed above (check names change when workflows are renamed).
3. Free SonarQube Cloud analysis applies to public repositories (this one is public, verified through the GitHub API); making the repository private later changes the plan limits.
4. ~~Decide containers or SHA-pinned actions~~: decided, containers (D-20).

## Costs and risks

* CI time: Sonar wraps the build (adds roughly a minute or two, not measured), Trivy DB download is cached, the image scan needs the image build (shared cache).
* Sonar noise on new code in an AOT-oriented codebase (reflection and `unsafe`-style rules, generated code). Handle with exclusions and written waivers, not by loosening the gate.
* The tools run with checkout access; hence pinning and least privilege (D-20). Scanning gives no guarantee: Gitleaks finds patterns, Trivy knows published CVEs, Sonar finds rule matches. They do not replace the security review in S22.
* Maintenance load for a single maintainer: Dependabot PRs for digests and SHAs weekly; group them to keep noise low.

## Follow-ups

* S03: the first `push` run of the `gitleaks` job scans the full history, including the Python history shared with `python-archive`. No separate one-off scan before the freeze: the project never used credentials. Assess any finding there (allowlist fixtures with a reason; rotate if real).
* S03: implement the layout above and the self-test; record the measured CI timings and the coverage format decision in the session log.
* S22: SBOM attestation and image signing (R-9) build on the SBOM artifact from S03; review waivers.
* Optional, not requested: CodeQL, `zizmor` (static analysis for the workflow files themselves), OpenSSF Scorecard.

## Implementation findings (S03, 2026-11-07)

* **Sonar before S03:** the SonarQube Cloud app ran **Automatic Analysis** (check `SonarCloud Code Analysis`, app id 12526, no workflow) on #40/#41: no coverage, C# without a build. Switched to CI-based analysis in S03 (maintainer: Analysis Method off, `SONAR_TOKEN` as Actions and Dependabot secret). A CI scan fails while Automatic Analysis is on, so a green `dotnet` job proves the switch.
* **Coverage format (decided):** `Microsoft.Testing.Extensions.CodeCoverage` (referenced by every project under `tests/` through `Directory.Build.props`), `dotnet test --coverage --coverage-output-format xml`, one GUID-named VS coverage XML per test project in `TestResults/`, read with `sonar.cs.vscoveragexml.reportsPaths=TestResults/*.xml`. No extra tool. Release builds are instrumented; test assemblies are excluded by default. A module without executable code (Core in S03: one empty static class) is reported as `skipped ... optimized_or_instrumented`, which is not an error.
* **Quality gate:** the gate reported for the project has no coverage condition while no coverable line exists; see the self-test below for what the gate does with uncovered new code.
* **Scanner images:** Trivy 0.76.2 by digest, the signature verified with cosign (keyless, identity `aquasecurity/trivy` release workflows, two signatures). Gitleaks 8.30.1 images are **not signed**; the digest pin is the only integrity check. Both run as the calling user, read-only root, `cap_drop: ALL`, repo mounted read-only; Gitleaks without network.
* **Trivy coverage, corrected:** Trivy checks NuGet lock files, `Directory.Packages.props`, `docs/requirements.txt`, the `Dockerfile` and `deploy/` (Kubernetes). It has **no checks for compose files or GitHub workflows** (the layout table above overstated this). Workflow linting would need `zizmor` or `actionlint` (optional, not added; actionlint was run once by hand in S03 and found a YAML bug in `docs.yml`).
* **One scan, three outputs:** each Trivy target is scanned once to JSON (`--exit-code 0 --list-all-pkgs`), then `trivy convert` writes the table with the gate (`--exit-code 1`), SARIF and CycloneDX. The severity filter and `ignore-unfixed` apply at scan time, so the SARIF holds only gate-relevant findings. Image scans use `--input` on a `docker save`/buildx tarball, no Docker socket.
* **SBOM in two parts:** `sbom-trivy-fs.cdx.json` (NuGet and PyPI from the lock files, 87 components) and `sbom-trivy-image.cdx.json` (8 OS packages of the chiseled base). The AOT binary hides its NuGet packages from the image scan. S22 (attestation, signing) builds on both.
* **Gitleaks working-tree mode** reads ignored files too; the 1.6 GB Trivy DB in `.cache/trivy` stalled it, so `dir` mode skips files over 10 MB. History and range modes are unaffected. The full history of all refs (313 non-merge commits, `python-archive` included) is clean.
* **Fork PRs** get no secrets: the Sonar step skips with a notice, and the required `SonarCloud Code Analysis` check never appears, so a fork PR cannot merge without a maintainer re-running it from a branch. Acceptable for a single-maintainer repository.
* **Timings (first PR run, #42):** `gitleaks` 7 s, `trivy-fs` 34 s (DB download ~15 s uncached), `trivy-image` 1 min 26 s (image build from the GHA cache), Sonar begin/end add about 40 s to the `dotnet` job (33 s before, 1 min 30 s after, including coverage).
