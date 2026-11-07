---
paths:
  - ".github/**"
  - "**/Dockerfile"
  - "compose*.yaml"
  - "deploy/**"
  - "scripts/**"
  - ".githooks/**"
  - ".config/dotnet-tools.json"
  - "Directory.*.props"
  - "nuget.config"
  - "trivy.yaml"
  - ".trivyignore.yaml"
  - ".gitleaks.toml"
---

# Security and quality scanning rules (D-20)

* Run `scripts/scan` before committing changes to workflows, Dockerfiles, compose files, scripts or build/package configuration (`scripts/scan image` too when the Dockerfile changed). It runs Gitleaks over the working tree and Trivy over the lock files, the Dockerfile and `deploy/`.
* Gates: any Gitleaks finding; Trivy HIGH/CRITICAL with a fix available; the SonarQube Cloud quality gate on new code (check `SonarCloud Code Analysis` on the PR). Fix the cause instead of loosening a gate.
* No suppression without a reason and an expiry: `.trivyignore.yaml` entries need `statement` and `expired_at` (at most 90 days); `.gitleaks.toml` allowlists only for test fixtures, by path and regex together, with a comment; Sonar exceptions get a comment in Sonar and a line in `docs/security.md`. `#pragma warning disable` / `[SuppressMessage]` for Sonar rules follow the same rule as compiler warnings (justification comment).
* Scanner images stay digest-pinned in `compose.scan.yaml`; never Trivy v0.69.4 to v0.69.6. No marketplace scanner actions (`aquasecurity/trivy-action`, `gitleaks/gitleaks-action`); scanners run through `scripts/scan` in CI as well.
* Every `uses:` is pinned to a full commit SHA with the release in a comment; no `pull_request_target`; secrets only in the steps that need them.
* When a SonarQube analysis tool is available in the session (SonarQube MCP server or IDE connected mode), analyse the changed files against the project's quality profile before finishing.
* Never print or commit tokens; `.env` files and scan outputs (`scan-results/`), `.sonarqube/`, coverage files and maintenance dumps (`dumps/`) are git-ignored.
