# Security Policy

## Supported versions

MicroDCS is pre-1.0. Only the latest release (and the `main` branch) receives security fixes.

## Reporting a vulnerability

Please do not open a public issue for a security problem.

- Use GitHub's private vulnerability reporting: the **Security** tab of the repository, then
  **Report a vulnerability**.
- If that is not available, email the maintainer at the address listed under `authors` in
  [pyproject.toml](pyproject.toml).

Include the affected version or commit, what you observed, how to reproduce it, and the impact as
you see it. A minimal reproduction (a message, a configuration, a command) helps most.

## What to expect

The project has a single maintainer and no service-level agreement. Reports are handled on a best
effort basis: acknowledgement first, then an assessment, a fix on `main`, and a release note that
names the issue once a fix is available. Please allow a reasonable time to fix a problem before
disclosing it publicly.

## Scope

In scope: the `microdcs` package, its container image and the example deployment manifests.

Out of scope: vulnerabilities in the MQTT broker, Redis, Kubernetes or other components you deploy
alongside it, and deployments that do not follow the hardening guidance. What the framework does and
does not protect, and what the operator must configure (broker ACLs, TLS, secrets, network policy),
is described in [docs/security.md](docs/security.md).
