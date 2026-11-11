---
paths:
  - "compose*.yaml"
  - ".github/workflows/**"
  - ".github/dependabot.yml"
  - "**/Dockerfile"
---

# Compose, CI and container rules

* Pin every image by digest (`image: name:tag@sha256:...`); the tag stays for readability. Dependabot (`docker`, `docker-compose`) proposes updates.
* Workflows: default `permissions: contents: read`, widen per job only; `actions/checkout` with `persist-credentials: false`. Every `uses:` is pinned to a full commit SHA with a version comment.
* Integration tests use the `compose.test.yaml` services with healthchecks (D-15): locally inside compose with `--abort-on-container-exit --exit-code-from tests`, in CI on the host against the same services with `--fail-skips on` (D-15 revised in S22a). Upload service logs on failure.
* CI path rules live in `scripts/ci-changes` (D-56). A required check must report on every PR: skip a non-matrix job with a job-level `if`, a matrix job with step-level `if`s. A new required check goes into the branch protection in the same session.
* CI restores in locked mode (`CI=true` sets `RestoreLockedMode`).
* Containers: multi-stage AOT build, minimal runtime image, non-root, no shell in the final stage.
* Renaming a workflow or job changes required check names in branch protection; update the protection in the same session.
* Run `scripts/scan` before committing changes to workflows, Dockerfiles or configuration (details in `security.md`).
