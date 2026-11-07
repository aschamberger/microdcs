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
* Integration tests run inside compose with healthchecks and `--abort-on-container-exit --exit-code-from tests`, identical locally and in CI (D-15). Upload service logs on failure.
* CI restores in locked mode (`CI=true` sets `RestoreLockedMode`).
* Containers: multi-stage AOT build, minimal runtime image, non-root, no shell in the final stage.
* Renaming a workflow or job changes required check names in branch protection; update the protection in the same session.
* Run `scripts/scan` before committing changes to workflows, Dockerfiles or configuration (details in `security.md`).
