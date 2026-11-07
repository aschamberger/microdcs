#!/usr/bin/env bash
# SessionStart hook: restore packages so the first build in a session is quick.
# Runs locally and in cloud sessions; silent unless restore fails.
set -uo pipefail
cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0
command -v dotnet >/dev/null 2>&1 || { echo "dotnet not on PATH: see docs/development.md (Toolchain)"; exit 0; }
[[ -f MicroDCS.slnx ]] || exit 0
dotnet restore MicroDCS.slnx >/dev/null 2>&1 || echo "dotnet restore failed: run it manually to see the error"
exit 0
