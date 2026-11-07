#!/usr/bin/env bash
# PostToolUse hook: whitespace-format a C# file after Claude edits it.
# The hook input arrives as JSON on stdin; the edited path is tool_input.file_path.
# Only *.cs files are formatted; everything else is ignored. Full `dotnet format`
# (style and analyzer fixes) runs at session end and in CI.
set -uo pipefail

input=$(cat)
if command -v jq >/dev/null 2>&1; then
  file=$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty')
elif command -v python3 >/dev/null 2>&1; then
  file=$(printf '%s' "$input" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("tool_input",{}).get("file_path",""))')
else
  file=$(printf '%s' "$input" | sed -n 's/.*"file_path"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)
fi

case "$file" in
  *.cs) ;;
  *) exit 0 ;;
esac
[ -f "$file" ] || exit 0
command -v dotnet >/dev/null 2>&1 || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0
# Never block the edit: report formatting problems on stderr only.
dotnet format whitespace --folder --include "$file" >/dev/null 2>&1 \
  || echo "dotnet format whitespace failed for $file" >&2
exit 0
