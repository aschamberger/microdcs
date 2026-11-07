---
name: session-end
description: Finish a porting session - definition of done, log entry, status and decision updates, handoff, then commit, submit and merge the session PR in the milestone stack.
disable-model-invocation: true
argument-hint: "<session id>"
---

# Session end

Session: $ARGUMENTS (default: the session of the current branch).

## 1. Definition of done (code sessions)

Run and report the real results:

* `dotnet build` with zero warnings, `dotnet test` green, `dotnet format --verify-no-changes` clean.
* `CI=true dotnet restore` passes (lock files committed).
* AOT publish without `IL2xxx`/`IL3xxx` warnings for the touched projects (once an app project exists, S02).
* Compose integration tests green for the touched transports (from S02).
* Public API has XML docs; no secrets or payloads in logs.
* From S03: scanners green (`scripts/scan` for changes to workflows, Dockerfiles or configuration; Gitleaks clean, Trivy without fixable HIGH/CRITICAL, Sonar quality gate on new code). Use SonarQube analysis tools when available. Waivers only with reason and expiry (D-20).
* From S11: every new stateful component ships its read-only inspector (D-19).
* Docs changed with the code (configuration tables, "Not yet ported" notices).

If something fails and cannot be fixed in this session, say so plainly and record it as `partial`.

## 2. Plan updates (part of the session's own PR)

* `docs/porting/log.md`: append an entry using the template at the top of the file, about 15 lines. Findings others depend on go to `docs/porting/research/` and are linked.
* `docs/porting/index.md`: session status in the table (`done` or `partial`), new decisions in the summary table, "Next free id", open questions struck through with the decision id or updated with the owner.
* `docs/porting/decisions.md`: full entries for new decisions; revisions keep the id and add a dated "Revised" line.
* `docs/porting/sessions.md`: only if the scope of later sessions changed.
* "Next session pickup" in the log entry: session id, exact first steps, anything the maintainer must prepare. A fresh session must not need chat history.

## 3. Commit, submit, merge (D-22)

1. Commit with the session id in the message (`S04: ...`) and the attribution trailer.
2. `gh stack submit` (in a non-interactive shell: `gh stack submit --auto --open`). This pushes all branches of the stack and creates or updates the PRs, each targeting the branch below; the bottom PR targets `main`.
3. Set title, body and milestone of the session PR: `gh pr edit <branch> --title "S04: <title>" --milestone "M1 Foundation" --body-file <file>`. Milestones: `M0 Setup` (S01-S03), `M1 Foundation` (S04-S15), `M2 Parity gate` (S16-S23), `M3 Extensions` (S24-S26).
4. Wait for the checks (`gh pr checks <branch> --watch`). Ask the user before merging.
5. Merge bottom-up with squash: `gh stack merge <pr number> --squash` (merges every PR below it too, atomically). GitHub then rebases the rest of the stack; run `gh stack sync` locally.
   A stack with a single open PR is not a stack on GitHub (it needs two or more PRs), and `gh stack merge` refuses it: use `gh pr merge <pr number> --squash` instead.
6. Fallback without `gh stack`: `git push -u origin <branch>`, `gh pr create --base <branch below or main>`, `gh pr merge --squash --delete-branch`.
7. After the merge: `git switch main && git pull --ff-only`, delete the local session branch, and do any post-merge steps of the session. Results that change the handoff go into a small follow-up PR in the same stack (`<session>-handoff`).

## 4. Report

Tell the user: what was done, what was verified and how, what is partial or skipped and why, the PR link, and the next session.
