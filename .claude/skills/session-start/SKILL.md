---
name: session-start
description: Start a porting session - read the plan, the own session block, the last log entry and the linked research, then set up the session branch in the milestone stack.
disable-model-invocation: true
argument-hint: "<session id, for example S04>"
---

# Session start

Session: $ARGUMENTS (if empty, take the first session with status `open` whose dependencies are `done` in the session table of `docs/porting/index.md`, and confirm it with the user).

## 1. Read (in this order, nothing else up front)

1. `docs/porting/index.md` (whole file: decisions summary, open questions, session table, working rules).
2. The session's block in `docs/porting/sessions.md`.
3. The last entry in `docs/porting/log.md`, and the entry whose "Next session pickup" names this session if that is a different one.
4. The research files and decisions (`docs/porting/decisions.md#d-n`) that the block links to. Other research files only when a question needs them.

Do not re-research settled decisions. If a decision looks wrong, say so and record a dated revision instead of deviating silently.

## 2. Check prerequisites

* Dependencies of the session are `done` (or merged far enough) in the session table.
* Manual steps named in the previous pickup are done; ask the user if unsure.
* Toolchain: `dotnet --info` shows the SDK from `global.json`; `docker compose version` works when the session touches compose.

## 3. Branch in the milestone stack (D-22)

Branch name: session id plus a short slug, for example `s04-library-spike`.

* Open PRs of this milestone's stack exist: `gh stack checkout <branch of the top PR>`, `gh stack sync`, `gh stack top`, then `gh stack add <branch>`.
* No open stack for this milestone (first session, or everything below is merged): `git switch main && git pull --ff-only`, then `gh stack init <branch>`.
* Fallback without `gh stack`: `git switch -c <branch>` from `main` (or from the branch of the unmerged session below).

## 4. Plan

Summarise for the user in a few lines: the goal, the exit criteria from the block, what needs their input (installs, external settings, decisions with defaults), and the order of work. Ask the questions that block the work in one batch, then start.
