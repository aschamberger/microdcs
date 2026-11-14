---
paths:
  - "docs/**"
  - "zensical.toml"
---

# Docs rules

* Configuration fields and their docs change in the same PR (the operations reference tables list name, type, default, description).
* Pages describe the .NET code on `main`; Python behaviour stays on `python-archive` and is not documented here (S23a ported every page).
* Configuration fields have one reference: the tables in `docs/operations.md#configuration-reference`. Component pages link there instead of repeating them.
* New pages need a nav entry in `zensical.toml`; check that tables, anchors (`{#id}`) and mermaid render with a local `zensical build`.
* The plan in `docs/porting/` stays small: log entries about 15 lines, analysis goes to `docs/porting/research/`. New decisions take the next free id from `docs/porting/index.md`.
* Write plain, short sentences; no marketing tone.
