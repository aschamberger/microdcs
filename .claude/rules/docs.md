---
paths:
  - "docs/**"
  - "zensical.toml"
---

# Docs rules

* Configuration fields and their docs change in the same PR (operations reference tables list name, type, default, description).
* Pages that still describe Python carry the "Not yet ported" warning at the top; remove it only when the page is fully rewritten for .NET. S23 checks that none is left.
* New pages need a nav entry in `zensical.toml`; check that tables, anchors (`{#id}`) and mermaid render with a local `zensical build`.
* The plan in `docs/porting/` stays small: log entries about 15 lines, analysis goes to `docs/porting/research/`. New decisions take the next free id from `docs/porting/index.md`.
* Write plain, short sentences; no marketing tone.
