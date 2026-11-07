# Inspection layer and MCP server

Background for [D-19](../decisions.md#d-19).

Question: add an MCP server early (live app state and Valkey access during development and debugging, without writing throwaway scripts)? Where does it fit, and should it be a REST API with MCP on top? Checked 2026-11-06: C# MCP SDK (`ModelContextProtocol`, `ModelContextProtocol.AspNetCore`) via NuGet, GitHub and the project files.

## Assessment

**Yes, and early.** For this project the value is higher than usual:

* The system is distributed state (Valkey documents, streams, a due-index, broker retained state, dedupe leases) that is hard to see. Debugging today means `valkey-cli`, `mosquitto_sub` and ad-hoc scripts.
* The port is done in Claude Code sessions (see the plan). An agent that can ask the running compose stack "why is job X stuck" through typed tools works much better than one that writes redis-cli one-liners and re-parses key names it has to guess. It is also a test aid: after an end-to-end test fails, the agent can inspect the same state.
* The new architecture produces exactly the data worth exposing: the pure SFC core has an explicit state document with a bounded decision log, an outbox and a due-index; the pipeline can keep a ring buffer of its last decisions.

## Where it fits: layered, but not "REST first, MCP on top"

Layers:

1. **Inspection services (the contract)**, in-process, typed, read-only: `MicroDCS.Inspection` (abstractions plus implementations registered by each component). Each stateful component registers its own inspector (pipeline, transports, Valkey layer, jobs, SFC), so adding a component adds its tools. No transport knowledge, no HTTP types.
2. **Adapters, thin, on the same Kestrel host, on a separate management listener:**
   * **REST** (minimal APIs, AOT friendly): the stable, versioned part that production uses (health, readiness, job state query API for R-19, metrics endpoint), authenticated (R-8).
   * **MCP** (`MicroDCS.Mcp`, Streamable HTTP): task-oriented tools for developers and operators, optional package.
3. **Not a literal stack:** the MCP adapter calls the inspection services directly, not the REST API. Reasons: no extra HTTP hop and no REST design constraints leaking into tools (tool design differs: few, task-oriented, compact and truncated output with cursors, descriptions written for a model, not resource-per-entity pagination); and the REST contract can stay small and stable while the tool set changes freely. What they share is the query model, not the DTOs.
4. **Optional later: out-of-process MCP** that talks to the REST API (for troubleshooting a remote cluster through `kubectl port-forward` without enabling MCP in the pod). Only worth it if remote use materialises; it works only for what REST exposes.

Alternative rejected for now: MCP as a sidecar or separate tool that talks to Valkey and the broker directly. It duplicates key-schema knowledge (the exact thing that breaks silently), bypasses the application's invariants and cannot see in-memory state (pending commands, pipeline decisions, transport connection state), which are the things hardest to debug.

## Proposal D-19

* The inspection service layer is part of the foundation. `MicroDCS.Mcp` is an optional package that the example app references; the framework itself does not depend on it. A production application simply does not reference it, so the code is not in its image (also relevant under AOT trimming).
* **Default off, read-only, loopback only.** Enabled by configuration (for example `APP_MANAGEMENT_MCP_ENABLED`; all configuration names in this section are placeholders until Q-ARCH-3 settles the naming scheme) and only in the dev and test compose profiles; the production manifest never sets it.
* **Writes are a separate, explicitly enabled tool group** (`APP_MANAGEMENT_MCP_WRITE_ENABLED`), used for testing and recovery drills: inject a CloudEvent into a binding, force a tick for a job, re-dispatch an action, abort a job. Each write tool goes through the normal pipeline or core API (never directly into Valkey), so invariants and the CAS contract hold.
* **Maintenance tools (cleanup, delete, repair) are a third, MCP-only group** (`APP_MANAGEMENT_MCP_MAINTENANCE_ENABLED`) for fixing data while the tests are not yet working. They are deliberately **not** offered over REST. Structural rule: the contract lives in a separate dev-only project (`MicroDCS.Maintenance`, interface `IMaintenanceOperations`) that the REST adapter and the production-oriented inspection package cannot reference; an architecture test in CI fails if `MicroDCS.Inspection` or the REST adapter reference it. Design in "Maintenance tools" below.
* **Schema-aware Valkey access, no raw commands in the default profile.** Tools read documents by job id or key kind through the DAO and key schema (list keys by kind and scope, show one document with its decoded type, show stream pending entries and consumer lag, show the due-index). A raw `GET/SCAN` tool limited to the configured key prefix exists only in a separate `dev-raw` group. Arbitrary commands are not offered.
* **Security rules (they also answer R-8, R-10, R-11 for this surface):**
  * Auth on anything but loopback: bearer token from a secret file, per tool group scopes (read, write). Never reachable through an Ingress; a NetworkPolicy and port-forward only.
  * **Payloads are redacted by default** (ids, types, sizes, timestamps, states); including payload bodies is a per-tool-group option (`include_payloads`), because event data can contain production data.
  * **Prompt injection:** event data and recipe text originate from equipment and MES and flow into a model's context through tool results. Treat all tool output as untrusted data; keep write tools off by default and require human confirmation in the client for them; never add a tool that fetches URLs or executes anything derived from state.
  * Audit: every tool call is logged in the security log category with caller, tool and arguments (not results).
  * Hard result size limits and rate limits per call, to protect both the app and the context window.
* **Open source and AOT:** both MCP packages are marked `IsAotCompatible` in the SDK repository (`net8.0` to `net10.0`), version 2.3.1, repository pushed on 2026-11-05, 4.6k stars. Not verified: that attribute-based tool discovery (`[McpServerTool]`, `WithTools<T>()`) works in a published AOT binary. A related open issue (#1150) shows source-generated JSON options are expected (a `TypeInfoResolver` is required). Check the MCP SDK in the S04 library AOT spike and prefer explicit, source-generated registration.

## Tool sketch (grows with the sessions, always read-only unless marked)

| Added in | Tools |
|---|---|
| S11 | `app_status` (version, uptime, replica id, enabled transports and their connection state), `config_effective` (redacted), `processors_list` (processors, bindings, rendered addresses/subscriptions), `pipeline_recent` (ring buffer of the last N decisions: received, duplicate, scope-rejected, dispatched, ack state, with ids and timing, no payloads), `dedupe_lookup(source, id)`, `valkey_keys(kind, scope)`, `valkey_document(kind, id)`, `stream_pending(scope)`; write group: `inject_event(binding, event)`; maintenance group: the plan/apply framework, `maint_dedupe_release`, `maint_purge_scope` |
| S17/S18 | `job_orders(scope, state)`, `job_order(id)` (with response and transitions), `publisher_status` (cursors, last published sequence, retained topics for a scope); maintenance: `maint_job_delete`, `maint_retained_clear`, `maint_publisher_reset` |
| S15, S19, S20 | `sfc_job(id)` (state, deadlines, action keys and attempts, decision log), `sfc_due(scope)`, `sfc_work_pending(scope)`, **`sfc_explain(id, event)` and `sfc_dryrun(recipe)`** which run the pure core with no side effects (a direct benefit of D-17); write group: `sfc_tick(id)`, `sfc_redispatch(id, action)`, `sfc_abort(id)`; maintenance: `maint_sfc_job_delete`, `maint_sfc_rebuild_due_index`, `maint_stream_requeue`, `maint_stream_ack` |
| S21/S22 | `health_detail`, `traces_hint` (links to the trace backend, not telemetry itself); OTLP telemetry stays in the Aspire dashboard or collector |

## Maintenance tools (cleanup, delete, repair; MCP only)

Purpose: while the tests and the new code are not stable, data gets into bad states (a half-written job, a stuck lease, an orphaned stream entry, a retained topic nobody owns). Fixing that by hand through `valkey-cli` is what the MCP is meant to replace, but with guardrails, because a model with a delete tool is the sharpest edge in the whole system. Not a REST feature: destructive, dev-only, and its semantics change with the data model.

**Safety design**

1. **Plan, then apply.** Every maintenance tool has two forms: `maint_x_plan(args)` returns exactly what would change (counts, ids, key kinds, affected derived structures, warnings) plus an opaque, short-lived plan token bound to the arguments and to a hash of the affected versions; `maint_apply(token)` executes that plan, nothing else. A model cannot "just delete"; it must show the plan first, and the client's confirmation prompt shows the plan. Apply fails if the targets changed since the plan (version mismatch).
2. **Environment guard, fail-fast.** The group registers only when the configured environment is `Development` or `Test` (and the flag is on). If the flag is on in any other environment the process refuses to start (consistent with the fail-fast startup decision). Optional second guard: a marker key in Valkey (`{prefix}:env`, written by the dev and test compose profiles); maintenance refuses when the marker is missing or says otherwise, so a mis-pointed connection string cannot reach real data.
3. **Scope and namespace limits.** Operations are bounded to the configured key prefix and to explicitly named scopes; a plan that touches more than N targets (default 100) or more than one scope needs an explicit `allow_large` argument. No wildcards beyond "kind within scope".
4. **Go through the domain layer, never raw keys.** Deleting a job removes everything derived from it in one atomic script: order and state document, response, SFC execution state, due-index entry, outstanding work-stream entries (XDEL and PEL clean-up), publisher index entry and, via the state-topic port, the retained topics for it. A tool that deleted only the document would create exactly the inconsistencies it is meant to fix. Each plan lists the derived structures it will touch, so gaps are visible in review.
5. **Compare-and-delete.** Deletes use the same version check as the engine (CAS), so a running replica that just advanced the job makes the apply fail instead of racing it.
6. **Honest labelling.** The tool descriptions say that these operations bypass the OPC UA state machine and that the MES is not told (`maint_job_delete` is not "abort job"; the latter is a write-group tool that goes through the pipeline and publishes the state change).
7. **Audit and snapshot/restore (decided: in the first slice).** Every plan and apply is logged in the security log category (caller, tool, arguments, counts, plan hash; not payloads). Every apply that deletes or overwrites data first writes a **snapshot** of the affected documents, and `maint_restore(snapshot_id)` puts them back (plan/apply as well). Design:
   * **Format:** one JSON file per apply in a dev dump directory (a mounted compose volume, path from configuration, outside the image), containing the plan hash, timestamp, tool, arguments, the key schema version, and for each affected document: its kind, id, the stored value as written (the exact string, no re-serialisation), and the version. Snapshots are written and flushed before the destructive script runs; if writing fails the apply is refused.
   * **What is covered:** Valkey state (documents, stream entries as id plus fields, due-index scores, dedupe entries, publisher cursors). **Not covered:** broker-side retained topics (their payload can be re-derived by the publisher, so `maint_publisher_reset` is the restore path) and consumed or acknowledged messages.
   * **Restore semantics:** restore writes through the same domain layer and CAS rules: it refuses when a target document exists with a different version (no silent overwrite) unless the plan is built with `overwrite`, re-creates derived structures from the restored state (due-index, outbox), and reports what it could not restore. Stream entries are re-added with new ids (original ids cannot be reused), so restore marks them in the audit record; consumers see them as new work.
   * **Safety:** payload data lands on the dev machine's disk, so the dump directory is dev-only (the group already is), excluded from git and from container images, with a retention limit (keep the last N snapshots, default 20, configurable) and a size cap per snapshot; `maint_snapshots_list` shows them without payloads.
   * **Tests:** delete then restore round-trips for each document kind (state equality, derived structures rebuilt), restore refusal on version conflict, apply refused when the snapshot directory is not writable, retention pruning.
8. **Not for tests to call through MCP.** Test code uses the same `IMaintenanceOperations` C# API directly (reset a scope between scenarios, wipe a prefix) in the test host and in the compose `tests` service. This keeps one implementation. Test isolation still relies on unique key prefixes (D-15); the API is the fallback for state that is shared on purpose (brokers).

**Repair tools are preferred over delete tools where possible**, because they remove a class of problems without losing data: rebuild the due-index from the stored states, recompute derived indexes, release an expired or stuck dedupe lease, requeue or acknowledge a stuck work-stream entry (`XCLAIM`/`XACK` with a reason), reset the publisher cursor so retained topics are republished. Each is also plan/apply.

**Lifecycle.** This group is explicitly temporary scaffolding: at the parity gate (S23) decide per tool whether it stays as a dev-profile tool (likely: repair tools, scope purge) or is removed. Added as a checklist item to S23. It never becomes part of the public REST contract.

**Open points:** the item cap default; the snapshot retention default (20) and per-snapshot size cap; whether the marker-key guard is worth the extra moving part; whether `maint_purge_scope` should also be offered for the brokers (clearing retained topics under a prefix is the broker-side half of a scope reset, and is only possible through the transport's own client).

## How it supports the development workflow

* Add the server to `.mcp.json` at the repository root, pointing at the compose service; `CLAUDE.md` tells agents to prefer these tools over ad-hoc `valkey-cli` or broker clients once the group they need exists (added to the S01 instruction files as a stub, filled in S11).
* Compose test failures: the CI job can start the stack with the MCP enabled and keep it up after a failure for local reproduction (`docker compose up` in the same profile). A failing e2e test also writes the `pipeline_recent` output into the artifacts (no payloads), which gives a post-mortem without access to the cluster.
* The session protocol gets one more rule: a new stateful component ships its inspector (and read tools) in the same session; the definition of done checks that.

## Costs and risks

* Scope creep for a hobby project: keep the first slice small (S11, about the 8 read tools above) and grow by use.
* The tool surface is an API that changes often; it is not a compatibility contract (D-8) and is documented as such. The REST part is the stable one.
* Security surface and the prompt injection issue above; mitigated by default-off, read-only, redaction and not shipping the package in production builds. The maintenance group adds the risk of a model deleting the wrong data: mitigated by plan/apply, the environment guard, scope limits and CAS (see "Maintenance tools").
* MCP spec and SDK are moving (2.x with open API-shape issues). Isolated in the optional package.
* Inspector code must not affect hot paths: the pipeline ring buffer is fixed size, lock-free or per-thread, and disabled when inspection is off.

## Follow-ups

* S06: inspection abstractions as part of the contracts (ADR): inspector interface, redaction policy, management listener and port layout (together with Q-OPS-1), the read/write split and the enable flags.
* S04: include `ModelContextProtocol.AspNetCore` in the library AOT spike (attribute-less registration), measure.
* S11 and S12: first slice (see the session blocks).
