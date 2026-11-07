# Python design decisions, sorted

Background for D-27 to D-30. The "Design Decisions" list of the Python `.github/copilot-instructions.md` (on `python-archive`) told reviewers what not to flag. S01 sorted it for the port: language-independent items became decisions, the rest is dropped or already covered elsewhere.

| Python item | Outcome |
|---|---|
| Comma-delimited config parsing is safe (MQTT topics cannot contain commas; `name:path` values) | Carried over as [D-30](../decisions.md#d-30), only if the configuration format keeps list values (Q-ARCH-3, S06) |
| Fail-fast startup (no try/except around processor `initialize()` or handler startup) | Carried over as [D-27](../decisions.md#d-27) |
| `_event_attributes` is not dead code (filled by mixins) | Dropped: Python mixin mechanics; handler metadata comes from source generators (D-25, S06) |
| Queue backpressure raises `RuntimeError` | Carried over as [D-28](../decisions.md#d-28) |
| `ContextVar` is async-only | Dropped: .NET uses `AsyncLocal`/`Activity` with thread-pool continuations; S10 designs context flow |
| Dual OTEL handler registration (plain plus instrumented pairs) | Dropped: tracing is built into the pipeline once (S10) |
| `get_protocol_handler()` same-module resolution | Dropped: Python module resolution; registration is explicit and source generated |
| Error context uses comma/equals encoding (`mdcserrorcontext`) | Dropped: D-2 clean wire break; S06 defines typed extension attributes (D-14). Same trust argument applies if a flat encoding returns |
| Python ≥ 3.14 is a hard requirement | Dropped: .NET 10 is pinned by `global.json` |
| `custommetadata` has no explicit size limit | Carried over as [D-29](../decisions.md#d-29); per-transport limits are documented in S22 (R-12) |
| MQTT TLS uses Python defaults for hostname verification | Dropped: S13 configures MQTTnet TLS explicitly (hostname validation on) |
| Expiration tasks are safe with `id=None` | Dropped: `id` is required in the .NET envelope |
| `push_command` response routing is instance-affine | Already [D-9](../decisions.md#d-9) |
| `recovery_idle_threshold_s` exists but is unused | Dropped as a decision; input for the recovery design (S07, S20) |
| `register_pull_completion_handler` is wired in the example app | Dropped as a decision; input for the pull-event wiring (S20) |
