# Docs port (S23a)

The eleven pages marked "Not yet ported" were rewritten against the .NET code and the existing .NET pages, not ported
from the old text. Python-only content was removed; it stays on `python-archive`. This file holds the parity list of
what the old pages described and the findings of the session.

## Page map

| Old page | New page | Notes |
|---|---|---|
| `index.md` | `index.md` | Reading path and feature list for .NET. |
| `concepts.md` | `concepts.md` | Framework concepts and glossary rewritten; MessagePack, dataclass and response-chain sections removed. |
| `overall-design.md` | `overall-design.md` | Roles, three layers with the .NET parts, replicas per D-9 and D-53, libraries; southbound section kept (D-11, Q-ARCH-11). |
| `technical-standards.md` | `technical-standards.md` | MQTT features as used by D-46, NATS added, schema documents of D-58, OTLP. |
| `information-model-standards.md` | `information-model-standards.md` | OPC UA stack 2.x models, extension NodeSet, station configuration from `Payloads.cs`. |
| `sfc_engine.md` | `sfc-engine.md` (renamed) | Concepts only; the reference stays in `sfc-core.md` (maintainer's choice: two pages). |
| `machinery-jobs-mes-publishing.md` | same | Design and MES resync for the .NET publisher; topics under `data/`. |
| `persistence.md` | same | Key schema from `KeySchema`, documents, streams, dedupe; API stays on `valkey-layer.md`. |
| `your-first-processor.md` | same | New tutorial (ping processor), built and run against Mosquitto and its unit test passed in S23a. |
| `development.md` | same | Python section removed, "Run the application" added, NATS in the dev stack. |
| `operations.md` | same | Single configuration reference for every options class; Valkey and broker requirements, outage behaviour, backpressure, shutdown, monitoring, sizing. |

The configuration tables of the component pages moved to `operations.md#configuration-reference`; the component pages
link there (`.claude/rules/docs.md`). A script compared every public property of the eight options classes with the
reference: none missing.

## Python parity list

Behaviour the old pages described, checked against `python-archive` where the page made a claim about code.

| # | Python behaviour (old page; source) | Disposition |
|---|---|---|
| DP-1 | MessagePack-RPC transport and sidecar pattern (concepts, technical standards, operations; `msgpack.py`) | dropped: driver 4 of the port |
| DP-2 | `dataclassgen` code generation from JSON Schema, hidden `_` fields carried in `custommetadata` (concepts, information models; `scripts/dataclassgen.py`, `dataclass.py`) | changed: C# records with `[CloudEventType]`, `[CarriedAs]` extension attributes (D-35, D-36) |
| DP-3 | Response chain: `.response()`, `takeover`, `__request_object__` (concepts, tutorial; `dataclass.py`) | changed: the handler returns the reply; carried attributes are copied by name (D-35) |
| DP-4 | Three abstract methods `process_response_cloudevent`, `handle_cloudevent_expiration`, `trigger_outgoing_event` (concepts; `common.py`) | changed: `[OnResponse]`, `[OnExpired]`, `IEventPublisher` (D-35) |
| DP-5 | `__pre_outgoing_callback__` and `__post_outgoing_callback__` (tutorial; `common.py`) | changed: processor filters, `IProcessorFilter` (D-35) |
| DP-6 | `post_start_singleton` with a Redis `SET NX EX` lock (tutorial, persistence; `core.py`) | kept: `[OnStarted(Singleton = true)]` with the start lease `lock:poststart:{processor}` |
| DP-7 | `APP_IS_PROCESSOR_INSTANCE`, `APP_IS_PUBLISHER_INSTANCE` (overall design, MES publishing; `core.py`) | changed: `MicroDcs:Roles` (D-51) |
| DP-8 | Redis Stack with JSON and search indexes (persistence, operations; `redis.py`) | changed: plain Valkey, hashes with explicit indexes (D-41) |
| DP-9 | Global sentinel stream `joborder:changes:_global` and `active-scopes` (persistence, MES publishing; `redis.py`) | changed: scope registry `scopes` (D-42) |
| DP-10 | Clear moves the job to `EndState` and keeps it in Redis (MES publishing; `machinery_jobs.py` `process_clear`) | changed: Cancel and Clear delete the order and leave a change entry; the job response stays (D-34) |
| DP-11 | Job response published only when the job reached a clearable state (MES publishing; `publishers/`) | kept: Completed, Closed or Aborted |
| DP-12 | State index in snake case (`seq`, `has_result`) at `{prefix}/{scope}/state-index` (MES publishing; `publishers/`) | changed: OPC UA JSON `JobOrderStateIndexDataType` under `…/data/` (D-2, D-51) |
| DP-13 | Publisher with `replicas: 1` and no locking (MES publishing) | changed: lease in Valkey (D-51) |
| DP-14 | Push-command response lost at another instance, re-dispatched by the recovery scan on restart (overall design, SFC engine; `sfc_engine.py`) | changed: the deadline resends; the recovery scan resends nothing with a deadline (D-9, D-53) |
| DP-15 | Pull event matched by scope and type, first waiting job (SFC engine; `sfc_engine.py`) | changed: `Selector`, waits oldest first, one event one job (D-53) |
| DP-16 | Equipment contract: deduplicate on `mdcsactionkey`, answer with `causationid` = delivery id, only the latest id accepted (SFC engine; `sfc_engine.py`) | changed: a response to an earlier attempt still completes the open action (D-17, D-44); the rest kept |
| DP-17 | Qualifiers `N`, `P`, `S`, `R`, `L`, `D` listed (technical standards, SFC engine) | changed: only `N`; Python never read the qualifier (D-44) |
| DP-18 | Recipe in snake case, condition `always` (SFC engine; `schemas/sfc_recipe.schema.json`) | changed: PascalCase, condition `true` (D-44 revised) |
| DP-19 | `OTELInstrumented*` handler variants, `APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED` (operations; `mqtt.py`, `msgpack.py`) | changed: OTLP export by the standard `OTEL_*` variables (D-54) |
| DP-20 | Configuration `APP_REDIS_*`, `APP_MQTT_*`, `APP_PROCESSING_*`, `APP_PUBLISHER_*` (operations; `__init__.py`) | changed: `MicroDcs` sections with `APP_` and `__` (D-39) |
| DP-21 | MQTT QoS 1 only with a response topic, else QoS 0 (technical standards; `mqtt.py`) | changed: QoS 1 throughout, `PublishQos` (D-46) |
| DP-22 | Correlation data = `causationid ?? id` (technical standards; `mqtt.py`) | kept as informational; matching uses `causationid` |
| DP-23 | Error back channel `dcs/errors/<dcs_id>` (technical standards) | changed: per-instance reply topic and error events `io.microdcs.error.v1` (D-46, D-37) |
| DP-24 | Local expiry timer per published command (technical standards; `mqtt.py` `_expiration_timeout_tasks`) | kept: `ExpiryTracker`, `[OnExpired]` |
| DP-25 | A Redis error ends the process through the task group (operations; `core.py`) | changed: the client reconnects; a failed claim or done mark is `Retry`, a failed handler a `Fault`; both end the host on MQTT, NATS retries (fixed in S23a, see below) |
| DP-26 | Station configuration with `ce_method` keyword (concepts, SFC engine; `machinery_jobs.py`) | kept: CloudEvent `method`, `DELETE` removes, anything else stores |
| DP-27 | `EUInformation` model and UNECE links (information models; `models/machinery_jobs.py`) | dropped: only a generated type in Python, no behaviour; the OPC UA stack has the type |
| DP-28 | Python 3.14 rationale, `uv`, `pytest`, MessagePack buffer limit (development) | dropped: Python tooling |
| DP-29 | Retained topics with a 48 h message expiry (technical standards, MES publishing) | kept: `RetainedTtl`, refreshed at half |

No row is open.

## Findings

* **A Valkey error in the dedupe step was not a fault.** `MessagePipeline.HandleAsync` let an exception from the claim or
  the done mark escape. The MQTT worker logged it and left the message unacknowledged, and the host kept running; MQTT
  does not redeliver inside a session, so the message waited for the next reconnect or restart. Found while writing the
  outage section of `operations.md`; fixed in S23a at the maintainer's request. The first fix made both steps a `Fault`;
  `/code-review high` then showed that on NATS a `Fault` counts against `MaxDeliver`, so an outage of about 20 s would
  have terminated messages as poison (before, they waited for `AckWait`). The final fix is a new outcome
  `DeliveryOutcome.Retry` (D-37 revised): the claim is given back, the decision is recorded as `Unavailable`, MQTT ends
  the host, the NATS worker keeps the message and retries it every `NakDelay` (a nak would still count against the
  server's `MaxDeliver`, which the new compose test caught). Unit tests for both transport kinds and a NATS compose test.
* **The old "Configuration Reference" was the only place with every field.** The .NET pages each had their own table,
  and some fields (`Roles=None`, binding `ReplyBase` and `SharedGroup`) were only described in prose. One reference now
  holds all of them.
* **Two research files were missing from the nav** (`m1-review.md`, `parity-acceptance.md`); added with the Design
  Record section.
