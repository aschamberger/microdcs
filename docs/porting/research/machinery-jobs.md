# Machinery Jobs processor (S17)

Implements [D-50](../decisions.md#d-50) in `src/MicroDCS.MachineryJobs`. Sources read on `python-archive`: `processors/machinery_jobs.py`, `models/machinery_jobs_ext.py`, `redis.py` (`JobOrderAndStateDAO`, `JobResponseDAO`, the list DAOs, `enqueue_work`), `sfc_engine.py` (start handling), `publishers/machinery_jobs.py` (change types), `tests/unit/test_machinery_jobs_processor.py`, `tests/integration/test_app_sfc_integration.py`.

## Findings

* **Store then Start never ran a recipe in Python.** `enqueue_work(START_RECIPE)` is called only from `_handle_store_transition` for StoreAndStart, although its comment says "when a job reaches AllowedToStart". No other code enqueues a start.
* **Python never deleted an order.** Cancel and Clear saved the `EndState` document and left the id in the list, so ended orders counted against the limit for ever.
* **Python's publisher handles only `Clear`** (`_dispatch`). A cancelled order would keep its retained topics. S18 has to handle both.
* **Mask bits matter for config payloads too:** the V2 material and equipment types only encode optional fields whose `EncodingMask` bit is set.
* **Status events and the metadata event leave through the outgoing queue,** after the handler returned. Tests that look at `Published` must wait for them.

## Python parity list

| Behaviour | Python | Disposition |
|---|---|---|
| 13 method handlers with scope from the subject | `process_*`, `scope_from_subject` | kept (`[Incoming]`, `[Scope]`) |
| Binding northbound | `processor_config` | kept, id `machinery_jobs` |
| Missing job order or id: `INVALID_REQUEST` | `_handle_*` | kept |
| Store of an order without an id: `ValueError`, then `UNABLE` | `save` | changed: `InvalidRequest` (a required field is missing, D-34) |
| Store overwrites an existing id | `save` without a check | changed (D-50): rejected, `UnableToAccept`, version guard |
| Acceptance before lookup, Update too | `_handle_update_transition` | kept |
| Checks: equipment, material class and definition, personnel, physical asset, work master, each with a switch; empty ids skipped | `is_job_acceptable` | kept (`JobAcceptanceCheck`) |
| Limit counted with `ZRANGE`; scoped limit ignored when the default is 0 | `is_job_acceptable` | changed (D-50): `ZCARD`; a scoped limit always applies |
| Update counted against the limit | `is_job_acceptable` | changed (D-50): it adds no order |
| Acceptance configuration passed by the application | `JobAcceptanceConfig` | changed: options section `MicroDcs:JobAcceptance` |
| Cancel and Clear keep the document as `EndState` | `save` | changed (D-34, D-50): removed with a change entry; the response stays |
| Cancel and Clear emit a status event | effects table | changed (D-34): catalog change, no event |
| Transition without an effect sends nothing | `send_event` | kept |
| Status event with subject, correlation and causation | `send_event` | kept |
| Serialization error of the status event is logged | `send_event` | changed: own models cannot fail; a full queue is logged and counted as handled (D-28 applies to the event, not the transition) |
| Save failure returns a definite status | `except Exception` | changed (D-50): fault, the transport redelivers; a version conflict after retries keeps `UnableToAccept` or `InvalidStatus` |
| `_notify_scope_handlers` | callback to the SFC | dropped: scope registry (D-42) |
| Start work entry on StoreAndStart, separate step, failure logged | `enqueue_work` | changed (D-50): every entry into AllowedToStart, same script |
| Work entry fields `job_id`, `action`, `scope`, `maxlen 5000` | `enqueue_work` | changed: action `start` (S-7), no `MAXLEN` (D-42) |
| Query by id: invalid, unknown, found | `process_request_job_response_by_id` | kept; another scope reads as unknown (D-50) |
| Query by state: exact normalized name, 1000 at most, empty result as `None` | `retrieve_by_state` | changed: a top-level entry matches its sub-states (S16 `Matches`); an empty array; the cap stays |
| RediSearch index, `initialize` | `JobResponseDAO` | dropped (D-41) |
| Metadata event at `post_start`, retained, intent META | `publish_metadata_event` | kept, one replica; additional attributes became named fields (D-2) |
| Response and expiry handlers that do nothing | `process_response_cloudevent`, `handle_cloudevent_expiration` | dropped: without a handler the pipeline drops and logs |
| `trigger_outgoing_event` | processor | dropped (D-35) |
| Config handlers, PUT by default, DELETE removes, no id is ignored | `process_config_*` | kept |
| `ConfigWorkMaster` with `data` under one type id | `machinery_jobs_ext.py` | changed (D-34): standard and extended type ids |
| Work master saved under any scope | `WorkMasterDAO` | changed (D-42): an id belongs to one scope; another scope is ignored with a warning |
| `ConfigJobAcceptance`: any integer | `process_config_job_acceptance` | changed: a negative limit is ignored with a warning |
| Comment logged at info, payload at debug | `loggable` | dropped: no payloads in logs |
| Global change stream, `active-scopes` | `JobOrderAndStateDAO` | changed (D-42): scope registry only |
| Document with `_dataschema`, read gated on it | `to_dict`, `retrieve` | dropped (D-41): the hash is the envelope |
| Job order and state as one document | `ISA95JobOrderAndStateDataType` | kept, V2 OPC UA JSON (D-34) |
| Call and response share one type id | `Config.type_id` | changed (D-50): the response has its own |
| SFC start over MQTT end to end | `test_app_sfc_integration.py` | moved to S19 |
| Retained publishing of job data | `test_mes_integration.py`, `publishers/` | moved to S18 |

## Not covered

Two replicas racing on the limit (accepted, D-50). A redelivered command after another command wrote over it (D-50). Job Control V1 commands. The `start` work entry has no consumer until S19.
