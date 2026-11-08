# Job models and state machine (S16)

Implements [D-34](../decisions.md#d-34) in `src/MicroDCS.JobControl`. Sources: `OPCFoundation/UA-.NETStandard` tag `2.0.1` (commit `07e62f7`), OPC 10031-4 V1 1.00.0 and V2 2.00.0 on reference.opcfoundation.org, and `python-archive`.

## Findings

* **V1 has no transition table.** The V1 specification text describes each command and which ones remove the order (Stop, Cancel) but not the states it is allowed from, and it gives no mapping between V1 and V2 states. The allowed-from sets are therefore the upstream provider rules and its tests are the vectors. `Clear` is not stated to remove in V1; the provider removes, and so do we.
* **V1 state mapping (maintainer decision, D-34 revision):** the upstream mapping: Waiting is NotAllowedToStart, Ready is AllowedToStart, `Loaded` and `Error` are V1-only and show as Running and Aborted in a V2 view. This does not match the V1 text, where Ready and Loaded are "awaiting a Start command", and it was chosen so the upstream vectors apply. Consequence: a V1 view shows all three Prepare sub-states of a parent as one name, and an order that V2 reports as `NotAllowedToStart/Ready` is `Waiting` in V1.
* **Bit 2** of `ReturnStatus` exists only in the V1 table ("Invalid Job Order Command", Annex B.2 Table 57); the V2 table does not list it. We report it for V1 only. A V1 command in a state that does not permit it reports bit 2, not bit 3, as upstream does (`ToV1ReturnStatus`).
* **V2 state array:** upstream writes one entry for NotAllowedToStart and AllowedToStart because its engine has no Prepare machine. The NodeSet defines those sub-state machines, so we write two entries (upstream vectors adapted). A bare top-level entry decodes to Ready, Held or Completed, so an array without a sub-state does not round-trip unchanged.
* **Sub-state browse names** carry the V2 namespace, as the NodeSet defines them. Upstream writes namespace 0.
* **Mismatched state arrays:** a query with a sub-state of another parent or an unknown sub-state number matches nothing. Read as a state, such an array falls back to its top-level entry.
* **Optional fields:** the neutral records keep the received V2 `EncodingMask`, so a field sent with its default value stays present in a same-version round trip. Upstream drops those.
* **Limits:** the stack defaults (65535 for strings and arrays, 2 MB per message) are off in the shared context, because recipes are opaque and the transport bounds a message (as D-29). The nesting limit stays.
* **Effects come from the NodeSet:** the sub-state machine transitions, including Close, have no `HasEffect`, so they emit nothing. The upstream engine emits a status event on Close. S18 sees those changes through the change stream.
* **Generator:** `[CloudEventType(Codec = OpcUaJson)]` is rejected by MDCS010. S17 registers `OpcUaJsonPayloadDescriptor<T>` by hand, which D-35 allows.
* The neutral records use the OPC UA core types (`LocalizedText`, `Variant`, `DateTimeUtc`, `EUInformation`, `ArrayOf<T>`) like upstream; only the ISA-95 generated types stay behind the conversions.
* The AOT binary is 33 MB with the job models linked in.

## Python parity list

| Behaviour | Python | Disposition |
|---|---|---|
| States incl. Prepare, Interrupted, Ended sub-states, Initial and End | `statemachine_test.py::test_state_machine_states` | kept (`JobOrderState`; End is `Removed`) |
| Triggers from the NodeSet | `test_state_machine_triggers` | kept as `JobOrderOperation` |
| Trigger `Run` for AllowedToStart to Running | `machinery_jobs_ext.py::Config.init` | changed (D-34): `BeginExecution` |
| Store lands in `NotAllowedToStart_Ready`, repeat rejected | `test_state_machine_trigger_store` | kept |
| Initial sub-states Ready, Suspended, Completed; entering a parent takes them | `Config.init` | kept |
| Pause lands in Suspended | initial sub-state | kept; Hold (new) lands in Held (D-34) |
| Update keeps the sub-state | `test_update_preserves_state` | kept |
| Store, StoreAndStart, Cancel, Clear added by hand | `Config.init` | kept (supplement in `JobStateMachine`) |
| Cancel and Clear emit a status event | `opcua_state_machine_effects` | changed (D-34): catalog removal |
| Transition without effect sends nothing | `test_send_event_without_effect` | kept (`JobOrderEffect.None`) |
| Return codes: rejected store is unable-to-accept, unknown id, invalid status, invalid request | `_handle_*_transition` | kept (`JobStateMachine`, `JobControlValidation`); missing payload object is S17 |
| `MethodReturnStatus` bits | `machinery_jobs_ext.py` | kept; bit 2 added for V1 (D-34) |
| Lifecycles of the B.3 example | `TestFullB3Lifecycle` | kept as tests |
| Composite state as a list of two entries | `build_job_state_object`, `test_build_*` | changed (D-2): browse paths per spec 7.3.2; locale `en` not written |
| State read back from the text entries | `reconstruct_state_name` | changed (D-2): numbers and browse paths; empty is Initial as in Python |
| State ids 0 and 7 for the pseudo states | `Config.init` | dropped: pseudo states are not on the wire |
| Name and tuple helpers | `test_state_name_to_tuple`, `test_tuple_to_state_name` | dropped: replaced by the enum and `JobStateArray` |
| Mermaid diagram snapshot | `test_graph`, `state_machine.md` | dropped: artefact of the `transitions` library |
| Benchmark of Store | `test_state_machine_trigger_store` | dropped: not a behaviour |
| `transitions` machine with model attach and `queued` | processor `__init__` | dropped (D-34): pure function |
| snake_case aliases to spec names | `Config.aliases` | changed (D-2, D-34): OPC UA JSON verbose field names |
| Work Master with `data` as dict or list, `dataschema` | `ISA95WorkMasterDataTypeExt` | changed (D-34): both strings in the NodeSet2 model |
| Woodworking job order serialization | `test_woodworking_*` | kept as round-trip tests |
| Commands, DAOs, `is_job_acceptable`, config deliveries (`Config*`, `ConfigJobAcceptance`), status event payload, method call and response models, response queries | `processors/machinery_jobs.py`, `machinery_jobs_ext.py` | changed: moved to S17 by its session block |
| `StateIndex` retained payload | `machinery_jobs_ext.py` | changed: moved to S18 by its session block |
| `JobOrderControl` object fields | `machinery_jobs.py` | dropped: OPC UA object view, used only as the carrier of the state tables |
| Machinery Jobs extras (`JobResult`, BOM, output information, `JobExecutionMode`, `ProcessIrregularity`) | `machinery_jobs.py` | dropped: generated from the schema, unused by any processor or publisher |

## Not covered

Cross-version conversion of nested optional fields beyond the tested ones, JSON Schema validation of incoming payloads (the schema is generated, not enforced), arm64 (CI builds it).
