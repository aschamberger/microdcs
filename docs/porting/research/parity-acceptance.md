# Parity acceptance (S23)

The parity gate of the port (branching item 8, Q-TEST-2): every Python integration scenario on `python-archive` has a .NET test
that passes, the scenarios that ran against the Python application run against the application image, and the wire format has
golden files (Q-TEST-1). Decision: [D-58](../decisions.md#d-58).

## How the scenarios run

The Python app tests (`@app_available`) drove a running application over MQTT. Their .NET counterparts are
`tests/MicroDCS.IntegrationTests/Acceptance/AppAcceptanceTests.cs`: black-box tests against the AOT image (service `app` in the
`image` profile of `compose.test.yaml`, every role, greetings and Machinery Jobs bindings, SFC host), with nothing but an MQTT
client. The work master with the recipe goes over the wire as `ConfigWorkMasterExt` (Python wrote it with a DAO), and the job
response is read with `RequestJobResponseByJobOrderID` and from the retained result topic. `scripts/image-tests` runs them with
the SFC replica tests (one trait, `Category=Image`); CI runs it in the `integration` job. Ten tests, about 8 s.

The library-level Python scenarios (dedupe DAO, retained publisher) map to the in-process tests that earlier sessions wrote
against the same compose services.

## Python parity list

Sources: `tests/integration/test_app_sfc_integration.py`, `test_dedupe_integration.py`, `test_mes_integration.py`,
`test_mqtt_integration.py` (`test_msgpack_integration.py` is dropped with MessagePack, driver 4).

| Id | Python scenario | .NET test | Disposition |
|---|---|---|---|
| A-1 | `test_example_app_completes_sfc_recipe_via_greetings_response` | `AppAcceptanceTests.A_recipe_runs_through_the_equipment_and_the_mes_reads_and_clears_the_result`; host level `SfcHostTests.ARecipeOfPushActionsRunsToTheEndAndCompletesTheJob` (H-23) | kept; changed: the work master is configured over the wire, recipe format v1.1.0 (D-57) |
| A-2 | `test_claim_busy_then_done` | `DedupeAndLeaseTests.ClaimFollowsTheLeaseSemantics` | kept |
| A-3 | `test_lease_of_dead_worker_expires_and_is_reclaimed` | `DedupeAndLeaseTests.ALapsedLeaseCanBeClaimedAgain` | kept |
| A-4 | `test_handler_waits_for_lease_then_processes` | `PipelineTests.A_message_whose_lease_another_worker_holds_waits_and_is_processed_once_the_lease_lapses` (new in S23) | kept; the gap was found here: the pipeline's wait loop had no test. Fake clock instead of a 1 s lease and sleeps |
| A-5 | `test_handler_treats_finished_message_as_duplicate` | `HostLifecycleTests.Duplicate_event_is_acknowledged_without_running_the_handler_again`, `MqttTransportTests` and `NatsTransportTests` `A_duplicate_is_processed_once_and_every_delivery_is_acknowledged` | kept; also seen against the image: a rerun with fixed event ids was dropped as duplicates |
| A-6 | `test_initial_state_index_published_on_startup` | `JobOrderPublisherTests.A_new_lease_holder_rebuilds_the_scope` | changed (D-51): the scope is rebuilt on the first visit under the lease, not on process start |
| A-7 | `test_store_publishes_retained_order` | `AppAcceptanceTests.A_late_mes_reads_the_retained_topics_and_sees_the_sequence_move`, `JobOrderPublisherE2ETests.Separate_processor_and_publisher_instances_keep_the_retained_topics` | kept; changed (D-51): topics under `data/`, the order payload is the job order alone |
| A-8 | `test_result_published_for_clearable_job` | A-1's test (result topic, `HasResult` in the index) | kept; changed (D-51): checked on every change of the job |
| A-9 | `test_clear_deletes_retained_topics` | A-1's test (Clear), `Separate_processor_…` (Cancel) | kept; changed (D-50): Cancel deletes as well |
| A-10 | `test_resync_detects_sequence_gap` | `A_late_mes_reads_the_retained_topics_and_sees_the_sequence_move` | kept for the wire (a late subscriber sees `Sequence` grow and all jobs); the `MESResyncClient` reference client was test code and is not ported, the protocol is documented in `docs/machinery-jobs-mes-publishing.md` |
| A-11 | `test_multi_job_cadence` | `AppAcceptanceTests.Ten_orders_stored_in_a_row_are_all_published` | kept |
| A-12 | `test_publish_raw_greetings_message` | `AppAcceptanceTests.An_event_without_a_handler_is_echoed_with_the_same_data` | changed (P-10): any event without a handler is echoed by the `[Fallback]` as the raw type, data and content type unchanged |
| A-13 | `test_publish_hello_greetings_message` | `AppAcceptanceTests.A_hello_is_answered_with_two_hellos`, `MqttTransportTests.Hello_over_mqtt_is_answered_on_the_response_topic` | kept |
| A-14 | `test_publish_hello_with_additional_payload_fields` | `AppAcceptanceTests.Unknown_fields_are_ignored_and_the_carried_attribute_comes_back` | changed (E-4, D-35): `x-hidden-str` became the carried attribute `hiddenstr`, copied to every reply whose type declares it (Alice's reply too); the object-valued `x-hidden-obj` is dropped (`[CarriedAs]` is string only) |
| A-15 | `test_publish_store_job_order_message` | `AppAcceptanceTests.A_store_is_answered_with_a_store_response`, `MachineryJobsMqttTests.A_job_goes_through_store_start_and_cancel_over_mqtt` | kept; response type id `…Store.response.v2` (D-50) |
| A-16 | `test_publish_store_and_start_job_order_message` | `AppAcceptanceTests.A_store_and_start_is_answered_with_its_response` | kept |
| A-17 | `test_publish_start_job_order_message` | `AppAcceptanceTests.A_stored_order_can_be_started` | kept |
| A-18 | `test_publish_store_job_order_raw_json` | `AppAcceptanceTests.A_store_written_by_hand_in_opc_ua_json_is_accepted` | changed (D-2, D-34): the hand-written text is OPC UA JSON verbose; property values are Variants (`{"UaType":12,"Value":"200"}`); the structured `Value` objects of the Python fixture (`ItemNumber`, …) are left out |
| A-19 | `TestMQTTPublisherIntegration.test_publish_retained_and_receive_on_reconnect` | `MqttTransportTests.State_topics_are_retained_with_expiry_and_deleted_by_an_empty_message` | kept |
| A-20 | `test_delete_retained_clears_topic` | same test | kept |
| A-21 | `test_publish_retained_string_payload` | none | dropped: `IStateTopicPort` takes bytes; encoding is the caller's |

No row is open.

## Acceptance checklist (branching item 8)

| Item | State |
|---|---|
| Parity scenarios pass in compose | done: compose integration suite and `scripts/image-tests` (replicas and acceptance) green |
| AOT image builds and runs | done: `scripts/aot-publish` with `smoke`, the image runs in the image tests |
| Security behaviours reproduced | done in S22 ([deployment and security review](deployment-security.md), K-1 to K-19) |
| Wire format golden files (Q-TEST-1) | done: schema documents in `docs/schemas/`, generated and compared (D-58) |
| Benchmark baseline in CI (R-18) | done: `scripts/bench`, job `bench` (D-58) |
| Maintenance tool group reviewed (D-19) | done: every tool kept, see below |
| Docs ported, WIP notice removed, README, `docs/porting/` archived, release `v0.1.0` | S23a |

## Maintenance tool review (D-19)

Every `maint_*` tool has an integration round trip in `MaintenanceTests` and stays dev/test only behind the environment guard
(D-45). Kept, all of them: `maint_dedupe_release_plan`, `maint_purge_scope_plan`, `maint_job_delete_plan`,
`maint_retained_clear_plan`, `maint_publisher_reset_plan`, `maint_sfc_job_delete_plan`, `maint_sfc_rebuild_due_index_plan`,
`maint_stream_requeue_plan`, `maint_stream_ack_plan`, `maint_restore_plan`, `maint_apply`, `maint_snapshots_list`. The only overlap
is the due-index rebuild with the recovery scan of D-53, which repairs lost entries at runtime; the tool stays as a manual trigger
(maintainer's choice in S23).

## Findings

* **The `dataschema` URLs pointed nowhere.** `https://aschamberger.github.io/schemas/…` had no site behind it, the job payloads
  used `aschamberger.github.com`, and the greetings and Machinery Jobs schema files were the Python ones, which no test compared
  with the .NET wire. D-58 moves the documents under the docs site and generates them.
* **The OPC UA schema generator orders `$defs` differently from run to run.** The golden files sort them.
* **The benchmarks had been failing silently.** Their setup took the single hosted service, and later sessions registered more;
  BenchmarkDotNet reported `NA` and exited 0. The gate now fails a benchmark without a result. Numbers after the fix match the S10
  baseline: decode 2392 B and 1.3 µs, encode 2264 B and 0.85 µs, hello end to end 2216 B and 1.6 µs.
* **Ids in tests against a long-lived instance must be unique per run.** Job identity is the id alone (D-42) and the dedupe claim
  keeps an event id for its TTL, so a rerun with fixed ids is refused or dropped.
