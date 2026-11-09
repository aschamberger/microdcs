# SFC host

Background for [D-52](../decisions.md#d-52) (S19). The host is the thin part around the pure core ([SFC core](sfc-core.md), [SFC rules](sfc-rules.md)). User documentation: [SFC core in .NET](../../sfc-core.md#the-valkey-host).

## How one event runs

1. Load the state document, build or fetch the graph of the pinned recipe.
2. Call `Evolve` with the event.
3. If the core applied it, one compare-and-set stores the state, the due-index entry and the active-set entry, and appends the work entries. A lost race reads again.

Work entries: `start`, `cancel`, `tick`, `resume`, `dispatch:{action}` and `job` (the job order transitions of one step). A wait is state and a withdrawn attempt concerns only its sender, so neither becomes an entry.

## Findings

* `IOutgoingCommands` had no implementation and the pipeline never reported responses; `IActionSink` carried only a command id. S19 built both ends (D-35 revision).
* The time of `start` cannot be the entry id. An order can wait in the stream longer than its first timeout, and the attempt was then lost without a command being sent. Found by the compose tests under load. `start` reads the server clock; a repeated start is absorbed, so nothing replays this time. Ticks keep the entry id (D-44 revision).
* The heartbeat is per scope and is written in the sweep script. A global heartbeat lets only the first scope of a round see an outage, and a second instance could pop due jobs between the heartbeat and the pop.
* After an outage the sweep takes every job of the index, not only the due ones: all open deadlines have to move by the gap.
* A tick that finds nothing writes nothing, so the host puts the job back on the index; otherwise a moved deadline would never fire.
* Pending entries are not removed on a response or a re-dispatch. They hold the round, which tells a late answer to an earlier pass of a loop from an answer to the current one. They run out 60 s after their deadline.
* A step that cannot be done with what is stored (recipe missing, state unreadable, contention) on the response path is logged, not raised: a fault on MQTT stops the host (D-27), and a redelivery would fail the same way. The deadline settles the action.

## Python parity list: host part of `sfc_engine.py` and the tests

Rows S-1 to S-40 of [SFC rules](sfc-rules.md) hold the decisions from S07. This list records what S19 did with the ones it owns and what S20 keeps.

| Id | Python behaviour | File / function | Disposition |
|---|---|---|---|
| H-1 | Consumer loop, `XREADGROUP` over known scopes, group created every iteration | `_run_loop`, `_read_work_items` | changed (S-1 to S-3): poller, group on scope discovery, read count `Sfc:ReadCount` |
| H-2 | `XAUTOCLAIM` of orphaned entries, min idle 30 s | `_autoclaim_orphaned` | kept (S-4), done in S19 instead of S20 because a failed entry depends on it; throttled per scope |
| H-3 | Invalid entry is logged and acked; unknown action logged | `_process_work_item`, `test_acks_invalid_work_items` | kept (S-5) |
| H-4 | Handler exception is logged and the entry acked | `_process_work_item` | changed (S-6): stays pending, dead stream after `Sfc:MaxDeliveries` |
| H-5 | `start_recipe`, `dispatch_action:*`, `resume` routed by action | `test_dispatches_*` | changed (S-7): names `start`, `dispatch:{action}`, `resume`; `tick`, `cancel`, `job` added |
| H-6 | Start skipped when state exists; job missing, wrong dataschema, no data, no initial step logged and left | `_handle_start_recipe`, `test_start_recipe_*` | changed (S-9): unusable recipe aborts the job with a reason; existing running state absorbs; an ended state of a re-stored id is replaced |
| H-7 | `Run` before the state is stored; failure logged and the recipe not started | `_handle_start_recipe` | changed (S-10): state first, `BeginExecution` through a `job` entry; a refusal in a non-running state ends the recipe (D-52) |
| H-8 | Scope from the work item or the first equipment requirement | `_scope_from_job`, `test_scope_from_job*` | changed: always in the entry, written by the job order script (D-50); the equipment fallback is dropped because no entry lacks a scope |
| H-9 | Push dispatch: CAS, then callback with job id, scope, action key, parameters; correlation id from the execution state | `_dispatch_push_command`, `test_dispatch_*` | changed (S-12, S-13): `StartWork` entry, `IOutgoingCommands`; correlation id is the job id, because the state keeps none (S-34 corrected) |
| H-10 | Routing entry `command_id -> job_id`, old entry removed on re-dispatch, kept if the callback raises | `_pending_commands`, `test_dispatch_push_command_*` | changed (S-14): entry added before the dispatch and removed only on a failed dispatch; kept across a re-dispatch for the round; purged by time |
| H-11 | A failed callback is logged and recovered by restart | `_dispatch_push_command` | changed: a full queue is counted and the deadline re-dispatches; other errors leave the entry pending |
| H-12 | Dispatch skips ended jobs and actions no longer open | `_handle_dispatch_action`, `test_dispatch_skips_*` | kept, by comparing the command id of the open attempt |
| H-13 | Processor missing is a runtime job failure | `_find_sb_processor`, `test_find_sb_processor_*` | changed (S-12): build error SFC014 through the capabilities |
| H-14 | `complete_action` by command id; no state or unknown id is ignored | `complete_action`, `test_complete_action_*` | changed (S-18): by action key from the routing map or `mdcsactionkey`; a response without either is dropped (D-9) |
| H-15 | `fail_action` from the demo's expiry handler ends the job | `fail_action`, `greetings.py` | changed: the local expiry decides nothing (D-18); a handler reports `ActionOutcome.Failed`, an error event from the peer is the same failure |
| H-16 | Job complete writes `{job}-response` in `Ended_Completed` | `_complete_job` | kept (S-26); the response carries the start time now, and is written before the state |
| H-17 | Job fail triggers `Abort`, writes no response | `_fail_job` | kept (S-27) |
| H-18 | MES Abort or Stop leaves the recipe running and commanding equipment | `process_abort`, `process_stop` | changed (D-52): the order write queues a `cancel` entry |
| H-19 | Pause and Resume do not touch the recipe | `process_pause` | kept in S19; changed in S20a: the recipe is held at the next step boundary (D-52) |
| H-20 | Recovery scan at start, `resume` per active job | `_recovery_scan`, `_handle_resume`, `test_recovery_*` | S20; `resume` entries are already handled, for the operator tool |
| H-21 | Pull-event matching and waits | `_handle_pull_event`, `pull_event_handler` | S20; the capability is off until then, so a recipe with a pull action is refused |
| H-22 | Scope registered by the app wiring | `register_scope`, `test_register_scope` | dropped (S-33): scope registry |
| H-23 | End-to-end: StoreAndStart, command on the greetings topic, answer on the reply topic, recipe completes, job response exists | `test_app_sfc_integration.py` | kept: `ARecipeOfPushActionsRunsToTheEndAndCompletesTheJob`; the job is stored directly, the command path of the MES is covered by `MachineryJobsMqttTests` |

No row is open.
