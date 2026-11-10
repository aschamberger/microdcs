# SFC host

Background for [D-52](../decisions.md#d-52) (S19). The host is the thin part around the pure core ([SFC core](sfc-core.md), [SFC rules](sfc-rules.md)). User documentation: [SFC core in .NET](../../sfc-core.md#the-valkey-host).

## How one event runs

1. Load the state document, build or fetch the graph of the pinned recipe.
2. Call `Evolve` with the event.
3. If the core applied it, one compare-and-set stores the state, the due-index entry and the active-set entry, and appends the work entries. A lost race reads again.

Work entries: `start`, `cancel`, `hold`, `release`, `tick`, `resume`, `event`, `dispatch:{action}` and `job` (the job order transitions of one step). A wait is state and a withdrawn attempt concerns only its sender, so neither becomes an entry.

## Findings

* `IOutgoingCommands` had no implementation and the pipeline never reported responses; `IActionSink` carried only a command id. S19 built both ends (D-35 revision).
* The time of `start` cannot be the entry id. An order can wait in the stream longer than its first timeout, and the attempt was then lost without a command being sent. Found by the compose tests under load. `start` reads the server clock; a repeated start is absorbed, so nothing replays this time. Ticks keep the entry id (D-44 revision).
* The heartbeat is per scope and is written in the sweep script. A global heartbeat lets only the first scope of a round see an outage, and a second instance could pop due jobs between the heartbeat and the pop.
* After an outage the sweep takes every job of the index, not only the due ones: all open deadlines have to move by the gap.
* A tick that finds nothing writes nothing, so the host puts the job back on the index; otherwise a moved deadline would never fire.
* Pending entries are not removed on a response or a re-dispatch. They hold the round, which tells a late answer to an earlier pass of a loop from an answer to the current one. They run out 60 s after their deadline.
* A step that cannot be done with what is stored (recipe missing, state unreadable, contention) on the response path is logged, not raised: a fault on MQTT stops the host (D-27), and a redelivery would fail the same way. The deadline settles the action.

## Hold and release (S20a)

Python never held a recipe (H-19), so this part is new behaviour. Design notes:

* **Where the hold lives.** In the core state (`heldAt`, format 1.2), not in the host. The core already owns deadlines, and the release reuses the outage rule: open deadlines move by the length of the hold. `NextDeadline` is empty while held, so the compare-and-set takes the job off the due-index and no tick is queued. Tick and resume of a held job are absorbed; a tick that was queued before the hold finds nothing to do.
* **What is recorded.** Responses to commands that are out, and events for waits that began before the hold (maintainer's choice in S20a), are recorded on their actions; the step stays active until the release. The waits index keeps a held job, so its event is not dropped. A recorded failure is routed on release.
* **Order of the entries.** A Pause and a Resume write `hold` and `release` entries in the order script. Two consumers can take them in either order, and a Pause, Resume, Pause can leave the last entry in front. Each entry reads the order and aligns the recipe with it (Interrupted means held), then reads the order again after its write and repeats until both agree (at most four rounds, then the entry is left for a redelivery). The read is not atomic with the write, so a single pass could apply a hold the order had already left; the last writer always sees the latest order. The time is the server clock when the hold or release is applied, not the entry time: entries taken out of order would pair a hold with the release of another Pause (`/code-review` finding). Held and Suspended both count as Interrupted: the internal `Hold` operation is a constraint, not a decision, but the recipe cannot run on either way.
* **Which commands the release sends.** The routing map (`SfcPendingCommands`) is in-process, so no instance knows whether another one sent a command. The core re-issues every open push attempt with its command id and attempt. The dispatch reads the marker `sent:{action} = {startedAt}:{commandId}` together with the state (one `HGETALL`) and skips the send when it matches; after the command is queued it writes the marker (a script that writes only while the state exists). The compare-and-set of the state writes only its own fields, so the marker lives as long as the job and goes with its delete and its snapshot. The start time tells apart a re-stored job id, whose command ids begin again at `#1`. The deadline of the send comes from the state, because a dispatch entry from before the hold carries the old one.
* **Side effect.** A dispatch entry redelivered after its command was queued and marked no longer sends the command again. A crash between the queue and the marker sends the command id twice on redelivery, as before S20a; receivers deduplicate by id. The first version wrote the marker before the queue, which could lose a command on a crash (`/code-review` finding).

Not chosen: dropping events during a hold and re-adding the wait on release (an answer from equipment that finishes its work would be lost and the action would time out); a set of skipped dispatches per job written by the consumer (a consumer that reads the held state, then loses the race against the release, would add its entry after the release read the set, and the command would wait for its deadline); a marker key with a time to live (a hold can outlast any fixed lifetime).

## Python parity list: host part of `sfc_engine.py` and the tests

Rows S-1 to S-40 of [SFC rules](sfc-rules.md) hold the decisions from S07. This list records what S19 and S20 did with the ones they own.

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
| H-19 | Pause and Resume do not touch the recipe | `process_pause`, `process_resume` (both only `_handle_existing_job_transition`) | changed in S20a (D-52): Pause writes a `hold` and Resume a `release` entry in the order script; the recipe is held at the next step boundary, responses and events are still recorded, deadlines do not expire, nothing is dispatched until the release |
| H-20 | Recovery scan at start, `resume` per active job; skips ended jobs | `_recovery_scan`, `_handle_resume`, `test_recovery_scan_*` | changed (D-53): only running jobs are listed; the scan resends nothing with a deadline and repairs the indexes; a job that cannot be evaluated is logged, not sent to the dead stream |
| H-21 | Pull dispatch marks the action waiting; no routing entry | `_dispatch_pull_event`, `test_dispatch_pull_event_*` | kept: a wait is state, and the waits index is written in the same compare-and-set |
| H-24 | Pull event as a work item, any replica; scope from the subject | `pull_event_handler` | changed: scope from the address (D-38); only queued when a job waits for the type |
| H-25 | Match by type, first job in set order; CAS miss tries the next job | `_handle_pull_event`, `test_handle_pull_event_race_condition_tries_next_candidate` | changed (D-53): oldest wait first; the compare-and-set loser re-evaluates and the next job is tried |
| H-26 | `ce_subject` and `ce_correlation_id` passed to the handler and never used | `_handle_pull_event` | changed (R-1): they are what `Selector` compares |
| H-27 | No active job, no waiting action, unknown type: logged and ignored | `test_handle_pull_event_skips_if_no_active_jobs`, `test_handle_pull_event_ignores_unknown_type_id` | kept: dropped and counted in `microdcs.sfc.events.unmatched` |
| H-28 | An event before its wait is lost | `_handle_pull_event` | kept: no buffering, the action times out (D-53) |
| H-22 | Scope registered by the app wiring | `register_scope`, `test_register_scope` | dropped (S-33): scope registry |
| H-23 | End-to-end: StoreAndStart, command on the greetings topic, answer on the reply topic, recipe completes, job response exists | `test_app_sfc_integration.py` | kept: `ARecipeOfPushActionsRunsToTheEndAndCompletesTheJob`; the job is stored directly, the command path of the MES is covered by `MachineryJobsMqttTests` |

No row is open.
