# SFC rules

Background for [D-44](../decisions.md#d-44) (S07). Builds on [SFC core](sfc-core.md) and [data layer](data-layer.md). Sources on `python-archive`: `sfc_engine.py`, `redis.py` (SFC DAO and scripts), `models/sfc_recipe.py`, `models/sfc_recipe_ext.py`, `publishers/machinery_jobs.py` (stream use). Tests named in `tests/unit/test_sfc_engine.py` (57), `test_sfc_recipe.py` (19) and `tests/integration/test_app_sfc_integration.py` are the specification for S15, S19 and S20; their bodies are read in those sessions.

## Findings from reading `sfc_engine.py`

* The engine **never reads** `SfcActionAssociation.qualifier`, `SfcActionAssociation.timeout_seconds`, `SfcTransition.condition` or `recovery_idle_threshold_s`. Only `priority` is used. Behaviour is qualifier N with no time limit.
* The recipe is reloaded from the work master at every step; a changed work master changes running jobs.
* `_evaluate_transitions` always takes the lowest `priority` outgoing transition, so a step with two transitions never branches on a result.
* Dispatch of the first push action of a step goes through the work stream, the others through `enqueue_work` after the CAS; a crash between the CAS and `enqueue_work` loses those dispatches (the startup `resume` is the only repair).
* A pull-event is matched by type id only: the first job in the scope's active set with a `WAITING` action of that type wins (set order, not time order).
* `_pending_commands` is instance-local and not rebuilt on resume (D-9, intentional).
* Resume re-dispatches `PENDING` and `DISPATCHED` actions; an action in `DISPATCHED` is re-sent with a new command id and attempt + 1, without a limit.
* The finish script concatenates the error text into JSON; a quote in the text breaks it.
* A processor lookup failure calls `_fail_job` after the CAS has already set the action to `DISPATCHED`.

## Python parity list: `sfc_engine.py`, recipe model, SFC part of `redis.py`

| Id | Python behaviour | File / function | Disposition |
|---|---|---|---|
| S-1 | Engine runs on every instance as an additional task; recovery scan, then loop | `SfcEngine.task` | changed: hosted service with the poller (D-43), `Resume` per scope at start (S20) |
| S-2 | Loop per iteration: ensure group for known scopes, autoclaim, read, sleep | `_run_loop` | changed: nudge plus fallback poll; groups on scope discovery (D-43, D-44) |
| S-3 | `XREADGROUP` over all known scopes with count; handles missing group | `_read_work_items` | changed: poll with count (D-43); missing group is created |
| S-4 | `XAUTOCLAIM` with min idle 30 s per scope, deleted ids dropped | `_autoclaim_orphaned` | kept (`Sfc:AutoclaimIdle`, default 30 s); deleted ids logged and counted (D-42) |
| S-5 | Work item validation: needs action and job id (not for `pull_event:`); invalid items are acked | `_process_work_item` | kept |
| S-6 | A handler exception is logged and the item is acked | `_process_work_item` | changed: an exception leaves the item pending for redelivery (execution is idempotent by action key); poison items stop after `Sfc:MaxDeliveries` (default 5) and go to `sfc:dead:{scope}` |
| S-7 | Work actions `start_recipe`, `dispatch_action:{name}`, `resume`, `pull_event:{scope}:{type}` | `SfcWorkAction` | changed: `start`, `dispatch:{action}`, `resume`, `event`, plus `tick` (D-18) |
| S-8 | Start is idempotent: existing state means skip | `_handle_start_recipe` | kept |
| S-9 | Start loads job order, scope from the work item or the first equipment requirement id, work master id, checks `dataschema`, requires data and an initial step | `_handle_start_recipe`, `_scope_from_job`, `_get_work_master_id` | kept; the checks become `Build` errors that abort the job with a reason (Python only logged and left the job in limbo) |
| S-10 | `Run` is triggered on the job state machine before the SFC state is saved | `_handle_start_recipe` | changed: order is state first, then trigger via the outbox (D-17); failure of `Run` aborts the start (S16) |
| S-11 | One execution record per recipe action, `type_id` copied for pull matching | `_handle_start_recipe` | kept |
| S-12 | Push dispatch: CAS `PENDING`/`DISPATCHED` to `DISPATCHED`, new command id, attempt + 1, then call the southbound processor; processor lookup by type id; payload type from the processor | `_dispatch_push_command`, `_find_sb_processor` | changed: `StartWork` command in the outbox with attempt, deadline and action key; type lookup is the registry from D-35; missing processor is a build error (capabilities), not a runtime job failure |
| S-13 | Kwargs for the callback: job id, scope, action key `{job}:{action}`, parameters | `_dispatch_push_command` | kept (action key is the work token id, D-44) |
| S-14 | Command routing entry `command_id -> job_id`, previous entry removed on re-dispatch | `_pending_commands` | kept as instance-local pending map (D-9); matching also by action key (D-44) |
| S-15 | Pull dispatch: CAS `PENDING` to `WAITING` with a command id | `_dispatch_pull_event` | changed: the wait is state (an open wait with its deadline), no command id needed |
| S-16 | Pull event: first job in the scope's active set with a `WAITING` action of the same type id wins; CAS `WAITING` to `COMPLETED` | `_handle_pull_event` | changed: still first match by type, but candidates ordered by wait start; selector (R-1) matching in S20; ambiguity rejected at build |
| S-17 | `pull_event_handler` takes the scope from the first subject segment and `XADD`s an event work item | `pull_event_handler` | changed: same, the scope comes from the parsed address (D-38) |
| S-18 | `complete_action` pops the command id, CAS to `COMPLETED`, check step completion | `complete_action` | changed: `Step(ActionCompleted(actionKey, outcome))`; late-response rule (D-17) |
| S-19 | `fail_action`: CAS to `FAILED`, fail the job | `fail_action` | changed: outcome `error` or `timeout`, routable (D-44); no matching transition means `Fault` |
| S-20 | Step completion: all actions of an active step `COMPLETED`, loop over newly activated steps | `_check_step_completion` | kept in the core; steps with no actions complete at once; 10,000-dispatch limit (D-44) |
| S-21 | Linear advance: lowest `priority` outgoing transition; no outgoing transition completes the job | `_advance_linear_step`, `_evaluate_transitions` | changed: outcome-based condition, lowest priority matching, default `"true"` (D-17); no outgoing transition from the last step still completes the job |
| S-22 | Branches: simultaneous activates all first steps; selection picks the path by entry-transition priority, else the first path; steps advance inside a path; convergence when `active_steps` is empty; exit transition is the one whose target is not a path start | `_enter_branch`, `_advance_branch_step`, `_converge_branch`, `_select_branch_path`, `_find_branch_exit` | kept as semantics; the selection uses outcome conditions; ambiguous exits rejected at build |
| S-23 | Action qualifier is not evaluated | `sfc_recipe.py` | changed: only `N` accepted, others rejected at build (D-44) |
| S-24 | `timeout_seconds` is required in the recipe and never read | `SfcActionAssociation` | changed: it is the action deadline (D-18, D-44) |
| S-25 | `condition` is required in the schema and never read | `SfcTransition` | changed: outcome name or `"true"` (D-17) |
| S-26 | Job complete: CAS finish, trigger `End`, write job response `{job}-response` in `Ended_Completed` | `_complete_job` | kept; the continuation `Complete` maps to the trigger, the response is written in the same outbox step |
| S-27 | Job fail: CAS finish with an error, trigger `Abort` (no response written) | `_fail_job` | kept; the error text is data, not concatenated |
| S-28 | Resume re-dispatches `PENDING`/`DISPATCHED` actions of the active steps, then checks completion; `WAITING` actions need nothing | `_handle_resume` | changed: `Resume` event compares open work with the snapshot's `LiveWork` and re-emits `StartWork` (sfc-core finding 6) |
| S-29 | Recovery scan at start enqueues `resume` for each active job (global set) | `_recovery_scan` | changed: per scope from the registry, plus the sweep makes the scan unnecessary for deadlines (S20) |
| S-30 | `recovery_idle_threshold_s` stored, unused | constructor | changed: `Sfc:OutageThreshold` is the used form (D-44) |
| S-31 | State machine over the job order (`transitions` library): reconstruct state from the state texts, `may_trigger`, `trigger`, save with the transition name as change type | `_trigger_job_transition`, `_reconstruct_state_name`, `_build_job_state_object` | changed: generated transition table and pure function (D-34, S16) |
| S-32 | Recipe helper lookups by name (initial step, action, branch, branch of a step, next step in a path) | static helpers | kept as graph indexes built once in `Build` |
| S-33 | `register_scope` is called by the app wiring | `register_scope` | dropped: scope registry (D-42) |
| S-34 | SFC execution state fields: job id, scope, work master id, current step, correlation id, active steps, actions (state, type id, command id, attempt, result, error), completed, failed, error | `SfcExecutionState`, `SfcActionExecution` | changed: `SfcState` adds recipe hash, version, sequence, deadlines, open waits, bounded decision log (sfc-core finding 6); correlation id kept. S19: the state keeps no correlation id, the job id serves as one ([SFC host](sfc-host.md)) |
| S-35 | Action states `pending`, `dispatched`, `waiting`, `completed`, `failed` | `SfcActionState` | kept; `failed` carries the reserved outcome |
| S-36 | Four CAS scripts operating on JSON paths | `_CAS_*_LUA` | changed: one generic script on the version (D-41, D-44) |
| S-37 | Active-job sets global and per scope, removed at finish | `SfcExecutionDAO.save`, `cas_finish` | changed: per scope only (`sfc:active:{scope}`) |
| S-38 | Consumer group name `sfc-engine`; consumer name from the instance | `SFC_CONSUMER_GROUP` | kept |
| S-39 | Recipe `dataschema` URI selects the interpreter | `SFC_RECIPE_DATASCHEMA` | kept (D-17) |
| S-40 | Recipe fields: steps (name, initial), transitions (source, target, condition, priority), actions (name, step, qualifier, interaction, type id, timeout, parameters), branches (name, type, paths) | `sfc_recipe.py` | kept (wire names unchanged, D-44); `max_attempts` added optional |

No row is open.

## Key details added by the SFC rules

| Key | Type | Content |
|---|---|---|
| `sfc:recipe:{hash}` | string | recipe text, written once per hash |
| `sfc:dead:{scope}` | stream | work items that exceeded `Sfc:MaxDeliveries`, with the reason |

All other SFC keys are in the [data layer](data-layer.md#key-schema).

## Configuration added (`MicroDcs:Sfc`)

| Name | Type | Default | Description |
|---|---|---|---|
| `SweepInterval` | duration | 1 s | Due-index sweep |
| `OutageThreshold` | duration | 30 s | Heartbeat gap that marks a tick `afterOutage` |
| `MaxAttempts` | int | 3 | Host default for the recipe field `max_attempts` |
| `ExpiryGrace` | duration | 2 s | Deadline minus wire expiry |
| `MicroDcs:Valkey:ScopeRefreshInterval` | duration | 5 s | Scope registry read (a poller setting) |
| `AutoclaimIdle` | duration | 30 s | Minimum idle time before `XAUTOCLAIM` |
| `MaxDeliveries` | int | 5 | Deliveries before a work item goes to the dead stream |

The documentation pages for these fields are written with the code (S19, S20).

## S15 test parity: `test_sfc_engine.py` (57) and `test_sfc_recipe.py` (19)

Bodies read in S15 (2026-11-08). The Python tests mock the DAOs, so most of them check host mechanics (CAS, work stream, pending map). Those belong to S19 and S20 and are listed as `host`. No row is open.

| Python tests | Behaviour | Disposition |
|---|---|---|
| `TestSfcEngineConstruction` (1) | Engine takes DAOs and processor lookup | dropped: construction of the Python engine; the host comes in S19 |
| `StartRecipe` creates state, idempotent | Start activates the initial step, a second start changes nothing | kept: `Start_*` tests |
| `StartRecipe` job not found, wrong dataschema | Skipped by the host | host (S19); the dataschema selects the interpreter (`DataSchema`), a state pinned to another recipe hash is rejected |
| `DispatchAction` CAS and callback, key stable across re-dispatch, skips completed job | Push command with stable action key; completed job is not dispatched again | kept: `Redispatch_KeepsTheActionKey...`, absorbed late responses |
| `DispatchAction` CAS already handled, pending-command registration, old id cleared on re-dispatch, registered even if callback raises | Instance-local routing map and CAS | host (S19); the core emits `CancelWork` with the old command id so the host can clear its entry |
| `ActionCompletion` CAS dispatched to completed, skips completed, no SFC state | Response completes the action once | kept: `Response_*`, `ADuplicateResponseIsAbsorbed`; missing state is host |
| `ActionCompletion` unknown command id, stale command id | Responses matched by command id are ignored | changed (D-44): matched by action key; unknown key or another job is rejected |
| `ActionCompletion` cascade to an action-free step | Steps without actions pass at once | kept: `AStepWithoutActionsIsPassedThrough` |
| `ActionCompletion` `fail_action` fails the job | Failure ends the job | changed (D-44): outcome `error`, routable; no matching transition gives `Fault` |
| `WorkItemDispatch` (4) | Work item routing and acking | host (S19) |
| `Recovery` scan for active jobs, skips completed | Startup scan enqueues `resume` | host (S20); the core side is the `Resume` event |
| `Recovery` resume re-dispatches dispatched push command | Re-dispatch after a crash | kept: `Resume_Redispatches...`, now limited by attempts and live work |
| `PullCompletion` pull dispatch is not a pending command, resume does not re-register routing | A wait is not routed by command id | kept: wait is state; `Resume_AnnouncesAWaitAgain...` keeps the same command id |
| `PullCompletion` command id passed to CAS, CAS fails, CAS completes waiting action | CAS of the wait | changed (S-15): the wait is state with a deadline; completion is `EventReceived` |
| `PullCompletion` unknown type id ignored | No action waits for the type | kept: `PullEvent_OfAnUnknownTypeIsAbsorbed` |
| `PullCompletion` no active jobs, race tries next candidate | Choosing the job | host (S20); the core offers `SfcState.IsWaitingFor` |
| `Helpers` initial step, none, find action, evaluate transitions | Lookups | kept as graph indexes; no initial step is build error `SFC001`; transitions evaluate by outcome and priority |
| `Helpers` scope from job, find processor, register scope | Host wiring | host (S19), `register_scope` dropped (scope registry, D-42) |
| `BranchHelpers` (6) | Branch lookups, path selection | kept as `PathOf`, `Entries`, `Exits`; selection changed (D-17): by condition and priority, default required |
| `SimultaneousBranch` (5) | Enter all paths, advance in path, last step leaves, converge, converge without exit completes | kept: `Simultaneous_*` tests |
| `SelectionBranch` (2) | Priority path, convergence | kept: `Selection_*` tests |
| `BranchResume` (1) | Resume re-dispatches all active branch steps | kept: `Resume_RedispatchesEveryActiveBranchStep` |
| `test_sfc_recipe` dataclass round trips, enums, minimal and full recipe | Recipe wire format | kept: `RecipeModelTests` with the same PascalCase names |
| `test_sfc_recipe` msgpack round trips, benchmark fixtures | MessagePack path | dropped (driver 4) |
| `test_sfc_recipe` `Config.type_id` and `type_schema` | mashumaro config | dropped: the recipe is identified by `dataschema` |

New in .NET without a Python counterpart: outcomes and reserved outcomes, deadlines and attempts, outage extension, late responses by action key, cancel, propagation limit, build validation, state determinism, schema tests.

### S15 design details

* **Failure and the default transition.** `true` matches only when no action of the step ended with `timeout` or `error`. Otherwise a recipe with a default path would silently continue after a failed action.
* **Outcome set of a step.** The outcomes of all its actions. Of a divergence: all actions of all its sequences (a selection divergence: the sequence taken). A transition matches when its condition is in the set.
* **Selection entry.** Evaluated with the outcomes of the step that led into the divergence.
* **Sequences cannot route failures.** A sequence has no transitions; a failed action in a sequence is a fault and cancels the open work of the other sequences.
* **Pull-event ambiguity (R-1) at build time.** Two pull-event actions of one type that can wait together (same step, or different sequences of a simultaneous divergence) are refused whatever their `Parameters`, because an event carries only the type. Matching by selector, within one job and across jobs, is S20; relax SFC011 then.
* **Wire names.** The D-44 text names `timeout_seconds` and `type_id`; the Python schema actually uses `TimeoutSeconds` and `TypeId` (PascalCase). The PascalCase names are kept so that existing recipes stay valid.
* **Naming (S15, second pass).** The names follow IEC 61131-3 clause 6.6 where it has a word: divergence and sequence instead of branch and path, the full qualifier list (N, R, S, L, D, P, SD, DS, SL, P1, P0; Python knew only eight and no SD, DS, SL), and `Evolve` for the step function because the standard speaks of the evolution of the chart. Table in [SFC core in .NET](../../sfc-core.md#terms). Sources: PLCopen and vendor documents, since the standard itself is not freely readable.
