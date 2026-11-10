# SFC core in .NET

This page describes `MicroDCS.Sfc` and `MicroDCS.Sfc.Simulation` (decisions [D-17](porting/decisions.md#d-17), [D-18](porting/decisions.md#d-18) and [D-44](porting/decisions.md#d-44)). The core is the sequence logic of the SFC engine as one pure function. It replaces the logic part of `sfc_engine.py`; [the Valkey host](#the-valkey-host) runs it. The older page [SFC Engine](sfc_engine.md) still describes the Python design.

```text
(graph, state, snapshot, event)  ->  (next state, continuation, commands)
```

The core reads no clock, does no I/O and keeps no memory between calls. `MicroDCS.Sfc` references no other project and no package; an architecture test enforces that. The host loads the state, calls the core, and stores the new state together with the commands in one atomic step.

## Terms

Names follow IEC 61131-3 (clause 6.6, sequential function chart) wherever the standard has a word for the thing. I could not read the standard text itself; the terms below are confirmed from PLCopen and vendor documents. Recipe wire names are unchanged from the Python schema, so a few C# names differ from the JSON names.

| IEC 61131-3 term | .NET name | Recipe wire name |
|---|---|---|
| step, initial step | `SfcStep`, `Initial`; `SfcState.ActiveSteps` (a step is active or inactive) | `Steps`, `Initial` |
| transition, transition condition | `SfcTransition`, `Condition` (a named Boolean condition: an outcome or `true`) | `Transitions`, `Condition` |
| action block (qualifier, action name, step it belongs to) | `SfcActionBlock` | `Actions` |
| action qualifier N, R, S, L, D, P, SD, DS, SL, P1, P0 | `SfcActionQualifier` (`NonStored`, `OverridingReset`, `Set`, `TimeLimited`, `TimeDelayed`, `Pulse`, `StoredAndTimeDelayed`, `DelayedAndStored`, `StoredAndTimeLimited`, `PulseRisingEdge`, `PulseFallingEdge`) | `Qualifier` (short form) |
| selection divergence and convergence, simultaneous divergence and convergence | `SfcDivergence` with `Kind`; the convergence is where its sequences end | `Branches[]`, `Type` |
| sequence (steps between divergence and convergence) | `SfcDivergence.Sequences`, `SfcSequencePosition` | `Branches[].Branches` |
| sequence loop, sequence skip | transitions back to an earlier step, or past steps; no extra element | transitions |
| evolution of the chart (enabling and clearing of transitions) | `IRecipeInterpreter.Evolve`, `SfcEvolution` | n/a |

Not in the standard, added by MicroDCS: outcomes, `Priority` (vendors resolve competing transitions differently, for example left to right), push and pull interactions, deadlines and attempts (`TimeoutSeconds` is the deadline of one attempt and is unrelated to the `L` qualifier), and the host-side events `Tick`, `Resume`, `Cancel`, `Hold` and `Release`. The function is called `Evolve`, not `Step` (a step is an SFC element and `Step` is a reserved word in some .NET languages, CA1716) and not `Evaluate` (too generic: it does not say that the chart evolves).

## Using the core

```csharp
var interpreter = new SfcInterpreter();
SfcBuildResult built = interpreter.Build(recipeUtf8Json, capabilities);   // validate once, reuse the graph
SfcGraph graph = built.Graph!;

SfcState state = SfcState.Create(graph, jobId: "job-7", scope: "plant.line1");
SfcStepResult r = interpreter.Evolve(graph, state, SfcSnapshot.Default, new SfcStart { At = nowMs });
// r.State is stored; r.Commands are applied in order; r.Continuation goes to the job state machine.
```

`At` is the host's time in epoch milliseconds. The host takes it from the stream entry id of the work item, except for `SfcStart` and responses, which read the server clock (D-44).

## Recipe

The wire format is the Python recipe: PascalCase names (`Steps`, `Transitions`, `Actions`, `Branches` for the divergences; `TimeoutSeconds`, `TypeId`). `dataschema` is `https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/`. The C# model is the source of the JSON Schema in [`schemas/sfc_recipe.schema.json`](https://github.com/aschamberger/microdcs/blob/main/schemas/sfc_recipe.schema.json); a test regenerates it and fails when the file is stale (`UPDATE_SCHEMAS=1 dotnet test` rewrites it).

| Field | Meaning |
|---|---|
| `Steps[].Initial` | Exactly one step is initial. |
| `Transitions[].Condition` | An outcome name, or `true` for the default. |
| `Transitions[].Priority` | Lower is tried first. Unique among the transitions of one source. |
| `Actions[].Interaction` | `push_command` (send a command, wait for the response) or `pull_event` (wait for an event). |
| `Actions[].Qualifier` | Short form of an IEC 61131-3 qualifier. Only `N` is supported; the others are known (`SfcActionQualifier`) and refused at build. |
| `Actions[].TimeoutSeconds` | Deadline of one attempt. |
| `Actions[].MaxAttempts` | Optional. Dispatch attempts of a push command; the host default (`Sfc:MaxAttempts`, 3) applies when absent. |
| `Actions[].Parameters` | Handed to the command. |
| `Actions[].Selector` | Optional, pull events only. `Subject` and `CorrelationId` must match the event exactly. The placeholder `{jobId}` stands for the job id; a recipe is shared by many jobs, so a literal cannot tell jobs apart. Without a selector any event of the type matches. |
| `Branches[]` | A divergence: `Type` is `simultaneous` (all sequences) or `selection` (one sequence). `Branches` inside it lists the sequences, each a list of step names. |

Existing Python recipes need one change: `Condition: "always"` becomes `true`, and a named condition must be an outcome that the action types declare.

## Semantics

* **Steps.** Entering a step starts all its actions. A step is done when every action has an outcome. A step without actions is passed through at once.
* **Outcomes.** A response, an event or a timeout gives an action an outcome. A transition fires when its condition equals one of the step's outcomes, or is `true`. The first match by priority wins. `timeout` (attempts used up) and `error` (failure reported) are reserved and routable.
* **Failures.** `true` does not match when an action failed; only a transition that names `timeout` or `error` does. If nothing matches, the continuation is `Fault` and the host decides what to do.
* **Simultaneous divergence.** The transition into the divergence activates the first step of every sequence. Steps in a sequence follow each other. When all sequences are done (the convergence), the transitions after it are cleared against the outcomes of the whole divergence. A failed action inside a divergence is a fault (a sequence has no transitions to route it); open work of the other sequences is cancelled.
* **Selection divergence.** The selection transitions from the divergence to the first steps of its sequences choose the sequence, by priority and condition. A default one is required.
* **No exit transition** completes the job.
* **Loops** are allowed when they wait; a cycle through steps without actions is refused at build time (a selection divergence counts as action-free when one of its sequences is). A step that is entered again starts with fresh action records.

## Time, attempts and late responses

* An attempt's deadline is `At + TimeoutSeconds`. `SfcState.NextDeadline` is the earliest open one; the host keeps one due-index entry from it and sends `SfcTick` when it passes.
* A push command whose deadline passes is dispatched again (same action key, new command id, `CancelWork` for the old id) until `MaxAttempts`, then the outcome is `timeout`. A pull event times out at once. When the last attempt expires the core also emits `CancelWork` with reason `timeout`, so the host drops the registration of the command or the awaited event.
* `SfcTick.OutageMs` above zero moves all open deadlines by that gap before expiry, so a platform outage does not abort running jobs.
* A response is matched by the action key `{jobId}:{action}`. A response from an earlier attempt completes the action while it is open; every further response is absorbed. A loop enters a step again; each entry has a `Round` that the command carries and the host may echo in the response, and a response with an older `Round` is absorbed as belonging to an earlier pass. A response with an unknown key, another job or a contradicting type is rejected, never matched to a sibling.
* `SfcResume` re-emits open work that `SfcSnapshot.LiveWork` does not contain. A re-dispatch uses up an attempt.
* `SfcCancel` cancels open work and ends the job (`SfcCancelled`).

## Hold and release

`SfcHold` holds a running job at the next step boundary, `SfcRelease` ends the hold (MES Pause and Resume, D-52). This is the usual ISA-88 meaning of pause. The equipment is not told to stop: a command that is out runs to its end.

* While held, no transition fires and nothing is dispatched, re-sends included. A response to a command that is out, or an event for a wait that began before the hold, is recorded on its action. A failure is recorded too, and routed on release.
* Deadlines do not expire during a hold. `NextDeadline` is empty, so the host has no due-index entry for the job, and a tick or a resume is absorbed.
* On release, every open deadline moves by the length of the hold (as with an outage), so the hold costs no attempt. Every open push attempt is issued again with its command id and attempt number; the host sends each command id once. Then the transitions that became possible fire.
* `SfcCancel` ends a held job as usual. A second hold, or a release of a job that is not held, is absorbed.

`SfcStepResult.Disposition` says `Applied`, `Absorbed` (duplicate or late) or `Rejected` (contradiction), with a reason.

## Build errors

| Code | Refused |
|---|---|
| `SFC000` | Not a valid recipe document. |
| `SFC001` | No initial step, or more than one. |
| `SFC002` | Duplicate step, divergence or action name; a name used for a step and a divergence. |
| `SFC003` | Reference to an unknown step or divergence. |
| `SFC004` | Divergence without sequences, with an empty sequence or with an unknown kind. |
| `SFC005` | A step in more than one sequence. |
| `SFC006` | A transition touches the inside of a sequence. |
| `SFC007` | A decision with several transitions, or a selection divergence, has no default; a sequence has no selection transition. |
| `SFC008` | Equal priorities among the transitions of one source. |
| `SFC009` | Unknown interaction, `TimeoutSeconds` or `MaxAttempts` out of range; a selector on a push command, an empty selector value, or a placeholder other than `{jobId}`. |
| `SFC010` | Qualifier other than `N`. |
| `SFC011` | Two pull-event actions of one type that can wait together and whose selectors do not set one field to two different literals. A value with `{jobId}` separates nothing, because the job id can equal any literal. |
| `SFC012` | A step that cannot be reached. |
| `SFC013` | A cycle with nothing to wait for. |
| `SFC014` | An action type that no processor of the station declares: an `[Outgoing]` factory for a push command, an `[Incoming]` handler for a pull event. |
| `SFC015` | A feature the station does not support (push, pull, simultaneous divergence, selection divergence). |
| `SFC016` | A condition names an outcome that no involved action type declares. |
| `SFC017` | An empty step, divergence or action name, or an action name with `:` (the action key is `{jobId}:{action}`). |
| `SFC018` | More than 20,000 steps, transitions, actions or divergences. |

## State and commands

`SfcState` is a versioned JSON document (`format` 1, minor 2) with the job identity, the pinned recipe hash, the active steps, one record per action, a sequence counter, the start time, the time of a hold (`heldAt`, while held) and the fault. Command ids are `{actionKey}#{sequence}`, so replaying the same events on the same state gives byte-identical results. The schema is [`schemas/sfc_state.schema.json`](https://github.com/aschamberger/microdcs/blob/main/schemas/sfc_state.schema.json).

Commands are inert data: `StartWork` (`PushCommand` or `AwaitEvent`, with the deadline and the recipe parameters) and `CancelWork` (`redispatch`, `teardown` or the cancel reason). The host applies them in order and idempotently by action key. The continuation is `SfcDefer`, `SfcComplete`, `SfcFault` or `SfcCancelled`; faults carry a code (`timeout`, `error`, `no_transition`, `propagation_limit`) and are verdicts, not policy.

## Simulation

`MicroDCS.Sfc.Simulation` is an in-memory host: a virtual clock, scripted equipment and a transcript of every evolution step. It is not durable and not distributed.

```csharp
var script = new SfcScript().For("tighten", SfcReply.Silence(), SfcReply.Ok(delayMs: 500));
var sim = new SfcSimulation(graph, script);
SfcRunEnd end = sim.Run();          // Completed, Faulted, Cancelled, TimeLimit or EvaluationLimit
string transcript = sim.Transcript.ToText();
```

Replies are chosen by how often the equipment is asked about an action, so re-dispatches and loop iterations see the next reply. A reply that arrives at the deadline wins over the tick. `SfcExplainer` describes a recipe and the state of a job as text.

## MCP tools

Two read-only tools in the dev MCP server (see [Inspection and MCP](inspection.md)) run the core on text you pass in. Nothing is sent or stored.

| Tool | Does |
|---|---|
| `sfc_explain` | Describes a recipe; with a job state it says where the job is, what it waits for and when the next deadline is due. Refuses invalid recipes with the build errors. |
| `sfc_dryrun` | Runs a recipe on the virtual clock with a script and returns the transcript. |

A script is JSON: `{"default":"ok","replies":{"tighten":["silent","ok@500"]},"events":[{"atMs":1000,"typeId":"t.qa","outcome":"ok"}]}`; an event can also carry `subject` and `correlationId` for a selector. A scripted reply to a pull event always matches its selector. A reply is `silent`, `error[:text][@delayMs]` or `outcome[@delayMs]`. Without a script every action answers `ok` at once. The tools share the rate limit and the audit log of the other tools. They accept any outcome name that the recipe itself uses, so they find structural errors but not outcome typos; a station checks outcomes against its processors at build time.

## The Valkey host {#the-valkey-host}

`MicroDCS.Sfc.Hosting` runs recipes for the jobs of the Machinery Jobs processor (D-52). `AddSfcHost` adds it; `serve` does so when the Machinery Jobs binding is configured. It runs only under the `Processor` role.

For each event the host loads the state, calls the core, and stores the new state, the due-index entry and the work entries in one compare-and-set. Work goes through the `sfc:work:{scope}` stream, read through the poller by the consumer group `sfc-engine`, so any instance can pick it up:

| Entry | Written by | Does |
|---|---|---|
| `start` | The job order script, on every entry into AllowedToStart | Re-reads the order and starts the recipe if the order is still allowed to start. A job without a usable recipe is aborted. |
| `cancel` | The job order script on Abort and Stop, or the `sfc_abort` tool | Ends the recipe; open work is withdrawn. |
| `hold` | The job order script when the order enters Interrupted (Pause) | Holds the recipe at the next step boundary. |
| `release` | The job order script when the order goes from Interrupted to Running (Resume) | Releases the recipe. |
| `dispatch:{action}` | A step, or a release | Sends one push command, only while that attempt is the open one and the job is not held. Each command id is sent once. |
| `job` | A step | Applies job order transitions: start of execution, complete (with the job response) or abort. |
| `tick` | The due-index sweep, or the `sfc_tick` tool | Fires expired deadlines. |
| `resume` | The `sfc_redispatch` tool, or the recovery scan | Issues an open action again. |
| `event` | An incoming event a job waits for | Offers the event to the waiting jobs. |

A failed entry stays pending, another pass takes it over after `AutoclaimIdle`, and after `MaxDeliveries` it moves to `sfc:dead:{scope}` with the reason. `sfc_work_pending` shows both; `maint_stream_requeue_plan` and `maint_stream_ack_plan` act on them.

**Push commands and responses.** The command goes through the `[Outgoing]` factory of its type id, as a command on that processor's binding, with the action key as `mdcsactionkey`. The instance that sent it waits on its own reply address (D-9). The `[OnResponse]` handler returns an `ActionOutcome` that a recipe can name in a condition; the outcome names are declared on the factory, and `done` is the default. An error event from the peer or `ActionOutcome.Failed` is the reserved outcome `error`.

**Pause and Resume.** A `hold` or `release` entry reads the order and aligns the recipe with it: held while the order is Interrupted (Held or Suspended), released when it is Running. The two entries of a Pause and a Resume can reach two consumers in either order, so the entry does not decide by its name; after its write it reads the order again and repeats until both agree. The hold length is measured with the server clock when the hold and the release are applied. A `dispatch` entry that reaches a consumer while the job is held is not sent and costs no attempt; the release issues the command again. After a command is queued, a marker `sent:{action}` in the state hash records its id, so a command sent before the hold, or a redelivered entry, does not reach the equipment again. A crash between the queue and the marker can send a command id twice (receivers deduplicate by id), never lose it. A dispatch takes the deadline from the state, which a release has moved. `sfc_job` shows `heldSince`.

**Deadlines.** Every instance sweeps the due-index of every scope; the script pops each job once. A sweep that finds a heartbeat gap above `OutageThreshold` moves every open deadline by the gap instead of expiring it, so a platform outage does not abort running jobs.

**Pull events.** A wait is state, so any replica can complete it (D-53). The `[Incoming]` handler of the event type runs first. It may return an `ActionOutcome`, named in `[Incoming(Outcomes = ...)]`; without one the outcome is `done`. If a job waits for the type, the event goes into the work stream and is offered to the waiting jobs, oldest wait first, until one takes it, so one event completes one action, also when the broker delivers it twice (CloudEvents `source` and `id` identify it). An event that arrives before the wait exists is dropped, and the action ends with `timeout` if nothing else arrives. A handler that answers with its own type and no reply address publishes to its own `events` address, so it would feed itself; give such events a reply address or return an `ActionOutcome` only.

**Recovery.** Each instance runs a scan when it first sees a scope, and queues a check of every running job there. The scan does not resend commands: the sweep owns every action with a deadline, and a resend on each restart would use up attempts. It re-issues only work that never got a deadline and puts back lost index entries. An instance that dies with an unanswered command does not lose it: another instance sends it again when the deadline passes.

| Setting (`MicroDcs:Sfc:`) | Type | Default | Description |
|---|---|---|---|
| `Enabled` | bool | `true` | Whether this instance runs recipes. |
| `SweepInterval` | duration | 1 s | Time between due-index sweeps. |
| `OutageThreshold` | duration | 30 s | Heartbeat gap that counts as a platform outage. Must be above `SweepInterval` and `MicroDcs:Valkey:PollInterval`, which paces the sweep. |
| `MaxAttempts` | int | 3 | Dispatch attempts of a push action whose recipe sets none. |
| `ExpiryGrace` | duration | 2 s | How long before the deadline the wire expiry of a command ends. |
| `AutoclaimIdle` | duration | 30 s | How long a work entry stays with a consumer before another one takes it over. |
| `MaxDeliveries` | int | 5 | Deliveries of a work entry before it goes to the dead stream. |
| `ReadCount` | int | 10 | Most work entries read at once. |
| `SweepLimit` | int | 100 | Most jobs one ordinary sweep of a scope takes. |
