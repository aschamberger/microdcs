# SFC Engine

This page describes the design and implementation plan for the SFC (Sequential Function Chart) engine — a station-level execution layer that bridges the northbound OPC UA Job Management interface with southbound equipment interactions.

## Problem Statement

The existing `MachineryJobsCloudEventProcessor` implements the OPC UA Machinery Job Management state machine. It handles Store, StoreAndStart, Start, Pause, Resume, Stop, Abort, Cancel, and Clear commands from the MES/MOM layer and manages job state transitions in Redis. However, the transition from `AllowedToStart` to `Running` — and everything that happens while running — is intentionally left open. The OPC UA specification treats this as an implementation detail.

MicroDCS sits at ISA-95 Level 2 (station/cell control). A station can involve:

- Arbitrary complexity of parallel operations (e.g. QA camera checks running alongside fitting/tightening sequences)
- Two distinct equipment interaction patterns: equipment that pulls work via request/response events, and equipment that publishes state and receives pushed commands
- Recipe-driven execution where the recipe content comes from the MOM layer as part of the Work Master

The SFC engine fills this gap by interpreting Work Master recipe content to orchestrate equipment interactions within a station, using IEC 61131-3 SFC semantics.

## ISA-95 Level Scoping

MicroDCS operates at Level 2 — a single station or cell. It receives fully-resolved Job Orders from MES (Level 3) and executes them locally. Line-level dispatching and routing across stations is the responsibility of the MES layer. The SFC engine does not attempt Level 3 orchestration.

```mermaid
flowchart TB
  mom["<b>MOM / MES</b><br/>Level 3 — dispatches Job Orders,<br/>pushes Work Masters with recipes"]
  microdcs["<b>MicroDCS Station</b><br/>Level 2 — NB Processor receives jobs,<br/>SFC Engine executes recipes"]
  equipment["<b>Equipment</b><br/>Level 1–0 — controllers, cameras,<br/>tightening tools, sensors"]

  mom -- "Job Orders<br/>Work Masters" --> microdcs
  microdcs -- "Job Responses<br/>Status Events" --> mom
  microdcs -- "Commands /<br/>Events" --> equipment
  equipment -- "State /<br/>Results" --> microdcs

  style microdcs fill:#3949ab,color:#fff
```

## Extended Work Master

### Current State

Today, `ISA95WorkMasterDataType` carries only an ID, description, and parameters — the OPC UA envelope. The Job Order references Work Masters by ID, and the `WorkMasterDAO` stores them in Redis. The `MachineryJobsCloudEventProcessor` validates that referenced Work Master IDs exist via `is_member()` but never inspects their content.

### Design: Opaque Data Envelope

Following the CloudEvent pattern where `data` is opaque and `dataschema` identifies its structure, the Work Master is extended with two fields:

| Field | Type | Purpose |
|---|---|---|
| `data` | `dict[str, Any] \| list[Any] \| None` | Recipe content as JSON — structure defined by `dataschema` |
| `dataschema` | `str \| None` | URI identifying the schema that `data` adheres to |

This is implemented as `ISA95WorkMasterDataTypeExt` in `machinery_jobs_ext.py`, inheriting from the generated `ISA95WorkMasterDataType`. Both fields default to `None`, so Work Masters without recipe content remain valid.

The content format is always JSON. The `dataschema` URI discriminates the semantic type — the SFC recipe schema is one possible value, but the design is open to other recipe formats (BPMN subsets, proprietary schemas) without any changes to the Work Master envelope, the NB processor, or the DAO layer.

```python
@dataclass(kw_only=True)
class ISA95WorkMasterDataTypeExt(ISA95WorkMasterDataType):
    data: dict[str, Any] | list[Any] | None = None
    dataschema: str | None = None
```

### Work Master Delivery

Work Masters with recipe content are pushed from the MOM layer into MicroDCS via CloudEvents and stored in Redis through the existing `WorkMasterDAO`. The NB processor's `is_job_acceptable()` validation works unchanged — it checks Work Master ID membership but does not interpret recipe content.

The `WorkMasterDAO.retrieve()` method is updated to deserialize as `ISA95WorkMasterDataTypeExt`, which is backward-compatible since both new fields have `None` defaults.

## SFC Recipe Schema

The recipe schema uses IEC 61131-3 SFC terminology and semantics without the graphical/PLC baggage of PLCopen TC6 XML. It is defined as a JSON Schema (`sfc_recipe.schema.json`) and follows the existing code generation pipeline.

### Why Not PLCopen TC6 XML Directly

PLCopen TC6 is a graphical exchange format for PLC IDEs. Its XSD defines SFC elements (`step`, `transition`, `actionBlock`, `selectionDivergence`, `simultaneousDivergence`) but embeds them in ~80% graphical metadata (x/y coordinates, connection routing, pin positions). Transition conditions and action bodies contain opaque PLC source code (Structured Text, FBD, Ladder Diagram) that cannot execute in Python. Generating dataclasses from the TC6 XSD would produce a PLC file parser, not a sequence execution model.

The SFC recipe schema takes the standardized SFC constructs and expresses them as a runtime/recipe definition suitable for the MicroDCS event-driven architecture.

### Core Elements

The schema defines these top-level structures using IEC 61131-3 SFC terminology:

#### Step

A named step in the sequence. Steps are the states of the SFC state machine.

| Field | Type | Description |
|---|---|---|
| `name` | string | Unique step identifier |
| `initial` | boolean | Whether this is the initial step (exactly one per recipe) |

#### Transition

A directed edge between steps (or branch constructs). Transitions carry conditions that determine when execution advances.

| Field | Type | Description |
|---|---|---|
| `source` | string | Source step or branch name |
| `target` | string | Target step or branch name |
| `condition` | string | Condition identifier — resolved at runtime |
| `priority` | integer | Priority for selection divergence (lower = higher priority) |

#### Action Association

Associates an action with a step, including the interaction pattern and IEC 61131-3 action qualifier.

| Field | Type | Description |
|---|---|---|
| `name` | string | Action identifier |
| `qualifier` | enum | IEC 61131-3 action qualifier: `N` (non-stored), `P` (pulse), `P0`/`P1` (falling/rising edge), `S` (set/stored), `R` (reset), `L` (time limited), `D` (time delayed) |
| `interaction` | enum | `push_command` or `pull_event` — see [Equipment Interaction Patterns](#equipment-interaction-patterns) |
| `cloudevent_type` | string | CloudEvent type for the outgoing command or expected incoming event |
| `timeout_seconds` | integer | Maximum wait time before expiration handling |
| `parameters` | object | Step-specific parameters passed to the action |

#### Selection Branch

OR-branching: one of N paths is taken based on transition priorities/conditions.

| Field | Type | Description |
|---|---|---|
| `name` | string | Branch identifier (used as source/target in transitions) |
| `type` | `"selection"` | Discriminator |
| `branches` | list[list[string]] | Each inner list is a sequence of step names forming one branch |

#### Simultaneous Branch

AND-branching: all N paths execute in parallel and must all complete before convergence.

| Field | Type | Description |
|---|---|---|
| `name` | string | Branch identifier (used as source/target in transitions) |
| `type` | `"simultaneous"` | Discriminator |
| `branches` | list[list[string]] | Each inner list is a sequence of step names forming one parallel branch |

### Equipment Interaction Patterns

Each SFC action declares its interaction pattern, reflecting the two real-world scenarios:

| Pattern | `interaction` value | MicroDCS mechanism | Example |
|---|---|---|---|
| Equipment pulls work | `pull_event` | SFC engine activates the step, waits for a matching `@incoming` CloudEvent from equipment | Camera publishes QA result, processor evaluates pass/fail |
| Station pushes commands | `push_command` | SFC engine calls `callback_outgoing()` on step entry, waits for `process_response_cloudevent()` or timeout | Processor sends tighten command to tightening controller |

### Example Recipe

Rear axle fitting station with parallel QA check:

```json
{
  "steps": [
    { "name": "Init", "initial": true },
    { "name": "Positioning" },
    { "name": "TightenBolts" },
    { "name": "QaCheck" },
    { "name": "VerifyTorque" },
    { "name": "Complete" }
  ],
  "branches": [
    {
      "name": "FitAndQa",
      "type": "simultaneous",
      "branches": [
        ["Positioning", "TightenBolts"],
        ["QaCheck"]
      ]
    }
  ],
  "transitions": [
    { "source": "Init", "target": "FitAndQa", "condition": "always" },
    { "source": "FitAndQa", "target": "VerifyTorque", "condition": "always" },
    { "source": "VerifyTorque", "target": "Complete", "condition": "torque_ok" }
  ],
  "actions": [
    {
      "name": "position_axle",
      "step": "Positioning",
      "qualifier": "N",
      "interaction": "push_command",
      "cloudevent_type": "com.example.station.position.v1",
      "timeout_seconds": 30
    },
    {
      "name": "tighten",
      "step": "TightenBolts",
      "qualifier": "N",
      "interaction": "push_command",
      "cloudevent_type": "com.example.station.tighten.v1",
      "timeout_seconds": 60,
      "parameters": { "torque_spec": "85Nm" }
    },
    {
      "name": "camera_qa",
      "step": "QaCheck",
      "qualifier": "N",
      "interaction": "pull_event",
      "cloudevent_type": "com.example.station.qa_result.v1",
      "timeout_seconds": 45
    },
    {
      "name": "verify",
      "step": "VerifyTorque",
      "qualifier": "N",
      "interaction": "pull_event",
      "cloudevent_type": "com.example.station.torque_result.v1",
      "timeout_seconds": 15
    }
  ]
}
```

This recipe would be stored in the Work Master as:

```python
ISA95WorkMasterDataTypeExt(
    id="WM-AXLE-FIT-001",
    description=LocalizedText(text="Rear axle fitting and QA"),
    parameters=[ISA95ParameterDataType(name="torque_spec", value="85Nm")],
    data={...},  # the recipe JSON above
    dataschema="https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/",
)
```

## SFC Engine Architecture

### Role in the System

The SFC engine is **not** a `CloudEventProcessor`. It is a separate orchestration layer that sits between the northbound and southbound processors and interacts with them via direct Python method calls — not CloudEvent round-trips. This separates three distinct concerns:

| Layer | Responsibility | Abstraction |
|---|---|---|
| **NB protocol** | OPC UA Job Management state machine, CloudEvent serialization, MQTT topics | `MachineryJobsCloudEventProcessor` |
| **SFC orchestration** | Recipe interpretation, step sequencing, branching, action dispatch | `SfcEngine` (`AdditionalTask`) |
| **SB protocol** | Equipment-specific CloudEvent shaping, transport binding | Equipment `CloudEventProcessor`(s) |

The engine holds references to the NB processor (to trigger `Run` / state transitions) and the SB processor(s) (to call `callback_outgoing()` and receive results). SB processors retain their full `@incoming` / `@outgoing` interface and can be used independently for testing or manual triggering.

```mermaid
flowchart LR
  mom["MOM / MES"]
  nb["NB Processor<br/>(MachineryJobs)"]
  sfc["SFC Engine<br/>(AdditionalTask)"]
  sb["SB Processor(s)<br/>(Equipment)"]
  equipment["Equipment"]
  redis[("Redis")]

  mom -- "Store / Start<br/>commands" --> nb
  nb -. "direct call:<br/>job state" .-> sfc
  sfc -. "direct call:<br/>trigger Run / Stop / Abort" .-> nb
  sfc -. "direct call:<br/>callback_outgoing()" .-> sb
  sb -. "direct call:<br/>action result" .-> sfc
  sb -- "CloudEvents" --- equipment
  nb & sfc --- redis

  style sfc fill:#3949ab,color:#fff
```

Dashed arrows are direct Python method calls within the same process. Solid arrows are CloudEvent messages over protocol transports (MQTT, MessagePack-RPC). The SFC engine never touches serialization, topic structures, or transport concerns.

### Execution Flow

1. **Job arrives**: MES sends `Store` or `StoreAndStart` → NB processor transitions job to `NotAllowedToStart` or `AllowedToStart`
2. **SFC engine picks up**: The NB processor notifies the SFC engine via a direct method call (or the engine polls Redis) when a job is ready to run. The engine:
    - Loads the Job Order from Redis
    - Resolves the Work Master ID → loads the extended Work Master with recipe from Redis
    - Dispatches on `dataschema` to select the recipe interpreter
    - Deserializes `data` into SFC recipe dataclasses
3. **Triggers Run**: The engine calls `trigger()` on the NB processor's `HierarchicalGraphMachine` to transition the job to `Running`, then persists the state change
4. **Walks the SFC**:
    - Steps map to states in the `transitions` library
    - For `push_command` actions: calls the SB processor's `callback_outgoing()` directly with the action's CloudEvent type and parameters, then awaits the result or timeout
    - For `pull_event` actions: registers interest with the SB processor and waits for it to deliver a matching incoming event via callback
    - Simultaneous branches execute via `asyncio.TaskGroup` — all branches must complete before convergence
    - Selection branches evaluate transition conditions/priorities to pick one path
5. **Completes**: On recipe completion, triggers the NB job state to `Ended_Completed` via direct call and writes the `ISA95JobResponseDataType`
6. **Handles failures**: On timeout, equipment error, or abort — triggers `Ended_Aborted` or `Aborted` on the NB processor as appropriate

### State Machine Integration

The SFC recipe steps map to the `transitions` library, reusing the same `HierarchicalGraphMachine` pattern as the OPC UA job state machine:

| SFC concept | `transitions` mapping |
|---|---|
| Step | State |
| Transition | Trigger with guard (condition) |
| Simultaneous branch | `HierarchicalGraphMachine` parallel states |
| Selection branch | Multiple transitions from same source, evaluated by priority |
| Action qualifier | `on_enter` / `on_exit` callbacks on states |

The SFC state machine is separate from the OPC UA job state machine. The SFC engine manages both:

- The **OPC UA state machine** (job lifecycle: `AllowedToStart` → `Running` → `Ended`) via the existing `HierarchicalGraphMachine` in the NB processor
- The **SFC state machine** (recipe execution: step → transition → step) as a second machine instance for the recipe

### Persistence

The SFC engine persists its execution state in Redis so that recipe execution can survive pod restarts:

- Current step(s) and active branch states
- Pending action completion status
- Job-to-recipe association

This builds on the existing Redis JSON persistence pattern used by `JobOrderAndStateDAO`.

## Implementation Plan

### Phase 1: Extended Work Master

**Goal**: Enable Work Masters to carry recipe content without changing the NB processor.

1. Add `ISA95WorkMasterDataTypeExt` to `machinery_jobs_ext.py` with `data` and `dataschema` fields
2. Update `WorkMasterDAO.retrieve()` to deserialize as `ISA95WorkMasterDataTypeExt`
3. Update `WorkMasterDAO.save()` type hint to accept the ext type
4. Add unit tests for serialization roundtrip with and without recipe data
5. Update existing `WorkMasterDAO` tests for the new return type
6. Update this document's "Extended Work Master" section with final field names and code snippets
7. Update `docs/concepts.md`: add "Extended Work Master" to the ISA-95 Job Management section explaining the opaque data envelope; add glossary entries for **Work Master**, **Extended Work Master**, **`dataschema` (Work Master)**
8. Update `docs/information-model-standards.md`: add "Extended Work Master (ISA95WorkMasterDataTypeExt)" subsection after OPC UA / Machinery Job Management with field table, code snippet, and DAO behavior

### Phase 2: SFC Recipe Schema and Dataclasses

**Goal**: Define the recipe format and generate typed dataclasses.

1. Create `schemas/sfc_recipe.schema.json` with steps, transitions, actions, branches
2. Generate dataclasses via `dataclassgen`: `sfc_recipe.py` in `models/`
3. Create `sfc_recipe_mixin.py` if needed for `transitions` library integration
4. Add the SFC recipe `dataschema` URI constant
5. Update `models/__init__.py` exports
6. Add unit tests for recipe dataclass serialization/deserialization
7. Update this document's "SFC Recipe Schema" section with the final JSON Schema and generated dataclass details
8. Update `docs/concepts.md`: add glossary entries for **SFC recipe**, **Step**, **Transition**, **Action association**, **Action qualifier**, **Selection branch**, **Simultaneous branch**, **`push_command`**, **`pull_event`**
9. Update `docs/information-model-standards.md`: add "SFC Recipe Schema" subsection covering recipe structure, generation command, and `dataschema` URI
10. Update `docs/technical-standards.md`: add "IEC 61131-3 SFC" section referencing SFC terminology usage and its relationship to the recipe schema
11. Update `docs/development.md`: add SFC recipe dataclass generation command (`uv run microdcs dataclassgen dataclasses schemas/sfc_recipe.schema.json`)

### Phase 3: SFC Engine (Core)

**Goal**: Implement the basic sequential execution engine as an `AdditionalTask`.

1. Create `src/microdcs/sfc_engine.py` with `SfcEngine(AdditionalTask)`
2. Accept NB processor and SB processor references via constructor injection
3. Implement recipe loading: Job Order → Work Master ID → Redis → recipe deserialization dispatched by `dataschema`
4. Implement `Run` trigger on the NB processor's `HierarchicalGraphMachine` via direct call
5. Implement linear step execution (no branching): step entry → action dispatch → wait for completion → transition evaluation → next step
6. Implement `push_command` action pattern: call SB processor's `callback_outgoing()` directly + response/timeout handling
7. Implement `pull_event` action pattern: register with SB processor for incoming event delivery + step completion signaling
8. Implement job completion: `Ended_Completed` state transition on NB processor + `ISA95JobResponseDataType`
9. Implement job failure: timeout → `Ended_Aborted`, equipment error → `Aborted` via NB processor
10. Add Redis persistence for execution state
11. Add unit tests with mocked processors and Redis
12. Update this document's "SFC Engine Architecture" section with final class design, method signatures, and persistence schema
13. Update `docs/concepts.md`: add a new "SFC Engine" section under Framework Concepts explaining the three-layer architecture (NB protocol → SFC orchestration → SB protocol) and the `AdditionalTask` base; add glossary entries for **SFC engine**, **AdditionalTask**, **Three-layer architecture**
14. Update `docs/overall-design.md`: add "Three-Layer Architecture" subsection describing the NB protocol / SFC orchestration / SB protocol layer separation
15. Update `docs/persistence.md`: expand key schema table with SFC execution state keys; add "SFC Execution State" section documenting `SfcExecutionDAO` and `ActionStateDAO` with usage examples
16. Update `docs/your-first-processor.md`: add note in overview that processors are stateless protocol adapters and point to SFC Engine docs for multi-step orchestration

### Phase 4: Branching

**Goal**: Add parallel and selection branching support.

1. Implement simultaneous (AND) branches via `asyncio.TaskGroup`
2. Implement selection (OR) branches via priority-based transition evaluation
3. Implement convergence logic (simultaneous: all must complete; selection: first wins)
4. Add unit tests for branching scenarios
5. Update this document's "State Machine Integration" section with final branching implementation details
6. Update `docs/concepts.md`: expand the SFC Engine section with branching semantics (simultaneous/selection divergence and convergence)

### Phase 5: Application Wiring

**Goal**: Integrate into the example application.

1. Instantiate `SfcEngine` in `app/__main__.py`, injecting the NB and SB processor references
2. Register via `microdcs.add_additional_task(sfc_engine)` — the engine runs as a long-lived task within the `SystemEventTaskGroup` and monitors `_shutdown_event` for graceful shutdown
3. SB processors are still registered with their own protocol bindings as usual — the engine calls them directly, it does not replace their transport wiring
4. Create example Work Master with SFC recipe for testing
5. Integration test with MQTT broker and Redis
6. Update `docs/your-first-processor.md`: add "SFC Engine Integration" subsection in "Wire It Up" explaining that the engine calls processors directly and showing wiring example
7. Update `docs/development.md`: add SFC engine runtime wiring example (`SfcEngine` instantiation + `add_additional_task`)
8. Update `docs/index.md`: add SFC Engine to the "Start Here" reading path and mention recipe-driven station orchestration in the overview

### Phase 6: Work Master Delivery

**Goal**: Enable MOM to push Work Masters with recipes into the system.

1. Add an `@incoming` handler for `ISA95WorkMasterDataType` CloudEvents (either on the NB processor or a dedicated processor)
2. Store received Work Masters via `WorkMasterDAO.save()`
3. Define the CloudEvent type and topic for Work Master delivery
4. Add unit tests for the delivery flow
5. Update this document's "Work Master Delivery" subsection with final topic structure and CloudEvent type

## Design Decisions

- **Opaque `data` + `dataschema` on Work Master**: Follows the CloudEvent envelope pattern. The NB processor and DAO stay ignorant of recipe content. Any future recipe format can be added by defining a new `dataschema` URI without touching existing code.
- **JSON only for `data`**: The Work Master lives in Redis JSON. Keeping `data` as a JSON-native structure (`dict | list`) avoids base64 encoding and enables Redis JSON path queries into recipe content if needed.
- **SFC engine as a separate orchestration layer, not a processor**: The engine is an `AdditionalTask`, not a `CloudEventProcessor`. This enforces a clean three-layer separation: NB protocol handling → SFC orchestration → SB protocol handling. The engine interacts with processors via direct Python method calls within the same process, avoiding CloudEvent round-trips for internal orchestration. This keeps the NB processor as a pure OPC UA protocol handler and SB processors as pure equipment protocol handlers — neither needs to know about SFC concepts. The engine can be replaced with a different execution strategy without changing any processor code.
- **`transitions` library reuse**: The `HierarchicalGraphMachine` is already proven in the codebase for the OPC UA job state machine. SFC steps/transitions map naturally to its states/triggers model.
- **IEC 61131-3 SFC terminology**: Uses standardized names (step, transition, action, qualifier, selection/simultaneous divergence) from IEC 61131-3 without adopting the PLCopen TC6 graphical exchange format.
- **Two interaction patterns**: `push_command` and `pull_event` cover the two real-world equipment integration scenarios observed in discrete manufacturing (automotive). The pattern is declared per action in the recipe, not globally.
