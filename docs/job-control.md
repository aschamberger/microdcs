# Job models and state machine

`MicroDCS.JobControl` holds the ISA-95 Job Control models. Application logic works on version-neutral records; the generated OPC UA types of V1 and V2 stay behind the conversions.

| Part | Type | Use |
|---|---|---|
| Records | `JobOrder`, `JobResponse`, `JobWorkMaster`, `JobParameter`, `JobProperty`, `JobMaterial`, `JobResource` | The neutral model |
| Conversions | `JobControlConversions`, `JobStateArray` | To and from the generated V1 and V2 types. Same-version round trips are lossless |
| Codec | `OpcUaJsonCodec`, `OpcUaJsonPayloadDescriptor<T>` | OPC UA JSON, verbose |
| Validation | `JobControlValidation` | Required ids; the decoder accepts missing fields |
| Schema | `JobControlSchemas.GenerateJson<T>()` | JSON Schema for `dataschema`, generated at runtime |
| State machine | `JobStateMachine.Apply(version, state, operation)` | Pure function, no I/O |

The codec sets no size limits of its own; the transport bounds a message. It keeps the OPC UA nesting limit, and an over-deep payload is a `PayloadException`.

## State machine

`Apply` returns a `JobTransition`: the `ReturnStatus` bitmap, the new state and the effect. A rejected operation is a result, not an exception, and leaves the state unchanged.

| Status bit | Meaning |
|---|---|
| 0 `Success` | Accepted |
| 1 `UnknownJobOrderId` | The order does not exist |
| 2 `InvalidCommand` | V1 only: the command does not apply |
| 3 `InvalidStatus` | V2: the state does not permit the operation |
| 4 `UnableToAccept` | Store of an order that exists |
| 32 `InvalidRequest` | A required field is missing |

The V2 rules come from a table generated from the vendored NodeSet2, with a test that regenerates it. Store, StoreAndStart, Cancel and Clear are a hand-written supplement. `Update` keeps the sub-state; any other entry into a state takes its initial sub-state.

Effects: a transition that the NodeSet marks with an effect emits a status event. Cancel, Clear and V1 Stop remove the order and emit a catalog change. Sub-state transitions, including Close, emit nothing.

## V1 and V2

| V1 state | Neutral state | V2 view |
|---|---|---|
| Waiting | `NotAllowedToStartWaiting` | NotAllowedToStart |
| Ready | `AllowedToStartReady` | AllowedToStart |
| Loaded | `V1Loaded` | Running |
| Running, Held, Suspended, Completed, Closed, Aborted | same | same |
| Error | `V1Error` | Aborted |

The Prepare sub-states exist in V2 only. A V1 view shows all three as Waiting or Ready. V1 has no Pause, Resume, Abort or RevokeStart. V1 Stop removes the order.

## Work Master extension

`ISA95WorkMasterExtDataType` is a subtype of the V2 Work Master with `DataSchema` and `Data`. Its model is `NodeSets/MicroDcs.JobControl.NodeSet2.xml`; the OPC UA source generator creates the type at build time. It has its own CloudEvent type (`JobControlTypes.WorkMasterExt`), because a subtype inside a job order decodes as the base type. Job orders reference Work Masters by id.

## Machinery Jobs processor

`MicroDCS.MachineryJobs` serves Job Control V2 on the binding `machinery_jobs`. It needs Valkey. `microdcs serve` runs it when that binding is configured.

| CloudEvent type | Handling |
|---|---|
| `org.opcfoundation.ISA95-JOBCONTROL_V2.{Store, StoreAndStart, Update}.v2` | Carry `JobOrder` and `Comment`; acceptance check, then the state machine |
| `org.opcfoundation.ISA95-JOBCONTROL_V2.{Start, Stop, Abort, Pause, Resume, RevokeStart, Cancel, Clear}.v2` | Carry `JobOrderID` and `Comment` |
| `org.opcfoundation.Machinery.Jobs.RequestJobResponseByJobOrder{ID, State}.v1` | Queries; a state query with only a top-level state matches its sub-states |
| `com.github.aschamberger.ISA95-JOBCONTROL_V2.config.{equipment, materialclass, materialdefinition, personnel, physicalasset, workmaster, workmaster.ext, jobacceptance}.v1` | Station configuration. CloudEvent method `DELETE` removes, anything else stores. No reply |

A response uses the call's type id with `.response` before the version. `ReturnStatus` is a JSON string, as OPC UA JSON writes every UInt64.

A status event (`ISA95JobOrderStatusEventType.v2`, intent `Event`, subject is the scope) follows each transition the NodeSet marks with an effect. Every entry into AllowedToStart also queues the job's recipe, atomically with the order. Cancel and Clear delete the order and leave a change entry; the job response stays. An order id belongs to one scope: a command from another scope gets `UnknownJobOrderId`.

Once at start one replica publishes a retained metadata event (`org.opcfoundation.Machinery.Jobs.Meta.v1`, intent `Meta`) with the supported commands.

| Configuration | Type | Default | Description |
|---|---|---|---|
| `MicroDcs:JobAcceptance:CheckEquipmentId` | bool | `true` | Equipment requirements must be in the scope's list. |
| `MicroDcs:JobAcceptance:CheckMaterialClassId` | bool | `true` | Material classes must be in the scope's list. |
| `MicroDcs:JobAcceptance:CheckMaterialDefinitionId` | bool | `true` | Material definitions must be in the scope's list. |
| `MicroDcs:JobAcceptance:CheckPersonnelId` | bool | `true` | Personnel requirements must be in the scope's list. |
| `MicroDcs:JobAcceptance:CheckPhysicalAssetId` | bool | `true` | Physical asset requirements must be in the scope's list. |
| `MicroDcs:JobAcceptance:CheckWorkMaster` | bool | `true` | Work Masters must be known in the scope. |
| `MicroDcs:JobAcceptance:CheckMaxDownloadableJobOrders` | bool | `true` | Limit the number of job orders of a scope. |
| `MicroDcs:JobAcceptance:MaxDownloadableJobOrders` | int | `0` | Limit for a scope without its own; 0 means no limit. A limit set through `config.jobacceptance` always applies, also 0, which accepts nothing. |

The inspector `jobs` backs the MCP tools `job_orders` and `job_order`. The maintenance tool `maint_job_delete_plan` removes one order; it bypasses the state machine.
