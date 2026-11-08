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
