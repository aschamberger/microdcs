# Information Model Standards

MicroDCS takes its payload models from industrial information-model standards instead of defining its own. This page
lists them and shows how they become .NET types and CloudEvent payloads. The code reference is
[Job models and state machine](job-control.md).

## OPC UA

MicroDCS uses OPC UA **information models** (data types, state machines, method signatures) as payload definitions. It
has no OPC UA server, client or PubSub transport ([D-4](porting/decisions.md#d-4)); the models travel as CloudEvents
over MQTT or NATS.

The models come from the OPC Foundation [UA-.NETStandard](https://github.com/OPCFoundation/UA-.NETStandard) stack 2.x
(MIT licence, [D-21](porting/decisions.md#d-21)), which generates C# types from NodeSet2 files at build time and
supports Native AOT. Payloads use the **OPC UA JSON encoding, verbose form** (`OpcUaJsonCodec`). In that form every
`UInt64` and `Int64` is a JSON string, so `ReturnStatus` and `Sequence` appear in quotes.

### ISA-95 Job Control (OPC 10031-4) and Machinery Jobs (OPC 40001-3)

[OPC 40001-3 Machinery Job Management](https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/) uses the ISA-95
Job Control model of [OPC 10031-4](https://reference.opcfoundation.org/ISA95JOBCONTROL/v200/docs/): job orders, job
responses, Work Masters, and a job order state machine with the methods Store, StoreAndStart, Update, Start, Stop,
Abort, Pause, Resume, RevokeStart, Cancel and Clear.

* **Version 2 on the wire.** The Machinery Jobs processor serves Job Control V2 only ([D-50](porting/decisions.md#d-50)).
  Application logic works on version-neutral records (`JobOrder`, `JobResponse`, …); conversions map them to the
  generated V1 and V2 types ([D-34](porting/decisions.md#d-34)).
* **State machine.** The transition table is generated from the V2 NodeSet2 at build time, with a test that
  regenerates it. `JobStateMachine.Apply` is a pure function over that table. The stack's own state machine engine is
  internal and not used.
* **Methods as CloudEvents.** Each method is a CloudEvent type, for example
  `org.opcfoundation.ISA95-JOBCONTROL_V2.Store.v2`; its response type adds `.response` before the version. The method
  arguments and the response are structures of the MicroDCS extension NodeSet (below), because OPC UA defines no data
  type for a method's argument list.
* **Event mode.** Each transition that the NodeSet marks with an effect emits an
  `ISA95JobOrderStatusEventType.v2` CloudEvent.
* **List mode.** The specification also exposes the lists `JobOrderList` and `JobOrderResponseList`. MicroDCS maps them
  to state topics: `order/{id}` and `result/{id}` per job plus a `state-index` per scope. See
  [Machinery Jobs – MES Publishing](machinery-jobs-mes-publishing.md).

Links: [OPC UA Machinery Jobs overview video](https://youtu.be/KOhYcezpJCw),
[ISA95 in the .NET stack](https://github.com/OPCFoundation/UA-.NETStandard/blob/master/docs/ISA95.md).

### MicroDCS Job Control extension

`src/MicroDCS.JobControl/NodeSets/MicroDcs.JobControl.NodeSet2.xml` (namespace `urn:microdcs:jobcontrol`) is an own
NodeSet2 model, compiled by the same OPC UA source generator. It adds:

| Type | Purpose |
|---|---|
| `ISA95WorkMasterExtDataType` | Work Master subtype with `DataSchema` and `Data`: the recipe that the Work Master carries. |
| `JobOrderMethodCallDataType`, `JobOrderIdMethodCallDataType`, `JobMethodResponseDataType` | Arguments and result of the job order methods. |
| `JobResponseByIdCallDataType`, `JobResponseByStateCallDataType` and their results | The job response queries. |
| `JobOrderStatusEventDataType` | Payload of the status event. |
| `JobOrderStateIndexDataType`, `JobOrderStateIndexEntryDataType` | The state index for the MES. |

The schema document is
[`schemas/microdcs-jobcontrol/v1.2.0.json`](https://aschamberger.github.io/microdcs/schemas/microdcs-jobcontrol/v1.2.0.json).

### Work Master with a recipe

A standard Work Master carries an id, a description and parameters. `ISA95WorkMasterExtDataType` adds two fields after
the CloudEvents pattern of opaque data with a schema:

| Field | Purpose |
|---|---|
| `DataSchema` | URI of the schema that `Data` follows. |
| `Data` | The recipe content as JSON. |

`DataSchema` decides how the recipe is read. An SFC recipe uses
`https://aschamberger.github.io/microdcs/schemas/sfc-recipe/v1.1.0.json` (v1.0.0 is still read). Other recipe formats
can be added with their own URI without changing the Work Master, the processor or the store. The extended Work Master
has its own CloudEvent type, `com.github.aschamberger.ISA95-JOBCONTROL_V2.config.workmaster.ext.v1`, because a subtype
inside a job order decodes as the base type. Job orders reference Work Masters by id.

### Station configuration

The MES pushes the station's resource lists and Work Masters as CloudEvents to the Machinery Jobs processor. The
processor checks every job order against them ([job acceptance](job-control.md#machinery-jobs-processor)). Each list
has its own CloudEvent type below `com.github.aschamberger.ISA95-JOBCONTROL_V2.config.` and reuses an ISA-95 data type:

| Type suffix | Payload | Checked field |
|---|---|---|
| `equipment.v1` | `ISA95EquipmentDataType` | `ID` |
| `materialclass.v1` | `ISA95MaterialDataType` | `MaterialClassID` |
| `materialdefinition.v1` | `ISA95MaterialDataType` | `MaterialDefinitionID` |
| `personnel.v1` | `ISA95PersonnelDataType` | `ID` |
| `physicalasset.v1` | `ISA95PhysicalAssetDataType` | `ID` |
| `workmaster.v1` | `ISA95WorkMasterDataType` | `ID` (the Work Master is stored) |
| `workmaster.ext.v1` | `ISA95WorkMasterExtDataType` | `ID` (the Work Master with its recipe is stored) |
| `jobacceptance.v1` | `ConfigJobAcceptance` (plain JSON, `max_downloadable_job_orders`) | the scope's order limit |

The CloudEvent attribute `method` selects the operation: `DELETE` removes the entry, anything else (also no `method`)
stores it. The CloudEvent `subject` names the scope, so each station keeps its own lists. Configuration events get no
reply.

### SFC recipe format

The SFC recipe is a MicroDCS format, defined by the .NET model in `MicroDCS.Sfc` and published as JSON Schema
([v1.1.0](https://aschamberger.github.io/microdcs/schemas/sfc-recipe/v1.1.0.json)). It uses IEC 61131-3 terms; see
[SFC Engine](sfc-engine.md).

## ISA-88 and ISA-95

ISA-95 defines the levels of manufacturing systems and the objects exchanged between them (job orders, responses,
Work Masters). ISA-88 defines batch control: procedures, unit procedures, operations and phases. MicroDCS sits at
level 2 of ISA-95 and runs recipes in the spirit of ISA-88 procedural control, expressed as SFCs.

* [ISA-95 introduction by Rhize](https://docs.rhize.com/isa-95/how-to-speak-isa-95/)
* [ISA-95 introduction video](https://youtu.be/OobhzbQoUnA)
* [ISA-88 and ISA-95 integrated consulting](https://iacsengineering.com/isa-88-and-isa-95-integrated-consulting/)
* [What is ISA-88 manufacturing](https://mdcplus.fi/blog/what-is-isa-88-manufacturing/)
* [Using ISA-88 and ISA-95 together](https://www.isa.org/products/isa-tr88-95-01-2008-using-isa-88-and-isa-95-togeth)

## Own payload models

An application defines its own payloads as C# records with `[CloudEventType]` and lists them in a
`JsonSerializerContext`; System.Text.Json source generation serialises them, and a test can generate and compare their
JSON Schema ([Processors](processors-dotnet.md#a-payload), [D-58](porting/decisions.md#d-58)). A payload field that
should travel as a CloudEvent attribute, not in the body, is marked `[CarriedAs]`. A generated OPC UA structure can be
a payload too, with the `OpcUaJson` codec.
