# OPC UA ISA-95 models spike (S05)

Question: can `Opc.Ua.ISA95*` give plain, serializable job models without an OPC UA server (D-4)? Throwaway AOT console project, packages from nuget.org, .NET 10.0.403, linux-x64, 2026-11-07. The project was not committed.

## Packages

| Package (2.0.1) | Target frameworks | Use |
|---|---|---|
| `OPCFoundation.NetStandard.Opc.Ua.ISA95` | net10.0, net9.0, net8.0, net48 | Common Model and Job Control V1/V2 types: `ISA95JobOrderDataType`, `ISA95JobResponseDataType`, `ISA95WorkMasterDataType`, `ISA95StateDataType`, enums, NodeId tables, server-side `*State` classes |
| `OPCFoundation.NetStandard.Opc.Ua.Machinery.Jobs` | same | Machinery Jobs types (depends on the ISA95 package) |
| `OPCFoundation.NetStandard.Opc.Ua.Core.Schema` | same | Runtime schema generation (JSON Schema, XSD, BSD) |
| `OPCFoundation.NetStandard.Opc.Ua.SourceGeneration` | analyzer | `[DataType]` attribute: `IEncodeable` for own classes |
| `OPCFoundation.NetStandard.Opc.Ua.ISA95.Server` | same | Server only. Not needed |

* All packages target net10.0 directly.
* **Licence:** each package ships `LICENSE.txt`, "OPC Foundation MIT License 1.00" (copyright 2005-2025), `licenseUrl` deprecated, no SPDX expression in the nuspec. Consistent with D-21. The packages contain no NodeSet XML files, so there is no separate NodeSet licence to track here; the NodeSets would be a build-time input only if we write our own (not needed, see Work Master).
* **Weight:** the ISA95 package depends on `Opc.Ua.Core` (the full client and server stack, BouncyCastle, `Microsoft.Extensions.*`), not only on the lightweight `Opc.Ua.Types`. The model assembly is 1.2 MB. A project that only wants models still carries the whole stack as a dependency; trimming and AOT remove what is unused (binary sizes below).

## Results

| Check | Result |
|---|---|
| Plain objects without a server | Works. `new ISA95JobOrderDataType { JobOrderID = ..., Priority = 1 }`; no host, no session |
| OPC UA JSON encode and decode | Works with `JsonEncoder` (over a `Stream`, `IBufferWriter<byte>` or `Utf8JsonWriter`) and `JsonDecoder` (string, stream, `ReadOnlySequence<byte>`) and `ServiceMessageContext.CreateEmpty(null)` |
| AOT publish | No `IL2xxx`/`IL3xxx` warnings. 9.1 MB for the job order round trip, 12.6 MB with schema generation and a source-generated type. Runs correctly |
| System.Text.Json on the generated types | Not usable: source generation serializes `ArrayOf<T>` as `{"Memory":[...]}`, includes `TypeId`, `BinaryEncodingId`, `XmlEncodingId` and `NodeId` internals, and raises `IL2026` (`Variant(object)`) |
| JSON Schema generation | Works at runtime (`DefaultSchemaProvider` + `JsonSchemaGenerator` + `EncodeableFactoryDefinitionSource`) for generated types and for own `[DataType]` types, verbose and compact flavour, AOT clean. The output is Part 6 JSON-encoding shaped (`$defs` for `LocalizedText`, `Variant`, etc.), not a hand-tuned schema |
| `[DataType]` source generation | Works: a partial class with three string properties got `Encode`, `Decode`, `TypeId` and a generated `Add<Namespace>DataTypes()` registration; round trip and schema generation work, AOT clean. Not chosen, see the NodeSet2 section |
| Job state machine standalone | **Not usable as it is.** `Isa95V2StateMachine` and `Isa95JobCanonicalState` in the server package are `internal`; the public generated state machines (`ISA95JobOrderReceiverObjectState`, ...) are address-space `NodeState` objects that need a server `SystemContext`. Only `Isa95JobExecutionTransition` (BeginExecution, Hold, Complete, Close) is public |

## Details worth knowing

* **Root encodeable:** `encoder.WriteEncodeable(null, value)` at the document root writes `{{...}}` (invalid JSON). Call `value.Encode(encoder)` for the root; `WriteEncodeable("name", value)` is for nested fields.
* **Optional fields are driven by `EncodingMask`.** Assigning `Priority` alone does not write it: the mask (a `uint` of `ISA95JobOrderDataTypeFields` flags) must carry the bit. `Compact` output includes `"EncodingMask":18`; `Verbose` output omits it and the decoder sets the mask from the present fields. Hand-built objects therefore need the mask set; decoded objects have it.
* **Encoder presets:** `JsonEncoderOptions.Compact` and `.Verbose` are static; the constructor takes a name. `Verbose` gives plain field names and nested objects (`{"JobOrderID":"Job-1","WorkMasterID":[{"ID":"wm1"}],"Priority":1}`).
* **Decoder strictness:** unknown fields are ignored, missing fields keep defaults (nothing is "required"), a wrong value type throws (`Field 'Priority' of type Int16 cannot be decoded from JSON String`). Required-field checks need the generated schema or our own validation.
* **Types:** arrays are `ArrayOf<T>` (collection expressions work), `Priority` is `Int16`, times are `DateTimeUtc`-style values with a 1601 default, text is `LocalizedText` arrays.
* **Work Master:** `ISA95WorkMasterDataType` has only `ID`, `Description`, `Parameters`. The extension (`DataSchema`, `Data`) is built as an own NodeSet2 model, see below.
* **Job Control V1** is covered in its own section below.

## NodeSet2 path for the Work Master extension

An own model file `MicroDcs.JobControl.NodeSet2.xml` (`urn:microdcs:jobcontrol`, requires `http://opcfoundation.org/UA/` and `http://opcfoundation.org/UA/ISA95-JOBCONTROL_V2/`) with one `UADataType`, `ISA95WorkMasterExtDataType`, `HasSubtype` of the V2 `ISA95WorkMasterDataType` (`ns=2;i=3007`), and two optional string fields in its `Definition` (only the new fields are listed). Added to the project as `<AdditionalFiles>` with `ModelSourceGeneratorModelUri` and `ModelSourceGeneratorPrefix`, next to the `OPCFoundation.NetStandard.Opc.Ua.SourceGeneration` package and a reference to the ISA95 package.

* **Dependency resolution works from the referenced assembly:** no copy of the V2 NodeSet is needed; the generator found the V2 types through the ISA95 assembly's model metadata.
* **Generated:** `ISA95WorkMasterExtDataType : ISA95WorkMasterDataType` (inherited `ID` usable), `...Fields` mask enum, activator, `AddMicroDcsJobControl()` for the encodeable factory and for node state factories, identifiers, XML schema classes. Optional fields need their `EncodingMask` bits like any V2 type.
* **AOT:** publish without warnings, 12.1 MB.
* **Round trip:** standalone `{"ID":"wm1","DataSchema":"urn:x","Data":"{...}"}` encodes and decodes with the factory registration.
* **Subtype inside a job order:** in `ISA95JobOrderDataType.WorkMasterID` (an array of the base type) the extension fields are encoded (the virtual `Encode` runs) but the decoder creates the base type, since the JSON carries no type discriminator. Not a problem for the design: the extended Work Master has its own CloudEvent type and is decoded as the subtype directly; job orders reference Work Masters by ID (D-34).
* `EmitCompilerGeneratedFiles` output must be excluded from compilation (`<Compile Remove="gen/**" />`), otherwise the types are defined twice.

## Job Control V1

* Same codec, same factory registration (`AddOpcUaISA95JobControlV1()`), AOT clean, round trip works.
* V1 types have no `EncodingMask`: all fields are always encoded, absent ones as `null` or empty arrays (`"Description":null,"WorkMasterID":[]`).
* Differences from V2: job order identity `ID` (V2 `JobOrderID`), job response `ID` (V2 `JobResponseID`), flat state enum `ISA95JobOrderStateEnum` (Undefined, Waiting, Ready, Loaded, Running, Completed, Aborted, Held, Suspended, Closed, Error) against hierarchical `ISA95StateDataType` arrays in V2, commands `ISA95JobOrderCommandEnum` (Store, StoreAndStart, Start, Update, Stop, Cancel, Clear; no Pause, Resume, Abort, RevokeStart), return codes `ISA95ReturnEnum`, no `ISA95JobOrderAndStateDataType`.

## Transition rules are data in the NodeSet

The V2 NodeSet (`UA-Nodeset/ISA95-JOBCONTROL/opc.ua.isa95-jobcontrol.nodeset2.xml`, model version 2.0.0, 275 KB, MIT; the copy in the stack repository `src/Opc.Ua.ISA95/Design/JobControl/V2` differs only in whitespace of the header comment) holds transitions as objects with `FromState`, `ToState`, `HasCause` (method) and `HasEffect` (event type) references. The Python port generated its state table from it. Extract (the sub-states machine type repeats the receiver type, 34 references in total):

| Machine | Transitions (from to, cause) |
|---|---|
| Receiver (13) | NotAllowedToStart to AllowedToStart (Start); NotAllowedToStart and AllowedToStart each to themselves (Update); AllowedToStart to NotAllowedToStart (RevokeStart); AllowedToStart to Running (no cause: execution); Running to Interrupted (Pause); Interrupted to Running (Resume); Running and Interrupted to Ended (Stop); NotAllowedToStart, AllowedToStart, Running and Interrupted to Aborted (Abort). All carry the effect `ISA95JobOrderStatusEventType` |
| Interrupted (2) | Held to Suspended, Suspended to Held (no cause) |
| Ended (1) | Completed to Closed (no cause) |
| Prepare (5) | Waiting to Ready, Ready to Waiting, Ready to Loaded, Loaded to Ready, Loaded to Waiting (no cause) |

Not in the NodeSet (the Python port added them by hand, from the specification text): Store and StoreAndStart from an initial state, Cancel (from NotAllowedToStart or AllowedToStart to the end), Clear (from Aborted or Ended to the end), the initial sub-states (Ready, Suspended, Completed) and the Python trigger name `Run` for the cause-less AllowedToStart to Running transition.

The generated C# model contains the transitions only as node factories (`CreateISA95JobOrderReceiverObjectType_FromRunningToInterrupted`, ...) that build address-space nodes, not as a table. The V1 NodeSet (`src/Opc.Ua.ISA95/Design/JobControl/V1` in the stack repository) has no state machine: the `ISA95JobOrderStateEnum`, `ISA95JobOrderCommandEnum`, `ISA95ReturnEnum` and three methods only.

Compared with the stack engine: same top-level rules, but the engine has no Prepare machine (it maps Waiting to NotAllowedToStart and Ready to AllowedToStart). How V1 `Waiting/Ready/Loaded` relate to the V2 Prepare sub-states under both top-level states is to be confirmed against the V1 and V2 specification text in S16.

## Upstream guide `docs/ISA95.md`

Read in full. What matters for the design:

* **Neutral store contract:** every standard V1/V2 job order and response field is preserved (work masters, requirements, actuals, nested parameters and properties, engineering units, V2 `EncodingMask` bits). Same-version round trips are lossless, cross-version views carry only what the target specification can represent. We adopt this as a test property.
* **V1 differs from V2:** `RequestJobResponse` needs exactly one selector, V1 `Stop` removes the stored order, `Loaded` and `Error` are V1 response states that survive round trips. So V1 is not simply the V2 table restricted (corrects the first reading in this session).
* **Operations:** Store, StoreAndStart, Start, Update, Stop, Pause, Resume, Abort, RevokeStart, Cancel, Clear. Update is a self-transition that emits the status event; Cancel, Clear and V1 Stop are removals that emit a catalog change (`Removed`) without a life-cycle transition.
* **Execution-system transitions** are separate from the client methods: BeginExecution, Hold, Complete, Close. They are the transitions without a method cause in the NodeSet, plus Hold and Complete reusing the Interrupted and Ended transitions with a chosen sub-state (Pause lands in Suspended, Hold in Held).
* **Return status:** UInt64 bitmap, Annex B.2 (`Success` bit 0, `UnknownJobOrderId` 1, `InvalidCommand` 2 for V1, `InvalidStatus` 3, `UnableToAccept` 4, `InvalidRequest` 32), several bits at once. Business failures are `Uncertain` with the bits, not `Bad`. The Python `MethodReturnStatus` has no bit 2.
* **Status events:** the standard `ISA95JobOrderStatusEventType` is abstract in OPC UA, so the server defines a concrete subtype. We publish CloudEvents, so there is no address space; the generated `ISA95JobOrderStatusEventTypeRecord` (`JobOrder`, `JobResponse`, `JobState`) is a candidate payload model, to be checked in S16/S18.
* **Bounds and retention** of the in-memory engine (`MaxJobOrders` 1024, `MaxJobResponses` 1024, `ResponseRetention` off by default, using `TimeProvider`) have a counterpart in our job acceptance configuration (S17).
* **High availability:** upstream states that the in-memory provider is not for production and that a durable provider must implement the provider interfaces over a shared store with an exactly-once-per-committed-mutation event contract. Our Valkey layer with outbox (D-9, S19) is that design for CloudEvents. A Valkey-backed provider for their OPC UA server would be possible later but is out of scope (D-4).
* **Provenance and tests:** NodeSets are pinned by upstream commit, with a test on model URI, version, publication date and node count (Common 390, V1 91, V2 258). Local NodeId mapping CSVs sit beside each NodeSet. The Common Model needed two repairs (missing ReferenceTypes `DefinedByMaterialClass` `i=5300` and `AssembledFromSublot` `i=5333`, ids assigned locally), so the Common Model in the package is not byte-identical to the published NodeSet. We use the Common Model types only through the package; any model that depends on those two reference types would see the local ids.
* **AOT:** upstream has AOT tests (`Isa95AotTests`) for the ISA-95 roots, which matches our measurement.
* Client and server parts (`Isa95Client`, `Isa95JobControlV1/V2Client`, `Isa95NodeManager`, builders, geo-location seam) are not used (D-4).

## What the stack does with V1 and V2 (reference for our design)

`Opc.Ua.ISA95.Server` (internal code, MIT, [source](https://github.com/OPCFoundation/UA-.NETStandard/tree/master/src/Opc.Ua.ISA95.Server/Providers)) is built on a version-neutral core: records `Isa95JobOrder`, `Isa95JobResponse`, `Isa95WorkMaster`, `Isa95Parameter`, `Isa95Property`, `Isa95Material`, `Isa95ResourceRequirement`, the enums `Isa95JobCanonicalState` (10 values, V1 `Loaded`/`Error` preserved) and `Isa95JobOperation` (12 values), and `Isa95JobControlConversions` (about 1.2k lines, the only place where generated V1/V2 types are touched). The engine, `InMemoryIsa95JobControlProvider` (1.3k lines), holds the dictionary, a lock, the transition rules (a predicate and a target state per operation, for example Pause: Running to Suspended; Resume: Held or Suspended to Running; Abort: any non-terminal to Aborted; RevokeStart: AllowedToStart to NotAllowedToStart; execution transitions BeginExecution, Hold, Complete, Close) and the subscriptions. `Isa95V2StateMachine` only converts between a canonical state and the V2 state array (top-level numbers 1 to 6, sub-state numbers 1 and 2).

Assessment: the neutral model and the conversions are the right architecture and what we adopt (D-34). The engine is not reusable: transitions are not separable from the lock, store and notification code, nothing is persisted, there is no compare-and-set, and it does not implement the Prepare machine of the NodeSet. Our table comes from the NodeSet itself; the upstream rules and tests are cross-check vectors.

Questions for upstream (maintainer to ask): can the neutral records, the canonical state, the operation enum, the conversions and the transition rules be published as a public, server-independent package (for example `Opc.Ua.ISA95.JobControl.Core`)? Would a NodeSet2 and generated models for the Work Master extension be welcome as a contribution?

## Not covered

Binary and XML encodings, arm64, building the table generator (S16), V1 to V2 field conversions, JSON Schema validation of incoming payloads (separate library), performance of the JSON encoder against hand-written System.Text.Json.
