# Information Model Standards

MicroDCS uses established industrial information-model standards and schema-driven code generation to keep application payloads typed, consistent, and transport-independent. This page summarizes the standards and model-generation patterns that shape the framework's data layer.

## OPC UA

OPC UA information models and companion specifications provide much of the semantic structure used by MicroDCS.

JSON Schemas representing OPC UA information models can be generated from NodeSet XML files. These schemas are then used to generate Python dataclasses through `datamodel-code-generator`. MicroDCS uses a custom generation template to add framework-specific behavior, including:

* `Config` inner class for mashumaro
* custom attributes `x-*` in `Config` inner class

```bash
uv run microdcs dataclassgen dataclasses \
  --imports microdcs.dataclass.DataClassConfig \
  --imports microdcs.dataclass.DataClassResponseMixin \
  --imports microdcs.dataclass.DataClassMixin \
  --imports microdcs.models.machinery_jobs_mixin.JobStateMixin \
  --imports dataclasses.field \
  --base-class microdcs.dataclass.DataClassMixin \
  --config-base-class microdcs.dataclass.DataClassConfig \
  --add-mixin 'ISA95JobOrderDataType->JobStateMixin' \
  machinery_jobs.schema.json
```

### [OPC 40001-3: Machinery Job Management](https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/)

For OPC UA Machinery Job Management, the custom template also generates:

* `opcua_state_machine` with serialized python dict from JSON Schema (use `ast.literal_eval()` to deserialize str to dict)
* custom attributes `opcua_state_machine_states` and `opcua_state_machine_transitions` with ready to use dictionaries for usage with `transitions` library
* custom attribute `opcua_state_machine_effects` storing transition events

The specification defines two integration approaches:

* **Event-based** — Clients subscribe to `ISA95JobOrderStatusEventType` events on each state transition. MicroDCS fully implements this mode through `MachineryJobsCloudEventProcessor`, which maps transitions to CloudEvents over MQTT.
* **List/variable-based** — The `ISA95JobOrderReceiverObjectType` exposes a `JobOrderList` variable and the `ISA95JobResponseProviderObjectType` exposes a `JobOrderResponseList` variable. MicroDCS maps these to retained MQTT topics: per-job `order/{id}` and `result/{id}` topics correspond to individual entries in the OPC UA lists, while a `state-index` retained topic provides a compact summary equivalent to reading the full list variables. See [Machinery Jobs – MES Northbound Publishing](machinery-jobs-mes-publishing.md) for the retained topic layout and MES reconnect protocol.

Some manual additions are still required for Machinery Job Management support:

* `MethodReturnStatus` enum
* additional `JobOrderControlExt` class that adds the dotted states and transitions from the specification, without the meta state `Prepared`
* fix missing transition name for `Run`
* add initial states to sub-state machines
* helpers to map between state names and state IDs

Links:

* [OPC UA Machinery Jobs overview video](https://youtu.be/KOhYcezpJCw)

### EUInformation

Engineering-unit metadata is derived from the OPC UA EUInformation model and related mapping tables.

* [OPC UA Part 8 EUInformation](https://reference.opcfoundation.org/Core/Part8/v105/docs/5.6.3)
* [UNECE to OPC UA CSV](http://www.opcfoundation.org/UA/EngineeringUnits/UNECE/UNECE_to_OPCUA.csv)
* [UA-Nodeset UNECE to OPC UA CSV](https://raw.githubusercontent.com/OPCFoundation/UA-Nodeset/refs/heads/latest/Schema/UNECE_to_OPCUA.csv)

### Extended Work Master (`ISA95WorkMasterDataTypeExt`)

The generated `ISA95WorkMasterDataType` carries only the OPC UA envelope fields (`id`, `description`, `parameters`). `ISA95WorkMasterDataTypeExt` (in `machinery_jobs_ext.py`) extends it with two optional fields following the CloudEvent opaque-data pattern:

| Field | Type | Default | Purpose |
|---|---|---|---|
| `data` | `dict[str, Any] \| list[Any] \| None` | `None` | Recipe content as JSON — structure defined by `dataschema` |
| `dataschema` | `str \| None` | `None` | URI identifying the schema that `data` adheres to |

```python
@dataclass(kw_only=True)
class ISA95WorkMasterDataTypeExt(ISA95WorkMasterDataType):
    data: dict[str, Any] | list[Any] | None = None
    dataschema: str | None = None
```

Both fields default to `None`, so existing Work Masters without recipe content remain valid. The `WorkMasterDAO` stores and retrieves `ISA95WorkMasterDataTypeExt` instances:

- **`save()`** serializes all fields (including `data` and `dataschema` when present) to Redis JSON via `to_dict()`
- **`retrieve()`** deserializes as `ISA95WorkMasterDataTypeExt`, so the caller always receives the extended type

The `dataschema` URI discriminates the semantic type of the `data` payload. For example, an SFC recipe uses `https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/`, but other recipe formats can be added by defining new `dataschema` URIs without touching the Work Master envelope, the NB processor, or the DAO layer.

### Station Configuration CloudEvents

Station configuration is delivered from MES to MicroDCS via CloudEvents on the NB processor. Each configuration type has a dedicated CloudEvent type under `com.github.aschamberger.ISA95-JOBCONTROL_V2.config.*` and reuses existing ISA-95 data types as payloads. The `method` CloudEvent extension attribute (`PUT` / `DELETE`) controls upsert vs. removal.

Thin data model subclasses (in `machinery_jobs_ext.py`) override `Config.cloudevent_type` so the NB processor registers separate `@incoming` handlers while reusing the ISA-95 data types for serialization:

| CloudEvent type | Data model | Payload type | DAO | Validated field(s) |
|---|---|---|---|---|
| `...config.equipment.v1` | `ConfigEquipment` | `ISA95EquipmentDataType` | `EquipmentListDAO` | `id` |
| `...config.materialclass.v1` | `ConfigMaterialClass` | `ISA95MaterialDataType` | `MaterialClassListDAO` | `material_class_id` |
| `...config.materialdefinition.v1` | `ConfigMaterialDefinition` | `ISA95MaterialDataType` | `MaterialDefinitionListDAO` | `material_definition_id` |
| `...config.personnel.v1` | `ConfigPersonnel` | `ISA95PersonnelDataType` | `PersonnelListDAO` | `id` |
| `...config.physicalasset.v1` | `ConfigPhysicalAsset` | `ISA95PhysicalAssetDataType` | `PhysicalAssetListDAO` | `id` |
| `...config.workmaster.v1` | `ConfigWorkMaster` | `ISA95WorkMasterDataTypeExt` | `WorkMasterDAO` | `id` (full object stored) |
| `...config.jobacceptance.v1` | `ConfigJobAcceptance` | `DataClassMixin` (custom) | `JobAcceptanceConfigDAO` | `max_downloadable_job_orders` |

Operation semantics:

- **`PUT`** (default): upsert — `add_to_list()` / `save()`
- **`DELETE`**: removal — `remove_from_list()` / `delete()`

All configuration is scoped via the CloudEvent `subject` field, allowing different stations to maintain independent resource sets.

### SFC Recipe Schema

The SFC recipe schema (`schemas/sfc_recipe.schema.json`) defines IEC 61131-3 SFC recipe structures — steps, transitions, action associations, and branches — as a JSON Schema. It follows the same code generation pipeline as other model schemas:

```bash
uv run microdcs dataclassgen dataclasses sfc_recipe.schema.json
```

The schema `$id` is `https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/`. This URI is used as the `dataschema` value on `ISA95WorkMasterDataTypeExt` (via the `SFC_RECIPE_DATASCHEMA` constant) to identify Work Masters that carry SFC recipe payloads. See [SFC Engine](sfc_engine.md) for full details on the recipe format and engine architecture.

## ISA-88/ISA-95

ISA-88 and ISA-95 provide the manufacturing-domain concepts used by the framework's sequence control and job-management examples.

### ISA-95 Introduction

* [ISA-95 introduction by Rhize](https://docs.rhize.com/isa-95/how-to-speak-isa-95/)
* [ISA-95 introduction video](https://youtu.be/OobhzbQoUnA)

### ISA-95 vs. ISA-88

* [ISA-88 and ISA-95 integrated consulting](https://iacsengineering.com/isa-88-and-isa-95-integrated-consulting/)
* [What is ISA-88 manufacturing](https://mdcplus.fi/blog/what-is-isa-88-manufacturing/)
* [Using ISA-88 and ISA-95 together](https://www.isa.org/products/isa-tr88-95-01-2008-using-isa-88-and-isa-95-togeth)

## Custom Models

The code generator also exposes options for extending generated models for application-specific protocols and behaviors.

Supported customization patterns include:

* Add `__custom_metadata__: InitVar[dict[str, Any] | None] = None` for use in `__post_init__()`. It is populated from the CloudEvent.

  ```bash
  uv run microdcs dataclassgen dataclasses --custom-metadata my.schema.json
  ```

* Add additional `InitVar` fields for use in `__post_init__()`. These can be populated through response helpers such as `event.response(mystatus=MyStatus.OKAY)`.

  ```bash
  uv run microdcs dataclassgen dataclasses \
    --init-fields 'mystatus->MyStatus' \
    --init-fields 'init->dict[str,str]' \
    my.schema.json
  ```

* Add hidden fields.

  ```bash
  uv run microdcs dataclassgen dataclasses \
    --hidden-fields '_hidden_str->str' \
    --hidden-fields '_hidden_obj->Obj1|Obj2' \
    my.schema.json
  ```

* Enable root-union generation workaround for schemas with top-level `oneOf` / `anyOf` where root collapsing causes unwanted flattening.

  ```bash
  uv run microdcs dataclassgen dataclasses \
    --collapse-root-workaround \
    my.schema.json
  ```

* Schema file names are converted into safe Python module names automatically (for example, `My Schema.schema.json` -> `my_schema.py`).

Example: generate the greetings models:

```bash
uv run microdcs dataclassgen dataclasses \
  --imports microdcs.dataclass.DataClassConfig \
  --imports microdcs.dataclass.DataClassResponseMixin \
  --imports microdcs.models.greetings_mixin.GreetingsDataClassMixin \
  --imports dataclasses.field \
  --base-class microdcs.models.greetings_mixin.GreetingsDataClassMixin \
  --config-base-class microdcs.dataclass.DataClassConfig \
  --hidden-fields '_hidden_str->str' \
  --hidden-fields '_hidden_obj->HiddenObject' \
  --validation \
  --request-object \
  --custom-metadata \
  greetings.schema.json
```