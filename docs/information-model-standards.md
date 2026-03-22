# Information Model Standards

## OPC UA

JSON Schemas representing the OPC UA information models can be generated from nodeset XMLs. These schemas are used to generate Python dataclasses via `datamodel-code-generator`. A custom template is used to generate:

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
  machinery_jobs.jsonschema.json
```

### [OPC 40001-3: Machinery Job Mgmt](https://reference.opcfoundation.org/Machinery/Jobs/v100/docs/)

The custom template also creates:

* `opcua_state_machine` with serialized python dict from JSON Schema (use `ast.literal_eval()` to deserialize str to dict)
* custom attributes `opcua_state_machine_states` and `opcua_state_machine_transitions` with ready to use dictionaries for usage with `transitions` library
* custom attribute `opcua_state_machine_effects` storing transition events

Manual additions for Machinery Job Management are required:

* `MethodReturnStatus` enum
* additional `JobOrderControlExt` class that adds "the dotted states and transitions" from the spec (without the meta state `Prepared`)
* fix missing transition name for `Run`
* add initial states to sub statemachines
* helpers to map to/from state names to ids

Links:

* [OPC UA Machinery Jobs overview video](https://youtu.be/KOhYcezpJCw)

### EUInformation

* [OPC UA Part 8 EUInformation](https://reference.opcfoundation.org/Core/Part8/v105/docs/5.6.3)
* [UNECE to OPC UA CSV](http://www.opcfoundation.org/UA/EngineeringUnits/UNECE/UNECE_to_OPCUA.csv)
* [UA-Nodeset UNECE to OPC UA CSV](https://raw.githubusercontent.com/OPCFoundation/UA-Nodeset/refs/heads/latest/Schema/UNECE_to_OPCUA.csv)

## ISA-88/ISA-95

### ISA-95 Introduction

* [ISA-95 introduction by Rhize](https://docs.rhize.com/isa-95/how-to-speak-isa-95/)
* [ISA-95 introduction video](https://youtu.be/OobhzbQoUnA)

### ISA-95 vs. ISA-88

* [ISA-88 and ISA-95 integrated consulting](https://iacsengineering.com/isa-88-and-isa-95-integrated-consulting/)
* [What is ISA-88 manufacturing](https://mdcplus.fi/blog/what-is-isa-88-manufacturing/)
* [Using ISA-88 and ISA-95 together](https://www.isa.org/products/isa-tr88-95-01-2008-using-isa-88-and-isa-95-togeth)

## Custom Models

The code generator has some options to support customization for other protocols/messages:

* Add `__custom_metadata__: InitVar[dict[str, Any] | None] = None` for usage in `__post_init__()``. It is populated from cloudevent.

  ```bash
  uv run microdcs dataclassgen dataclasses --custom-metadata my.jsonschema.json
  ```

* Add additional `InitVar` fields for usage in `__post_init__()`. You can populate them via `event.response(mystatus=MyStatus.OKAY)`.

  ```bash
  uv run microdcs dataclassgen dataclasses \
    --init-fields 'mystatus->MyStatus' \
    --init-fields 'init->dict[str,str]' \
    my.jsonschema.json
  ```

* Add hidden fields.

  ```bash
  uv run microdcs dataclassgen dataclasses \
    --hidden-fields '_hidden_str->str' \
    --hidden-fields '_hidden_obj->Obj1|Obj2' \
    my.jsonschema.json
  ```

Generate greetings:

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
  greetings.jsonschema.json
```