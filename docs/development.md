# Development

This page summarizes the local development workflow for MicroDCS, including environment setup, test execution, documentation work, and container packaging.

## Environment Setup

MicroDCS requires Python ≥ 3.14 and uses `uv` for dependency management, virtual environment creation, and packaging.

```bash
# Install dependencies and create the local virtual environment
uv sync
```

### Why Python 3.14?

Python 3.14 is the minimum version where the annotation semantics MicroDCS needs work correctly end-to-end without workarounds. The requirement is driven by **PEP 749 (Deferred Evaluation of Annotations)** and the new **`annotationlib`** module.

MicroDCS relies heavily on runtime type inspection:

* **mashumaro** resolves type annotations at runtime to build its JSON (orjson) and MessagePack serialization codecs. This means annotations must be real types at runtime, not opaque strings.
* **`DataClassResponseMixin[R]`** uses `Generic[R]` with runtime introspection of `__orig_bases__` to determine the response type. Generated model code like `DataClassResponseMixin["Hello"]` passes a forward reference that must resolve correctly.
* **`ProtocolBinding[PH]`** resolves its generic type parameter at runtime via `get_args()` to find the associated `ProtocolHandler` class, including lazy `ForwardRef` resolution through the 3.14 `annotationlib.ForwardRef`.
* **Generated dataclasses** use `InitVar[Hello | None]`, union type aliases (`type Greetings = Hello | Bye`), and forward references freely. These all need to round-trip through `typing.get_type_hints()` correctly.
* **`register_callback()`** unwraps `TypeAliasType` (the runtime object behind the `type X = Y` statement) and `UnionType` (`X | Y`) to register individual dataclass handlers.

Before Python 3.14, there were two problematic alternatives:

1. **PEP 563** (`from __future__ import annotations`) — stringifies all annotations at definition time. This breaks mashumaro's runtime codegen, `get_type_hints()` resolution of generic parameters, and any code that inspects annotations as live type objects.
2. **No PEP 563** on older Python — requires careful class ordering to avoid `NameError` on forward references, manual `update_forward_refs()` calls, and `if TYPE_CHECKING` guards with duplicated imports. This is fragile and error-prone with generated code.

PEP 749 solves this by making annotation evaluation lazy by default: annotations are stored in a form that is evaluated on access rather than at class creation time. This means forward references, union syntax, and type aliases all work correctly both as static type information and when inspected at runtime — exactly the semantics mashumaro, `get_type_hints()`, and the generic resolution patterns in MicroDCS require.

Additional 3.14 features used throughout the codebase:

* **`annotationlib.ForwardRef`** — the new lazy forward reference type, used in `ProtocolBinding.get_protocol_handler()` for generic parameter resolution
* **`type` statement** (PEP 695, Python 3.12+) — used for type aliases like `type Greetings = Hello | Bye` in generated models and `type JobOrderIdCall = AbortCall | CancelCall | ...` in processors
* **`StrEnum`** (Python 3.11+) — used for `Direction`, `MessageIntent`, `ProcessorBinding`, and `ErrorKind` enums
* **`kw_only` dataclasses** (Python 3.10+) — used universally for all model dataclasses
* **`match`/`case`** (Python 3.10+) — used in validation and content-type routing

## Common Commands

Run the main unit test suite:

```bash
uv run pytest tests/ --ignore=tests/test_mqtt_integration.py --ignore=tests/test_msgpack_integration.py
```

Run test coverage:

```bash
uv run pytest --cov=microdcs --cov-report=term-missing tests/ --ignore=tests/test_mqtt_integration.py --ignore=tests/test_msgpack_integration.py
```

Run the example application:

```bash
# Requires a reachable MQTT broker and Redis server
uv run python -m app
```

## MessagePack Buffer Limit

The MessagePack-RPC transport sets `msgpack.Unpacker(max_buffer_size=...)` to bound
the size of buffered, not-yet-decoded payload data.

* Default limit: `8388608` bytes (8 MiB)
* Runtime override: set `APP_MSGPACK_MAX_BUFFER_SIZE`

This helps protect the process from excessive memory usage caused by oversized or
malformed input streams.

## Configuration Validation

At startup, the runtime configuration is validated before protocol handlers enter
their main loops. Validation fails fast with a clear error message if any check
does not pass.

Checked constraints include:

* Required non-empty values:
	* instance_id
	* redis.hostname
	* redis.key_prefix
	* mqtt.hostname
	* mqtt.identifier
	* msgpack.hostname
* Port ranges:
	* redis.port, mqtt.port, msgpack.port must be in 1..65535
* Positive values:
	* mqtt.connect_timeout, mqtt.publish_timeout, mqtt.message_workers
	* msgpack.max_queued_connections, msgpack.max_concurrent_requests
	* msgpack.max_buffer_size
* Non-negative values:
	* mqtt.incoming_queue_size, mqtt.outgoing_queue_size
	* mqtt.dedupe_ttl_seconds, mqtt.binding_outgoing_queue_size
	* msgpack.binding_outgoing_queue_size
	* processing.shutdown_grace_period
	* processing.message_expiry_interval (when configured)

Startup pre-check sequence:

1. Validate static field constraints (required fields, ranges, and bounds).
2. Check TCP connectivity to Redis using redis.hostname:redis.port.
3. Check TCP connectivity to MQTT broker using mqtt.hostname:mqtt.port.
4. Check MessagePack server bindability on msgpack.hostname:msgpack.port.

Environment variables are still parsed from APP_* settings, then validated by
these runtime checks.

Generate typed models from a JSON Schema file:

```bash
uv run microdcs dataclassgen dataclasses my-schema.schema.json
```

Generate models with advanced options (custom metadata, init fields, validation, and root-union workaround):

```bash
uv run microdcs dataclassgen dataclasses \
	--custom-metadata \
	--init-fields 'mystatus->MyStatus' \
	--validation \
	--collapse-root-workaround \
	my-schema.schema.json
```

## Documentation

The documentation site is built with MkDocs Material.

```bash
# Start the local docs server
mkdocs serve
```

## Container Build

The project ships with a distroless multi-stage Docker build.

```bash
docker build -t aschamberger/microdcs .
```

## Main Libraries

The framework is built around a small set of core dependencies:

* [aiomqtt](https://github.com/empicano/aiomqtt) and [Paho MQTT Python](https://github.com/eclipse-paho/paho.mqtt.python) for MQTT communication
* [mashumaro](https://github.com/Fatal1ty/mashumaro) for dataclass serialization
* [msgpack-python](https://github.com/msgpack/msgpack-python) for MessagePack encoding
* [redis-py](https://github.com/redis/redis-py) for persistence
* [datamodel-code-generator](https://github.com/koxudaxi/datamodel-code-generator) for model generation
* [Typer](https://github.com/fastapi/typer) for CLI tooling

## Reference Links

* [Crow's Nest MQTT](https://github.com/koepalex/Crow-s-Nest-MQTT)
* [Eclipse Mosquitto Docker image](https://hub.docker.com/_/eclipse-mosquitto)
* [Astral uv Docker integration guide](https://docs.astral.sh/uv/guides/integration/docker/#available-images)
* [Distroless container images tutorial](https://labs.iximiuz.com/tutorials/gcr-distroless-container-images)
* [Distroless Python with uv](https://www.joshkasuboski.com/posts/distroless-python-uv/)
* [Mermaid sequence diagram syntax](https://mermaid.js.org/syntax/sequenceDiagram.html)