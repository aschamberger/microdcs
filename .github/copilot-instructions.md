# MicroDCS – Copilot Instructions

## What This Project Is

MicroDCS is a Python framework for building distributed sequence control (DCS) applications in OT/manufacturing environments. It uses open standards (MQTTv5, CloudEvents, OPC UA companion specs, OpenTelemetry) to build event-driven control apps with cloud-native principles. The framework handles protocol transport (MQTT, MessagePack-RPC), message de/serialization to Python dataclasses, CloudEvent processing, OPC UA Machinery Job Management state machines, and Redis persistence.

## Tech Stack

- **Python ≥ 3.14** (uses modern features: `type` aliases, `|` union syntax, `StrEnum`, `kw_only` dataclasses)
- **uv** – package manager, virtualenv, lockfile (`uv.lock`), build backend (`uv_build`)
- **Key dependencies**: `aiomqtt`, `mashumaro[orjson]`, `msgpack`, `redis[hiredis]`, `transitions`, `opentelemetry-distro`
- **Dev dependencies**: `datamodel-code-generator[ruff]`, `pytest`, `pytest-asyncio`, `pytest-cov`, `typer`
- **Container**: Distroless Docker image via multi-stage build (`ghcr.io/astral-sh/uv:python3.14-trixie-slim` → `gcr.io/distroless/cc-debian13:nonroot`)
- **Formatting/linting**: Ruff (invoked via `datamodel-code-generator`; no standalone ruff config file)

## Project Structure

```
src/microdcs/             # Library package (installed as "microdcs")
  __init__.py             # RuntimeConfig, dataclass configs (Redis/MQTT/MessagePack/Logging/Processing)
  core.py                 # MicroDCS app class: handler/binding/processor registration, main loop
  common.py               # CloudEvent dataclass, CloudEventProcessor ABC, @incoming/@outgoing decorators,
                          #   ProtocolHandler/ProtocolBinding ABCs, ProcessorBinding, MessageIntent enums
  dataclass.py            # DataClassMixin (orjson+msgpack), DataClassResponseMixin, DataClassConfig, validation
  mqtt.py                 # MQTTHandler/MQTTProtocolBinding + OTEL-instrumented variants
  msgpack.py              # MessagePackHandler/MessagePackProtocolBinding + OTEL-instrumented variants
  redis.py                # RedisKeySchema, DAOs (CloudEventDedupeDAO, JobOrderAndStateDAO, JobResponseDAO, ...)
  models/                 # Data models (dataclasses)
    greetings.py           # Auto-generated from JSON Schema – DO NOT edit manually
    greetings_mixin.py     # Hand-written mixin for greetings (hidden fields, custom metadata)
    machinery_jobs.py      # Auto-generated from JSON Schema – DO NOT edit manually
    machinery_jobs_mixin.py  # Hand-written mixin (JobStateMixin for `transitions` library)
    machinery_jobs_ext.py  # Manual extensions: MethodReturnStatus, JobOrderControlExt
  processors/             # CloudEventProcessor implementations
    greetings.py           # GreetingsCloudEventProcessor (demo/example processor)
    machinery_jobs.py      # MachineryJobsCloudEventProcessor (OPC UA Job Mgmt)
  scripts/
    dataclassgen.py        # Typer CLI for generating dataclasses from JSON Schema
    template/dataclass.jinja2  # Custom Jinja2 template for code generation
app/                      # Example application entry point
  __main__.py             # Wires up MicroDCS: registers handlers, bindings, processors; runs asyncio loop
schemas/                  # JSON Schema files (input for dataclassgen)
tests/                    # Pytest test suite
deploy/k8s.yaml           # Kubernetes deployment manifest
scripts/init_app.sh       # Bootstraps a new MicroDCS app project (for external consumers)
```

## Common Commands

```bash
# Install dependencies (creates .venv automatically)
uv sync

# Run unit tests (must exclude integration tests that fail at collection without external services)
uv run pytest tests/ --ignore=tests/test_mqtt_integration.py --ignore=tests/test_msgpack_integration.py

# Run tests with coverage
uv run pytest --cov=microdcs --cov-report=term-missing tests/ --ignore=tests/test_mqtt_integration.py --ignore=tests/test_msgpack_integration.py

# Run the example app (requires MQTT broker + Redis)
uv run python -m app

# List available JSON schemas
uv run dataclassgen index

# Generate dataclasses from a JSON schema (output goes to src/microdcs/models/)
uv run dataclassgen dataclasses <schema_file.schema.json> [options]

# Docker build
docker build -t aschamberger/microdcs .
```

## Coding Guidelines

### Dataclasses & Models

- All model dataclasses use `@dataclass(kw_only=True)` and extend `DataClassMixin` (which provides `orjson` + `msgpack` serialization via mashumaro).
- Each CloudEvent-capable model has an inner `class Config(DataClassConfig)` with `cloudevent_type` and `cloudevent_dataschema` string attributes.
- Files in `models/` named `*_mixin.py` and `*_ext.py` are hand-written. Files matching a JSON schema name (e.g. `greetings.py`, `machinery_jobs.py`) are **auto-generated** – never edit them directly. Regenerate with `uv run dataclassgen dataclasses`.
- Package exports are static: whenever adding a new model module or public model class, update `src/microdcs/models/__init__.py` so users can import from `microdcs.models`.
- Hidden fields (prefixed `_`) are excluded from serialization by `DataClassMixin.__post_serialize__`.
- Validation constraints use `field(metadata={"min_length": N, "max_length": N})` with `DataClassValidationMixin`.
- When creating a new JSON schema, the top-level `title` must **not** match any `$defs` class name, or the code generator will suffix the class with a number (e.g. `Ping` → `Ping1`).

### Processors

- Subclass `CloudEventProcessor` and decorate the class with `@processor_config(binding=ProcessorBinding.NORTHBOUND|SOUTHBOUND)`.
- Package exports are static: whenever adding a new processor class, update `src/microdcs/processors/__init__.py` (including `__all__`) so users can import from `microdcs.processors`.
- **Must implement** three abstract methods: `process_response_cloudevent(self, cloudevent)`, `handle_cloudevent_expiration(self, cloudevent, timeout)`, and `trigger_outgoing_event(self, **kwargs)`. All return `list[CloudEvent] | CloudEvent | None`.
- Use `@incoming(MyDataClass)` to register a handler for incoming CloudEvents of that type.
- Use `@outgoing(MyDataClass)` to register a handler for producing outgoing CloudEvents.
- Incoming handlers receive the deserialized dataclass plus keyword args for CloudEvent attributes listed in `_event_attributes`. Return `list[T] | T | None`.
- Optional hooks `__pre_outgoing_callback__` and `__post_outgoing_callback__` can intercept/transform callback flow.
- Helper decorators `@scope_from_subject` and `@asset_id_from_subject` extract info from the CloudEvent subject.

### Configuration

- `RuntimeConfig` reads all settings from environment variables with prefix `APP_` and nested structure: `APP_{SECTION}_{FIELD}` (e.g. `APP_MQTT_HOSTNAME`, `APP_REDIS_PORT`, `APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED`).
- In Kubernetes, `POD_ID` env var is set from the pod UID; locally it falls back to a random UUID.

### Testing

- Tests live in `tests/` and use `pytest` + `pytest-asyncio`.
- Async tests must be decorated with `@pytest.mark.asyncio`.
- Integration tests requiring external services (MQTT broker, Redis) use skip markers from `conftest.py` (`mqtt_available`, `redis_available`, `msgpack_server_available`) and `@pytest.mark.integration`.
- Unit tests mock external dependencies extensively (`unittest.mock.AsyncMock`, `MagicMock`, `patch`).
- Test classes group related tests (e.g., `class TestHello:`, `class TestGreetingsCloudEventProcessor:`).
- All code (source and tests) must have zero Pylance errors. Use type narrowing (e.g. `assert x is not None`) where needed to satisfy the type checker.

### Async Patterns

- The framework is fully async (`asyncio`). The main loop uses `SystemEventTaskGroup` (a custom `asyncio.TaskGroup` subclass) for graceful signal handling (SIGINT/SIGTERM).
- Protocol handlers run as tasks within this group; each handler manages its own connection lifecycle with retry/backoff.

### Design Decisions

These are intentional choices — do not flag them as issues in reviews:

- **Comma-delimited config parsing is safe**: `set[str]`/`list[str]` env var fields (e.g. MQTT topics, `name:path` pairs) use comma splitting without escaping. This is fine because MQTT topic names cannot contain commas per the spec, and all current list/set fields use structured `name:path` values where commas are not valid.
- **Fail-fast startup**: `MicroDCS.main()` has no try/except around processor `initialize()` or handler startup. If a component can't start, the process should crash immediately rather than run in a degraded state. Kubernetes restart policies handle recovery.
- **`_event_attributes` is not dead code**: It is populated by processor mixin classes (e.g. `machinery_jobs_mixin.py`), not in the base `CloudEventProcessor.__init__`. Check subclass mixins before assuming it's unused.
- **Queue backpressure uses `RuntimeError`**: `ProtocolBinding._enqueue_outgoing_event()` raises on queue full intentionally. Silent blocking would hide overload; an explicit error surfaces the problem for the caller to handle.
- **`ContextVar` is async-only**: The framework is fully async with no thread-pool executors. `ContextVar` works correctly in asyncio task-local scope. If threading is ever introduced, this needs revisiting.
- **Dual OTEL handler registration**: Plain + instrumented handler pairs are a pragmatic pattern. A factory/strategy would add abstraction without clear benefit — the wiring happens once in `app/__main__.py`.
- **`get_protocol_handler()` same-module resolution**: Handler references are resolved within the registering module. This is by design — handlers are always registered in the app entry point, not across library modules.
- **Error context uses comma/equals encoding**: `mdcserrorcontext` serializes as `key=value,key=value`. Values containing `,` or keys containing `=` will corrupt the dict. This is acceptable because error context is only set by application code with simple single-word keys and numeric/string values — it is never populated from external input.
- **`custommetadata` has no explicit size limit**: Transport-level limits already bound payload size (MessagePack `max_buffer_size=8MB`, MQTT broker max packet size). An explicit size check on `custommetadata` would be redundant defense-in-depth.
- **MQTT TLS uses Python defaults for hostname verification**: `aiomqtt.TLSParameters(ca_certs=...)` relies on `ssl.create_default_context()` which sets `check_hostname=True` and `verify_mode=CERT_REQUIRED` by default. These defaults are secure.
- **Expiration tasks are safe with `id=None`**: There is an explicit `cloudevent.id is not None` guard before creating expiration tasks, so `None` IDs never enter the `_expiration_timeout_tasks` dict.

### Known Issues / Workarounds

- MQTT handler has FIXMEs for aiomqtt manual ack pending upstream PR merge (`aiomqtt#346`).
