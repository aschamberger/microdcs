# MicroDCS App – Copilot Instructions

## What This Project Is

This is an application built on the [MicroDCS](https://github.com/aschamberger/microdcs) framework — a Python framework for distributed sequence control (DCS) in OT/manufacturing environments. MicroDCS provides protocol transport (MQTTv5, MessagePack-RPC), CloudEvent processing, OPC UA Machinery Job Management state machines, and Redis persistence. This project contains the application-specific processors, models, and wiring.

## Tech Stack

- **Python ≥ 3.14** (uses modern features: `type` aliases, `|` union syntax, `StrEnum`, `kw_only` dataclasses)
- **uv** – package manager, virtualenv, lockfile (`uv.lock`)
- **MicroDCS** – installed as a git dependency (`git+https://github.com/aschamberger/microdcs`)
- **Dev dependencies**: `datamodel-code-generator[ruff]`, `pytest`, `pytest-asyncio`, `pytest-cov`
- **Container**: Distroless Docker image via multi-stage build

## Project Structure

```
app/                      # Application package
  __init__.py
  __main__.py             # Entry point: registers handlers, bindings, processors; runs asyncio loop
  models/                 # Application-specific data models
  processors/             # Application-specific CloudEventProcessor implementations
schemas/                  # JSON Schema files (input for dataclassgen)
tests/                    # Pytest test suite
deploy/k8s.yaml           # Kubernetes deployment manifest
```

## Common Commands

```bash
# Install dependencies (creates .venv automatically)
uv sync

# Run unit tests
uv run pytest tests/

# Run the app (requires MQTT broker + Redis)
uv run python -m app

# List available JSON schemas
uv run dataclassgen index

# Generate dataclasses from a JSON schema (output goes to app/models/)
uv run dataclassgen dataclasses <schema_file.jsonschema.json> app/models [options]

# Docker build
docker build -t myapp .
```

## Coding Guidelines

### Dataclasses & Models

- All model dataclasses use `@dataclass(kw_only=True)` and extend `DataClassMixin` (provides `orjson` + `msgpack` serialization via mashumaro).
- Each CloudEvent-capable model has an inner `class Config(DataClassConfig)` with `cloudevent_type` and `cloudevent_dataschema` string attributes.
- Files matching a JSON schema name (e.g. `greetings.py`) are **auto-generated** by `dataclassgen` – never edit them directly. Hand-written companion files use `*_mixin.py` and `*_ext.py` suffixes.
- Hidden fields (prefixed `_`) are excluded from serialization by `DataClassMixin.__post_serialize__`.
- Validation constraints use `field(metadata={"min_length": N, "max_length": N})` with `DataClassValidationMixin`.
- When creating a new JSON schema, the top-level `title` must **not** match any `$defs` class name, or the code generator will suffix the class with a number (e.g. `Ping` → `Ping1`).

### Processors

- Subclass `CloudEventProcessor` and decorate the class with `@processor_config(binding=ProcessorBinding.NORTHBOUND|SOUTHBOUND)`.
- **Must implement** two abstract methods: `process_response_cloudevent(self, cloudevent)` and `handle_cloudevent_expiration(self, cloudevent, timeout)`. Both return `list[CloudEvent] | CloudEvent | None`.
- Use `@incoming(MyDataClass)` to register a handler for incoming CloudEvents of that type.
- Use `@outgoing(MyDataClass)` to register a handler for producing outgoing CloudEvents.
- Incoming handlers receive the deserialized dataclass plus keyword args for CloudEvent attributes listed in `_event_attributes`. Return `list[T] | T | None`.
- Helper decorators `@scope_from_subject` and `@asset_id_from_subject` extract info from the CloudEvent subject.

### Wiring in `app/__main__.py`

- Create a `MicroDCS` instance and register protocol handlers (MQTT, MessagePack) with both plain and OTEL-instrumented variants.
- Create processor instances and register them with protocol bindings (`MQTTProtocolBinding`, `MessagePackProtocolBinding`).
- Run via `asyncio.run(microdcs.main())`.

### Configuration

- `RuntimeConfig` reads all settings from environment variables with prefix `APP_` and nested structure: `APP_{SECTION}_{FIELD}` (e.g. `APP_MQTT_HOSTNAME`, `APP_REDIS_PORT`, `APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED`).
- In Kubernetes, `POD_ID` env var is set from the pod UID; locally it falls back to a random UUID.

### Testing

- Tests live in `tests/` and use `pytest` + `pytest-asyncio`.
- Async tests must be decorated with `@pytest.mark.asyncio`.
- Unit tests mock external dependencies (`unittest.mock.AsyncMock`, `MagicMock`, `patch`).
- Test classes group related tests (e.g., `class TestMyProcessor:`).

### Known Issues / Workarounds

- `MessagePackProtocolBinding` does not currently support outgoing events.
