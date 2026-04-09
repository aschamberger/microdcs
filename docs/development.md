# Development

This page summarizes the local development workflow for MicroDCS, including environment setup, test execution, documentation work, and container packaging.

## Environment Setup

MicroDCS targets Python 3.14 and uses `uv` for dependency management, virtual environment creation, and packaging.

```bash
# Install dependencies and create the local virtual environment
uv sync
```

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