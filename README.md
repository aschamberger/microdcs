# MicroDCS

MicroDCS: An Open-Standard Framework for Distributed Sequence Control.

## Quickstart

```bash
# Install dependencies
uv sync

# Run unit tests
uv run pytest tests/ --ignore=tests/test_mqtt_integration.py --ignore=tests/test_msgpack_integration.py

# Run the example app
# Requires a reachable MQTT broker and Redis server.
uv run python -m app
```

## Features

* Transport protocols: MQTTv5 RPC/CloudEvents & MessagePack-RPC over TCP/CloudEvents
* High-availability, horizontal scalability leveraging MQTTv5 shared subscriptions
* Deduplication of messages with at least once delivery guaranties and flow control with expiry intervals
* Generic handling of JSON/MessagePack payloads via content type
* De-/serialization to Python dataclasses with handling of custom user properties
* Multiple CloudEventProcessors handling incoming requests
* Dataclass generation from JSON Schema with request/response handling mechanisms
* OPC UA Machinery Job Management: Receiver/Event Publisher
* OPC UA Machinery Job hierarchical state machine via `transitions` library driven by OPC UA companion spec
* Redis-backed persistence for job orders/responses and CloudEvent/transaction deduplication
* OpenTelemetry auto instrumentation plus manual instrumentation of internals (metrics and tracing)

## Usage

### Initialize a new project

Bootstrap a new MicroDCS application project using the `init` command. The product name is MicroDCS; the Python distribution and CLI package name are `microdcs`. The command scaffolds the full project structure, pins Python 3.14, and installs all required dependencies.

```bash
# Initialize in the current directory
uvx --from git+https://github.com/aschamberger/microdcs microdcs init

# Initialize in a new directory
uvx --from git+https://github.com/aschamberger/microdcs microdcs init my-app
```

The command will:

1. Pin Python ≥ 3.14 and create a `uv` project (`uv init --python=>=3.14 --bare`)
2. Add `microdcs` and dev dependencies (`datamodel-code-generator`, `pytest`, `pytest-asyncio`, `pytest-cov`)
3. Create the project directory structure:
   ```
   app/
     __init__.py
     __main__.py       # wiring of handlers, bindings and processors
     models/           # place for generated and hand-written model mixins
     processors/       # place for CloudEventProcessor implementations
   schemas/            # JSON Schema input files for dataclassgen
   deploy/
     k8s.yaml          # Kubernetes deployment manifest
   tests/
   .vscode/
     settings.json
     tasks.json
   .github/
     copilot-instructions.md
   Dockerfile
   ```
4. Append build-system and pytest configuration to `pyproject.toml`

After initialization, generate dataclasses from your JSON Schema files:

```bash
uv run microdcs dataclassgen dataclasses my-schema.jsonschema.json
```

## Additional Documentation

* [Overall Design](https://aschamberger.github.io/microdcs/overall-design/)
* [Development](https://aschamberger.github.io/microdcs/development/)
