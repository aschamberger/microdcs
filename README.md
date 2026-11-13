# MicroDCS

[![ci](https://github.com/aschamberger/microdcs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/aschamberger/microdcs/actions/workflows/ci.yml)
[![security](https://github.com/aschamberger/microdcs/actions/workflows/security.yml/badge.svg?branch=main)](https://github.com/aschamberger/microdcs/actions/workflows/security.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=aschamberger_microdcs&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=aschamberger_microdcs)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=aschamberger_microdcs&metric=coverage)](https://sonarcloud.io/summary/new_code?id=aschamberger_microdcs)

> [!WARNING]
> **Port to .NET in progress.** `main` is being rewritten in C# (.NET 11, Native AOT) and is not a
> working product until the parity gate. The Python implementation is archived on the
> [`python-archive`](https://github.com/aschamberger/microdcs/tree/python-archive) branch; the
> sections below from "Quickstart" on still describe it. Plan and progress:
> [Port to .NET](https://aschamberger.github.io/microdcs/porting/) ([source](docs/porting/index.md)).
>
> Build the .NET solution: `dotnet build` and `dotnet test` (SDK pinned in `global.json`).

MicroDCS: An Open-Standards-Based Framework for Distributed Sequence Control Applications.

MicroDCS is a Python framework for building distributed sequence control applications for OT and manufacturing. It uses MQTT v5, CloudEvents, JSON Schema, OPC UA companion specifications, and OpenTelemetry to support event-driven control flows with typed data models, protocol abstraction, and open-standard interoperability.

## Quickstart (Python, archived)

Get the framework installed, run the unit tests, and start the example application:

```bash
# Install dependencies
uv sync

# Run unit tests
uv run pytest tests/unit

# Run the example app
# Requires a reachable MQTT broker and Redis server.
uv run python -m app
```

## Features

* Open-standards foundation built around MQTT v5, CloudEvents, JSON Schema, OPC UA companion specifications, and OpenTelemetry
* Protocol abstraction for MQTT v5 and MessagePack-RPC transports with a shared event-processing model
* Outgoing CloudEvents over MessagePack-RPC via server-to-client notification frames
* Typed application models generated from JSON Schema and serialized to JSON or MessagePack
* Event-driven processor architecture for handling commands, data, events, and metadata
* Distributed operation features including MQTT shared subscriptions, deduplication, and message expiry handling
* Redis-backed persistence for job state, responses, and delivery coordination
* Built-in support for OPC UA Machinery Job Management patterns, including hierarchical state-machine execution
* Observability support through OpenTelemetry auto-instrumentation and internal tracing and metrics hooks

## Usage

### Initialize a new project (Python, archived)

Create a new MicroDCS application with the `init` command. It scaffolds the project structure, pins Python 3.14, and prepares the dependencies needed to build a distributed sequence control application.

```bash
# Initialize in the current directory
uvx --from git+https://github.com/aschamberger/microdcs microdcs init

# Initialize in a new directory
uvx --from git+https://github.com/aschamberger/microdcs microdcs init my-app
```

The generated project includes:

1. A `uv` project pinned to Python ≥ 3.14
2. The `microdcs` package and core development dependencies
3. A starter project structure:
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
4. Build-system and pytest configuration in `pyproject.toml`

After initialization, generate typed application models from your JSON Schema files:

```bash
uv run microdcs dataclassgen dataclasses my-schema.schema.json
```

## Status and Support

MicroDCS is pre-1.0 (version 0.1.0): interfaces can still change between releases. It is maintained
by a single maintainer on a best-effort basis, without a service-level agreement. Questions and bug
reports go to the GitHub issue tracker. Security problems follow [SECURITY.md](SECURITY.md). The
code on `main` is licensed under the MIT licence (see [LICENSE](LICENSE)); the archived Python
implementation on `python-archive` stays under the GNU Lesser General Public License v2.1.

## Additional Documentation

* [Your First Processor](https://aschamberger.github.io/microdcs/your-first-processor/)
* [Overall Design](https://aschamberger.github.io/microdcs/overall-design/)
* [Development](https://aschamberger.github.io/microdcs/development/)
