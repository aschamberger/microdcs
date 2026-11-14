# MicroDCS

[![ci](https://github.com/aschamberger/microdcs/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/aschamberger/microdcs/actions/workflows/ci.yml)
[![security](https://github.com/aschamberger/microdcs/actions/workflows/security.yml/badge.svg?branch=main)](https://github.com/aschamberger/microdcs/actions/workflows/security.yml)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=aschamberger_microdcs&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=aschamberger_microdcs)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=aschamberger_microdcs&metric=coverage)](https://sonarcloud.io/summary/new_code?id=aschamberger_microdcs)

MicroDCS: an open-standards-based framework for distributed sequence control applications.

MicroDCS is a .NET framework for event-driven sequence control in OT and manufacturing. It carries CloudEvents over
MQTT 5 and NATS, uses OPC UA companion specifications (ISA-95 Job Control, Machinery Jobs) as payload models, keeps
its state in Valkey, and reports through OpenTelemetry. It runs as a Native AOT binary in a small, non-root container.

**Documentation: <https://aschamberger.github.io/microdcs/>**

## Features

* Processor model with attributes and a Roslyn source generator: typed handlers, no reflection, compile-time
  diagnostics, Native AOT throughout.
* One message pipeline for every transport: deduplication in Valkey, scope checks, error events, bounded outgoing
  queues, request/response with expiry.
* Transports: MQTT 5 (MQTTnet) and NATS JetStream; AMQP 1.0 and Zenoh are planned.
* OPC UA Machinery Jobs (ISA-95 Job Control V2) as a ready processor: job order state machine generated from the
  NodeSet, station configuration from the MES, status events, state topics with a sequence-numbered index for resync.
* SFC engine: IEC 61131-3 style recipes from the Work Master, push commands and pull events, deadlines and retries,
  pause and equipment hold, shared by all replicas through Valkey.
* Read-only inspection, health probes, a REST API for job state, and a development MCP server with inspection and
  maintenance tools.
* Signed multi-platform container image (linux-x64, linux-arm64) with SBOMs, and an example Kubernetes manifest.

## Quickstart

Requirements: the .NET SDK from `global.json`, Docker.

```bash
dotnet build                       # whole solution; warnings are errors
dotnet test                        # unit tests; integration tests skip without the compose services
docker compose up -d --wait        # Mosquitto, NATS, Valkey and the Aspire dashboard

# The greetings demo on the local broker
APP_MicroDcs__CloudEventSource=urn:microdcs:dev \
APP_MicroDcs__Bindings__greetings__Prefix=dev \
APP_MicroDcs__Bindings__greetings__ScopeLevels=2 \
dotnet run --project src/MicroDCS.App -- serve
```

The container image:

```bash
docker pull ghcr.io/aschamberger/microdcs:0.1.0
docker run --rm ghcr.io/aschamberger/microdcs:0.1.0 smoke
```

Next steps: [Your First Processor](https://aschamberger.github.io/microdcs/your-first-processor/),
[Concepts](https://aschamberger.github.io/microdcs/concepts/),
[Operations](https://aschamberger.github.io/microdcs/operations/) and
[Development](https://aschamberger.github.io/microdcs/development/).

## Status and Support

MicroDCS is pre-1.0 (0.1.x): interfaces can still change between releases, and NuGet packages and `dotnet new`
templates are not published yet. It is maintained by a single maintainer on a best-effort basis, without a
service-level agreement. Questions and bug reports go to the GitHub issue tracker. Security problems follow
[SECURITY.md](SECURITY.md).

The code on `main` is licensed under the MIT licence (see [LICENSE](LICENSE)). MicroDCS started as a Python framework;
that implementation is archived on the
[`python-archive`](https://github.com/aschamberger/microdcs/tree/python-archive) branch and stays under the GNU Lesser
General Public License v2.1. The design record of the port is in [`docs/porting/`](docs/porting/index.md).
