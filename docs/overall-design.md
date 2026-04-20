# Overall Design

MicroDCS is designed to help build OT and manufacturing applications with modern software architecture patterns while staying grounded in open standards. Instead of centering the system on custom protocol glue or tightly coupled device integrations, the framework uses typed events, protocol abstraction, and standard specifications to support distributed sequence control applications.

## Design Goals

* Make manufacturing control and OT applications buildable with cloud-native and event-driven software architecture principles
* Reduce custom integration work by relying on open standards such as MQTT v5, CloudEvents, JSON Schema, OpenTelemetry, and OPC UA companion specifications
* Generate as much application model code as practical from standard specifications to reduce implementation effort and improve consistency
* Keep application logic focused on typed models and processors rather than transport-specific payload handling

## Premises

* The system follows an event-driven architecture, with MQTT as the primary transport rather than OPC UA client/server or OPC UA PubSub communication
* OPC UA information models and companion specifications are used to define application semantics and payload structure over the chosen transports
* Metadata required to identify and process payloads is carried through MQTT properties and CloudEvent attributes instead of being embedded in custom transport formats
* Application implementations should work with generated dataclasses and processor abstractions rather than directly parsing MQTT or MessagePack payloads
* The application assumes a UNS-style topic structure with at least `data`, `events`, and `commands` topics, with optional `metadata` topics for retained capability publication
* The MQTT broker must be configured with retained message persistence enabled so that retained topics survive broker restarts. Without persistence, a broker restart clears all retained topics and components relying on retained state (e.g. the Job Order Publisher) lose their recovery anchor until the next publish cycle. For Mosquitto this requires `persistence true` in `mosquitto.conf`.

## Deployment Modes

MicroDCS supports two deployment modes controlled by two boolean flags on `RuntimeConfig`:

| Mode | `APP_IS_PROCESSOR_INSTANCE` | `APP_IS_PUBLISHER_INSTANCE` | Description |
|---|---|---|---|
| **Single-container** | `true` (default) | `true` (default) | Both processor and publisher run in the same process. Simplest setup for development and low-scale deployments. |
| **Split processor + publisher** | `true` / `false` | `false` / `true` | Processors scale horizontally (multiple replicas) with shared MQTT subscriptions while a single publisher replica maintains retained topics. Avoids conflicting retained writes. |

When `is_processor_instance` is `false`, protocol handlers and bindings are not started. When `is_publisher_instance` is `false`, additional tasks that are `MQTTPublisher` instances are skipped.

## Three-Layer Architecture

MicroDCS separates concerns into three layers. Each layer has a distinct responsibility and communicates with its neighbours via direct Python method calls (not CloudEvent round-trips):

```mermaid
flowchart LR
  mom["MOM / MES"]
  nb["NB Protocol Layer<br/>(MachineryJobsProcessor)"]
  sfc["SFC Orchestration Layer<br/>(SfcEngine)"]
  sb["SB Protocol Layer<br/>(Equipment Processors)"]
  equipment["Equipment"]

  mom -- "CloudEvents<br/>(MQTT / MsgPack-RPC)" --> nb
  nb -. "direct call" .-> sfc
  sfc -. "direct call" .-> nb
  sfc -. "direct call" .-> sb
  sb -- "CloudEvents<br/>(MQTT / MsgPack-RPC)" --> equipment

  style sfc fill:#3949ab,color:#fff
```

| Layer | Responsibility | Key class |
|---|---|---|
| **NB protocol** | OPC UA Job Management state machine, CloudEvent serialization, MQTT topic structure, station configuration delivery | `MachineryJobsCloudEventProcessor` |
| **SFC orchestration** | Recipe interpretation, step sequencing, action dispatch, multi-instance coordination via Redis consumer groups and atomic CAS | `SfcEngine` (`AdditionalTask`) |
| **SB protocol** | Equipment-specific CloudEvent shaping, transport binding, protocol translation | Equipment `CloudEventProcessor`(s) |

The SFC engine is **not** a processor. It is an `AdditionalTask` that runs on every instance and coordinates through Redis. This means:

- The NB processor remains a pure OPC UA protocol handler — it does not know about SFC recipes or step sequencing
- SB processors remain pure equipment protocol handlers — they can be tested and triggered independently
- The SFC engine can be replaced with a different execution strategy without changing any processor code

### Multi-Instance Model

Unlike the publisher (single-instance to avoid duplicate retained writes), the SFC engine runs on **every** instance. Multiple instances coordinate through a Redis Stream consumer group (`XREADGROUP` + `XAUTOCLAIM`) and atomic compare-and-swap Lua scripts. This eliminates single points of failure and lets Kubernetes horizontal scaling naturally increase throughput. See [SFC Engine Architecture](sfc_engine.md#sfc-engine-architecture) for details.