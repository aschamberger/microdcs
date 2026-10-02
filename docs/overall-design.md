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

Unlike the publisher (single-instance to avoid duplicate retained writes), the SFC engine runs on **every** instance. Multiple instances coordinate through a Redis Stream consumer group (`XREADGROUP` + `XAUTOCLAIM`) and atomic compare-and-swap Lua scripts. This eliminates single points of failure and lets Kubernetes horizontal scaling naturally increase throughput.

Action completion routing is partially instance-affine: `push_command` response routing uses in-memory tables (`_pending_commands`) on the dispatching instance. `pull_event` routing is now stream-based and fully distributed: any live instance that receives the incoming CloudEvent writes a `pull_event:` work item to `sfc:work:{scope}`, and any consumer can complete the waiting action via `_handle_pull_event`.

The two interaction patterns have different recovery characteristics:

- **`push_command`**: the response message is lost if delivered to the wrong instance, but the action stays `dispatched` in Redis. On restart, `_recovery_scan` → `resume` → `_handle_resume` re-dispatches the command. The equipment receives the command again and produces a new response. This is a **delay until restart** — no step is permanently missed, provided equipment tolerates re-delivery (see the [idempotency contract](sfc_engine.md#idempotency-contract)).
- **`pull_event`**: any live instance that receives the CloudEvent writes to the Redis stream, and `XAUTOCLAIM` ensures the work item survives pod restarts. The action is completed by whichever engine instance processes the stream entry. No event is permanently lost.

See [SFC Engine Architecture](sfc_engine.md#sfc-engine-architecture) for the full multi-instance safety matrix.

## Southbound Connectivity

### Context

MicroDCS orchestrates equipment from the SFC engine and the southbound processors. Plants run many
industrial protocols (OPC UA client/server, Modbus, S7, vendor APIs). The framework is built on
MQTT v5 and CloudEvents, and uses OPC UA information models as payload definitions, not as a
transport ([Concepts](concepts.md#opc-ua-and-companion-specifications)).

### Decision

MicroDCS does not contain southbound industrial protocol clients. Equipment is reached over the two
transports the framework already has, with the protocol translation outside the framework core:

1. **A gateway or the equipment publishes CloudEvents on MQTT.** A southbound processor subscribes to
   `data`, `events` and `metadata` and publishes `commands`
   ([topic structure](operations.md#mqtt-topic-structure)). Bridges such as OPC UA to MQTT gateways
   or edge platforms own the OT protocol.
2. **A sidecar talks MessagePack-RPC.** A container in the same pod connects to the MessagePack-RPC
   handler on `localhost` and exchanges CloudEvents
   ([deployment model](operations.md#deployment-model)).
3. **A southbound processor translates in-process.** Equipment-specific CloudEvent shaping and
   protocol translation belong in a processor ([three-layer architecture](#three-layer-architecture)).
   A processor may use a protocol library, but then it is application code, not part of the
   framework.

### Consequences

- The framework stays protocol-agnostic and its dependencies stay small. Protocol licences, security
  patching and certification of the OT stack remain with the gateway.
- The SFC engine runs on every replica ([multi-instance model](#multi-instance-model)). A direct
  protocol connection inside each replica would make several replicas compete for the same equipment
  session, which is another reason to keep it behind a gateway that owns the connection.
- Commands are delivered at least once, so the gateway or equipment must follow the
  [idempotency contract](sfc_engine.md#idempotency-contract): deduplicate on `mdcsactionkey`, answer
  every delivery with `causationid` set to the delivery's `id`, and respond before the message
  expires.
- A gateway is one more component to deploy, secure and monitor. Its identity and topic permissions
  are covered by the [authorization model](security.md#topic-acl-model).
- The system has no direct visibility of the equipment connection. Equipment availability has to be
  reported by the gateway as `events` or `data`.

### Alternatives considered

- **An OPC UA client inside the framework.** Rejected because of the replica issue above and because
  it ties the framework to one protocol and its security model.
- **Using OPC UA PubSub or client/server as the main transport.** Rejected in favour of MQTT with
  CloudEvents ([design goals](#design-goals)), which fits the cloud-native deployment and
  shared-subscription scaling.