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