# Overall Design

Build OT apps based on open standards like MQTTv5, CloudEvents, OpenTelemetry, OPC UA companions specs, ... to apply the speed of software to the mostly hardware-defined OT base.

## Design Goals

* Manufacturing control/OT apps should be buildable like IT applications with cloud native/modern architecture principles
* Code is generated as much as possible from standard specs (e.g. OPC UA) to lower the burden to adhere to these standards

## Premises

* Event driven architecture via MQTT as transport protocol (no OPC UA Client/Server or Pub/Sub)
* OPC UA information models/companion specs are used for communication over MQTT
* Meta information is transported via MQTT user properties/cloud event headers to identify the message payload
* Implementations must only work via the generated dataclasses and not directly with the MQTT payloads (they are decoded/encoded in the background)
* There is an app UNS that has at least subtopics for `data` (variable publication/maybe setting), `events` and `commands` (method calls); optionally `metadata`to publish what is offered by the app in a retained topic