# micro-dcs

MicroDCS: An Open-Standard Framework for Distributed Sequence Control.

## TODO

App:
* MQTTProcessor interface
  * handling responses/mrpc for published messages
  * sending of outgoing messages which are not responses
  * processing config/plugin model: https://gist.github.com/dorneanu/cce1cd6711969d581873a88e0257e312

* distroless container image python
* Read the bookmarks on DCS internals
* implement OTELInstrumentedMQTTHandler

* OPC UA Job Spec
  * publish opc ua meta object with retain on app startup for dicovery functionality
  * build dataset handler with key frame support: https://reference.opcfoundation.org/Core/Part14/v105/docs/5
  * redis example implementation

* Notes
  * deployment of additional HTTP services container (with e.g. FastAPI) servicing to the outside in same pod or different one with node affinity?
  * what about copying the data n-times and mem consumption + GC impact
  * parallel container instances means always read from redis

## Overall Design

Build OT apps based on open standards like MQTTv5, CloudEvents, OpenTelemetry, OPC UA companions specs, ... to apply the speed of software to the mostly hardware-defined OT base.

### Design Goals

* Manufacturing control/OT apps should be buildable like IT applications with cloud native/modern architecture principles
* Code is generated as much as possible from standard specs (e.g. OPC UA) to lower the burden to adhere to these standards

### Premises

* Event driven architecture via MQTT as transport protocol (no OPC UA Client/Server or Pub/Sub)
* OPC UA information models/companion specs are used for communication over MQTT
* Meta information is transported via MQTT user properties/cloud event headers to identify the message payload
* Implementations must only work via the generated dataclasses and not directly with the MQTT payloads (they are decoded/encoded in the background)
* There is an app UNS that has at least subtopics for `data` (variable publication/maybe setting), `events` and `invoke` (method calls); optionally `meta`to publish what is offered by the app in a retained topic

## Technical Standards

### MQTTv5

### JSON Schema

* https://json-schema.org/

### MessagePack-RPC

* https://msgpack.org/
* https://github.com/msgpack-rpc/msgpack-rpc/blob/master/spec.md

### CloudEvents

* https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/primer.md#versioning-of-cloudevents
* https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md
* https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/mqtt-protocol-binding.md

As only MQTT v5 is supported only `Binary Content Mode` is implemented from MQTT protocol binding!
`id` and `datacontenttype` are populated from MQTT properties and `time` set on object creation.
`source` is set from `APP_PROCESSING_CLOUDEVENT_SOURCE` env var and `subject` is set to `processor.identifier`.
`type` and `dataschema` must be set individually by processor.

### OpenTelemetry

## Information Model Standards

### OPC UA

#### ...

#### EUInformation:

* https://reference.opcfoundation.org/Core/Part8/v105/docs/5.6.3
* http://www.opcfoundation.org/UA/EngineeringUnits/UNECE/UNECE_to_OPCUA.csv
* https://raw.githubusercontent.com/OPCFoundation/UA-Nodeset/refs/heads/latest/Schema/UNECE_to_OPCUA.csv

### ISA-88/ISA-95

## Python Libs

* https://github.com/empicano/aiomqtt // https://github.com/eclipse-paho/paho.mqtt.python
* https://github.com/Fatal1ty/mashumaro
* https://github.com/koxudaxi/datamodel-code-generator

## Misc

### ISA-95 vs. ISA-88

* https://iacsengineering.com/isa-88-and-isa-95-integrated-consulting/
* https://mdcplus.fi/blog/what-is-isa-88-manufacturing/
* https://www.isa.org/products/isa-tr88-95-01-2008-using-isa-88-and-isa-95-togeth

### Distroless Container

https://labs.iximiuz.com/tutorials/gcr-distroless-container-images
https://github.com/GoogleContainerTools/distroless

Add Python builds to gcr.io/distroless/base-debian13 from:
https://gregoryszorc.com/docs/python-build-standalone/main/running.html

### Links

* https://github.com/koepalex/Crow-s-Nest-MQTT
* https://hub.docker.com/_/eclipse-mosquitto
