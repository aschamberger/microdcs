# micro-dcs

MicroDCS: An Open-Standard Framework for Distributed Sequence Control.

## TODO

App:
* MQTTProcessor interface: send/recieve pattern
  * CloudEvents handling send/recieve or better UserProperties handling??? --> CloudEventsMQTTProcessorMixin
  * processing config/plugin model
  * topic handling, response topic for answer or sending status
  * handling responses/mrpc

https://gist.github.com/dorneanu/cce1cd6711969d581873a88e0257e312
https://github.com/koepalex/Crow-s-Nest-MQTT

* distroless container image python
* what about copying the data n-times and mem consumption + GC impact?!!


EUInformation:
https://reference.opcfoundation.org/Core/Part8/v105/docs/5.6.3
http://www.opcfoundation.org/UA/EngineeringUnits/UNECE/UNECE_to_OPCUA.csv
https://raw.githubusercontent.com/OPCFoundation/UA-Nodeset/refs/heads/latest/Schema/UNECE_to_OPCUA.csv

* Read the bookmarks on DCS internals
* https://documentation.unified-automation.com/uasdknet/4.3.0/html/L3ServerTuMachineDemoServerOverview.html

* implement OTELInstrumentedMQTTHandler (mit OTELObservabilityMixin????)
* OTEL service name vs. cloud events source
* redis example implementation
* does redis have persistance?
* parallel container instances means always read from redis?!

* OPC UA
  * publish opc ua meta object with retain on app startup for dicovery functionality
  * build dataset handler with key frame support: https://reference.opcfoundation.org/Core/Part14/v105/docs/5
  * how to integrate the state machine in the app
  * is it required to define also the persistance model? or only a callback mechanismto retrieve the data (most of the data model is already fixed by the ISA95... DataTypes)

## Overall Design

<TODO>

### Design Goals

* Manufacturing control/OT apps should be buildable like IT applications with cloud native/modern architecture principles
* Code is generated as much as possible from the OPC UA specs to lower the burden to adhere to the standards

### Premises

* Event driven architecture via MQTT as transport protocol (no OPC UA Client/Server or Pub/Sub)
* OPC UA information models/companion specs are used for communication over MQTT
* Meta information is transported via MQTT user properties/cloud event headers to identify the message payload
* Implementations must only work via the generated dataclasses and not directly with the MQTT payloads (they are decoded/encoded in the background)
* There is an app UNS that has at least subtopics for `data` (variable publication/maybe setting), `events` and `invoke` (method calls)

## Python Libs

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

* https://github.com/empicano/aiomqtt
* https://github.com/eclipse-paho/paho.mqtt.python