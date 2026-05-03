# MicroDCS Documentation

MicroDCS is an open-standards-based Python framework for building distributed sequence control applications for OT and manufacturing. The documentation focuses on how the framework is structured, which standards it builds on, and how to work with the codebase and deployment model.

## Start Here

If you are new to the project, use this reading path:

1. [Concepts at a Glance](concepts.md) for the key terms, standards, and framework abstractions
2. [Overall Design](overall-design.md) for the architectural model and project assumptions
3. [SFC Engine](sfc_engine.md) for recipe-driven station orchestration and distributed execution
4. [Technical Standards](technical-standards.md) for the protocol and specification choices
5. [Information Model Standards](information-model-standards.md) for the data-model background
6. [Your First Processor](your-first-processor.md) for a hands-on tutorial building a CloudEvent processor
7. [Machinery Jobs – MES Publishing](machinery-jobs-mes-publishing.md) for retained-topic-based MES integration and reconnect resync
8. [Development](development.md) for local development workflow and implementation details
9. [Persistence](persistence.md) for the Redis-backed persistence model

## What MicroDCS Provides

* A framework for event-driven sequence control applications built on open standards
* Protocol handling for MQTT v5 and MessagePack-RPC with CloudEvents-based messaging
* Typed application models generated from JSON Schema
* Processor abstractions for handling commands, data, events, and metadata
* Redis-backed persistence for delivery coordination and job state
* OPC UA Machinery Job Management support, including hierarchical state-machine execution
* Recipe-driven station orchestration via the SFC engine, including distributed work recovery
* Retained MQTT topics for MES northbound integration with sequence-based reconnect resync
* OpenTelemetry support for tracing and metrics

## Repository

Source code, issues, and release history are available in the GitHub repository:

* [aschamberger/microdcs](https://github.com/aschamberger/microdcs)