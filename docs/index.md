# MicroDCS Documentation

MicroDCS is an open-standards-based Python framework for building distributed sequence control applications for OT and manufacturing. The documentation focuses on how the framework is structured, which standards it builds on, and how to work with the codebase and deployment model.

## Start Here

If you are new to the project, use this reading path:

1. [Overall Design](overall-design.md) for the architectural model and project assumptions
2. [Technical Standards](technical-standards.md) for the protocol and specification choices
3. [Information Model Standards](information-model-standards.md) for the data-model background
4. [Your First Processor](your-first-processor.md) for a hands-on tutorial building a CloudEvent processor
5. [Development](development.md) for local development workflow and implementation details
6. [Persistence](persistence.md) for the Redis-backed persistence model

## What MicroDCS Provides

* A framework for event-driven sequence control applications built on open standards
* Protocol handling for MQTT v5 and MessagePack-RPC with CloudEvents-based messaging
* Typed application models generated from JSON Schema
* Processor abstractions for handling commands, data, events, and metadata
* Redis-backed persistence for delivery coordination and job state
* OPC UA Machinery Job Management support, including hierarchical state-machine execution
* OpenTelemetry support for tracing and metrics

## Repository

Source code, issues, and release history are available in the GitHub repository:

* [aschamberger/microdcs](https://github.com/aschamberger/microdcs)