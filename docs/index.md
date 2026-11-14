# MicroDCS Documentation

MicroDCS is a .NET framework for distributed sequence control applications in OT and manufacturing. It builds on open
standards: CloudEvents over MQTT 5 and NATS, OPC UA companion specifications as payload models (ISA-95 Job Control,
Machinery Jobs), Valkey for state, and OpenTelemetry. It runs as a Native AOT binary in a small container.

MicroDCS is pre-1.0 (0.1.x): interfaces can still change between releases.

## Start here

1. [Concepts at a Glance](concepts.md): terms, standards and framework building blocks.
2. [Overall Design](overall-design.md): goals, premises, layers and instance roles.
3. [Your First Processor](your-first-processor.md): a hands-on tutorial.
4. [SFC Engine](sfc-engine.md): recipe-driven station control on several replicas.
5. [Machinery Jobs – MES Publishing](machinery-jobs-mes-publishing.md): state topics for the MES and resync after an
   outage.
6. [Technical Standards](technical-standards.md) and [Information Model Standards](information-model-standards.md):
   the protocols and models used.
7. [Development](development.md): toolchain, tests and CI. The pages below it are the reference per component.
8. [Operations](operations.md), [Deployment](deployment.md) and [Security](security.md): running MicroDCS in
   production.

## What MicroDCS provides

* A processor model with attributes and a source generator: typed handlers, no reflection, compile-time diagnostics.
* One message pipeline for all transports, with deduplication, scope checks, error events and bounded outgoing queues.
* MQTT 5 and NATS JetStream transports; AMQP 1.0 and Zenoh are planned.
* OPC UA Machinery Jobs (ISA-95 Job Control V2) as a ready processor, with station configuration and a job order state
  machine generated from the NodeSet.
* An SFC engine that runs IEC 61131-3 style recipes from the Work Master, shared by all replicas through Valkey.
* State topics for the MES with a sequence-numbered state index for resync after an outage.
* Read-only inspection, health probes, a REST API for job state, and a development MCP server with inspection and
  maintenance tools.
* OpenTelemetry traces and metrics over OTLP.
* A signed container image with SBOMs, and an example Kubernetes manifest.

## Repository

Source code, issues and releases: [aschamberger/microdcs](https://github.com/aschamberger/microdcs). The code is MIT
licensed. The earlier Python implementation is archived on the
[`python-archive`](https://github.com/aschamberger/microdcs/tree/python-archive) branch.
