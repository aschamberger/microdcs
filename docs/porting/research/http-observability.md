# HTTP surface and observability (S21)

Background for [D-54](../decisions.md#d-54) and the S21 part of [D-40](../decisions.md#d-40). Answers Q-OPS-1 and the OpenTelemetry coverage question.

## Starting point (2026-11-10)

* The management listener (S11) serves `/health/live`, `/health/ready` and `/mcp`. `/health/ready` asks `HealthCheckService`, but no check is registered: `ValkeyHealthCheck` exists as a service only, and `ITransport.IsConnected` ("feeds readiness") is read by nobody. Readiness is always 200.
* Components have counters on three meters (`MicroDCS` for the pipeline, the transports and the job publisher, `MicroDCS.Valkey`, `MicroDCS.Sfc`); the pipeline has the one `ActivitySource` `MicroDCS` with a consumer span per message. No OpenTelemetry SDK, no exporter: nothing leaves the process.
* Outgoing envelopes take `traceparent` from `Activity.Current`. A command the SFC host dispatches from its work stream has no current activity, so it goes out without trace context.
* StackExchange.Redis 3.4 has no `ActivitySource` or `Meter` of its own; `OpenTelemetry.Instrumentation.StackExchangeRedis` has no stable release (checked 2026-11-10).
* The OpenTelemetry Prometheus exporter for ASP.NET Core is a prerelease package; the maintainer chose OTLP only (2026-11-10).

## Python parity

Python had no HTTP surface. Telemetry came from `opentelemetry-distro` auto-instrumentation.

| # | Python behaviour | Where | Disposition |
|---|---|---|---|
| O-1 | OTel switched on by `processing.otel_instrumentation_enabled`, which picks the `OTELInstrumented*Handler` variants; exporters and endpoints come from the standard `OTEL_*` variables of `opentelemetry-distro` | `core.py` `run`, `__init__.py` `ProcessingConfig`, `app/__main__.py` | changed (D-54): no flag and no handler variants (D-37); the OTLP exporters register when `OTEL_EXPORTER_OTLP_ENDPOINT` (or a signal endpoint) is set, all other `OTEL_*` variables as in the SDK |
| O-2 | aiomqtt instrumentation (own fork) creates the MQTT receive and publish spans | `pyproject.toml`, fork | changed: the pipeline's consumer span per message and a producer span per published event or command (D-54); MQTTnet has no instrumentation |
| O-3 | The process span gets `messaging.destination.subscription.name` = the matched subscription filter | `mqtt.py` `OTELInstrumentedMQTTHandler._process_message` | kept: `InboundMessage.Subscription`, set by MQTT and NATS, tagged on the consumer span |
| O-4 | Redis auto-instrumentation: one client span per command | `opentelemetry-instrumentation-redis` | changed: no per-command spans (no stable instrumentation for StackExchange.Redis); the Valkey layer's counters and the SFC work span cover it (D-54) |
| O-5 | MessagePack-RPC server spans and the `rpc.server.duration` histogram | `msgpack.py` | dropped: transport dropped (driver 4) |
| O-6 | Console logging is turned off when `OTEL_LOGS_EXPORTER` is set (`logging.disable_if_otel_enabled`) | `__init__.py` `LoggingConfig` | changed: console logging stays; the OTLP log exporter is added next to it. Console output is what `kubectl logs` shows, and `Logging:Console` turns it down |
| O-7 | `traceparent`/`tracestate` CloudEvent attributes | `common.py` `CloudEvent` | kept (S08), and now also set on dispatched SFC commands |
| O-8 | Redis client `health_check_interval` (connection ping) | `__init__.py` `RedisConfig`, `core.py` | changed: readiness pings Valkey through `ValkeyHealthCheck`; StackExchange.Redis keeps its own heartbeat |
| O-9 | No probes, no liveness or readiness, no job query API (R-19) | `deploy/k8s.yaml` | changed: `/health/live`, `/health/ready` with registered checks, `/api/v1/jobs` |

No row is open.

## Design

**Telemetry project.** `MicroDCS.Telemetry` holds `AddMicroDcsTelemetry(IConfiguration)`, so the OpenTelemetry SDK stays out of the libraries. It registers every MicroDCS `ActivitySource` and `Meter` with the wildcard `MicroDCS*` (a new component's meter is picked up without a change here), `NATS.Net`, the built-in .NET meters (`System.Runtime`, `Microsoft.AspNetCore.Hosting`, `Microsoft.AspNetCore.Server.Kestrel`) and the ASP.NET Core source of the management listener with health probes filtered out. Only two packages: `OpenTelemetry.Extensions.Hosting` and `OpenTelemetry.Exporter.OpenTelemetryProtocol`; .NET 10 runtime and ASP.NET Core emit their own metrics, so no instrumentation packages are needed. Exporters register only when an OTLP endpoint is configured; without one the SDK is not added at all (no listeners, no cost).

**Spans.**

* Consumer: `process {type}` (exists), now with `messaging.system` (transport name) and `messaging.destination.subscription.name`.
* Producer: `send {type}` around building the envelope in `PublishAsync`, `DispatchAsync` and replies, so `traceparent` names the producer span. A reply inside a handler is a child of the consumer span.
* SFC host: `sfc {action}` (internal) per work entry, with job id and scope; a dispatched command is its child, so the equipment's spans join the recipe's trace. Histogram `microdcs.sfc.work.duration`.
* Valkey: no spans (O-4).

**Readiness.** `AddMicroDcs` registers the health-check service with a `transports` check (every started transport connected) and `AddMicroDcsValkey` adds `valkey`. `/health/ready` answers 503 with the names of the failing checks (no exception text). Liveness stays "the process answers": a component that fails ends the process (D-27).

**REST adapter (`/api/v1`).** A management module in `MicroDCS.Inspection`, on when `Management:ApiEnabled` (default off). Read-only, through `InspectionService` (audit, cap, redaction), with its own rate-limit window per caller so MES polling does not starve MCP. Stable DTOs with source-generated JSON:

* `GET /api/v1/jobs?scope=&state=&limit=` — orders of a scope (`scope` required, as in the inspector).
* `GET /api/v1/jobs/{id}` — order, response state and, when the SFC host runs, the recipe state.
* Errors: 400 (missing or invalid argument), 404 (unknown job), 429 (rate limit), RFC 9457 problem details.
* Jobs are identified by id alone (D-42). The OpenAPI document is a checked-in file served at `/api/v1/openapi.json`; a test compares its paths with the mapped routes.

**`/metrics` is dropped** (D-40 revised): metrics leave by OTLP; a Prometheus server scrapes the OpenTelemetry Collector.
