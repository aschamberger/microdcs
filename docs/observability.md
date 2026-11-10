# HTTP surface and observability

The management listener ([inspection](inspection.md)) is also the production HTTP surface: health probes and a small, read-only REST API for job state. Telemetry leaves the process over OTLP only ([D-54](porting/decisions.md#d-54)).

## Endpoints

All on the management listener (`MicroDcs:Management:Url`, default `http://127.0.0.1:8081`). Off loopback every route except `/health/*` needs the bearer token from `MicroDcs:Management:BearerTokenFile`.

| Route | Needs | Answer |
|---|---|---|
| `GET /health/live` | `Enabled` | `200 live` while the process answers. A component that fails ends the process (D-27), so liveness has nothing else to check. |
| `GET /health/ready` | `Enabled` | `200 ready`, or `503 not ready: <checks>` with the names of the failing checks (never their error text). |
| `GET /api/v1/jobs?scope=&state=&limit=` | `ApiEnabled`, Machinery Jobs processor | The job orders of a scope. `scope` is required; `state` filters by Job Control V2 state name; `limit` is 1 to `MaxResults` (default `MaxResults`). |
| `GET /api/v1/jobs/{id}` | `ApiEnabled`, Machinery Jobs processor | One job order with its response state and, when the SFC host runs, its recipe state. |
| `GET /api/v1/openapi.json` | `ApiEnabled` | The OpenAPI 3.1 document ([`src/MicroDCS.Inspection/openapi.json`](https://github.com/aschamberger/microdcs/blob/main/src/MicroDCS.Inspection/openapi.json)). |
| `/mcp` | `McpEnabled` | The MCP tools (dev and test only, [inspection](inspection.md)). |

Readiness checks:

| Check | Registered by | Healthy when |
|---|---|---|
| `transports` | `AddMicroDcs` | Every registered transport is connected. An instance with `Roles=None` starts none and is ready. |
| `valkey` | `AddMicroDcsValkey` | Valkey answers `PING`. |

An application adds its own check with `services.AddSingleton<T>().AddMicroDcsHealthCheck<T>(name)` (idempotent) or the standard `AddHealthChecks().AddCheck`; `/health/ready` asks all of them.

### REST API

The API is the stable, versioned part of the management surface; the MCP tools may change freely. It is read-only: MES commands go over the message bus. Jobs are identified by id alone (D-42).

```bash
curl -s 'http://127.0.0.1:8081/api/v1/jobs?scope=plant/line1&state=Running'
# {"jobs":[{"id":"job-1","scope":"plant/line1","state":"Running","version":7,"priority":3}]}

curl -s http://127.0.0.1:8081/api/v1/jobs/job-1
# {"id":"job-1","scope":"plant/line1","state":"Running","version":7,"priority":3,"responseState":"Running",
#  "recipe":{"status":"Running","current":"Fill","activeSteps":["Fill"],"nextDeadline":"2026-11-10T10:00:00.000Z"}}
```

Errors are RFC 9457 problem details (`application/problem+json`): 400 for a missing or invalid parameter, 404 for an unknown job or an instance without the Machinery Jobs processor, 429 with `Retry-After` when the rate limit is reached. Every call goes through `InspectionService`: it is written to the audit log (`MicroDCS.Security`), capped and redacted like the MCP tools; values are not cut, because the DTO fields are typed. One HTTP request takes one rate-limit token; REST and MCP have separate windows (`RequestsPerMinute` each).

## Telemetry

`AddMicroDcsTelemetry(configuration)` (package `MicroDCS.Telemetry`) adds the OpenTelemetry SDK with the OTLP exporter. Call it before `AddMicroDcs`: hosted services start in order, and the providers must listen before the transports deliver. Everything is set through the standard [OpenTelemetry SDK variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/); `microdcs serve` reads them unprefixed.

| Variable | Effect |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Turns on traces, metrics and logs. Without it, or a signal endpoint below, the SDK is not added at all. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `..._METRICS_ENDPOINT`, `..._LOGS_ENDPOINT` | Turn on one signal. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` (default) or `http/protobuf`. |
| `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT` | As in the SDK. |
| `OTEL_SERVICE_NAME` | `service.name`, default `microdcs`. `service.instance.id` is the instance id the application uses (`MicroDcs:InstanceId`, else `POD_ID`, else generated), the same as in reply addresses. |
| `OTEL_SDK_DISABLED` | `true` turns the SDK off. |

There is no `/metrics` endpoint: a Prometheus server scrapes an OpenTelemetry Collector instead. Console logging stays on next to the OTLP log exporter.

### Traces

| Span | Source | Kind | Attributes |
|---|---|---|---|
| `process {type}` | `MicroDCS` | consumer | `messaging.operation.type`, `messaging.message.id`, `messaging.system` (transport), `messaging.destination.subscription.name` (MQTT filter or NATS consumer), `microdcs.binding`, `cloudevents.type`. Parent: the message's `traceparent`. |
| `send {type}` | `MicroDCS` | producer | `messaging.message.id`, `cloudevents.type`. Created for `PublishAsync` and command dispatch; the envelope's `traceparent` names it. |
| `sfc {action}` | `MicroDCS.Sfc` | internal | `microdcs.sfc.action`, `microdcs.job.id`, `microdcs.scope`. One per SFC work entry; a command it dispatches is its child, so the equipment's spans join the recipe's trace. |
| NATS.Net spans | `NATS.Net` | | From the client library. |
| HTTP requests | `Microsoft.AspNetCore` | server | Management listener; health probes are not recorded. |

Valkey calls have no spans of their own: StackExchange.Redis has no instrumentation, and the contrib package has no stable release. The `microdcs.valkey.*` counters and the SFC work span cover it.

### Metrics

| Meter | Instruments |
|---|---|
| `MicroDCS` | `microdcs.messages.received` (by `outcome`), `microdcs.messages.published`, `microdcs.outbound.rejected`, `microdcs.commands.expired`, `microdcs.mqtt.*`, `microdcs.nats.*`, `microdcs.jobs.publisher.*` |
| `MicroDCS.Valkey` | `microdcs.valkey.write.retries`, `...write.already_applied`, `...stream.entries_lost`, `...stream.entries_trimmed`, `...poll.errors` |
| `MicroDCS.Sfc` | `microdcs.sfc.dispatched`, `...dispatch.dropped`, `...work.dead`, `...events.matched`, `...events.unmatched`, `...outages`, `microdcs.sfc.work.duration` (histogram in seconds, by `action` and `outcome`) |
| `NATS.Net` | The client library's metrics. |
| `System.Runtime`, `Microsoft.AspNetCore.Hosting`, `Microsoft.AspNetCore.Server.Kestrel` | Built-in .NET runtime and listener metrics. |

New MicroDCS meters and sources are picked up by the pattern `MicroDCS*`.

## Local dashboard

`docker compose up -d --wait` starts the Aspire dashboard (UI on `http://localhost:18888`, OTLP on `4317` gRPC and `4318` HTTP). The compose profile `app` sends its telemetry there. For a local `dotnet run`:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
APP_MicroDcs__CloudEventSource=urn:microdcs:dev \
APP_MicroDcs__Bindings__greetings__Transport=memory \
APP_MicroDcs__Bindings__greetings__Prefix=dev \
APP_MicroDcs__Bindings__greetings__ScopeLevels=2 \
dotnet run --project src/MicroDCS.App -- serve
```
