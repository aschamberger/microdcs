# Operations

This page covers operational concerns for running MicroDCS in production:
configuration reference, backpressure behaviour across the processing pipeline,
graceful shutdown, observability, and capacity planning.

## Deployment Model

A MicroDCS deployment has four moving parts:

``` mermaid
flowchart LR
  subgraph Pod
    API["Sidecar container\n(e.g. FastAPI)"] -- "MessagePack-RPC\nlocalhost:8888" --> DCS["MicroDCS container\nasync event loop"]
    DCS --- Redis[(Redis)]
  end
  DCS -- "MQTT v5" --> Broker[MQTT Broker]
```

The MicroDCS container runs a single async event loop. All protocol handlers, bindings,
and processors share this loop — isolation between them is achieved through bounded
async queues, not separate threads or processes. Understanding where those queues sit
and what happens when they fill is the foundation for operating the system reliably.

---

## Configuration Reference

All settings are read from environment variables with the prefix `APP_` and nested
structure `APP_{SECTION}_{FIELD}`.

### MQTT (`APP_MQTT_*`)

| Variable | Type | Default | Description |
|---|---|---|---|
| `APP_MQTT_HOSTNAME` | `str` | `localhost` | MQTT broker hostname |
| `APP_MQTT_PORT` | `int` | `1883` | MQTT broker port |
| `APP_MQTT_IDENTIFIER` | `str` | `app_client` | MQTT client identifier |
| `APP_MQTT_CONNECT_TIMEOUT` | `int` | `10` | Broker connection timeout (seconds) |
| `APP_MQTT_PUBLISH_TIMEOUT` | `int` | `5` | Publish confirmation timeout (seconds) |
| `APP_MQTT_SAT_TOKEN_PATH` | `Path` | `/var/run/secrets/tokens/broker-sat` | Path to SAT token for broker auth |
| `APP_MQTT_TLS_CERT_PATH` | `Path` | `/var/run/certs/ca.crt` | CA certificate for TLS connections |
| `APP_MQTT_INCOMING_QUEUE_SIZE` | `int` | `0` (unbounded) | Max queued incoming messages in aiomqtt client |
| `APP_MQTT_OUTGOING_QUEUE_SIZE` | `int` | `0` (unbounded) | Max queued outgoing messages in aiomqtt client |
| `APP_MQTT_MESSAGE_WORKERS` | `int` | `5` | Concurrent tasks processing incoming messages |
| `APP_MQTT_DEDUPE_TTL_SECONDS` | `int` | `600` | TTL for Redis deduplication keys (seconds) |
| `APP_MQTT_BINDING_OUTGOING_QUEUE_SIZE` | `int` | `5` | Per-binding outgoing queue capacity |

### MessagePack RPC (`APP_MSGPACK_*`)

| Variable | Type | Default | Description |
|---|---|---|---|
| `APP_MSGPACK_HOSTNAME` | `str` | `localhost` | RPC server listen address |
| `APP_MSGPACK_PORT` | `int` | `8888` | RPC server listen port |
| `APP_MSGPACK_TLS_CERT_PATH` | `Path` | `/var/run/certs/ca.crt` | CA certificate for TLS |
| `APP_MSGPACK_TLS_CLIENT_AUTH` | `bool` | `false` | Require client certificate authentication |
| `APP_MSGPACK_KEEP_ALIVE` | `bool` | `true` | Enable TCP keep-alive on client connections |
| `APP_MSGPACK_MAX_QUEUED_CONNECTIONS` | `int` | `100` | TCP backlog for the RPC server socket |
| `APP_MSGPACK_MAX_CONCURRENT_REQUESTS` | `int` | `10` | Per-client concurrent RPC request cap (semaphore) |
| `APP_MSGPACK_MAX_BUFFER_SIZE` | `int` | `8388608` | Max msgpack unpacker buffer (8 MB) |
| `APP_MSGPACK_BINDING_OUTGOING_QUEUE_SIZE` | `int` | `5` | Per-binding outgoing queue capacity |

### Processing (`APP_PROCESSING_*`)

| Variable | Type | Default | Description |
|---|---|---|---|
| `APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED` | `bool` | `false` | Use OTEL-instrumented handlers at runtime |
| `APP_PROCESSING_CLOUDEVENT_SOURCE` | `str` | `None` | CloudEvent `source` attribute for outgoing events |
| `APP_PROCESSING_MESSAGE_EXPIRY_INTERVAL` | `int` | `None` | Default message expiry interval (seconds) |
| `APP_PROCESSING_SHARED_SUBSCRIPTION_NAME` | `str` | `None` | MQTT shared subscription group name |
| `APP_PROCESSING_TOPIC_PREFIXES` | `set[str]` | `∅` | Comma-separated `name:prefix` pairs for topic routing |
| `APP_PROCESSING_TOPIC_WILDCARD_LEVELS` | `set[str]` | `∅` | Comma-separated `name:levels` pairs for subscription wildcards |
| `APP_PROCESSING_RESPONSE_TOPICS` | `set[str]` | `∅` | Comma-separated response topic names |
| `APP_PROCESSING_SHUTDOWN_GRACE_PERIOD` | `int` | `30` | Seconds to wait for in-flight work during shutdown |
| `APP_PROCESSING_BINDING_OUTGOING_QUEUE_MAX_SIZE` | `int` | `1000` | Global upper cap on any binding's outgoing queue |

---

## Backpressure

### Queue Boundaries

The pipeline from an incoming MQTT message to a dispatched processor, and from a
processor response to a published outgoing message, crosses several bounded queues.
Each queue has a distinct fill condition and a distinct consequence when full.

| Queue | Config variable | Default | Fills when | Producer behaviour when full |
|---|---|---|---|---|
| MQTT incoming | `APP_MQTT_INCOMING_QUEUE_SIZE` | `0` (unbounded) | Messages arrive faster than `message_workers` can dispatch | With default `0`: unbounded growth until OOM. With a finite value: backpressure to aiomqtt receive loop — new messages are not read from the socket until space is available |
| MQTT outgoing | `APP_MQTT_OUTGOING_QUEUE_SIZE` | `0` (unbounded) | Processors produce outgoing events faster than the MQTT handler can publish | With default `0`: unbounded growth until OOM. With a finite value: paho client blocks publish calls until space is available |
| MQTT binding outgoing | `APP_MQTT_BINDING_OUTGOING_QUEUE_SIZE` | `5` | A single binding's outgoing events accumulate faster than the handler drains them | **Raises `RuntimeError`** — the producer is not blocked, the error propagates to the caller |
| MessagePack binding outgoing | `APP_MSGPACK_BINDING_OUTGOING_QUEUE_SIZE` | `5` | Outgoing notification frames queue faster than connected clients consume them | **Raises `RuntimeError`** — same behaviour as MQTT binding queues |
| MessagePack concurrent requests | `APP_MSGPACK_MAX_CONCURRENT_REQUESTS` | `10` | More than N simultaneous `publish` RPC calls arrive from the same client | Server stops reading from the socket — TCP-level backpressure to the client |

!!! note "Global queue cap"
    All per-binding queue sizes are subject to `APP_PROCESSING_BINDING_OUTGOING_QUEUE_MAX_SIZE`
    (default `1000`). If a protocol-level setting exceeds this cap, it is silently reduced
    to the cap value with a warning log. If the protocol-level setting is `0`, the cap value
    is used as the effective size.

!!! warning "Unbounded MQTT queues by default"
    The MQTT incoming and outgoing queues default to `0` (unbounded). This means they will
    never apply backpressure — instead, memory grows without bound under sustained overload.
    For production deployments, set explicit finite values for `APP_MQTT_INCOMING_QUEUE_SIZE`
    and `APP_MQTT_OUTGOING_QUEUE_SIZE` based on your expected burst profile.

### Isolation

The binding-level queues (`APP_MQTT_BINDING_OUTGOING_QUEUE_SIZE`,
`APP_MSGPACK_BINDING_OUTGOING_QUEUE_SIZE`) are per-binding instances. A slow or
saturated binding does not affect other bindings — a tightening controller processor
that falls behind does not block a QA camera processor from publishing its results.

The shared queues (`APP_MQTT_INCOMING_QUEUE_SIZE`, `APP_MQTT_OUTGOING_QUEUE_SIZE`)
sit at the handler level and are shared across all bindings registered with that
handler. A sustained fill on either of these affects the whole handler and therefore
all processors attached to it. Sizing these queues appropriately for the expected burst
profile is the primary tuning lever for the shared transport layer.

### Tuning Signals

Before a queue fills, it will show up as latency — the time between an MQTT message
arriving and the processor dispatching it, or between a processor returning a response
and the outgoing publish completing. Monitor these latencies under load to establish
a baseline. A latency spike that precedes a queue-full `RuntimeError` is the signal
to act.

**`APP_MQTT_MESSAGE_WORKERS`** (default `5`) controls how many concurrent asyncio tasks
process incoming MQTT messages. This is the most important concurrency knob — if
processing latency is high but the event loop is not saturated, increasing workers
allows more messages to be dispatched in parallel.

**`APP_MSGPACK_MAX_CONCURRENT_REQUESTS`** is both a backpressure mechanism and a
resource bound. If a sidecar is legitimately sending bursts above this limit, increase
the limit before increasing queue sizes — the concurrent request cap is the first gate.

**`APP_MSGPACK_MAX_QUEUED_CONNECTIONS`** (default `100`) caps the TCP backlog for
incoming sidecar connections. If multiple sidecar replicas connect simultaneously at
startup, connections beyond this limit are refused at the kernel level.

**`APP_MQTT_DEDUPE_TTL_SECONDS`** (default `600`) controls how long Redis remembers
processed message IDs. After a broker reconnect, messages replayed within this window
are deduplicated. If your broker's session expiry exceeds this TTL, you may see
duplicate processing after reconnects.

Redis operation latency is a secondary signal: slow `await` on DAO calls inside a
processor handler holds the worker for that message and reduces effective throughput
even if the queues are not full.

### Known Failure Modes

**MQTT broker unreachable during a burst**

The MQTT handler retries with backoff. Messages that were produced by processors and
placed on the binding outgoing queue during the disconnected period accumulate there.
Because binding queues use `put_nowait()`, once full they raise `RuntimeError` rather
than blocking. Operators should monitor broker connectivity and alert before the
reconnect window exceeds the time it takes to fill the binding outgoing queue at peak
publish rate.

**Redis connection loss**

Processor handlers that persist state via DAOs (`JobOrderAndStateDAO`,
`WorkMasterDAO`, `JobResponseDAO`, etc.) do so inside the handler coroutine. If Redis
becomes unreachable, DAO calls raise connection errors that propagate up and fail the
message processing for that event. The MQTT handler does not retry failed messages —
unacknowledged QoS 1 messages will be redelivered by the broker. Monitor Redis
connectivity and command latency as part of pre-startup health checks and runtime
alerting.

**Redis slow under load**

A Redis latency spike translates directly into a reduction in effective
`message_workers` throughput, because workers are occupied waiting on Redis while MQTT
messages accumulate on the incoming queue. This is distinct from a full connection loss
— operations complete, but slowly.

**Sidecar RPC client faster than the DCS event loop**

The `APP_MSGPACK_MAX_CONCURRENT_REQUESTS` cap prevents a fast sidecar (e.g. a
FastAPI container receiving a burst of HTTP requests) from overwhelming the MicroDCS
event loop with simultaneous `publish` RPC calls. If the sidecar regularly hits this
cap, the right response is to increase the cap *and* profile the processors being
called — a processor that holds the loop for a long time under load is the root cause,
not the cap itself.

---

## Graceful Shutdown

When the process receives `SIGTERM` or `SIGINT`, the `SystemEventTaskGroup` initiates
graceful shutdown:

1. **Signal received** — no new messages are accepted from protocol handlers.
2. **Grace period starts** — controlled by `APP_PROCESSING_SHUTDOWN_GRACE_PERIOD`
   (default `30` seconds).
3. **In-flight work completes** — message workers finish processing their current
   message, including any pending Redis DAO operations.
4. **Expiration tasks cancel** — outstanding CloudEvent expiration timers are
   cancelled without triggering expiry callbacks.
5. **Binding queues drain** — outgoing messages already enqueued are published.
6. **Handler connections close** — MQTT client disconnects cleanly; MessagePack RPC
   server stops accepting new connections and closes existing ones.
7. **Process exits** — if work does not complete within the grace period, remaining
   tasks are cancelled and the process exits.

In Kubernetes, ensure the pod's `terminationGracePeriodSeconds` exceeds
`APP_PROCESSING_SHUTDOWN_GRACE_PERIOD` by at least a few seconds to allow for signal
propagation delay.

---

## Observability

MicroDCS emits OpenTelemetry traces and metrics through `OTELInstrumented` handler
variants. This section documents what the instrumentation emits, how to interpret it
operationally, and how to get started with a working setup.

### Signal Inventory

#### MQTT Handler (`OTELInstrumentedMQTTHandler`)

| Signal | Type | Measures | Key Attributes |
|---|---|---|---|
| `process MQTT {topic}` | Span (CONSUMER) | Time from message receipt to processor dispatch completion | `messaging.system`, `messaging.operation.type`, `messaging.destination.name`, `messaging.mqtt.qos`, `messaging.message.id` |
| `messaging.process.counter` | Counter | Total MQTT messages processed | `messaging.system`, `messaging.destination.name`, `status` (success/error) |
| `messaging.process.duration` | Histogram | MQTT message processing time (milliseconds) | `messaging.system`, `messaging.destination.name`, `status` (success/error) |

#### MessagePack Handler (`OTELInstrumentedMessagePackHandler`)

| Signal | Type | Measures | Key Attributes |
|---|---|---|---|
| `{rpc.method}` | Span (CONSUMER) | Time from RPC call receipt to response | `rpc.system`, `rpc.service`, `rpc.method`, `network.transport`, `server.address`, `server.port` |
| `rpc.server.call.count` | Counter | Total MessagePack RPC calls processed | `rpc.system`, `rpc.service`, `rpc.method`, `status` (success/error) |
| `rpc.server.call.duration` | Histogram | RPC call processing time (milliseconds) | `rpc.system`, `rpc.service`, `rpc.method`, `status` (success/error) |

All signal names and attributes follow
[OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/).

!!! note "Manual instrumentation"
    There is currently no OpenTelemetry auto-instrumentation library for `aiomqtt`.
    The `OTELInstrumentedMQTTHandler` and `OTELInstrumentedMessagePackHandler` provide
    manual instrumentation that wraps the handler's message processing and RPC dispatch
    loops. This means OTEL signals cover application-level processing but not low-level
    MQTT client operations (connect, subscribe, ping). If an auto-instrumentation
    library for aiomqtt becomes available in the future, it can be layered on top.

### Operational Questions

**Is the system healthy?**

A healthy system shows:

- `messaging.process.duration` histogram completing within the expected command →
  response round-trip budget for the station. At ISA-95 Level 2, a typical tightening
  command cycle is O(seconds); a QA camera pull event may be O(tens of seconds).
  Establish baselines per `messaging.destination.name` during commissioning.
- `messaging.process.counter` with `status=error` at zero or near-zero. A rising
  error count indicates processor exceptions or infrastructure failures.
- Span error rate low and stable. A sudden spike in errored spans after a deployment
  indicates a regression in processor logic.

**Is something slow?**

Look at the `messaging.process.duration` histogram broken down by
`messaging.destination.name` (topic). A latency increase on a specific topic without
a corresponding infrastructure issue points to processor logic. A latency increase
that affects all topics simultaneously points to the event loop being held — check for
a processor handler that does synchronous I/O or CPU-bound work without `await`.

For RPC calls, `rpc.server.call.duration` broken down by `rpc.method` provides the
same signal for the MessagePack path.

**Is something stuck?**

Symptoms of a stuck system:

- `messaging.process.counter` rate drops to zero while the broker reports queued
  messages — indicates the handler is not consuming.
- RPC spans show increasing duration without completing — the processor may be
  blocked on an external dependency (Redis, equipment).
- Kubernetes liveness probe failures — if the event loop is blocked, health endpoints
  stop responding.

### Getting Started

To enable OTEL instrumentation, set `APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED=true`
and configure the standard OpenTelemetry environment variables for your collector:

```bash
APP_PROCESSING_OTEL_INSTRUMENTATION_ENABLED=true
OTEL_SERVICE_NAME=microdcs.app
OTEL_TRACES_EXPORTER=otlp        # or "console" for local debugging
OTEL_METRICS_EXPORTER=otlp
OTEL_LOGS_EXPORTER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
```

In the application wiring, register both the plain and instrumented handler variants.
The framework selects the instrumented variant at runtime when
`otel_instrumentation_enabled` is `true`:

```python
from microdcs.mqtt import MQTTHandler, OTELInstrumentedMQTTHandler
from microdcs.msgpack import MessagePackHandler, OTELInstrumentedMessagePackHandler

microdcs.register_protocol_handler(
    MQTTHandler(...),
    OTELInstrumentedMQTTHandler(...),
)
microdcs.register_protocol_handler(
    MessagePackHandler(...),
    OTELInstrumentedMessagePackHandler(...),
)
```

The non-instrumented handler variants (`MQTTHandler`, `MessagePackHandler`) remain
available for local development where an OTEL collector is not present.

### Alert Recommendations

The following alert thresholds provide a starting point. Tune them during
commissioning based on observed baselines.

| Condition | Suggested threshold | Rationale |
|---|---|---|
| `messaging.process.duration` p95 | > 2× baseline for 5 min | Processing is degrading; investigate before queues fill |
| `messaging.process.counter{status=error}` rate | > 0 sustained for 2 min | Processor exceptions need immediate attention |
| `rpc.server.call.duration` p95 | > 2× baseline for 5 min | Sidecar-facing latency is rising |
| Redis command latency p95 | > 50 ms for 5 min | DAO calls are slowing down workers |
| MQTT broker disconnection | Any occurrence | Outgoing events will queue; reconnect should complete before queue fills |
| Pod restart count | > 2 in 10 min | Crash loop — check logs for startup failures |

---

## Capacity Planning

### Sizing Message Workers

`APP_MQTT_MESSAGE_WORKERS` (default `5`) determines how many messages can be
processed concurrently. The effective throughput is:

```
throughput = message_workers / avg_processing_latency_seconds  [messages/s]
```

For example, with 5 workers and 200 ms average processing time (including Redis
round-trips): 5 / 0.2 = 25 messages/s. If your peak arrival rate exceeds this,
either increase workers or reduce processing latency.

### Sizing Binding Outgoing Queues

The binding outgoing queue must absorb bursts where a processor produces events faster
than the handler can publish them. Size it to cover the burst duration:

```
queue_size >= burst_rate_msg_per_s × publish_latency_s
```

The default of `5` is conservative. For processors that emit multiple outgoing events
per incoming event (e.g. fan-out patterns), increase proportionally.

Remember that `APP_PROCESSING_BINDING_OUTGOING_QUEUE_MAX_SIZE` (default `1000`) is the
hard upper cap — any per-protocol value above this is silently reduced.

### Sizing MQTT Shared Queues

For production deployments where the defaults of `0` (unbounded) are not acceptable:

- **Incoming queue**: size to accommodate `burst_duration × arrival_rate`. A queue of
  100–500 handles typical industrial burst profiles where equipment reports in batches.
- **Outgoing queue**: size based on peak processor output rate and publish latency.
  Usually smaller than incoming since publish is fast when the broker is healthy.

### MessagePack Concurrency

`APP_MSGPACK_MAX_CONCURRENT_REQUESTS` (default `10`) should match the sidecar's
expected parallelism. If the sidecar spawns up to N concurrent HTTP handlers that each
call the RPC server, set this to at least N. Going higher than needed wastes no
resources (it's a semaphore, not pre-allocated).
