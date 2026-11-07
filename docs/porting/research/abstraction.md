# Abstraction mapping (MQTT, NATS, AMQP, Zenoh)

Source of this section: `src/microdcs/common.py` (`ProtocolHandler`, `ProtocolBinding`, `CloudEventProcessor`), `mqtt.py`, `publishers/machinery_jobs.py`. Statements about NATS, AMQP 1.0 and Zenoh are from general knowledge of those protocols and must be verified in spikes (marked *verify*).

## What the abstraction is today

* `ProtocolHandler` is only a task loop plus `register_binding`. `ProtocolBinding` is only a bounded outgoing queue, a type filter and a `publish_handler`. Both are registration skeletons.
* The actual inbound/outbound pipeline lives entirely in `MQTTHandler` (~500 of the 900 lines of `mqtt.py`): decode to `CloudEvent`, dedupe claim (Valkey lease), cancel expiry timer, scope check, dispatch to the processor, route responses, publish them, `mark_done`, ack, plus retry/reconnect. `msgpack.py` has its own copy of parts of this (and no dedupe). A third transport would copy it again.
* Processor side is already transport-neutral: `CloudEventProcessor` only sees `CloudEvent`, `MessageIntent` and `ProcessorBinding` (north/south). That part carries over as is.

So D-3 holds for the processor-facing model, but not for the transport layer: **the transport-neutral pipeline has to be extracted, not just ported.**

## Where MQTT leaks

| Leak | Where | Effect on other transports |
|---|---|---|
| `transportmetadata` keys `mqtt_topic`, `mqtt_response_topic`, `mqtt_retain`, `mqtt_correlation_data` | `common.py` (`callback_outgoing` writes `mqtt_topic`), `processors/machinery_jobs.py` (`mqtt_retain`), `mqtt.py` | Processor code names MQTT. Needs neutral keys/typed fields (address, reply-to, retained) |
| Address derivation: topic prefix + subject (`.` to `/`) + discriminator + intent suffix, wildcard levels | `MQTTProtocolBinding` | Each transport has its own address syntax; the *logical* address (prefix, scope path, discriminator, intent) is neutral, the *rendering* is not |
| `MessageIntent` = topic suffix `data`/`events`/`commands`/`metadata` | `common.py`, binding | Neutral concept, rendering differs (NATS token, AMQP address/queue, Zenoh key segment) |
| Subject vs. topic scope check (`subject_matches_topic`) | `MQTTProtocolBinding` | Security control tied to topic ACLs. Needs a neutral "logical address vs. subject" check, and each transport's ACL model documented |
| Request/response via MQTT5 response topic and correlation data (`uuid.UUID(id).bytes`) | `_publish_message`, binding | Correlation data requires the event id to be a UUID. Replace by `correlationid`/`causationid` attributes plus transport reply address |
| Per-instance response address `{base}/{instance_id}` | binding | Core of the instance-affine push routing (R-2) |
| Shared subscriptions (`$share/name/...`) | binding | Competing consumers exist in all other transports but are configured differently |
| Retained publishing with TTL and zero-byte delete | `MQTTPublisher`; `JobOrderPublisher` inherits from it; `core.py` special-cases `isinstance(task, MQTTPublisher)` | The publisher is MQTT by inheritance. Needs a neutral "last value / state topic" port |
| Metadata carrier: all CloudEvent attributes as MQTT user properties (binary content mode), `custommetadata` flattened | `_publish_message`, `_cloudevent_from_message` | Neutral idea (CloudEvents binary mode); the SDK bindings handle the carrier |
| Manual puback after `mark_done`, and puback on duplicates | `_process_message` | Ack is a transport callback; pipeline must call an abstract `AckAsync`/`NackAsync` |
| Expiry: wire-level message expiry interval, plus a local timer that triggers `handle_cloudevent_expiration` for commands with a response topic | `_publish_message` | Wire-level expiry is transport specific; the local timer is neutral |
| OTEL: plain + instrumented handler pairs (`OTELInstrumentedMQTTHandler`) | `mqtt.py` | Disappears if tracing is done once in the pipeline (ActivitySource, W3C context from `traceparent`/`tracestate` attributes) |

## Capability mapping

| Concern | MQTT 5 (today) | NATS (+ JetStream) | AMQP 1.0 | Zenoh |
|---|---|---|---|---|
| Address / routing | Topic, `/` levels, `+` `#` | Subject, `.` tokens, `*` `>`. Tokens must not contain `.` `*` `>` or whitespace, so subjects with `.` need an escaping rule | Broker-defined address (queue/topic/multicast); wildcards depend on the broker | Key expression, `/` chunks, `*` `**` |
| Metadata carrier | User properties, content type, response topic, correlation data | Headers (NATS 2.2+) | Application properties + standard properties (`message-id`, `correlation-id`, `reply-to`, `content-type`, `subject`) | Attachment (key/value) + encoding |
| CloudEvents binding | Spec binding exists (binary mode in user properties) | Spec binding exists (headers) | Spec binding exists (application properties) | None in the spec; define our own (attachment or structured mode) *verify* |
| Request/response | Response topic + correlation data | Native reply subject (inbox), request-reply | `reply-to` + `correlation-id` (dynamic receiver link) | Native: `get`/queryable, reply goes to the querier |
| Competing consumers | `$share/group/topic` | Queue groups (core), shared durable consumer (JetStream) | Multiple receivers on one queue | None for pub/sub (all subscribers receive). Work distribution via queryables or app level |
| Delivery guarantee | QoS 0/1 (2 unused), manual ack | Core: at-most-once. JetStream: ack, redelivery, `max_deliver` | Settlement modes: accept/reject/release/modified, at-least-once natively | Best-effort or reliable per link, no consumer ack |
| Duplicate detection | Our Valkey lease claim on `source`+`id` | JetStream `Nats-Msg-Id` window on the publish side, plus our claim | Our claim (broker-specific otherwise) | Our claim, and redelivery is not a Zenoh concept |
| Message expiry | Message expiry interval (broker drops) | Per-message TTL header (JetStream, recent server versions) *verify*; stream `max_age` | `ttl` header / `absolute-expiry-time` | Not supported by the transport; local timer only |
| Retained / last value | Retained flag, zero-byte delete, expiry | JetStream stream with `max_msgs_per_subject=1`, or KV (put/delete/purge, TTL) | No standard. Broker-specific (e.g. Artemis last-value queue) | Storage plugin or advanced pub/sub cache *verify* |
| Ordering | Per topic/session | Per subject per stream | Per link | Per publisher and key |
| ACL / scope enforcement | Broker topic ACLs; subject-vs-topic check in client | Accounts, NKeys/JWT, subject permissions | SASL + broker authorization | ACL config, TLS/mTLS identity |
| Reconnect | Library | Library | Library (AMQPNetLite: manual) | Session reconnect in zenoh-c |

## Proposed abstraction changes (to decide)

* **A-1 Extract the inbound pipeline** into one transport-neutral component: `decode, claim, expiry-cancel, scope check, dispatch, publish responses, mark done, ack`. A transport only supplies: subscribe, a decoded `InboundMessage` with `AckAsync`/`NackAsync`, `PublishAsync(OutboundMessage)`, and reconnect handling. Also the natural place for the single `ActivitySource` and for logging hygiene. Removes the OTEL handler pairs.
* **A-2 Neutral transport metadata.** Replace the `dict[str, Any]` with typed fields: `Address`, `ReplyTo`, `Retain`/`StateTopic`, `ExpiryInterval`, plus an opaque per-transport bag. Processors never see a transport name.
* **A-3 Logical address model** (`prefix`, `scope path`, `discriminator`, `intent`) owned by the binding configuration, with one renderer per transport (MQTT `/`, NATS `.`, AMQP broker address, Zenoh `/`). Decide the escaping/mapping rule for scopes and subjects that contain separators.
* **A-4 State-topic port** (retained/last value), implemented natively per transport (D-10): MQTT retained messages, NATS KV or a last-per-subject JetStream stream, Zenoh storage/advanced-publisher cache. AMQP 1.0 has no standard equivalent: supported only through a broker's native feature (e.g. last-value queue) or declared "not supported". `JobOrderPublisher` depends on the port, not on `MQTTPublisher`. The HTTP query API (Q-OPS-1) is an addition, not a substitute.
* **A-5 Delivery profile per binding**: declared native guarantee (`AtMostOnce`/`AtLeastOnce`). The pipeline applies dedupe only for `AtLeastOnce`. Zenoh has no redelivery, so command delivery relies on the SFC action state machine (re-dispatch after TTL, D-9).
* **A-6 Request/response contract in the model, not the transport.** `correlationid`/`causationid`/`mdcsactionkey` plus a `ReplyTo` address; each transport maps it to its native mechanism (MQTT response topic, NATS inbox, AMQP `reply-to`, Zenoh query/reply). The reply address stays per instance (D-9). Drop the UUID-only id restriction.
* **A-7 Scope enforcement as a neutral policy** (compare logical address with subject), configurable per binding, plus one security document section per transport for ACLs (see R-8).
* **A-8 Publisher/AdditionalTask hierarchy** becomes `IHostedService`/`BackgroundService` implementations in .NET (Q-ARCH-3). The `isinstance(task, MQTTPublisher)` check in `core.py` disappears; the publisher instance mode becomes a hosting/DI option.

Transport expectations that should be written down per binding (done in spikes): NATS core vs. JetStream default, AMQP broker target (RabbitMQ 4 vs. Artemis vs. Service Bus), Zenoh router vs. peer mode, southbound direction for Zenoh (see Q-ARCH-10).
