# NATS transport (S14)

Findings for `MicroDCS.Transport.Nats` ([D-47](../decisions.md#d-47)). The session is the abstraction check of the transport contract (D-37): the MQTT transport was the first implementation, NATS is the second, so everything in `ITransport`, `IStateTopicPort`, `InboundMessage` and `OutboundMessage` that only fits MQTT shows up here. There is no Python NATS code, so there is no parity list; the Python behaviour that matters is already covered by [MQTT transport](mqtt-transport.md) and the pipeline.

Versions: NATS.Net 3.3.2 (AOT clean, D-33), nats-server 2.15 with JetStream. Per-message TTL needs server 2.11 or later.

## Mapping decisions

| Topic | NATS form | Why |
|---|---|---|
| Request path | JetStream stream per binding prefix (`{StreamPrefix}_{prefix with _}`), durable pull consumer per binding, explicit ack | `AtLeastOnce` profile, dedupe claim applies, redelivery by the server |
| Response path | plain subject `{ReplyBase}.{instance token}`, core subscription, no queue group | NATS request/reply is the native pattern (D-10); D-9 already accepts a lost response (instance affinity, SFC re-dispatch) |
| Reply address | header `Mdcs-Reply-To` | a JetStream delivery uses the reply field for the acknowledgement subject, and the stored message does not keep a reply field |
| Shared group | the durable consumer is named `{binding}-{group}`, so the instances compete on it; without a group the consumer is `{binding}-{instance token}` and every instance sees every message | the native form of a shared subscription is a queue group or a shared durable consumer; JetStream needs the latter |
| Consumer start | `DeliverPolicy.New` | an MQTT subscription does not replay history either |
| Consumer cleanup | `InactiveThreshold` (1 h) | a generated instance id changes on restart; same reasoning as the MQTT session expiry (D-46) |
| Attributes | headers under their own name, `Content-Type` for `datacontenttype` | the CloudEvents NATS binding specifies structured mode only; binary mode in headers is what MQTT and AMQP do. Attribute names are lowercase letters and digits, so they never meet `Nats-*` or `Mdcs-*` headers |
| Expiry | `Nats-TTL` header (seconds, rounded up) on stream messages; stream option `AllowMsgTTL` | the server drops the message itself (D-10, `NativeExpiry`) |
| Duplicate publish | `Nats-Msg-Id` = `subject:source:id`, duplicate window 2 min | a publish repeated after a lost acknowledgement stores once |
| Fault | `Nak` with delay (`NakDelay`, 5 s); from `MaxDeliver` (5) deliveries on, `Term` plus log plus counter `microdcs.nats.poisoned` | the server would drop the message after `MaxDeliver` without a trace; `CanRedeliver` is true, so the pipeline does not end the host (D-37) |
| Undecodable, invalid subject | `Term`, counted as dropped | not redelivered |
| State topics | key-value bucket `StateBucket`, key = address and name with dots; put = JetStream publish to `$KV.{bucket}.{key}` with `Nats-TTL`; delete = KV delete | a KV bucket is a stream; `Put` has no TTL overload, a publish with the header has. The bucket uses `LimitMarkerTTL`, which switches message TTL on |
| Reconnect | client library (infinite retries, jittered back-off); the transport restarts the consume loop on every reconnect | without the restart a pull request of the lost connection stays pending until the idle heartbeat check (about 30 s) |
| Tenancy (R-20) | one NATS account per tenant, selected by the credentials (`CredsFile`, `NKeyFile`, user and password, token); nothing in the code | accounts isolate subjects, streams and KV buckets in the server; tested with two accounts on the same subjects |
| Instance id | characters other than letters, digits, `_`, `-` become `_` in subjects and consumer names | a pod name can contain dots |
| Prefix and reply base | `/` and `.` both separate segments, rendered with `.` | one configuration serves MQTT and NATS |

## MQTT leaks in the core contract

| Id | Finding | Disposition |
|---|---|---|
| L-1 | `OutboundMessage.Retain` and `PublishOptions.Retain` are the MQTT retain flag. NATS has no per-message retain | **decided (D-47)**: it stays a hint; transports without it ignore it and log a warning once per binding. XML docs say so. Durable last values go through `IStateTopicPort` |
| L-2 | `InboundMessage.ReplyTo` was filled from an MQTT property | no change: `ReplyAddress` is opaque (A-6), NATS fills it from a header |
| L-3 | `GetReplyAddress(binding, instance)` assumes the instance id is usable as an address segment | no change in core: the transport maps the id to a token (a collision of two ids that differ only in invalid characters needs a pod name like `a.b` and `a_b` at once) |
| L-4 | MQTT sets correlation data on commands and responses; the pipeline never reads it (the `causationid` attribute does the work) | no change; NATS does not send it. The MQTT correlation data is redundant and can go in a later clean-up |
| L-5 | The delivery profile and `DeliveryOutcome` were enough: `Fault` maps to `Nak` when `CanRedeliver`, `Drop` to `Term`, `Ack` to `Ack` | no change |
| L-6 | `ITransport.StartAsync` for a transport with several kinds of subscriptions (consumers plus a plain subscription) | no change; the transport hands the pipeline one `InboundMessage` per delivery with the binding id of the consumer, no matching across bindings needed (the broker side filters) |
| L-7 | With `CanRedeliver`, a faulted message is redelivered while its dedupe claim is still held (lease 30 s, token of the first attempt). The redelivery waits for the lease to lapse in `ClaimAsync` (T-20) and holds a worker | **open, S19**: release the claim on a fault (compare-and-delete with the token, in `IDedupeStore`, Valkey and in-memory) and cap the wait for a busy lease (the S13 candidate). No interface change was made here because MQTT does not redeliver in the session and would not benefit |
| L-8 | Without a reply address the pipeline answers on the request's own address. Two instances without a shared group then answer each other's replies without end (found by an integration test) | no change: this is the behaviour of Python and MQTT (a processor that replies on its own input subject is a deployment error). The docs say to give requests a reply subject |

## Library findings (NATS.Net 3.3.2)

* `NatsRawSerializer<byte[]>` for receive and `NatsRawSerializer<ReadOnlyMemory<byte>>` for send avoid the serializer registry; AOT publish stays clean.
* The library has its own `ActivitySource` and `Meter` named `NATS.Net` (metrics `nats.client.*`). They are not added to the `MicroDCS` meter; applications add `AddSource("NATS.Net")` and `AddMeter("NATS.Net")` to OpenTelemetry. The transport adds `microdcs.nats.dropped`, `microdcs.nats.reconnects` and `microdcs.nats.poisoned` on the `MicroDCS` meter. This closes the NATS.Net part of the OpenTelemetry row. StackExchange.Redis stays open.
* `NatsJSOpts.Domain` is read only; it is set through the constructor.
* `ConnectAsync` throws when the server is not reachable at start (`RetryOnInitialConnect` false), which gives D-27 without extra code.
* `NakAsync` takes the delay in `AckOpts`.

## Tests

* 49 unit tests: renderer, mapper, options validator (also the NATS bindings), client options.
* 12 integration tests in compose (service `nats`, config `tests/MicroDCS.IntegrationTests/Nats/nats-test.conf` with three accounts): reply over the header, duplicate and acknowledgement, scope rejection with error event, undecodable message, command with reply header and message id, response on the inbox, state topics (value, delete, TTL expiry), shared group split, fan-out without group, nak and poison termination (with a faulting handler), reconnect through a TCP proxy that is cut, account isolation.
* Not covered: TLS and credentials files against a server (the client option factory is unit tested), a server restart (the proxy cut covers the client side), JetStream clusters (`Replicas` > 1).
