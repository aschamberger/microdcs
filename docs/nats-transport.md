# NATS transport

This page describes `MicroDCS.Transport.Nats` ([D-47](porting/decisions.md#d-47)), the NATS binding on NATS.Net with JetStream. It is new in the .NET version; the Python version had no NATS transport. Findings: [NATS transport](porting/research/nats-transport.md).

The server must be nats-server 2.11 or later with JetStream enabled (message time to live).

## Registration

```csharp
services.AddMicroDcs(configuration);
services.AddNatsTransport(configuration);   // after AddMicroDcs
services.AddProcessor<MyProcessor>();
```

A binding selects the transport with `MicroDcs:Bindings:{id}:Transport = nats`. `AddNatsTransport` registers the transport as `ITransport` and as `IStateTopicPort`, the `transport.nats` inspector and the options. A server that cannot be reached at start ends the process ([D-27](porting/decisions.md#d-27)); later connection losses are retried by the client library. Streams, consumers and the state bucket are created at start (create or update).

## Configuration

Section `MicroDcs:Nats`; environment names use `APP_` and `__`, for example `APP_MicroDcs__Nats__Url`.

Every field is in the [configuration reference](operations.md#config-nats).

Use one authentication method. Binding fields used by this transport (see [Processors in .NET](processors-dotnet.md)): `Prefix`, `ScopeLevels`, `Discriminator`, `SharedGroup` and `ReplyBase` (required when the processor publishes commands). Message workers, dedupe lease and queue sizes are core options (`MicroDcs:MessageWorkers`, `DedupeLease`, `OutgoingQueueMaxSize`).

Credentials over a connection without TLS are sent, with a warning in the log.

## Subjects, streams and consumers

| Purpose | Name |
|---|---|
| Message | `{Prefix}.{scope…}.{Discriminator}.{data｜events｜commands｜metadata}` |
| Subscription | the same with `*` for every scope level from 0 to `ScopeLevels` |
| Stream | `{StreamPrefix}_{Prefix}` (dots become `_`), subjects `{Prefix}.>`; one stream per prefix, shared by the bindings with that prefix |
| Consumer | `{binding}-{SharedGroup}-{hash}`, or `{binding}-{instance}` without a group. The hash covers the filter subjects |
| Reply (per instance) | `{ReplyBase}.{instance}`, a plain subject, never shared |
| State topic | key `{address}.{name}` in the bucket `StateBucket` |

A prefix or reply base may be written with `/` as for MQTT; it is rendered with `.`. Segments use only `A-Z a-z 0-9 _ -` ([D-38](porting/decisions.md#d-38)); other characters of the instance id become `_`. Two prefixes where one extends the other (`dcs` and `dcs.jobs`) overlap in the server and are refused at start.

A binding **without** `SharedGroup` gets a consumer of its own per instance, so every instance sees every message. With a group the instances compete on one consumer. Instances with different filters (during a rollout that changes intents or scope levels) get separate consumers; the old one ends after `ConsumerInactiveThreshold` ([D-47](porting/decisions.md#d-47)). Give requests a reply subject: a request without one is answered on its own subject, and two instances then answer each other's replies.

## Message mapping

The envelope travels in binary content mode: attributes are NATS headers under their own names, `datacontenttype` is `Content-Type`, the data is the payload.

| Header | Used for |
|---|---|
| CloudEvent attributes | CloudEvent attributes and extension attributes. The first value of a repeated name wins |
| `Content-Type` | `datacontenttype` |
| `Nats-TTL` | `expiryinterval` in whole seconds rounded up, on stream messages; the server drops the message when it ends. On receipt it fills `expiryinterval` when the sender set none |
| `Nats-Msg-Id` | `subject:source:id`; the stream stores a publish repeated within two minutes once |
| `Mdcs-Reply-To` | The reply subject of a command (this instance's inbox) |

## Delivery

* Requests are delivered by JetStream and acknowledged after the pipeline finished them (dedupe `mark done` first). A crash in between means redelivery and the dedupe claim answers the duplicate.
* A message that cannot be decoded, or whose subject has an invalid scope segment, is terminated, counted and logged. A duplicate is acknowledged.
* A pipeline that reports `Retry` (the dedupe store is unavailable) keeps the message in its worker and runs it again every `NakDelay`, with "in progress" acknowledgements meanwhile. No delivery is used, so a Valkey outage does not turn messages into poison (warning event 219, inspector counter `retried`). A stop during the outage naks the message.
* A handler fault sends a negative acknowledgement and the server redelivers after `NakDelay`. NATS can redeliver inside the session, so the host keeps running ([D-37](porting/decisions.md#d-37)). After `MaxDeliver` failed deliveries the message is terminated, logged and counted (`microdcs.nats.poisoned`). The failed attempt releases its dedupe claim, so the redelivery is processed at once.
* After a connection loss a message that the server handed to the pull request of the lost connection is redelivered only after `AckWait`, so recovery can take up to that long.
* Each consumer pulls one message at a time. A message that waits for a worker or is being processed gets an "in progress" acknowledgement every `AckWait / 3`, so a slow worker does not cause redeliveries. A consumer that the server deleted (for example after `ConsumerInactiveThreshold`) is created again, also when it was deleted while no pull request was open (between pulls or while disconnected): the server then answers no pull at all, and the transport takes the client's "no responders" or missed-heartbeat notification as the sign (warning `ConsumerNotAnswering`, event 218).
* Credentials are read from their files on every connect, so rotated secrets are used after the next reconnect. A credential the server rejects does not end the attempts ([D-49](porting/decisions.md#d-49)).
* A `Mdcs-Reply-To` from a peer is checked before the reply is published there ([D-48](porting/decisions.md#d-48)): `$` subjects (`$JS.API`, `$KV`), wildcards and subjects below a binding prefix of this instance are rejected, and the message is terminated.
* Responses are plain messages and are not stored. A response that arrives while the instance is gone is lost; the SFC re-dispatches the command ([D-9](porting/decisions.md#d-9)).
* Publishing to a stream waits for the server's acknowledgement, and waits and retries while the connection is down; an attempt that saw a reconnect is repeated even if the client is already back (`Nats-Msg-Id` keeps it idempotent).
* `Retain` on an event is ignored with a warning (NATS has no per-message retain); use state topics.

## State topics

`IStateTopicPort.PublishAsync` writes the value to the key-value bucket with the time to live as `Nats-TTL`; the server removes the value when the time ends. `DeleteAsync` deletes the key. A reader gets the last value with `GetEntryAsync` or a watch. The job order publisher (S18) uses this port.

## Tenancy

One NATS account per tenant. The credentials of the connection (`CredsFile`, `NKeyFile`, user and password, token) select the account; subjects, streams and the bucket are separate per account, so two tenants can use the same prefix. The server configuration is a deployment matter (see the server's account documentation).

## Observability

The NATS client has its own `ActivitySource` and `Meter` named `NATS.Net`. Add both to OpenTelemetry (`AddSource("NATS.Net")`, `AddMeter("NATS.Net")`). The transport adds the counters `microdcs.nats.dropped`, `microdcs.nats.reconnects` and `microdcs.nats.poisoned` on the `MicroDCS` meter.

The inspector `transport.nats` (MCP tool `inspect`) shows the server, connection name, state, counters (received, acknowledged, redelivered, poisoned, dropped, published, reconnects) and every consumer with its stream and filters. Credentials and payloads are not part of it.

## Local run

`docker compose up -d --wait nats` starts a server with JetStream. Then, for example:

```bash
APP_MicroDcs__Bindings__greetings__Transport=nats \
APP_MicroDcs__Bindings__greetings__Prefix=dev \
APP_MicroDcs__Bindings__greetings__ScopeLevels=2 \
APP_MicroDcs__Bindings__greetings__ReplyBase=dev-reply \
dotnet run --project src/MicroDCS.App -- serve
```

## Tests

Unit tests cover the renderer, the mapping, the options validator (also the NATS bindings) and the client option building. The compose tests (`NatsTransportTests`, service `nats` with three accounts) cover request/response, duplicates and acknowledgement, scope rejection, undecodable messages, the reply header and message id, the inbox, state topics with expiry, shared and unshared consumers, redelivery and poison messages, reconnect and account isolation. TLS against a server is part of the security review (S22).
