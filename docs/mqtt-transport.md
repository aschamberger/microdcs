# MQTT transport

This page describes `MicroDCS.Transport.Mqtt` ([D-46](porting/decisions.md#d-46)), the MQTT 5 binding on MQTTnet. It replaces `mqtt.py` of the Python version. Topics and payloads are a clean break ([D-2](porting/decisions.md#d-2)).

## Registration

```csharp
services.AddMicroDcs(configuration);
services.AddMqttTransport(configuration);   // after AddMicroDcs
services.AddProcessor<MyProcessor>();
```

A binding selects the transport with `MicroDcs:Bindings:{id}:Transport = mqtt`. `AddMqttTransport` registers the transport as `ITransport` and as `IStateTopicPort`, the `transport.mqtt` inspector and the options. A broker that cannot be reached at start ends the process ([D-27](porting/decisions.md#d-27)); later connection losses are retried.

## Configuration

Section `MicroDcs:Mqtt`; environment names use `APP_` and `__`, for example `APP_MicroDcs__Mqtt__Hostname`.

Every field is in the [configuration reference](operations.md#config-mqtt).

Binding fields used by this transport (see [Processors in .NET](processors-dotnet.md)): `Prefix`, `ScopeLevels`, `Discriminator`, `SharedGroup` and `ReplyBase` (required when the processor publishes commands). Message workers, dedupe lease and queue sizes are core options (`MicroDcs:MessageWorkers`, `DedupeLease`, `OutgoingQueueMaxSize`).

Credentials over a connection without TLS are sent, with a warning in the log.

## Topics

| Purpose | Topic |
|---|---|
| Message | `{Prefix}/{scope…}/{Discriminator}/{data｜events｜commands｜metadata}` (scope and discriminator are optional) |
| Subscription | the same with `+` for every scope level from 0 to `ScopeLevels`; with `SharedGroup` as `$share/{group}/…` |
| Reply (per instance) | `{ReplyBase}/{instance id}`, never shared |
| State topic | `{address}/{name}` with retain |

Segments use only `A-Z a-z 0-9 _ -` ([D-38](porting/decisions.md#d-38)), so no escaping exists and a segment cannot contain a wildcard.

## Message mapping

The envelope travels in binary content mode: attributes are MQTT user properties, `datacontenttype` is the content type, the data is the payload.

| MQTT 5 property | Used for |
|---|---|
| User properties | CloudEvent attributes and extension attributes. The first value of a repeated name wins |
| Content type | `datacontenttype` |
| Message expiry interval | `expiryinterval`, in whole seconds rounded up. On receipt the remaining broker expiry fills `expiryinterval` when the sender set none |
| Response topic | The reply address of a command (this instance's reply topic) |
| Correlation data | UTF-8 id of the command, or `causationid` of a response. Informational |
| Retain | State topics and events published with `Retain` |

## Delivery

* Subscriptions and publishes use QoS 1 and a persistent session. A message is acknowledged after the pipeline finished it (dedupe `mark done` first), so a crash in between means redelivery and the dedupe claim answers the duplicate.
* A message that cannot be decoded, or that matches no subscription, is dropped, counted and acknowledged. A duplicate is acknowledged.
* A handler fault leaves the message unacknowledged and ends the host, because MQTT cannot redeliver inside a session ([D-37](porting/decisions.md#d-37)). The session redelivers after the restart.
* Publishing waits while the transport reconnects and sends again afterwards. A message is sent at most 5 times; a message that makes the broker drop the connection every time (an ACL with disconnect, a packet above the broker's limit) then fails, and the host ends ([D-27](porting/decisions.md#d-27)). Waiting for the reconnect does not count as an attempt.
* Every reconnect attempt reads the password file and the SAT token again, so rotated secrets are used; a credential the broker rejects does not end the attempts ([D-49](porting/decisions.md#d-49)). A failed subscribe after a reconnect closes the connection and tries again.
* A response topic from a peer is checked before the reply is published there ([D-48](porting/decisions.md#d-48)): `$` topics, wildcards and topics below a binding prefix of this instance are rejected, and the message is dropped.

## State topics

`IStateTopicPort.PublishAsync` publishes a retained message with the time to live as message expiry; `DeleteAsync` publishes a zero-byte retained message. The job order publisher (S18) uses this port.

## Inspection

The inspector `transport.mqtt` (MCP tool `inspect`) shows the broker, client id, connection state, counters (received, acknowledged, dropped, published, reconnects), the last disconnect reason and every subscription with its binding. Credentials and payloads are not part of it.

## Tests

Unit tests cover the renderer, the mapping, the options validator and the client option building (credentials, SAT, TLS, client certificate). The compose tests (`MqttTransportTests`, Mosquitto) cover request/response, duplicates and acknowledgement, scope rejection, undecodable messages, the reply topic, state topics, shared subscriptions and reconnect. TLS against a broker and ACLs are part of the security review (S22).
