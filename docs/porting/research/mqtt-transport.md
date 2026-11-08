# MQTT transport (S13)

Findings and the Python parity list for `MicroDCS.Transport.Mqtt` ([D-46](../decisions.md#d-46)). Sources read on `python-archive`: `src/microdcs/mqtt.py` (all), `MQTTConfig` in `src/microdcs/__init__.py`, the unit test names of `tests/unit/test_mqtt.py` (148 tests) and the scenarios of `tests/integration/test_mqtt_integration.py`.

## Python parity list

| Behaviour (file, function) | Disposition |
|---|---|
| `create_mqtt_client`: persistent session (`clean_start=False`), session expiry never | **changed**: persistent session kept; `SessionExpiry` defaults to 1 hour. A client id derived from a generated instance id is new after every restart, so a session that never expires leaks and keeps queueing shared-group messages (D-46) |
| `create_mqtt_client`: K8S-SAT token read when the file exists | kept (`SatTokenFile`, enhanced authentication method `K8S-SAT`) |
| `create_mqtt_client`: CA file turns TLS on, `require_tls`, client certificate and key, the three error cases | kept (validated at start, D-27); unit tests in `MqttOptionsValidatorTests` and `MqttClientOptionsFactoryTests` |
| `create_mqtt_client`: warning when credentials go out without TLS | kept (`CredentialsWithoutTls`) |
| `MQTTConfig.resolve_password` (file or value) | kept; one trailing line break is removed; both set is an error |
| `identifier` default `app_client`, `-proc` and `-pub` suffixes (two connections) | **changed**: one connection per instance, client id `microdcs-{instance id}` unless configured |
| `_compile_topic_pattern` / `_topic_matches` (`+`, `#`, `$share/` stripped) | kept for `+` and the share prefix (`MqttAddressRenderer.Matches`); `#` is never subscribed, so it is not supported |
| `_publish_message`: QoS 0 unless a response topic is set (then QoS 1) | **changed**: QoS 1 for everything by default (`PublishQos`, 0 allowed). The profile is `AtLeastOnce` (D-37) and subscriptions are QoS 1; Python subscribed at QoS 0, so its `puback` calls never had a packet id |
| `_publish_message`: message expiry from `expiryinterval`, content type, response topic, retain, all attributes as user properties | kept (`MqttMessageMapper`); expiry rounds up to whole seconds, at least 1 |
| `_publish_message`: correlation data = UUID bytes of `causationid` or `id` | **changed**: UTF-8 bytes of `causationid` or `id` (ids are not UUIDs, A-6); set on commands and on responses |
| `_publish_message`: error log and return when no topic | **changed**: an outbound message without address and reply address throws (programming error); the pipeline no longer sends one (see findings) |
| `_publish_message`: schedules the expiry task | moved to the pipeline (D-37, keyed by command id) |
| `_claim_message`: wait for the lease holder | in the pipeline (T-20), unchanged |
| `_cloudevent_from_message`: user properties to attributes, rest to `custommetadata`, content type, message expiry | kept: the codec decodes attributes, unknown ones become `Extensions` (E-3); the remaining broker expiry fills `ExpiryInterval` when the sender set none |
| `_cloudevent_from_message`: correlation data parsed as UUID (`..._invalid_correlation_data` test) | **dropped**: correlation data is not read, the `causationid` attribute carries the relation |
| `_cloudevent_from_message`: transport metadata (`mqtt_topic`, `mqtt_qos`, `mqtt_retain`, `mqtt_message_id`) | **changed**: typed `LogicalAddress`, `ReplyAddress`, `Redelivered` (A-2) |
| `_process_message`: duplicate is acknowledged | kept; verified end to end by `A_duplicate_is_processed_once_and_every_delivery_is_acknowledged` |
| `_process_message`: `mark_done` before `puback` | kept (pipeline marks, transport acknowledges after `HandleAsync`) |
| `_process_message`: one message to every matching binding, response and request subscriptions | kept (`MqttTransport.ProcessAsync`; an unacknowledged fault in any binding leaves the message unacknowledged) |
| `subject_matches_topic` (`enforce_subject_topic_match`) | **changed**: `ScopePolicy` in the pipeline (A-7); the transport parses the topic and drops a topic with an invalid scope segment so it cannot skip the check |
| `_process_messages`: `message_workers` workers, shielded processing | kept (`MicroDcs:MessageWorkers`, bounded inbox, workers finish in-flight messages on stop) |
| `_outgoing_message_publisher`: retry while the connection is down | kept (`PublishWhenConnectedAsync` waits for the reconnect) |
| `task()`: subscribe every binding topic and the response topic, Redis ping first | kept (subscribe at QoS 1; Valkey is the Valkey layer's start, D-27) |
| `task()`: graceful shutdown (unsubscribe, workers, drain, publishers, expiry) | kept as the host's shutdown phases (D-37); unsubscribe failure is logged and the shutdown goes on |
| `OTELInstrumentedMQTTHandler` | **dropped** (D-37 revision: one `ActivitySource`) |
| `MQTTProtocolBinding`: topics from prefix, wildcard levels, discriminator, `$share/` | kept (`MqttAddressRenderer`, D-38) |
| `MQTTProtocolBinding`: `response_topic` = base + `/` + instance id; subscribed unshared | kept (`ReplyBase`, D-9); a binding that publishes commands without `ReplyBase` fails at start |
| `mqtt_path_from_subject` option | **dropped** (T-24: the address always comes from the scope of the subject) |
| `enrich_publish_transportmetadata`: no publish intent falls back to the bare prefix | **changed** (T-23): publishing an undeclared intent is an error in the pipeline |
| `enrich_response_transportmetadata`: answer on the request's response topic, warn when none | kept; without a reply address the answer goes to the `events` address (D-37); a reply to a *response* has neither and is logged and dropped (fix in this session) |
| `MQTTPublisher.publish_retained`: QoS 1, retain, message expiry | kept (`IStateTopicPort.PublishAsync`) |
| `MQTTPublisher.delete_retained`: zero-byte retained message | kept (`DeleteAsync`) |
| `MQTTPublisher`: own connection with reconnect | **changed**: shares the transport's connection |
| `MQTTConfig`: `dedupe_ttl_seconds`, `dedupe_lease_seconds`, `message_workers`, `binding_outgoing_queue_size` | moved to `MicroDcs:` (core) and the binding section |
| `TestQoS`, `TestCloudEvent` (to user properties), `TestTopicDiscriminator` (config parsing) | covered by renderer, mapper and options tests; configuration parsing is `BindingOptions` (D-39) |
| Reconnect tests (`reconnect=True`, persistent session) | kept; `The_transport_reconnects_after_the_broker_closes_the_connection` takes the connection away with a session takeover |
| Integration tests (greetings raw/hello/additional fields, job orders, retained publish/delete) | greetings and retained scenarios ported (`MqttTransportTests`); the job order scenarios belong to S17/S18 |

No row is open.

## Findings

* **MQTTnet 5** has no managed client: the transport runs its own reconnect loop (delay doubles from `ReconnectMinDelay` to `ReconnectMaxDelay`, on the `TimeProvider`). Manual acknowledgement (`AutoAcknowledge = false` and `AcknowledgeAsync` later) works as measured in S04. `MqttClientOptions.AuthenticationMethod`/`AuthenticationData` are set after `Build()`; the builder has no method for them. User property values are bytes (the string overload is obsolete); `ReadValueAsString` lives in `MQTTnet.Packets`. The payload is a `ReadOnlySequence<byte>`.
* **A pipeline bug found by the real transport:** a reply to a *response* message (a response handler or the fallback that returns an event) has no `ReplyTo` and no address, and the in-memory transports accepted it. The MQTT transport threw, the pipeline counted a handler fault and stopped the host. The pipeline now logs and drops such a reply (Python did the same: "No topic specified", test `test_publish_message_no_topic`), with a regression test.
* **Duplicate while the first is running:** the pipeline waits for the lease holder for the remaining lease (T-20, as in Python), so with several workers a QoS 1 redelivery of a message that is still being processed is held up to a full `DedupeLease` (30 s by default) after the first one finished. Correct, but slow. Not changed here; a poll cap (re-claim every second) is a candidate for S19.
* **Session takeover** (a second connection with the same client id) is a reliable way to drop the connection in a test without access to the broker; the transport reconnects and the old instance takes the session back, so the intruder must disconnect at once.
* **AOT:** MQTTnet publishes without trim or AOT warnings in the App; the AOT binary on Mosquitto answered a hello with the two greetings.

## Not covered by automated tests

TLS against a real broker and the SAT token against a broker that checks it (the option building is unit tested; Mosquitto in the harness runs without TLS and authentication). Broker-side TLS and ACL belong to S22 (security review).
