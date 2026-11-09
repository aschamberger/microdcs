# Job order publisher

Implements [D-51](../decisions.md#d-51) in `src/MicroDCS.MachineryJobs/Publishing`. Sources read on `python-archive`: `publishers/machinery_jobs.py`, `mqtt.py` (`MQTTPublisher`), `tests/unit/test_job_order_publisher.py`, `tests/unit/test_publisher.py`, `tests/integration/test_mes_integration.py`, `docs/machinery-jobs-mes-publishing.md`, `docs/development.md`, `deploy/k8s.yaml`.

## Findings

* **A publish inside the poller would stall the control path.** `ValkeyPoller` calls its consumers one after the other, S19 adds the SFC work and the due-index sweep to the same loop, and an MQTT publish waits while the broker reconnects. `PollAsync` therefore only marks the scope; a separate task publishes.
* **A result stored before the job ended was never published in Python.** `_publish_job_result` ran on `ResultUpdate` only and returned when the job had not ended. The state index then said `has_result` with no result topic.
* **The retained order went stale.** Python stored order and state, and republished only on Store and Update.
* **Topics expired on idle orders.** Nothing refreshed a retained topic in Python, so an order without a change for 48 h lost its topic while it was still live.
* **The Generic Host started transports only for the processor role,** so a publisher-only instance had no connection. The transports now connect in every role and subscribe only under `Processor`.
* **The port registered last won.** With two transports configured, `IStateTopicPort` resolved to whichever `Add…Transport` ran last. The publisher now takes the transport the binding names.
* **Sequence and resync:** the MES compares `Sequence` with its last value (`docs/machinery-jobs-mes-publishing.md`). A refresh or a new lease holder raises it with nothing changed; the MES then reads the topics once for nothing, which is harmless.

## Python parity

| Behaviour | Python | Disposition |
|---|---|---|
| Retained `state-index`, `order/{id}`, `result/{id}` per scope | `_publish_*` | changed (D-51): behind `IStateTopicPort`; topics carry the intent segment, `prefix/site/area/data/...`; payloads are OPC UA JSON (D-2) |
| Topic TTL 48 h | `PublisherConfig.retained_ttl_seconds` | kept: `RetainedTtl` |
| Order topic on Store, StoreAndStart, Update | `_dispatch` | kept |
| Order payload with state | `job.to_jsonb()` | changed (D-51): the job order alone |
| Result only for Ended or Aborted, with a response | `_publish_job_result`, `CLEARABLE_STATE_PREFIXES` | kept for the rule; changed (D-51): checked on every change of the job, not on `ResultUpdate` only |
| Zero-byte delete of both topics on Clear | `_delete_job_topics` | changed (D-50): Cancel as well |
| Every change updates the state index | `_dispatch` | changed (D-51): once per batch |
| State index entry: id, state, `has_result` | `_publish_state_index` | changed (D-51): `HasResult` is true only when the result topic exists, so not for a running job. It reads the by-order index without decoding the response |
| `EndState` jobs left out | `_publish_state_index` | kept: `Removed`; D-50 deletes the document, so the case only guards |
| Sequence from `INCR pubseq:{scope}`, survives restarts | `_publish_state_index` | kept |
| Missing job is skipped with a warning | `_publish_job_order` | kept: skipped, the removal entry follows |
| Cursor hash per scope, saved after the batch | `_save_cursors` | changed: read on every round so a reset works without a restart; the cursor still moves after the topics are written |
| `_global` stream for scope discovery | `_run` | changed (D-42): scope registry and poller |
| Blocking `XREAD` with read count 50 | `_run` | changed (D-7, D-43): `XRANGE` after the cursor, `ReadCount` 50 |
| Initial state index for all scopes at start | `_run` | changed (D-51): the whole scope is rebuilt on the first visit under a lease |
| No detection of trimmed entries | `_run` | changed (D-51): a gap or a missing cursor rebuilds the scope |
| Own MQTT client id for the publisher | `MQTTPublisher` | changed (D-46): one connection per instance; a publisher-only instance has its own |
| Reconnect loop | `MQTTPublisher.task` | kept in the transport (T-16) |
| `is_publisher_instance` flag; no leader election | `core.py`, `docs/machinery-jobs-mes-publishing.md` | changed (D-51): `Roles`, plus a lease as a guard |
| Topic prefix missing raises at start | `__init__` | kept (D-27): the binding is required, a transport without state topics ends the start |
| RESP2 and RESP3 reply shapes, byte decoding | `_run`, `_load_cursors` | dropped: handled by the client library |
| `isinstance(publisher, MQTTPublisher)` | `test_is_instance_of_mqtt_publisher` | dropped: D-10, the publisher depends on the port |
| `StateIndex` msgpack round trip | `test_publisher.py` | dropped: MessagePack is gone (driver 4) |
