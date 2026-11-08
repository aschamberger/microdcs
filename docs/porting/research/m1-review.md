# M1 review findings (2026-11-08)

Three reviews of `main` at `a44b649` after M1: two cloud reviews scoped through review-only PRs (#62 Core and Valkey, #63 MQTT and NATS, both closed without merge) and a local security review of Inspection, MCP and Maintenance. All findings were triaged against the decisions; none conflicts with an intentional design (CLAUDE.md list). They are fixed in one follow-up PR (`m1-review-fixes`, size rule lifted for this PR by the maintainer).

Dispositions: **fix** (in this PR, with a regression test), **decision** (fix plus a decision or revision).

## Core and Valkey (cloud review, PR #62)

| Id | Finding | Disposition | Fix and test |
|---|---|---|---|
| RV-1 | Transports deliver before `StartPublishing()`: a handler that publishes in that window faults; with a persistent MQTT session this is a crash loop | fix | The outbound queues are created in the pipeline constructor, the pumps start before the transports. Test: a handler publishes from the first message delivered during `StartAsync` |
| RV-2 | Shutdown phase 2 lets the `OperationCanceledException` of the drain wait escape; phases 3 to 6 never run | fix | Every phase catches the cancellation of the grace period and the next phase runs with the cancelled token (its "grace ended" path). Same for a transport's `StopReceivingAsync` that rethrows. Test: in-flight handler past the grace period, shutdown hooks still run |
| RV-3 | Unfiltered `catch (OperationCanceledException)` in the outbound pump and in `DispatchAsync`: a timeout inside a handler or transport silently ends the pump or escapes the outcome mapping | fix | Filters on the own token; a foreign cancellation is a fault (`Faulted` outcome, pump fault ends the host, D-27). Also the expiry handler. Tests: handler throws `TaskCanceledException`, publish throws `OperationCanceledException` |
| RV-4 | Valkey `LeaseRemainingAsync` returns the TTL of the `done` marker: a duplicate sleeps up to the retention (10 min) | fix | One script returns 0 for `done` or a missing key. Test in the Valkey integration tests |
| RV-5 | `InMemoryDedupeStore` never evicts | fix | Expired entries are removed on access, with a sweep at most once per lease time. Test: entry count drops after the retention |
| RV-6 | Expiry timers keyed by event id, but one envelope goes to every binding that declares the intent: the second binding replaces the first timer | fix | Timers keyed by (binding, id); a response cancels the timer of its own binding. Test: two command bindings, both expiry handlers run |
| RV-7 | Partial publish when one of several binding queues is full; the retry duplicates on the healthy bindings | decision (D-28 revised) | All or nothing: capacity of every target is checked under one lock before anything is enqueued. Test: two bindings, one full, nothing enqueued |
| RV-8 | `MessageExpiry` (or a received `ExpiryInterval`) above about 49.7 days makes `Task.Delay` throw in the pump | fix | Validator rejects `MessageExpiry` above the timer limit; `ExpiryTracker.Start` clamps. Tests: validator, large interval |
| RV-9 | `ProcessorRegistry` silently overwrites a second payload descriptor for the same CLR type (two `BinaryPayloadDescriptor`s) | fix | A CLR type with descriptors of different type ids is ambiguous: building an outgoing envelope from it fails with a message that names the types; incoming dispatch is unchanged. Test |
| RV-10 | `ExpiryTracker.Cancel` cancels outside the lock; races with the disposal in `RunAsync` | fix | Cancel inside the lock, as `Start` does |

## MQTT and NATS (cloud review, PR #63)

| Id | Finding | Disposition | Fix and test |
|---|---|---|---|
| RV-11 | MQTT: subscribe fails after a successful reconnect; the loop calls `ConnectAsync` on a connected client forever | fix | A failed subscribe disconnects before the next attempt |
| RV-12 | MQTT: lost wakeup when the connection drops between the reconnect and the reset of `_reconnecting` | fix | After the reset the loop checks the connection and starts again; a stale "up" signal is replaced. Compose test: repeated session takeover |
| RV-13 | Workers of both transports end on any `OperationCanceledException` | fix | Workers only stop with the inbox; the pipeline maps foreign cancellations to `Fault` (RV-3) and the worker catch no longer excludes them |
| RV-14 | NATS: consume recovery only calls `GetConsumerAsync`; a deleted consumer is never recreated | fix | Recovery calls `CreateOrUpdateConsumerAsync` with the start configuration. Compose test: consumer deleted on the server, the binding receives again |
| RV-15 | NATS: publish retry decided by `IsConnected` at catch time; a lost PubAck in a short blip stops the host | fix | Retry when the attempt saw a disconnect or reconnect (epoch counter of opened connections); `Nats-Msg-Id` keeps it idempotent |
| RV-16 | MQTT: `PublishWhenConnectedAsync` retries without limit; a message that makes the broker disconnect cycles forever | fix | At most `MqttTransport.MaxPublishAttempts` (5) sent attempts per message, then the exception reaches the pump (D-27). Waiting for a reconnect does not count |
| RV-17 | Secret files (SAT token, password and token files) are read once; rotated secrets break reconnects | decision (D-49) | MQTT rebuilds the client options on every reconnect attempt; NATS reads its secret files in the auth callback on every connect and sets `IgnoreAuthErrorAbort` (found by the new test: the client stopped after two rejected attempts). The same compose test on both transports, checked to fail with a cached credential |
| RV-18 | The reply address from the peer (`ResponseTopic`, `Mdcs-Reply-To`) is used unchecked: confused deputy towards `$JS.API`, `$KV` or retained state topics | decision (D-48) | `ReplyAddressPolicy` in Core, applied by both transports when a message arrives: no `$`, no wildcards, not under a binding prefix of this instance (own reply bases allowed), optional `AllowedReplyPrefixes` per binding. A message with a rejected reply address is dropped before the pipeline with a security log line. Unit and compose tests |
| RV-19 | NATS: with a shared group, the last instance to start overwrites filters, `AckWait` and `MaxDeliver` of the shared consumer; old pods `AckTerminate` new types during a rollout | decision (D-47 revised) | The shared consumer name carries a hash of filters, `AckWait` and `MaxDeliver`; instances with a different configuration get their own consumer, the old one ends through `InactiveThreshold` |
| RV-20 | NATS: pulled messages wait in local buffers while `AckWait` runs; early redeliveries, early poison | fix | `MaxMsgs` 1 per consumer, and every delivery that is queued or in flight gets `AckProgress` every `AckWait / 3` until it is settled. Residual: one prefetched message per consumer is not tracked; the poison rule still uses `NumDelivered` |

## Inspection, MCP and Maintenance (local security review)

| Id | Finding | Disposition | Fix and test |
|---|---|---|---|
| RV-21 | Delete apply checks only hash document versions; index sets, streams, strings and registry members can change between the digest recheck and the script | fix | The script compares a fingerprint per target (type and length; stream last id; field or member presence and value) taken from the plan images. Compose test: concurrent index write between plan and apply |
| RV-22 | Restore has no compare step; writes after the recheck are overwritten without a snapshot | fix | The restore transaction carries conditions per target (version, absence, length or value). Compose test |
| RV-23 | The dev MCP start command in `CLAUDE.md` fails (no `CloudEventSource`, no binding) | fix | Command with `CloudEventSource` and an in-memory `greetings` binding |
| RV-24 | `/mcp` does not check the `Origin` header (MCP spec requirement; covered today by the host and content-type checks) | fix | On loopback, a request with an `Origin` that is not a loopback origin gets 403. Unit test |

Tests that are not regression tests in the strict sense: RV-10 is a narrow race without a deterministic test (the fix moves the cancel into the lock, as `Start` does); RV-11, RV-12 and RV-16 need a broker that refuses a subscribe or disconnects on a publish, which Mosquitto in the harness cannot do on demand, so they are covered by review and the existing reconnect tests; RV-15 needs a lost PubAck.

Hardening for S21/S22 (not changed here): plain-HTTP bearer token on a non-loopback listener, no authentication on loopback in a shared pod network namespace, snapshot file mode.

## Dedupe wait (L-7, T-20)

Fixed here instead of S19, together with RV-4: a faulted message releases its own claim (compare-and-delete on the token) so the redelivery can claim at once, and a waiting duplicate polls at most every second instead of sleeping the whole remaining lease.
