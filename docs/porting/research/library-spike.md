# Library spike (S04)

Background for [D-33](../decisions.md#d-33), Q-ARCH-1, Q-DATA-1 and Q-DATA-2. Run on 2026-11-07 on linux-x64 (.NET SDK 10.0.403, Native AOT), against `valkey-bundle` (digest as in `compose.test.yaml`), Mosquitto 2, NATS 2.15 with JetStream, `eclipse/zenoh:1.10.0` and ActiveMQ Artemis 2.42. One small AOT console project per library, then one combined project. The spike code was thrown away (`spikes/` is not on `main`); the results that matter are below.

Pass criterion: `dotnet publish -r linux-x64` with `PublishAot` and `IL2026`/`IL3050` as errors, `TrimmerSingleWarn` off so that every warning shows.

## Summary

| Library | Version | AOT publish | Runtime checks (AOT binary) |
|---|---|---|---|
| StackExchange.Redis | 3.4.31 | clean | Lua string scripts, Streams, `XAUTOCLAIM`, raw `JSON.*` and `FT.*`, reconnect |
| MQTTnet | 5.2.0.1603 | clean | v5 user properties, response topic, correlation, expiry, retained, shared subscription, manual ack |
| NATS.Net | 3.3.2 | clean | request/reply with headers, JetStream (dedupe, ack, redelivery), KV with CAS |
| Kestrel minimal API + `ModelContextProtocol.AspNetCore` | 2.4.0 | clean | `/healthz`, MCP `initialize`, `tools/list`, `tools/call` over HTTP |
| Zenoh-CS (`sanri/zenoh-csharp`) | 0.5.0 | clean (see caveat) | put/subscribe with attachment works; **get/queryable crashes the process** |
| AMQPNetLite | 2.5.5 | **13 warnings** (6 `IL2070`, 7 `IL3050`), all in `Amqp.Serialization` | send and receive with a `Data` body work when the warnings are not errors |
| All except AMQP in one project | | clean, 24.3 MB binary | `/healthz`, Redis connect, MQTT and NATS client creation, `Config.Default()` of Zenoh |

Single-project binary sizes (linux-x64, unstripped): Redis 9.0 MB, MQTTnet 7.5 MB, NATS 11.4 MB, Kestrel + MCP 17.3 MB, Zenoh-CS 2.1 MB.

## StackExchange.Redis (D-7, Q-DATA-1, Q-DATA-2)

* **Lua:** `ScriptEvaluateAsync(string, RedisKey[], RedisValue[])` works under AOT. A script that calls `TIME` and then writes (`HSET`) is accepted. `cjson.decode` is available in scripts. CAS on a version field works (stale write returns 0).
* **Streams:** `StreamReadGroupAsync`, `StreamAcknowledgeAsync` and `StreamDeleteAsync` as expected. `StreamAutoClaimAsync` returns the deleted ids typed (`DeletedIds`): with 5 pending entries and 2 deleted, it claimed 3 and reported 2 deleted. The raw `XAUTOCLAIM` reply has 3 elements (next id, entries, deleted ids).
* **Stream entry id as event time (D-18):** the id's millisecond part equals server `TIME` within 1 ms (`1794073650438-0` against `1794073650439`). The proposal holds.
* **Polling versus nudge (D-7):** 100 messages, random 20 to 60 ms apart, one consumer on one stream, reads of up to 10 entries:

  | Strategy | p50 | p95 | max | server commands (100 messages) |
  |---|---|---|---|---|
  | poll 250 ms | 125 ms | 237 ms | 247 ms | 238 |
  | poll 100 ms | 51 ms | 96 ms | 106 ms | 284 |
  | poll 50 ms | 25 ms | 50 ms | 55 ms | 362 |
  | pub/sub nudge, 1000 ms fallback poll | 1.0 ms | 1.7 ms | 2.3 ms | 503 (includes 100 `PUBLISH`) |

  Latency of plain polling is about half the interval at the median and the interval at p95, so every hop of a step chain pays it. An idle poller costs one `XREADGROUP` per interval per consumer and stream (10 per second at 100 ms). The nudge removes the latency and keeps the poll as the fallback; its cost is one `PUBLISH` per `XADD` and a subscription per instance. Recommendation for S07: nudge plus a slow fallback poll (1 s or more), one poller component per instance.
* **Documents (Q-DATA-1):** 2000 documents, sequential round trips on localhost:

  | Operation | valkey-json + valkey-search (raw `Execute`) | hash `{v, doc}` + explicit sets (one Lua script) |
  |---|---|---|
  | write | `JSON.SET` 0.22 ms each | Lua put with index maintenance 0.14 ms each |
  | find by `JobOrderID` | `FT.SEARCH` 0.21 ms each | `GET` index key, then `HGET` 0.29 ms (two round trips) |
  | find by state | `FT.SEARCH @state:{running}` returned 667 of 2000 (correct) | `SMEMBERS` returned 667 |
  | read document | `JSON.GET` 0.12 ms | `HGET` |

  The flat-array `FT.SEARCH` reply (`[total, key, ...]`) parses without trouble through `ExecuteAsync`. A document was visible to `FT.SEARCH` immediately after `JSON.SET` (0 ms in this run; still poll with a timeout in tests). There is no performance reason to drop the modules. The remaining arguments are operational: a hard dependency on valkey-json and valkey-search (and their version drift, see [Valkey findings](valkey-findings.md)) against a plain image and explicit indexes that the same script must keep consistent.
* **Reconnect:** killing Valkey raises `ConnectionFailed`; the command in flight fails at once with `RedisConnectionException` (`SocketClosed`); commands issued during the outage wait in the backlog and fail with a timeout after about 6 seconds if the server is still down; `ConnectionRestored` fires once the server accepts connections again (not timed precisely), and later commands succeed with no reconnect code in the application. Commands that fail are not retried by the library, so every write path must be idempotent under unknown outcome (R-4, S09).
* Admin commands (`CONFIG RESETSTAT`) need `AllowAdmin = true`; the production configuration does not.

## MQTTnet 5.2

* All listed features work against Mosquitto 2: user properties (value is `ReadOnlyMemory<byte>`; the `string` overload is obsolete), response topic, correlation data, content type, message expiry (an expired message was not delivered to a returning persistent session; the delivered one showed the remaining interval), retained messages (delivered to a late subscriber, cleared by an empty retained publish), shared subscription `$share/g1/...` (20 messages split 10/10 over two clients).
* **Manual ack:** set `AutoAcknowledge = false` in the received-event arguments and call `AcknowledgeAsync` later.
* **Persistent sessions** need `WithSessionExpiryInterval`; with the interval at 0 the session ends at disconnect even if `CleanSession` is false.
* The payload is a `ReadOnlySequence<byte>`.

## NATS.Net 3.3

* Request/reply with headers, JetStream publish with `Nats-Msg-Id` (duplicate detected on the second publish), explicit ack with redelivery after `AckWait` (`NumDelivered` = 2), KV `PutAsync`/`UpdateAsync` with revision CAS (a stale revision raises `NatsKVWrongLastRevisionException`).
* Only `string` payloads were used. Custom types need a source-generated `JsonSerializerContext` registered through the serializer registry; not measured.

## Kestrel and MCP

* `WebApplication.CreateSlimBuilder`, `MapGet`, `MapMcp("/mcp")`. Tool registration with the generic `WithTools<T>(JsonSerializerOptions)` overload and a source-generated context published without warnings and the tool was listed and called. The server ran in stateless mode (no `Mcp-Session-Id` header).
* Not tested: the non-generic `WithTools(IEnumerable<McpServerTool>)` registration, resources and prompts, and structured output types. The D-19 rule "explicit registration" holds for the generic overload; S11 verifies richer tools.

## AMQPNetLite 2.5.5 (Q-ARCH-1)

* 13 warnings, all in `Amqp.Serialization` (`AmqpSerializer`, `MemberAccessor`, `MethodAccessor`, `AmqpContractResolver`): `MakeGenericType`, `DynamicMethod`, `GetMembers`, `GetInterfaces` and similar. They remain with a plain `Data` body, so the object serializer is reachable from the core codec and cannot be trimmed away.
* The AOT binary was built with the warnings downgraded and ran against Artemis: attach a receiver, send a message with properties and application properties, receive it and accept it. The path that avoids `AmqpSerializer` works; any use of the serializer would not.
* The library has no trimming annotations of its own. Upstream is the only place to fix this; until then the package must be isolated (D-33).

## Zenoh-CS 0.5.0 and zenoh-c 1.10.0

* **Native library:** the official release `zenoh-c-1.10.0-<target>-standalone.zip` exists for `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`. Its header defines `Z_FEATURE_UNSTABLE_API` and `Z_FEATURE_SHARED_MEMORY`, so no custom zenoh-c build is needed. `libzenohc.so` is 17.9 MB (x64) and 16.8 MB (arm64) and must sit next to the binary (`DllImport("zenohc")`). Not run on arm64 in this session.
* **Binding shape:** 617 `DllImport` declarations (80 `MarshalAs`), callbacks through delegates marshalled with `Marshal.GetFunctionPointerForDelegate`. The `netstandard2.0` assembly carries no trim or AOT annotations, so a clean publish says nothing about safety; only runtime checks do.
* **Works in the AOT binary:** `Session.Open` against `zenohd` (client mode), publisher `Put` and subscriber callback with an attachment (`ce_id=1;ce_type=t` arrived intact, encoding `zenoh/bytes`). Three runs of publish/subscribe in a row closed cleanly.
* **Fails:** `Session.Get` with a FIFO reply channel against a queryable crashes the process with `free(): invalid pointer` at `Session.Close()` or at exit, with the reply received correctly before. It also happens with the queryable not replying, without payload and without attachment, in AOT and in JIT (in JIT it happened earlier, which points at a corrupted heap, not at AOT). A queryable that is only declared and closed is fine.
* **Hypothesis, not verified:** the header of zenoh-c 1.10.0 ends `z_get_options_t` with `cancellation_token` (under `UNSTABLE_API`); the binding's `ZGetOptions` struct stops at `timeout_ms`. zenoh-c would write 8 bytes past the allocation made from `Marshal.SizeOf`. A patch could not be built in this session (the sandbox did not allow building the cloned third-party source), so neither the cause nor the fix is confirmed. Other structs may have drifted in the same way.
* **Consequence:** the spot-check of P/Invoke declarations against the headers that the session block asks for has to cover every struct the processors use (get/query options, query reply options, publisher and subscriber options), not only function signatures. This stays an input to the Zenoh decision before S25 (contribute a fix upstream, fork, or ZenohDotNet), which this session does not make.

Reproduction (client mode against `zenohd` on `tcp/localhost:7490`):

```csharp
var cfg = Config.FromStr("{mode:'client', connect:{endpoints:['tcp/localhost:7490']}}")!;
Session.Open(cfg, out var session);
session!.DeclareQueryable(Keyexpr.FromString("spike/cmd/*")!, new QueryableOptions(),
    q => q.Reply(q.GetKeyexpr(), ZBytes.FromString("reply"), new QueryReplyOptions()), out var qb);
session.Get(Keyexpr.FromString("spike/cmd/x")!, new GetOptions { Timeout = 3000 }, null, ChannelType.Fifo, 8, out var ch);
ch!.Recv(out var reply);
session.Close(); // free(): invalid pointer
```

## Not covered

* arm64 for any library (CI is the reference, D-32; zenoh-c arm64 is only checked as an existing release asset).
* ZenohDotNet (the fallback) was not tried; the Zenoh-CS result is mixed enough that S25 starts with it.
* Valkey pub/sub nudge under load with many scopes, `XAUTOCLAIM` under a real crash, Valkey cluster mode.
