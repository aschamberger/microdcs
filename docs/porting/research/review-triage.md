# review.md triage

`review.md` has two parts: the pull-event change plan and the open topics from the 2026-10-02 review. Dispositions: **Obsolete** (gone through the port or by construction), **Day-one** (design right in .NET), **Carry-over** (same requirement, new implementation), **Verify** (behaviour to re-test in .NET).

## Routing (architecture)

| Id | Finding | Disposition | Plan |
|---|---|---|---|
| R-1 | Pull-event routing matches only by scope + `type_id` (first waiting action of the first active job); no selector in the recipe schema. The callback contract (full `CloudEvent`) is already done | Day-one | Recipe model gets an explicit selector (`subject` or `correlationid`, as in the plan). Waits persisted in Valkey, claimed atomically by any replica. Ambiguous recipes rejected at validation time. The Python plan's file-by-file steps are not ported, only the design target and the acceptance criteria |
| R-2 | Push-command response routing is instance-affine; recovery time unbounded; `recovery_idle_threshold_s` unused | Carry-over (D-9) | Instance affinity is intended: the sending instance waits on its own reply address and its local expiry timer ends the wait; after the TTL a late response is stale anyway. `_pending_commands` stays. The only gap is a crash of the sending instance, which loses both the in-memory map and the timer, so the action stays `dispatched` until a restart. Fix: persist the dispatch deadline (`dispatched_at` + TTL) with the action state, and let the due-index sweep of D-18 on any instance re-dispatch actions whose deadline has passed (the equipment already has to tolerate re-delivery via `mdcsactionkey`). This replaces the unused `recovery_idle_threshold_s` with the real TTL |
| R-3 | Large files, `common.py` mixes model, processor ABC, decorators, protocol ABCs | Obsolete / Day-one | Project layout by responsibility: `Core` (CloudEvent, pipeline, processor), `Transports.*`, `Valkey`, `Sfc`, `Jobs`, `Models`. Avoid a monolith class like `redis.py` (DAOs per aggregate) |
| R-4 | Redis HA: single Redis with AOF, no Sentinel option, no reconnect, retried CAS may report `ALREADY_HANDLED` | Deferred (D-12) | HA is a deployment concern for later. Code-level requirement stays: StackExchange.Redis reconnects on its own, so every write path must be idempotent under unknown outcome (token-based Lua, outcome re-read) |
| R-5 | Git-pinned `aiomqtt` and forked OTEL instrumentation | Obsolete | Released NuGet packages only; tracing is written once in the pipeline (A-1). Add NuGet audit (`dotnet list package --vulnerable`, Dependabot) to CI |

## Security

| Id | Finding | Disposition | Plan |
|---|---|---|---|
| R-6 | Level-0 topics: scope check verifies nothing when no scope path | Day-one | Part of A-7: scoped clients must be rejected on addresses without a scope path, or the policy declares it must be enforced by ACLs. Decide the rule once for all transports |
| R-7 | Dedupe key is publisher-controlled (`source`+`id`) | Accepted (D-13) | Publisher identity is guaranteed by the transport (authn, ACLs), so the framework adds nothing. Document the trust model: dedupe is only as strong as the transport's access control |
| R-8 | MessagePack-RPC without per-method authorization | Obsolete | Transport dropped. **New:** the in-process HTTP surface (Q-OPS-1) needs authn/z from the start |
| R-9 | Kubernetes: `:latest`, no egress NetworkPolicy, no signing/SBOM; manifest has no probes | Carry-over | New image (chiseled/distroless AOT) pinned by digest, SBOM (produced in S03, attested and signed in S22), signing in CI, egress policy for Valkey/broker/OTLP. Probes become possible and expected with the HTTP surface |
| R-10 | Exception messages can leak values into logs | Carry-over | `LoggerMessage` source generator, log exception type/code, never `ex.Message` of application code at the default level; payload never logged |
| R-11 | No security audit trail (auth failures not recorded) | Day-one | Emit auth/authorization failures and scope-check drops as structured OTEL events/log category `security`. Transport support differs (MQTTnet only sees connect failures), document per transport |
| R-12 | `custommetadata` size limits: deliberately not an issue | Carry-over | Still true. Document the operational limits per transport (MQTT max packet size, NATS `max_payload`, AMQP max message size) and Kestrel request limits |

## Developer experience and platform

| Id | Finding | Disposition | Plan |
|---|---|---|---|
| R-13 | No generated API reference | Day-one | XML docs on public API plus a doc generator in CI |
| R-14 | `dataclassgen` flags, `Ping1` title collision | Obsolete | Replaced by the OPC UA model compiler/source generators (Q-ARCH-2, schema generation exploration). Re-evaluate collisions with the new generator |
| R-15 | No test helpers for processors | Day-one | `MicroDCS.Testing` package: in-memory transport, fake clock, processor harness. Containers come from compose (D-15); Testcontainers stays optional for consumers |
| R-16 | No health/metrics endpoint | Day-one | Q-OPS-1 |
| R-17 | Not released (no package, semver, changelog) | Day-one | NuGet packages, semver via MinVer or Nerdbank.GitVersioning, changelog |
| R-18 | No sizing/benchmarks | Day-one | BenchmarkDotNet suite for pipeline hot paths (driver 5 needs measurements) |
| R-19 | No synchronous job-state query API | Day-one | HTTP endpoint (driver 7) |
| R-20 | Multi-tenancy only by Redis `key_prefix` and ACLs | Verify | Tenancy per transport (NATS accounts, AMQP vhost/address prefix, Zenoh namespace *verify*) vs. a single `key_prefix` convention |
| R-21 | Support model, zones/conduits and HA reference architecture, security event log guidance | Carry-over | Documentation, independent of the language |

## Behaviours from "Done in the session" that the .NET version must reproduce

TLS plus `require_tls` for every transport, MQTT username/password and client certificates, password files, subject/topic scope check, payloads kept out of logs, Valkey timeouts, dedupe lease (crash mid-processing never loses a message, duplicates are acknowledged), stable `mdcsactionkey` on push commands. These become acceptance tests (Q-TEST-1/2).

## "Unverified in the session" items to cover with .NET integration tests

Kubernetes hardening settings applied on a real cluster; MQTT username/password against AIO's broker; broker-side proof of "no unacknowledged message" for the dedupe lease (Testcontainers with a broker that exposes it, or MQTTnet-level assertions).
