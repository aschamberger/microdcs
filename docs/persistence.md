# Persistence

Valkey is the system of record of an island: dedupe claims, job orders and responses, Work Masters and station lists,
SFC job state, work and change streams, leases. This page describes the data model. The .NET API is on
[Valkey layer](valkey-layer.md); how to run Valkey in production is on [Operations](operations.md#valkey).

## Requirements

* **Plain Valkey.** MicroDCS uses hashes, sets, sorted sets, strings, streams, pub/sub and Lua scripts. It does not use
  valkey-json or valkey-search ([D-41](porting/decisions.md#d-41)), so managed offerings without modules work too. The
  compose files and tests use `valkey/valkey-bundle:9` ([D-26](porting/decisions.md#d-26)).
* **One endpoint, no cluster.** Scripts touch several keys without hash tags. Valkey cluster mode is out of scope; a
  replica with Sentinel needs no code change ([D-12](porting/decisions.md#d-12)).
* **No blocking commands.** Streams are read by polling, never with `BLOCK` ([D-7](porting/decisions.md#d-7)).
* **Durable storage.** Run with an append-only file on a persistent volume ([Operations](operations.md#valkey)).

## Keys

Every key starts with `MicroDcs:Valkey:KeyPrefix` (default `microdcs`), one prefix per island. All names come from one
`KeySchema` class, and a script receives every key it touches in `KEYS`. Key names are a clean break from the Python
implementation ([D-2](porting/decisions.md#d-2), [D-42](porting/decisions.md#d-42)).

| Key below `{prefix}:` | Type | Content |
|---|---|---|
| `dedupe:{hash}` | string | Dedupe claim of a received event. `{hash}` is the first 16 bytes of SHA-256 over `source:id`. |
| `lock:poststart:{processor}` | string | Lease of an `[OnStarted(Singleton = true)]` hook. |
| `joborder:{id}` | hash | Job order document. |
| `joborder:list:{scope}` | sorted set | Job orders of a scope by priority. |
| `jobresponse:{id}` | hash | Job response document. |
| `jobresponse:list:{scope}` | sorted set | Job responses of a scope by start time. |
| `jobresponse:byorder:{jobOrderId}` | string | Index: job response of a job order. |
| `jobresponse:bystate:{scope}:{state}` | set | Index: job responses in a state. |
| `workmaster:{id}` | hash | Work Master document, with the recipe when it has one. |
| `workmaster:list:{scope}` | set | Work Masters of a scope. |
| `ref:{kind}:{scope}` | set | Station lists: `equipment`, `materialclass`, `materialdefinition`, `personnel`, `physicalasset`. |
| `jobacceptance:{scope}` | string | Order limit of a scope, set by the MES. |
| `scopes` | set | Every scope that has stored a document. The publisher and the SFC host discover scopes here. |
| `changes:{scope}` | stream | Change entries of job orders and responses, read by the publisher. Trimmed to about `ChangeStreamMaxLength`. |
| `pubseq:{scope}` | string | `Sequence` of the scope's state index. |
| `publisher:cursors` | hash | Last change entry the publisher handled, per scope. |
| `publisher:lease` | string | Instance id of the active job order publisher. |
| `sfc:state:{jobId}` | hash | SFC state of a job. |
| `sfc:work:{scope}` | stream | SFC work and outbox entries; consumer group `sfc-engine`. |
| `sfc:dead:{scope}` | stream | Work entries that failed `MaxDeliveries` times, with the reason. |
| `sfc:active:{scope}` | set | Running SFC jobs. |
| `sfc:due:{scope}` | sorted set | Due-index: jobs by next deadline. |
| `sfc:waits:{scope}:{type}` | sorted set | Jobs waiting for a pull event type, by wait start. |
| `sfc:event:{scope}:{key}` | string | Marks a pull event as applied, so that a redelivery cannot complete a second job. |
| `sfc:heartbeat` | hash | Last sweep time per scope, for the outage rule. |
| `sfc:recipe:{hash}` | string | Recipe text by content hash; a running job is pinned to it. |
| `env` | string | Environment marker for the maintenance tools (dev and test only). |
| `nudge:{kind}` | pub/sub channel | Tells the pollers that a stream of a scope has new entries. |

A job, a job response and a Work Master are identified by their id alone, not by scope and id. An id belongs to one
scope: writing it under another scope fails, and a command from another scope gets `UnknownJobOrderId`.

## Documents

Job orders, job responses, Work Masters and SFC states are hashes:

| Field | Content |
|---|---|
| `v` | Version, raised by every write. |
| `scope` | The scope. |
| `op` | Operation id of the last write. |
| `doc` | The serialized document (OPC UA JSON for job documents, JSON for SFC states). |
| `score`, `state`, `order` | Index values where the type needs them. |

### Writes

Each document type has one Lua script that checks the version, stores the hash, updates the indexes, appends the change
entry and publishes a nudge, all in one atomic step. A write passes the version it read (`0` to create); a stale version
fails and the caller reads again. There are no locks.

After a connection error or a timeout the outcome of a write is unknown. The DAO then reads the hash: if `op` holds the
operation id of this write, the write happened; otherwise it runs the script again (`WriteAttempts`, `WriteRetryDelay`).
So every write path is idempotent under an unknown outcome.

### Indexes

Indexes (`list`, `byorder`, `bystate`, `scopes`) are derived data, written by the same scripts.
`ValkeyIndexMaintenance` can compare them with a scan of the documents and rebuild them, as a maintenance task.

## Streams

| Stream | Writer | Reader | Trimming |
|---|---|---|---|
| `changes:{scope}` | Job order and response scripts | Job order publisher, by cursor | `MAXLEN ~ ChangeStreamMaxLength`. A cursor below the oldest entry makes the publisher rebuild the scope. |
| `sfc:work:{scope}` | Job order script, SFC host, operator tools | SFC host, consumer group `sfc-engine` | Only acknowledged entries are removed (`XTRIM MINID` below the lowest pending id). |
| `sfc:dead:{scope}` | SFC host | Operators (`sfc_work_pending`) | None. |

Reading follows [D-43](porting/decisions.md#d-43): one poller per instance. A script publishes the scope on
`nudge:{kind}` and the poller reads that stream at once. Because pub/sub can lose messages, the poller also reads every
stream each `PollInterval` (1 s). A work entry that stays with a consumer longer than `AutoclaimIdle` is claimed by
another (`XAUTOCLAIM`). Entries deleted while pending are logged and counted.

## SFC state

`SfcStateDao.CompareAndSetAsync` is the one step of the SFC host: compare the version, store the state, append the work
entries, set or clear the due-index entry, update the active set and the waits, register the scope and nudge, all in one
script. Follow-up work is therefore never lost between "state stored" and "work queued", and a stale work entry finds a
newer version and changes nothing. The state document follows
[`schemas/sfc-state/v1.0.0.json`](https://aschamberger.github.io/microdcs/schemas/sfc-state/v1.0.0.json).

## Dedupe and leases

* **Dedupe claim.** Before a handler runs, the pipeline sets `dedupe:{hash}` with `SET NX` to a token of this worker
  for `DedupeLease` (30 s). A claim held by another worker is awaited. When the handler is done, the key becomes `done`
  for `DedupeRetention` (10 min), so a later delivery of the same event is a duplicate. A failed handler releases the
  claim on transports that redeliver at once (NATS) and leaves it to expire otherwise. The claim is used only on
  transports with at-least-once delivery ([D-37](porting/decisions.md#d-37)).
* **Start lease.** `lock:poststart:{processor}` is `SET NX` with `StartLeaseTtl`; it makes an
  `[OnStarted(Singleton = true)]` hook run on one replica.
* **Publisher lease.** `publisher:lease` holds the instance id of the active publisher with `LeaseTtl`, renewed at a
  third of it.

The dedupe key trusts the CloudEvent `source` that the sender set; who may publish with which source is a matter of
transport access control ([D-13](porting/decisions.md#d-13)).

## Inspection and maintenance

The inspector `valkey` and the MCP tools `valkey_keys`, `valkey_document`, `dedupe_lookup` and `stream_pending` read
keys through the key schema, without raw command access. The maintenance tools (`maint_*`, dev and test only) plan and
apply changes with a snapshot first ([Inspection and MCP](inspection.md)).
