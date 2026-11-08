# Valkey layer in .NET

This page describes `MicroDCS.Valkey` (decisions [D-41](porting/decisions.md#d-41) to [D-43](porting/decisions.md#d-43)). It replaces the Redis layer of the Python version. The key names and the document format are a clean break ([D-2](porting/decisions.md#d-2)).

## Registration

```csharp
services.AddMicroDcs(configuration);
services.AddMicroDcsValkey(configuration);   // either order
```

`AddMicroDcsValkey` binds `MicroDcs:Valkey`, connects at host start (a failure ends the process, [D-27](porting/decisions.md#d-27)), registers the DAOs, the streams helper, the poller and the health check, and replaces the in-memory `IDedupeStore` and `IStartLease` with the Valkey ones. Lease and retention come from `MicroDcs:DedupeLease` and `MicroDcs:DedupeRetention`.

## Configuration

Section `MicroDcs:Valkey`; environment names use `APP_` and `__`, for example `APP_MicroDcs__Valkey__KeyPrefix`.

| Name | Type | Default | Description |
|---|---|---|---|
| `ConnectionString` | string | `localhost:6379` | StackExchange.Redis connection string. May contain the password; never logged |
| `KeyPrefix` | string | `microdcs` | Prefix of every key. One per island. No `*?[]\` and no trailing `:` |
| `PollInterval` | duration | 1 s | Fallback read of all streams when no nudge arrives |
| `ScopeRefreshInterval` | duration | 5 s | Read of the scope registry |
| `WriteAttempts` | int | 4 | Attempts of a write whose outcome is unknown after a connection error |
| `WriteRetryDelay` | duration | 100 ms | Pause before the write is checked and repeated |
| `ChangeStreamMaxLength` | int | 5000 | Approximate length of the change streams (not of work streams) |

## Documents

Job orders, job responses, work masters and SFC states are hashes with the fields `v` (version), `scope`, `op` (operation id of the last write), `doc` (the serialized document) and, where needed, `score`, `state` and `order`. The DAOs store the text; the caller's codec produces it.

* A write is one Lua script per document type. It checks the version, stores the hash, maintains the indexes, appends the change entry and publishes a nudge, atomically.
* `expectedVersion` is `0` to create, the read version to update, `null` to overwrite. A stale version returns `WriteResult(false, currentVersion)`.
* A document id belongs to one scope. Writing it under another scope throws.
* Reads: `GetAsync`, `ListAsync` (by rank, with paging), `GetByJobOrderAsync` and `IdsByStateAsync` for job responses.

### Unknown outcome

After a connection error or timeout, a write may or may not have happened. The DAO reads the hash: if `op` equals the operation id of this write, it counts as done; otherwise the script runs again. Pass an `operation` to make a retry across calls recognisable. The counters `microdcs.valkey.write.retries` and `microdcs.valkey.write.already_applied` show how often this happens.

## Dedupe and start lease

`ValkeyDedupeStore` keeps the Python semantics: `SET NX` with the lease time, the same token claims again, `done` stays for the retention time. The key hashes `source:id` with SHA-256 (16 bytes). `ValkeyStartLease` is `SET NX` with a lifetime.

## Streams and the poller

* `ValkeyStreams` creates groups, appends, reads without blocking, acknowledges and trims. Work and outbox streams have no length limit; `TrimAcknowledgedAsync` removes only entries below the lowest pending id. One group per stream is assumed.
* `AutoClaimAsync` logs and counts entries that were deleted while pending (`microdcs.valkey.stream.entries_lost`).
* `ValkeyPoller` is the one poller per instance. Scripts publish the scope on `{prefix}:nudge:{kind}`; the poller reads the named stream at once. It also reads every stream each `PollInterval`, because pub/sub can lose messages. It reads again at once after a read that returned entries. There are no blocking commands ([D-7](porting/decisions.md#d-7)).
* Implement `IStreamConsumer` to read a kind of stream and `IPollerTask` for periodic work such as the SFC sweep.

## SFC state

`SfcStateDao.CompareAndSetAsync` is the generic step of [D-44](porting/decisions.md#d-44): compare the version, store the state, append the work entries, update the due-index and the active set, register the scope and nudge, in one script. It also holds the sweep heartbeat and the recipe texts by content hash.

## Indexes

Indexes are derived data. `ValkeyIndexMaintenance.VerifyAsync` compares every index with a scan of the documents and returns the differences; `RebuildAsync` rewrites them. Both scan the keyspace: run them as maintenance, and rebuild while no writer is active.

## Health

`ValkeyHealthCheck` pings Valkey and reports the round trip time. Register it with `AddHealthChecks().AddCheck<ValkeyHealthCheck>("valkey")`.

## Tests

The integration tests run against Valkey in compose ([development](development.md)).

## Inspection

`ValkeyInspector` (inspector `valkey`) reads keys by kind and scope, documents, dedupe state and streams through the key schema for the dev MCP tools; it has no raw command access. See [Inspection and MCP](inspection.md).
