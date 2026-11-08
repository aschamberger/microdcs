# Data layer

Background for [D-41](../decisions.md#d-41), [D-42](../decisions.md#d-42) and [D-43](../decisions.md#d-43) (S07). Sources: `redis.py` and `sfc_engine.py` on `python-archive`, [library spike](library-spike.md), [Valkey findings](valkey-findings.md).

## Deployment assumption

Resilience comes from **islands**: a plant is split into several independent deployments, each with its own Valkey, not one large deployment per site (D-12, revised 2026-11-08). Valkey cluster mode is never used. Replication or Sentinel failover does not change key design. Hash tags and `(scope, id)` identity are therefore not needed.

## Documents and indexes: comparison (Q-DATA-1)

Python needs whole-document writes, two secondary-index queries on job responses (by `JobOrderID`, one result; by scope plus normalized state, limit 1000) and plain sets, sorted sets and streams for everything else. Partial JSON path updates appear only in the four SFC Lua scripts, and D-17 (whole versioned state, CAS on the version) removes them.

| Criterion | Chosen: hash `{v, scope, state, doc}` + explicit indexes | valkey-json + valkey-search |
|---|---|---|
| Speed (S04, 2000 docs) | write 0.14 ms, find by order id 0.29 ms, by state correct | write 0.22 ms, search 0.21 ms, by state correct |
| Atomic with change entry | document, index and entry in one script | document and entry atomic, search index asynchronous |
| Read-after-write | exact | not guaranteed (S04 saw 0 ms) |
| Dependencies | none beyond Valkey | two modules, version drift, managed services may lack them (unchecked) |
| Reply shape | plain types | `FT.SEARCH` flat array on Valkey, map on Redis: Valkey-specific parser |
| Code we own | index maintenance in three scripts, rebuild routine, consistency test | reply parser, index bootstrap, query escaping, `ft.*` ACL |
| New query later | new index in the script plus rebuild | drop and recreate the index |
| Ad hoc queries | no | yes |

Also found in the Python scripts: they build JSON by string concatenation (`'"' .. error_msg .. '"'`), which breaks on a quote in an error text, and read values with `string.match` on `JSON.GET` output. Scripts in .NET read hash fields and do not build JSON.

## Key schema

`{KeyPrefix}:` precedes every key (default `microdcs`; one prefix per island, tests use random prefixes).

| Key | Type | Content | Python key |
|---|---|---|---|
| `ce:dedupe:{hash}` | string | `processing:{token}` with lease TTL, or `done` with TTL | `cededupe:` |
| `lock:poststart:{configId}` | string | `1` with TTL (core contracts P-14) | `poststartlock:` |
| `joborder:{id}` | hash | `v`, `scope`, `op`, `doc` | `joborder:` (JSON) |
| `joborder:list:{scope}` | zset | score priority | same |
| `jobresponse:{id}` | hash | `v`, `scope`, `state`, `order`, `op`, `doc` | `jobresponse:` (JSON) |
| `jobresponse:list:{scope}` | zset | score start time | same |
| `jobresponse:byorder:{orderId}` | string | response id | FT TAG `JobOrderID` |
| `jobresponse:bystate:{scope}:{state}` | set | response ids | FT TAG `_scope` + `_normalized_state` |
| `workmaster:{id}` | hash | `v`, `scope`, `op`, `doc` | `workmaster:` (JSON) |
| `workmaster:list:{scope}` | set | ids | same |
| `ref:{kind}:{scope}` | set | ids; kind = `equipment`, `materialclass`, `personnel`, `physicalasset`, `materialdefinition` | five key families |
| `jobacceptance:{scope}` | string | integer | same |
| `scopes` | set | scope ids with at least one document | `active-scopes` |
| `changes:{scope}` | stream | change entries, `MAXLEN ~ 5000` | `joborder:changes:{scope}` |
| `pubseq:{scope}` | string | publish sequence | same |
| `publisher:cursors` | hash | last id per scope | `publisher:stream-cursors` |
| `sfc:state:{jobId}` | hash | `v`, `scope`, `op`, `doc` | `sfc:execution:` (JSON) |
| `sfc:work:{scope}` | stream | work and outbox entries, no `MAXLEN` | same |
| `sfc:active:{scope}` | set | job ids with running state | `sfc:activejobs:{scope}` |
| `sfc:due:{scope}` | zset | job id, score next deadline (D-18) | new |
| `sfc:heartbeat` | string | last sweep time (outage grace, D-18) | new |
| channel `nudge:{kind}` | pub/sub | message = scope | new (D-43) |

SFC key details are completed by the SFC rules below.

## Python parity list: `redis.py`

| Id | Python behaviour | File / function | Disposition |
|---|---|---|---|
| R-1 | Key prefix on every key, default `microdcs-test` | `RedisKeySchema`, `prefixed_key` | kept (default `microdcs`, D-42) |
| R-2 | Dedupe key is a Blake2b-16 hash of `source:id` | `cloudevent_dedupe_key` | changed: SHA-256 prefix, 16 bytes (D-42) |
| R-3 | Claim: `SET NX EX lease`, three tries; own token returns claimed; `done` returns done; other value returns busy | `CloudEventDedupeDAO.claim` | kept (S09) |
| R-4 | `mark_done`: `SET done EX ttl`; `lease_remaining_ms`: `PTTL` floored at 0 | `mark_done`, `lease_remaining_ms` | kept |
| R-5 | Transaction dedupe by scope and transaction id | `TransactionDedupeDAO` | dropped: unused (D-37 parity list) |
| R-6 | Post-start lock `SET NX EX 30` | `PostStartLockDAO.acquire` | kept (core contracts P-14) |
| R-7 | Named counters | `CounterDAO` | dropped: unused |
| R-8 | Job order save: JSON document, `ZADD` priority, change entry to scope and global stream, `SADD active-scopes`, one transaction | `JobOrderAndStateDAO.save` | changed: one script per document type, hash document (D-41), scope registry replaces the global stream (D-42) |
| R-9 | Job order retrieve checks `_dataschema`, returns none if absent | `retrieve` | changed: absence of the hash is "not found"; the schema URI travels in `doc` and is checked by the codec |
| R-10 | Delete removes document and list entry (one transaction); separate `remove_from_list` | `delete`, `remove_from_list` | kept (delete also removes index entries in the script) |
| R-11 | `list(scope)`: all ids, by priority / start time | `list` | kept (`ZRANGE`; paging added for the REST API in S21) |
| R-12 | Job response save with `_scope` and `_normalized_state` metadata inside the document, `ZADD` start time, change entry `ResultUpdate` | `JobResponseDAO.save` | changed: metadata as hash fields, indexes in the script |
| R-13 | `normalize_state`: state texts joined with `_`; empty means not indexed | `normalize_state` | kept |
| R-14 | `retrieve_by_job_order_id`: first match | same | changed: `GET jobresponse:byorder:` |
| R-15 | `retrieve_by_state(scope, state)`: up to 1000 matches; empty state returns empty | same | changed: `SSCAN` on the state set; the limit becomes a page size |
| R-16 | TAG values escaped for the query | `_escape_tag` | dropped: no query language |
| R-17 | `FT.CREATE` index bootstrap, idempotent | `JobResponseDAO.initialize` | dropped: no search index |
| R-18 | RESP3 versus RESP2 reply handling | `_resp3_get`, `_json_get` | dropped: RESP2 plain types |
| R-19 | Work master save: JSON document, `SADD` list; retrieve; delete; `is_member` | `WorkMasterDAO` | kept (hash document; the script also registers the scope) |
| R-20 | Five reference lists: add, remove, `is_member` | `EquipmentListDAO` and four more | changed: one generic `ref:{kind}:{scope}` DAO |
| R-21 | Job acceptance limit per scope: `SET`, `GET`, `DEL` | `JobAcceptanceConfigDAO` | kept |
| R-22 | Change streams written with `MAXLEN ~ 5000` | all `xadd` calls | kept for change streams; dropped for work and outbox streams (D-42) |
| R-23 | `active-scopes` is set on every job order save; the publisher reads it at start and discovers scopes from the global stream | `save`, publisher `_run` | changed: `scopes` registry, read at start and by the poller (D-42) |
| R-24 | Publisher cursor hash, `pubseq` counter | publisher `_load_cursors`, `_publish_state_index` | kept (S18) |
| R-25 | Publisher and engine loops: block, sleep after every iteration, create groups every iteration | publisher `_run`, engine `_run_loop` | changed: nudge plus fallback poll (D-43) |
| R-26 | `SfcExecutionDAO.save`: JSON, `SADD` to global and scoped active sets only when not finished | `save` | changed: hash, script (SFC rules) |
| R-27 | `list_active_jobs` (global) used by the recovery scan | `list_active_jobs` | changed: per scope from the registry (SFC rules) |
| R-28 | Lua CAS scripts: action state, advance step, finish, branch advance (each also `XADD` follow-up with `MAXLEN`) | `_CAS_*_LUA` | changed: one generic "compare version, store state, append work entries, maintain due-index and active set" script (D-17, SFC rules) |
| R-29 | `XADD` work item with `MAXLEN ~ 5000`; `XGROUP CREATE` with `MKSTREAM`, `BUSYGROUP` ignored | `enqueue_work`, `ensure_consumer_group` | changed: no `MAXLEN`; group created on scope discovery (D-42, D-43) |
| R-30 | Unconsumed result of `XAUTOCLAIM` deleted ids discarded | `_autoclaim_orphaned` | changed: logged and counted (D-42) |

SFC engine rows (`sfc_engine.py`, recipe model) are added below with the SFC rules.
