# Valkey findings

Background for [D-5](../decisions.md#d-5) and Q-DATA-1: Python integration tests against `valkey/valkey-bundle`, run on 2026-11-07 before the freeze. This run also covers the `FT.*` and ACL checks the plan review asked for.

Setup: throwaway containers, `valkey/valkey-bundle:latest` (digest `sha256:c64007296a2a38398d4be63847e1c9890954e45574c16cbe607883ba71c6b985`; Valkey 9.1.3 reporting `redis_version:7.2.4`; modules `json` 1.0.3, `search` 1.1.1 (`ver` 66049), `bloom`, `ldap`), Mosquitto 2, the unmodified example app plus `tests/integration` with redis-py (the Python code uses RESP3, `protocol: 3` in `core.py`). Nothing was committed; temporary local edits made to probe causes were reverted.

| Area | Result |
|---|---|
| `JSON.SET/GET` (documents, pipelines with `transaction=True`) | Works. Dedupe, job order, work master and SFC execution DAOs pass |
| Lua (`EVALSHA` scripts, `redis.call('JSON.GET', ...)` inside a script) | Works |
| Streams (`XADD`, consumer groups, `XREAD BLOCK`, `XAUTOCLAIM`) | Works. The SFC push_command e2e test reaches job response, command dispatch and completion |
| `FT.CREATE ... ON JSON PREFIX ... SCHEMA $.path AS alias TAG`, `FT.INFO`, `FT._LIST` | Works. An unknown index returns `Index with name '...' not found` (an error reply, as the code expects) |
| `FT.SEARCH ... NOCONTENT LIMIT` on RESP3 | **Different reply shape, breaks the Python code.** Valkey-search returns the flat RESP2-style array `[total, key1, key2, ...]` even on a RESP3 connection. Redis returns a map (`total_results`, `results`, ...) there. redis-py either returns a raw `list` (code then fails with `'list' object has no attribute 'total'`) or raises in `Result.from_resp3` (`'list' object has no attribute 'items'`) |
| ACL | Works with a user limited to `~prefix:*` plus the needed command categories and `+ft.create +ft.search +ft.info`. An `FT.CREATE` whose PREFIX is outside the user's key patterns is rejected with `NOPERM` (a good property) |

Integration run (24 tests; without the app and broker only the 4 dedupe tests run): with the example app and Mosquitto running, 17 failed and 7 passed; the app's publisher crashed in `JobResponseDAO.retrieve_by_job_order_id` on the `FT.SEARCH` reply and the failing tests are consequences of that. With a temporary list-shape workaround at the two `FT.SEARCH` call sites in `redis.py`, the SFC e2e test passed and 6 MES tests still failed at the same RESP3 parse step (`Result.from_resp3`, which expects a map). Conclusion: one root cause (the RESP3 `FT.SEARCH` shape); no other incompatibility seen in `JSON.*`, Lua, Streams or `FT.*` index creation. The MES tests were not re-run after the failure analysis, so they may hide further differences.

Consequences for the .NET port:

* **StackExchange.Redis (RESP2 by default) sends `FT.SEARCH` through `Execute`/`ExecuteAsync`, which returns the raw reply.** Parse the flat array `[total, key, ...]` yourself and test it against valkey-search in S09. Do not rely on a map-shaped reply, and do not use RESP3 for search commands without re-checking the shape. Redis Stack/Redis 8 behave differently, so this parser is Valkey-specific and must be covered by an integration test.
* Indexing is asynchronous in principle: in the probe a document written one second before the search was found, but timing was not measured. Poll with a bounded timeout in .NET tests instead of asserting immediately.
* Q-DATA-1: `JSON.SET/GET` and the `$.JobOrderID`/`$._scope`/`$._normalized_state` TAG index work against valkey-json 1.0.3 and valkey-search 1.1.1. Keeping JSON plus search through raw `Execute` is viable; dropping them for plain strings plus Lua remains an option, now a design choice and not a necessity (S04 spike).
* The image reports a Redis 7.2 compatibility version, so client feature probes by `redis_version` see 7.2 (use `valkey_version` where a check is needed).
* Housekeeping for the port: pin `valkey-bundle` by digest in the .NET compose files; the Python test docstrings still say "Redis Stack" and need no change.
