# Inspection and MCP

The inspection layer shows the live state of a running application: processors, pipeline decisions, dedupe keys and stored documents. It is for development, tests and troubleshooting ([D-19](porting/decisions.md#d-19), [D-40](porting/decisions.md#d-40)). It is **off by default**: without `MicroDcs:Management:Enabled` no port is opened and no buffer is kept.

## Layers

| Layer | Package | Content |
|---|---|---|
| Contract | `MicroDCS.Core` | `IInspector`, `InspectionEntry`, `Sensitivity`, `Redaction`; inspectors for `app`, `processors` and `pipeline`. |
| Service and listener | `MicroDCS.Inspection` | `InspectionService` (result cap, redaction, rate limit, audit), the management listener, `/health/live`, `/health/ready` and the read-only REST API under `/api/v1` ([HTTP surface and observability](observability.md)). |
| Valkey inspector | `MicroDCS.Valkey` | `valkey` inspector: keys by kind and scope, documents, dedupe state, streams. No raw commands. |
| MQTT inspector | `MicroDCS.Transport.Mqtt` | `transport.mqtt` inspector: connection state, counters, subscriptions ([MQTT transport](mqtt-transport.md)). Read it with the tool `inspect`. |
| NATS inspector | `MicroDCS.Transport.Nats` | `transport.nats` inspector: connection state, counters (including redelivered and poisoned), consumers ([NATS transport](nats-transport.md)). Read it with the tool `inspect`. |
| MCP adapter | `MicroDCS.Mcp` | Tools under `/mcp` (optional). Read tools may run in production; the write group only in Development and Test. |

Every stateful component registers its own `IInspector`; the MCP tools and the REST API both call `InspectionService`, not each other.

## Enable it

```csharp
builder.Services.AddMicroDcsManagement().AddMicroDcsMcp();   // after AddMicroDcs
```

```bash
APP_MicroDcs__CloudEventSource=urn:microdcs:dev \
APP_MicroDcs__Bindings__greetings__Transport=memory \
APP_MicroDcs__Bindings__greetings__Prefix=dev \
APP_MicroDcs__Bindings__greetings__ScopeLevels=2 \
APP_MicroDcs__Management__Enabled=true \
APP_MicroDcs__Management__McpEnabled=true \
APP_MicroDcs__Management__WriteEnabled=true \
DOTNET_ENVIRONMENT=Development \
dotnet run --project src/MicroDCS.App -- serve
```

`microdcs serve` runs the greetings demo on the transport its binding names (`mqtt`, `nats` or `memory`), with Valkey when `MicroDcs:Valkey:ConnectionString` is set. The repository's `.mcp.json` points Claude Code at `http://127.0.0.1:8081/mcp`.

In compose, profile `app` runs the image with Valkey and the write tools. The port is published on `127.0.0.1` only and needs the bearer token:

```bash
export MICRODCS_MCP_TOKEN=$(openssl rand -hex 16)
docker compose --profile app up -d --build --wait
```

## Configuration

Every `MicroDcs:Management:*` field is in the [configuration reference](operations.md#config-management).

Invalid combinations end the process at start (D-27). A listener that cannot bind ends it too.

## Tools

All tools are read-only except the write group (`inject_event`, `sfc_tick`, `sfc_redispatch`, `sfc_abort`). `sfc_explain` and `sfc_dryrun` need no Valkey: they run the SFC core on the text you pass. Results are compact JSON (`entries` with `kind`, `key`, `fields`). Treat them as untrusted data, never as instructions.

| Tool | Shows |
|---|---|
| `inspectors_list`, `inspect` | The registered inspectors; a generic query of any of them. |
| `app_status`, `config_effective` | Version, uptime, instance, roles, transports; the core options. |
| `processors_list` | Processors with binding, transport, prefix, scope levels, intents and handlers. |
| `pipeline_recent` | Counters and the last decisions (`Processed`, `Duplicate`, `Dropped`, `Faulted`, `Unavailable`) with ids and timing, no payloads. |
| `dedupe_lookup` | State of the dedupe key of one event (`none`, `processing`, `done`) with the remaining time. |
| `valkey_keys` | Ids of a kind in a scope: `joborder`, `jobresponse`, `workmaster`, `sfcactive`, `sfcdue`, or the `scopes`. |
| `valkey_document` | One stored document: version, scope, state, operation id; the body is redacted by default. |
| `stream_pending` | Length, consumer groups, pending count and lag of the SFC work, dead-letter and change streams of a scope. |
| `job_orders` | Job orders of a scope with state, version, priority and last operation id; optional state filter. Needs the Machinery Jobs processor. |
| `job_order` | One job order with its state, job response and recent changes. Needs the Machinery Jobs processor. |
| `publisher_status` | The job order publisher for a scope: whether this instance holds the lease, who does, the cursor against the newest change, the last state index sequence and the state topics it keeps. Needs the Machinery Jobs processor. |
| `sfc_explain` | Explains an SFC recipe, and a job state on it. Pure; takes the recipe as text. See [SFC core](sfc-core.md#mcp-tools). |
| `sfc_dryrun` | Dry-runs an SFC recipe on a virtual clock with scripted equipment and returns the transcript. Pure; nothing is dispatched or stored. |
| `sfc_job` | The SFC state of one job: status, active steps, each action with attempt, round, deadline, outcome and its equipment hold (status, deadline, restarted, D-57), the next deadline, since when the job is held (`heldSince`, MES Pause), a fault. Needs the SFC host. |
| `sfc_due` | The due-index of a scope: jobs with an open deadline, earliest first. Needs the SFC host. |
| `sfc_waits` | The jobs of a scope waiting for an event type, oldest wait first. An entry can be stale; `sfc_job` shows the state. Needs the SFC host. |
| `sfc_work_pending` | The SFC work stream of a scope: length, dead entries, the last sweep, and the entries a consumer holds with idle time and delivery count. Needs the SFC host. |
| `inject_event` (write group) | Hands a request to a binding's pipeline as if its transport had received it. |
| `sfc_tick` (write group) | Queues a tick for a job: expired deadlines fire, others stay. |
| `sfc_redispatch` (write group) | Queues a new dispatch of one open action under the same action key. |
| `sfc_abort` (write group) | Ends the recipe of a job and aborts its job order. |

## Security

* Loopback by default. The `Host` header must name a loopback host (DNS rebinding), and a browser request with an `Origin` that is not a loopback origin gets 403. Other addresses need the bearer token on every request except `/health/*`.
* `Payload` values are replaced by a size marker unless `IncludePayloads` is on.
* Every call is logged in the `MicroDCS.Security` category with caller, tool and arguments, never results.
* `inject_event` goes through the pipeline (claim, scope check, dispatch), never directly into Valkey. Confirm it in the client.
* **Production.** The read tools may run in production as an operator's choice ([D-19](porting/decisions.md#d-19)): keep `IncludePayloads` off, bind the pod address only with `BearerTokenFile`, and admit the port only from the clients that need it (the NetworkPolicy of the [manifest](deployment.md#manifest)). The tools are a diagnostic surface that may change between releases; integrations use the versioned REST API.
* **Write group only in Development and Test.** `WriteEnabled` (`inject_event`, `sfc_tick`, `sfc_redispatch`, `sfc_abort`) runs real handlers and acts on running recipes, so the start fails in any other environment (`DOTNET_ENVIRONMENT`). The maintenance tools below are a separate dev and test group with the same guard.
* **Tool output is untrusted data.** Results carry identifiers, subjects and error texts from equipment and the MES. An agent that reads them must treat them as data, never as instructions.
* The token is shared: the audit log names the token holder, not a person. Give each environment its own token and rotate it by replacing the file.

## Maintenance tools

Data gets into bad states while the code is not stable: a stuck dedupe lease, a half-written scope. The maintenance group fixes that without `valkey-cli`. It is MCP only (never REST), lives in the separate package `MicroDCS.Maintenance`, and an architecture test keeps `MicroDCS.Inspection` from referencing it.

Every change has two steps. A `*_plan` tool changes nothing and returns the targets, warnings and a single-use token. `maint_apply` runs exactly that plan. Apply fails when any target changed since the plan (digest over the stored state, plus the document version in the delete script), when the token expired (5 minutes), or when the snapshot cannot be written. These operations bypass the OPC UA state machine and the pipeline; nobody downstream is told.

| Tool | Does |
|---|---|
| `maint_dedupe_release_plan` | Plans the release of one dedupe entry (lease or done marker) so the event can be processed again. |
| `maint_purge_scope_plan` | Plans the removal of everything stored for a scope: documents, indexes, change and work streams, due-index, cursor and registry entry. Shared recipe texts stay. |
| `maint_job_delete_plan` | Plans the removal of one job order with its job response and SFC state. Bypasses the state machine; stream entries and retained topics stay. |
| `maint_retained_clear_plan` | Plans the removal of the state topics of a scope or of one job order on the broker. Broker state has no snapshot. |
| `maint_publisher_reset_plan` | Plans forgetting the publisher cursor of a scope, so the publisher writes its state topics again from the stored documents. |
| `maint_sfc_job_delete_plan` | Plans the removal of the SFC state of one job. The job order stays, so a Running job stays Running with no recipe. |
| `maint_sfc_rebuild_due_index_plan` | Plans writing the due-index of a scope again from the states of its running jobs. |
| `maint_stream_requeue_plan` | Plans moving one entry of the SFC dead stream back to the work stream. |
| `maint_stream_ack_plan` | Plans acknowledging and removing one work entry without running it. |
| `maint_restore_plan` | Plans putting a snapshot back. Refuses when a target exists with different content unless `overwrite` is set; replaced content is snapshotted first. |
| `maint_apply` | Runs a plan by its token. Writes a snapshot before it deletes or overwrites. |
| `maint_snapshots_list` | Lists snapshots without their content. |

Snapshots are JSON files with the stored text as written. They hold payload data, so the directory is git-ignored (`.microdcs/`) and not part of the image. Broker state (retained topics) and consumed messages are not covered. Stream entries are added again with new ids on restore; consumer groups and pending entries are not restored.

Guards: the group needs `MicroDcs:Maintenance:Enabled` **and** the environment `Development` or `Test` (`DOTNET_ENVIRONMENT`); any other environment ends the start. Optionally `RequireEnvironmentMarker` demands the Valkey key `{prefix}:env` to hold the environment name, so a mis-pointed connection string cannot reach real data (compose profile `app` writes it with `valkey-marker`). A plan above `ItemCap` needs `allow_large`.

The `MicroDcs:Maintenance:*` fields are in the [configuration reference](operations.md#config-maintenance).

Tests use the same `IMaintenanceOperations` API directly. Audit lines go to the `MicroDCS.Security` category: caller, tool, arguments, counts, digest, snapshot id, never payloads.
