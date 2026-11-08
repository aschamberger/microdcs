# Core contracts (S06 working file)

Sources read on `python-archive`: `common.py`, `core.py`, `dataclass.py`, `mqtt.py`, `__init__.py` (config), `processors/greetings.py`, `processors/machinery_jobs.py` (structure and the handlers listed below, not the DAO logic: S17), `models/greetings_mixin.py`, `redis.py` (dedupe, post-start lock, key schema list), `publishers/machinery_jobs.py`, `sfc_engine.py` (the push-command dispatch only: S19), and the test names of `test_common.py`, `test_mqtt.py`, `test_core.py`, `test_payload_logging.py`, `test_publisher.py`. Not read (later sessions own them): SFC engine and recipe tests (S15, S19), `test_machinery_jobs_processor.py` and `test_job_order_publisher.py` bodies (S17, S18), integration tests (S23).

## Python parity list

Disposition: **kept** (same behaviour), **changed** (with reason or decision), **dropped** (with reason), **open** (to decide in S06; none is left at the end of the session).

### Processor framework (`common.py`)

| # | Python behaviour | Where | Disposition |
|---|---|---|---|
| P-1 | `@incoming(T)` / `@outgoing(T)` register by CloudEvent `type_id`; a union or type alias registers every member | `incoming`, `register_callback` | changed: one handler method per payload type (a union is several methods that delegate); type aliases are not needed. Analyzer reports duplicate type ids |
| P-2 | `processor_config(binding)` sets default subscribe and publish intents (northbound: subscribe command, publish data/events/meta; southbound: the reverse); both overridable | `_BINDING_*_INTENTS` | kept, `[Processor(Binding, SubscribeIntents, PublishIntents)]` |
| P-3 | Handler kwargs are injected by name from CloudEvent attributes (`subject`, `correlationid`, `cloudevent_id`, `ce_method`), filtered to what the handler accepts | `_event_attributes`, `_filter_kwargs_for` | changed: typed parameters resolved at build time by the generator (`EventContext`, `[FromSubject]`, `CancellationToken`) |
| P-4 | `scope_from_subject`: scope is the part of `subject` before the first `/`; `asset_id_from_subject(name, factory)` is the same with a parser | decorators | kept as parameter attributes `[Scope]` and `[SubjectPart(Index, Parser)]` |
| P-5 | A handler returns one payload, a list, or `None`; each becomes a response event | `callback_incoming` | kept |
| P-6 | Response event gets `correlationid` from the request, `causationid` = request `id`, `subject` copied | `callback_incoming` | kept (pipeline, not handler code) |
| P-7 | Payload types without a `Config`/type id are skipped with a warning | `callback_incoming` | dropped: compile error (analyzer), no runtime case |
| P-8 | Unknown type id: error log, no response; payload decode error: error log without the field value unless payload logging is on, no response | `callback_incoming`, `_describe_error` | changed, D-37: undecodable, unknown type and scope-rejected messages are dropped (acknowledged) with a log line, a counter and an inspection entry; a request with a reply address gets an error event (`mdcserrorkind`, new behaviour, the `ErrorKind` values existed unused) |
| P-9 | `__pre_outgoing_callback__` can skip the handler and supply responses; `__post_outgoing_callback__` can rewrite the responses (hook names are misleading: they run on incoming) | `callback_incoming` | changed, D-35: `IProcessorFilter` (wraps invocation, can skip the handler or change replies); registered explicitly, not found by method name |
| P-10 | `process_cloudevent` override for events without a handler (greetings echoes raw events) | greetings processor | kept as `[Fallback]` handler taking the raw envelope |
| P-11 | `process_response_cloudevent`: events on the instance's response topic; used by SFC to complete actions by `causationid` | abstract method | changed: `[OnResponse(typeof(T))]` handlers plus a raw fallback; keeps instance-affine routing (D-9) |
| P-12 | `handle_cloudevent_expiration(event, timeout)`: local timer after publishing a command with response topic and expiry; cancelled when the response arrives (key = `causationid` or `id`) | `mqtt.py` `_publish_message` | kept as `[OnExpired]`; timer lives in the pipeline (A-1), not in MQTT |
| P-13 | `@outgoing(T)` handlers are type-id-keyed payload factories. `callback_outgoing(payload_type, intent, topic, **kwargs)` calls the handler and publishes. The SFC engine uses it for every `push_command` (`sfc_engine.py:529`): it knows only `type_id` and free-form `parameters`, finds the southbound processor and payload class by type id, and passes `cloudevent_id` (stable command id, D-9), `correlation_id`, `action_key` (`{job}:{action}` to `mdcsactionkey`), `subject`. `trigger_outgoing_event(**kwargs)` is an abstract stub in both processors ("not implemented"), only tests call it | **kept, narrowed**: `[Outgoing]` factory handler (direction names the attribute; intent is separate) receiving a `CommandRequest` (scope, job id, action key, command id, correlation id, parameters) and returning the payload; the pipeline sets the ids and attributes; generated type-id table behind `IOutgoingCommands.DispatchAsync(typeId, request)` replaces `_find_sb_processor` and `_type_classes`. Direct sends use `IEventPublisher`. `trigger_outgoing_event` dropped |
| P-14 | `initialize()`, `post_start()` (optionally under a Valkey lock so one replica runs it: `post_start_singleton`, TTL `post_start_lock_ttl`), `shutdown()` | `CloudEventProcessor`, `core.py` | kept: lifecycle attributes `[OnInitialize]`, `[OnStarted(Singleton = true)]`, `[OnShutdown]`; lock is a Valkey-layer service (S09) |
| P-15 | `create_event()` fills `source` from config, `expiryinterval` from `message_expiry_interval`, default content type `application/json; charset=utf-8`, `recordedtime`, fresh `id` and `correlationid` | `create_event` | kept in the envelope factory |
| P-16 | Callback registered per processor are looked up by the first binding whose topic matches; "each message is processed by only one processor" is assumed, but the loop calls every matching binding | `mqtt.py` | changed, D-37: a subscription belongs to one binding; the transport reports the binding id and the pipeline dispatches to that processor only |
| P-17 | Registration hooks for SFC: `register_scope_handler` (scope discovered), `register_action_completion_handler`, `register_action_failure_handler`, `register_pull_completion_handler` (called after every incoming event with subject and type) | `CloudEventProcessor` | changed, D-35: `IActionSink` (complete, fail, event received, scope discovered); the pipeline never references the SFC; default implementation does nothing |

### Envelope (`CloudEvent` in `common.py`, `dataclass.py`)

| # | Python behaviour | Disposition |
|---|---|---|
| E-1 | Attributes: standard ones plus `traceparent`, `tracestate`, `expirytime`, `expiryinterval`, `recordedtime`, `correlationid`, `causationid`, `mdcserrorkind`/`mdcserrormessage`/`mdcserrorcontext`, `method`, `mdcsactionkey` | kept (names may change, D-2); typed extension attributes behind the codec seam (D-14) |
| E-2 | `mdcserrorcontext` is serialised as `k=v,k=v`; `mdcserrormessage` defaults to "Unknown error occurred." when a kind is set | kept: `mdcserrorcontext` stays `k=v,k=v` text on the wire; keys and values must not contain `,` or `=` (D-30), checked on encode |
| E-3 | Unknown attributes on the wire land in `custommetadata`; keys that collide with a standard attribute are removed (field injection guard) on decode; on encode, custom keys are written as extra attributes | kept: must be a test (collision guard) |
| E-4 | **Hidden payload fields** (names starting with `_`) are not serialised in the payload but travel as CloudEvent extension attributes (`x-hidden-str`, `x-hidden-obj`) and are restored on the response (`__request_object__`) | changed, D-35: `[CarriedAs(name)]` on payload properties; the generator writes them as extension attributes and restores them on the response |
| E-5 | `response()` creates the typed response from the request and can copy named fields (`takeover`) and hidden fields | changed, D-35: handlers build responses with constructors or `with`; the pipeline copies `[CarriedAs]` attributes of the request to the response when the response type declares the same attribute name and the handler left it unset; `takeover` dropped |
| E-6 | Payload content types: octet-stream, JSON (`; charset=utf-8` variants), MessagePack | changed: MessagePack dropped; JSON and octet-stream, plus OPC UA JSON for job models (D-34) |
| E-7 | Type id and `dataschema` come from the payload class (`Config.type_id`, `type_schema`); `matches_type_id_pattern` supports `fnmatch` | kept as `[CloudEventType]`; pattern matching used by outgoing type filters (see T-5) |
| E-8 | MQTT correlation data is `uuid.UUID(id).bytes`, so event ids must be UUIDs | dropped (A-6) |
| E-9 | `method` attribute (`PUT` default, `DELETE`) used by station config handlers | kept: attribute `method` |

### Pipeline and MQTT transport (`mqtt.py`)

| # | Python behaviour | Disposition |
|---|---|---|
| T-1 | Order: decode, dedupe claim (wait while another worker holds the lease, returns "duplicate" when done), cancel expiry timer, response handlers, scope check, dispatch, publish responses, `mark_done`, ack | kept as pipeline A-1; order is a test |
| T-2 | Duplicate: still acknowledged (puback) | kept |
| T-3 | `mark_done` before ack: crash in between gives a redelivery, never a lost message | kept |
| T-4 | Scope check: subject must match the address path (`.` to `/`, or the first part of the subject); skipped when no subject, no path between prefix and intent, or `enforce_subject_topic_match` off | kept as A-7; level-0 gap is R-6 |
| T-5 | Outgoing type filter: set of types or `fnmatch` patterns per binding | kept: comma separated type ids with `*` wildcards (D-30), checked at publish |
| T-6 | Outgoing queue per binding, bounded (config, max cap 1000), full queue raises (D-28); dropped event with no type is skipped with a warning | kept (D-28) |
| T-7 | Publish retries after reconnect (`client.connected()`), indefinitely | kept: publish waits while the transport reconnects (indefinitely, until shutdown); readiness is false meanwhile (D-37) |
| T-8 | QoS 0 by default, QoS 1 when a response topic is set; `retain` from transport metadata; message expiry from `expiryinterval`; content type; user properties = all attributes except data (non-string values stringified) | kept per transport (D-10), declared in the delivery profile (A-5) |
| T-9 | Commands get the per-instance response topic `{base}/{instance_id}` automatically; responses are published to the request's response topic; own response topic is also set on responses | kept (D-9) |
| T-10 | Subscriptions: prefix + n wildcard levels (`/+`) + optional discriminator + intent; `$share/name/` prefix when shared subscription is configured; discriminator uniqueness check at startup (fail-fast) | kept in address model A-3; startup validation kept |
| T-11 | `message_workers` parallel workers; in-flight message processing is shielded from cancellation during shutdown | kept: concurrency setting and drain-on-shutdown |
| T-12 | Graceful shutdown in five phases: unsubscribe, finish workers, drain outgoing queues, stop publishers, wait for expiry timers; forced cancel after `shutdown_grace_period` (30 s) | kept: shutdown contract in S06 (`IHostedService` stop + grace period) |
| T-13 | Startup pings Valkey before MQTT connects (fail-fast, D-27) | kept (D-27) |
| T-14 | Publisher (retained state topics): `publish_retained` with TTL (QoS 1, retain, expiry), `delete_retained` as zero-byte retained; reconnect loop | kept behind the state-topic port (A-4), MQTT implements it natively |
| T-15 | Instance roles `is_processor_instance` / `is_publisher_instance` (publishers only run when the flag is set; processors only initialise as processor) | kept as `InstanceRoles` flags (Processor, Publisher; default both); Q-ARCH-9 (S18) decides the split of deployments |
| T-16 | Client identifier suffixes `-proc` and `-pub` | kept (two clients per role) |
| T-17 | Session expiry "never" (`2**32-1`), SAT token path, TLS CA path, mTLS cert/key, username/password and password file, `require_tls` | kept as transport config (S13) |

### Configuration (`__init__.py`)

| # | Python behaviour | Disposition |
|---|---|---|
| C-1 | Env names `APP_<SECTION>_<FIELD>`; sets and lists from comma-separated strings; bool accepts 1/true/True/TRUE | changed: .NET `IConfiguration` with `APP__SECTION__FIELD` (decided with the user at the start of S06); lists stay comma-separated strings (D-30) |
| C-2 | `RuntimeConfig.validate()` before start: non-empty, port, non-negative, positive, files exist, TCP reachability checks | kept: options validation at start, fail-fast (D-27) |
| C-3 | Topic settings are sets of `identifier=value` strings (`topic_prefixes`, `topic_wildcard_levels`, `topic_discriminators`, `response_topics`) looked up by the processor's `config_identifier` | changed, D-39: `MicroDcs:Bindings:{id}` sections (`Prefix`, `ScopeLevels`, `Discriminator`, `ReplyBase`, `SharedGroup`, `OutgoingTypeFilter`, queue size, scope policy); the processor names its id in `[Processor(Id = ...)]` |
| C-4 | `POD_ID` as instance id, UUID fallback | kept |
| C-5 | Payload logging off by default, `log_payloads` allows payloads in DEBUG logs; error messages hide the field value unless on | dropped: no payload logging switch (CLAUDE.md); the dev-only inspection option `include_payloads` (D-19, D-40) replaces it. Error messages never contain field values |
| C-6 | Log destination: file in addition to console, `disable_if_otel_enabled` | dropped: M.E.Logging and OTel |

### Hosting (`core.py`)

| # | Python behaviour | Disposition |
|---|---|---|
| H-1 | `MicroDCS` builds config, Redis pool, key schema; registers handlers, bindings, processors, additional tasks | changed: `HostApplicationBuilder` extension methods (`AddMicroDcs()`, `AddProcessor<T>()`, `AddMqtt()`) |
| H-2 | Binding refers to its handler class through generics; unregistered handler is an error | dropped: DI, compile-time registration |
| H-3 | OTel-instrumented handler variants chosen by config | dropped: one ActivitySource in the pipeline |
| H-4 | Order at start: initialise processors, start handlers and tasks, then `post_start` (with singleton lock); at the end `shutdown` of processors, close Valkey | kept as the hosting lifecycle order |
| H-5 | `SystemEventTaskGroup`: SIGINT/SIGTERM set a shutdown event, grace period, then force-cancel | changed: Generic Host `ShutdownTimeout` |

### Machinery Jobs processor (kept for S17, listed so S06 contracts support it)

| # | Python behaviour | Disposition |
|---|---|---|
| J-1 | 13 call types and responses, each handler with `scope`, `correlationid`, `cloudevent_id` | contract must allow many small handlers per class; sync-looking one-liners delegating to shared methods |
| J-2 | Status event (`ISA95JobOrderStatusEventType`) published with intent `EVENT` and `subject = scope` only when the transition has an effect | publisher injected into handlers |
| J-3 | Metadata event with `mqtt_retain` at `post_start`, intent `META` (late subscribers get the capabilities) | state-topic port, a "retain" flag on the outgoing message (typed, A-2) |
| J-4 | Station configuration handlers (`ConfigEquipment` ... `ConfigJobAcceptance`) use `ce_method` PUT/DELETE | kept (S17) |
| J-5 | Constructor takes Valkey pool, key schema, acceptance config; processor owns its DAOs | DI with constructor injection |

### From the tests and the remaining sources (added during S06)

| # | Python behaviour | Where | Disposition |
|---|---|---|---|
| P-18 | The pull-completion handler runs for every incoming event that has a subject and a type, after the handler, before responses are built | `callback_incoming`, `test_common.py` | kept: `IActionSink.EventReceivedAsync` after the handler |
| P-19 | `TransactionDedupeDAO` (scope + transaction id) and `CounterDAO` exist but nothing in `src/` uses them | `redis.py` | dropped: unused |
| T-18 | A failed message is left claimed only: no `mark_done`, no ack, so it is redelivered after the lease lapses (`test_process_message_failure_leaves_message_claimed_only`) | `test_mqtt.py` | kept, D-37 (`DeliveryOutcome.Fault`) |
| T-19 | A worker crash propagates and ends the application; a crash of one handler task ends the others (`test_worker_crash_propagates_exception`, `test_handler_exception_propagates_as_exception_group`) | `test_mqtt.py`, `test_core.py` | kept: an unhandled exception in the pipeline stops the host (D-27, D-37) |
| T-20 | The claim uses the same token on every attempt and waits for the lease holder, minimum 100 ms between attempts | `_claim_message` | kept in the pipeline (`IDedupeStore`), delay through `TimeProvider` |
| T-21 | Startup fails when a binding has no prefix, when the processor lacks the binding configuration, when a handler has no binding, and when prefix + discriminator repeat (warning in Python) | `test_core.py`, `test_mqtt.py` | changed: all are startup errors (D-27), duplicates too |
| T-22 | Queue size 0 falls back to the processing maximum, larger values are capped to it | `test_mqtt.py` | kept (`OutgoingQueueSize`, `OutgoingQueueMaxSize`) |
| T-23 | The publish topic: an explicit topic is kept; intents the processor does not publish fall back to the bare prefix | `enrich_publish_transportmetadata` | changed: publishing an intent outside `PublishIntents` is an error at the publisher (analyzer where static) |
| T-24 | `subject_matches_topic` accepts the full subject path and the scope part alone; resource parts of the subject (`scope/resource`) became extra topic levels because `.` and `/` were both mapped | `mqtt.py` | changed, D-38: only the scope part of the subject forms the address path; the segment rule excludes separators and wildcards for all transports; level-0 subjects are rejected unless declared (R-6) |
| PUB-1 | State publisher: retained `state-index` per scope with a per-scope sequence, `order/{id}`, `result/{id}` (only for Ended/Aborted jobs), zero-byte delete on Clear; TTL 48 h; cursors per scope persisted in Valkey; initial state-index for all active scopes at start (covers the restart gap); scope discovery through a global change stream; XREAD with a block time | `publishers/machinery_jobs.py` | S18 on `IStateTopicPort` and the S07 polling decision (no blocking reads, D-7); not designed here |
| PUB-2 | The publisher connects with its own client identifier suffix and reconnects in a loop | `MQTTPublisher` | kept (T-16) |
| L-1 | Hosting order and failure tests: validate fails, initialise fails, no binding for a handler end the run; Valkey pool closes after the task group and after processor shutdown; the lock service is built with the configured TTL | `test_core.py` | kept as host lifecycle tests in S08 |
| L-2 | Valkey connection options: auth, TLS, CA, timeouts, keepalive, password from file; missing password file fails fast | `test_core.py` | S09 |
| T-25 | MQTT client options: reconnect on, persistent session, TLS rules (client cert needs key and TLS, require_tls needs CA), warning when credentials go out without TLS | `test_mqtt.py` | S13 |

## Example processors (draft for the checkpoint)

Written as they should read, before any interface is defined. Names are provisional.

### 1. Greetings (southbound, plain request/response)

```csharp
[CloudEventType("com.github.aschamberger.microdcs.greetings.hello.v1",
    Schema = "https://aschamberger.github.io/schemas/microdcs/greetings/v1.0.0/hello")]
public sealed record Hello(string Name)
{
    [CarriedAs("x-hidden-str")] public string? HiddenStr { get; init; }   // E-4
}

[CloudEventType("com.github.aschamberger.microdcs.greetings.bye.v1")]
public sealed record Bye(string Name);

[Processor(ProcessorBinding.Southbound)]
public sealed partial class GreetingsProcessor(ILogger<GreetingsProcessor> logger)
{
    [Incoming]
    public IEnumerable<Hello> OnHello(Hello hello)          // one payload, many, or null: all allowed
    {
        logger.HelloReceived(hello.Name);                    // [LoggerMessage], no payload content
        yield return hello with { };                         // reply keeps the carried attribute
        yield return new Hello("Alice");
    }

    [Fallback]                                               // P-10: no handler for this type
    public RawEvent OnUnknown(RawEvent raw) => raw.Echo("com.github.aschamberger.microdcs.greetings.raw.v1");
}
```

The class is `partial` because the generator adds the registration (type id to handler table, payload decoders, intents). No base class, no interface. Handlers are plain methods and are unit-testable without any framework.

### 2. Command with a response and a timeout (southbound, push)

```csharp
[Processor(ProcessorBinding.Southbound)]
public sealed partial class StationProcessor(IEventPublisher publisher)
{
    public Task SayByeAsync(string subject, CancellationToken ct) =>
        publisher.PublishAsync(new Bye("Bob"), Intent.Command, new() { Subject = subject, ActionKey = $"{subject}:bye" }, ct);

    [OnResponse]                                              // P-11, arrives on this instance's reply address (D-9)
    public Task OnByeAck(ByeAck ack, EventContext ctx, IActionSink sink, CancellationToken ct) =>
        sink.CompleteAsync(ctx.CausationId!, ct);

    [OnExpired]                                               // P-12: local timer from the pipeline
    public Task OnByeExpired(Bye original, EventContext ctx, IActionSink sink, CancellationToken ct) =>
        sink.FailAsync(ctx.EventId, ct);
}
```

`IActionSink` is the seam to the SFC (P-17): the pipeline never knows about SFC.

### 3. Job order handler (northbound, many commands, state events)

```csharp
[Processor(ProcessorBinding.Northbound)]
public sealed partial class MachineryJobsProcessor(IJobOrderStore jobs, IEventPublisher publisher)
{
    [Incoming]
    public async Task<StoreResponse> OnStore(StoreCall call, [Scope] string scope, EventContext ctx, CancellationToken ct)
    {
        var result = await jobs.StoreAsync(scope, call.JobOrder, ct);
        if (result.HasStatusEvent)
            await publisher.PublishAsync(result.StatusEvent, Intent.Event, ctx.Reply(scope), ct);   // J-2
        return call.Respond(result.ReturnStatus);                                                    // Annex B bitmap, D-34
    }

    [OnStarted(Singleton = true)]                              // P-14: one replica publishes the metadata
    public Task PublishMetadata(CancellationToken ct) =>
        publisher.PublishAsync(MetadataEvent.For(this), Intent.Meta, new() { Retain = true }, ct);   // J-3
}
```

## Authoring API shape (proposal, to confirm at the checkpoint)

* **Payloads** are records with `[CloudEventType(id, Schema, Codec)]`. `Codec` is `Json` (System.Text.Json source generation, the generator emits the `JsonSerializerContext` entry), `OpcUaJson` (D-34 job models) or `Binary` (`ReadOnlyMemory<byte>`). Properties can be marked `[CarriedAs(name)]` (E-4).
* **Processors** are `partial` classes with `[Processor(binding, ...)]`. Constructor injection only. No base class; a source generator emits an `IProcessorDescriptor` (type id to handler delegate, intents, parameter binding). Explicit registration stays available: `AddProcessor<T>(d => d.Incoming<Hello>(p => p.OnHello))`.
* **Handler attributes:** `[Incoming]`, `[Outgoing]`, `[OnResponse]`, `[OnExpired]`, `[Fallback]`, `[OnInitialize]`, `[OnStarted(Singleton)]`, `[OnShutdown]`. The payload type comes from the first parameter (no type argument in the attribute).
* **Parameters** resolved at build time: the payload, `EventContext` (subject, ids, correlation, method, transport-neutral metadata), `[Scope]`, `[SubjectPart(i, Parser)]`, `CancellationToken`, DI services.
* **Returns:** `T`, `IEnumerable<T>`, `IAsyncEnumerable<T>`, `Task<...>`, `void`/`Task`, null for none; mixed response types through a `Replies` builder.
* **Analyzer diagnostics (`MDCS0xx`):** class not partial; handler without payload parameter; duplicate type id in one processor; return type not a payload type; `[OnResponse]` on a northbound-only processor without publish of commands (warning); payload without `[CloudEventType]`; unsupported parameter; `[CarriedAs]` on a type with the `Json` codec that is not a string-convertible property.
* **Test harness (`MicroDCS.Testing`):** `ProcessorHarness.For<T>(services)`, `await harness.SendAsync(new Hello("Bob"), subject: "plant1.line2")`, assertions on `Replies`, `Published`, with a fake clock for `[OnExpired]`. No broker, no Valkey.

## Checkpoint answers (2026-11-08)

1. A `partial` class with attributes and a generator: accepted (D-35).
2. Hidden payload fields: kept as `[CarriedAs]` (E-4).
3. Pre/post hooks: replaced by `IProcessorFilter` (P-9).
4. `[Outgoing]` factories: kept, narrowed, because the SFC dispatches push commands by type id (P-13). The name stays `[Outgoing]`, symmetric to `[Incoming]`.

## Decisions and code

Decisions D-35 to D-40 in [decisions.md](../decisions.md). Compiling stubs in `src/MicroDCS.Core` (`Authoring`, `Messaging`, `Transport`, `Inspection`, `Hosting`); the pure rules (segment rule, scope policy, intent names, redaction) have unit tests in `tests/MicroDCS.Core.Tests/ContractTests.cs`.
