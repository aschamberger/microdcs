# Processors

This page is the reference of the processor model (decisions D-35 to D-39): payloads, handlers, registration and the pipeline that every transport uses. A walkthrough is [Your First Processor](your-first-processor.md).

## A payload

A payload is a record marked with `[CloudEventType]`. The application lists it in one `JsonSerializerContext`; the generator uses that context for the descriptor.

```csharp
[CloudEventType("com.example.greetings.hello.v1", Schema = "https://example.com/schemas/hello")]
public sealed record Hello(string Name)
{
    [CarriedAs("hiddenstr")]   // travels as a CloudEvent attribute, not in the JSON body
    [JsonIgnore]               // required: keeps it out of the body
    public string? HiddenStr { get; init; }
}

[JsonSerializable(typeof(Hello))]
internal sealed partial class AppJsonContext : JsonSerializerContext;
```

* The record must be a non-generic reference type in the same assembly as the processor.
* `[CarriedAs(name)]` needs a settable `string` property, `[JsonIgnore]`, and a name of lowercase letters and digits, at most 20 characters (CloudEvents extension attribute rule).
* Only the `Json` codec is generated. `Binary` and `OpcUaJson` payloads use handwritten descriptors and `AddPayloads(...)`.

## A processor

```csharp
[Processor(ProcessorBinding.Southbound, Id = "greetings")]
public sealed partial class GreetingsProcessor(ILogger<GreetingsProcessor> logger)
{
    [Incoming]
    public IEnumerable<Hello> OnHello(Hello hello, [Scope] string scope) { ... }

    [Fallback]
    public static EventEnvelope OnUnknown(EventEnvelope raw) { ... }
}
```

The class is `partial`; the generator adds `Describe()` (the `IDescribedProcessor` interface). There is no base class and no reflection.

| Attribute | Meaning |
|---|---|
| `[Incoming]` | Handles a request. The first parameter is the payload. For a type an SFC recipe waits for, it may return an `ActionOutcome`; `[Incoming(Outcomes = ...)]` names the outcomes a recipe can use. |
| `[Outgoing(typeof(T))]` | Factory for command payloads, called by type id with a `CommandRequest`. |
| `[OnResponse]` | Handles a response to a command this instance sent. May return an `ActionOutcome`, which is not published and tells a recipe how the action ended; without one the outcome is `done`. |
| `[OnExpired]` | Called when a command expired without a response. |
| `[Fallback]` | Takes events no handler claims. First parameter is the `EventEnvelope`. |
| `[OnInitialize]`, `[OnStarted(Singleton = true)]`, `[OnShutdown]` | Lifecycle hooks. |

Parameters: the payload, `EventContext`, `CancellationToken`, `[Scope] string`, `[SubjectPart(i)] string`, `IServiceProvider` or any registered service. Returns: a payload, `IEnumerable<T>`, `IAsyncEnumerable<T>`, `Replies`, `Task`/`ValueTask` of these, `void`, or `null` for no reply. A fallback may return a prebuilt `EventEnvelope`. Handlers can be `static`.

Replies take `correlationid` from the request (or a new one), `causationid` = request id and the request subject. Attributes the request carried are copied to a reply whose type declares the same `[CarriedAs]` name.

## Registration and configuration

```csharp
builder.Services
    .AddMicroDcs(builder.Configuration)
    .AddProcessor<GreetingsProcessor>()
    .AddInMemoryTransport();   // or AddMqttTransport / AddNatsTransport
```

Configuration binds the `MicroDcs` section with source-generated binding. Environment variables use the prefix `APP_` and `__` as separator, for example `APP_MicroDcs__MessageWorkers=7`. Every error in the options ends the process at start (D-27).

The core fields (`CloudEventSource`, `InstanceId`, `Roles`, workers, queue sizes, dedupe lease and retention) and the binding fields (`MicroDcs:Bindings:{id}:Prefix`, `ScopeLevels`, `Discriminator`, `ReplyBase`, `SharedGroup`, `ScopeCheck`, `Level0Subjects`, queue size, type filter, reply prefixes) are listed in the [configuration reference](operations.md#configuration-reference).

A reply address that a peer sends along (MQTT response topic, NATS `Mdcs-Reply-To`) is checked before the instance publishes to it with its own permissions (D-48). Rejected are addresses that start with `$` (broker and server namespaces such as `$JS.API` or `$KV`), contain wildcards, white space or empty segments, or lie below a binding prefix of this instance (its own event and state topics). Addresses below one of the instance's reply bases stay allowed. A message with a rejected reply address is dropped before the pipeline and logged in the `MicroDCS.Security` category without the address. The `MicroDcs:Management:*` fields (inspection and MCP) are in [Inspection and MCP](inspection.md).

## Pipeline

One pipeline serves every transport. For a received message the steps run in this order:

1. Claim the event id (only on transports with at-least-once delivery). A claim held by another worker is awaited; a processed event is a duplicate and is acknowledged without running the handler.
2. Cancel the expiry timer of the command the message answers (`causationid`).
3. Check the scope of the subject against the address (D-38). A mismatch is dropped and logged in the `MicroDCS.Security` category.
4. Decode the payload, run the filters and the handler, run the action sink.
5. Publish the replies, to the reply address of the request or to the `events` address.
6. Mark the event done, then the transport acknowledges.

The pipeline returns one of three outcomes to the transport:

| Outcome | When | Transport action |
|---|---|---|
| `Ack` | Processed, or a duplicate. | Acknowledge. |
| `Drop` | Undecodable, no handler for the type, scope rejected. | Acknowledge; the message is not redelivered. A request with a reply address gets an error event of type `io.microdcs.error.v1` with the error kind and a fixed message, never the payload. |
| `Fault` | The handler threw. | Do not acknowledge. The claim is not marked done, so the broker redelivers after the lease. A transport that cannot redeliver in the same session ends the host (D-27). |
| `Retry` | The dedupe store failed (Valkey unreachable); the message is not at fault. | Do not acknowledge. NATS keeps the message and runs it again after `NakDelay`, without using a delivery; MQTT ends the host like a fault, and the session redelivers after the restart. The claim is given back. |

**Publishing events.** Inject `IEventPublisher`. An event goes to a bounded queue per binding and a pump hands it to the transport, in order; the transport waits while it reconnects. A full queue throws `OutboundQueueFullException` (D-28). The event goes to every binding whose processor declares the intent and whose type filter accepts the type; `PublishOptions.BindingId` selects one binding. An intent no processor declares throws. A command carries this instance's reply address.

**Expiry.** A command with an expiry interval and a reply address starts a timer in this instance when it is published. The response (an event whose `causationid` is the command id) cancels it. When it fires, the `[OnExpired]` handler of the processor runs with the original payload and the events it returns are published. A failing expiry handler is logged and does not end the host. Timers are local to the instance (D-9).

**Shutdown.** Receiving stops, in-flight messages finish, the outgoing queues drain, the transports stop, pending expiry timers are awaited, then the processors' `[OnShutdown]` hooks run. When the grace period ends the rest is cancelled. Events returned by an `[OnExpired]` handler during shutdown are not published, because the queues are closed by then.

**Telemetry.** The activity source and the meter are both named `MicroDCS`. A message with `traceparent` and `tracestate` continues that trace in a consumer activity `process {type}`; events created while it runs carry the context. Counters: `microdcs.messages.received` (tag `outcome`), `microdcs.messages.published`, `microdcs.outbound.rejected`, `microdcs.commands.expired`. Logs carry ids and sizes, never payloads.

## Testing a processor

`MicroDCS.Testing` drives a processor through the real lifecycle and pipeline on the in-memory transport with a fake clock. No broker, no Valkey.

```csharp
await using var harness = await ProcessorHarness.StartAsync<GreetingsProcessor>();
var result = await harness.SendAsync(new Hello("Bob"), subject: "plant1.line2/res");
result.Published.Length.ShouldBe(2);
harness.Decode<Hello>(result.Published[0]).Name.ShouldBe("Bob");
```

## Diagnostics

The generator reports these errors while you type.

| Id | Meaning |
|---|---|
| MDCS001 | Processor class must be `partial`, top level and not generic. |
| MDCS002 | Handler without the required first parameter. |
| MDCS003 | Two handlers of one kind for the same type id, or two lifecycle hooks of one kind. |
| MDCS004 | Unsupported parameter. |
| MDCS005 | Unsupported return type. |
| MDCS006 | Payload type without `[CloudEventType]`. |
| MDCS007 | Invalid `[CarriedAs]` (not a settable string, no `[JsonIgnore]`, or a bad name). |
| MDCS008 | Two payload types with the same type id. |
| MDCS009 | Payload not listed in a `JsonSerializerContext` of the assembly. |
| MDCS010 | Codec without a generated descriptor: `Binary`, or `OpcUaJson` on a type that is not a generated OPC UA structure with a public parameterless constructor. |
| MDCS011 | Payload type is generic, a value type or nested. |
| MDCS012 | The `[Processor]` argument does not resolve (for example a missing `using` for `ProcessorBinding`). |
