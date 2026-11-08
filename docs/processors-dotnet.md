# Processors in .NET

This page describes the processor model of the .NET core (decisions D-35 to D-39). The pipeline here is the minimal one of S08; queues, expiry timers and the real transports follow in later sessions.

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
| `[Incoming]` | Handles a request. The first parameter is the payload. |
| `[Outgoing(typeof(T))]` | Factory for command payloads, called by type id with a `CommandRequest`. |
| `[OnResponse]` | Handles a response to a command this instance sent. |
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
    .AddInMemoryTransport();   // real transports arrive with S13 and later
```

Configuration binds the `MicroDcs` section with source-generated binding. Environment variables use the prefix `APP_` and `__` as separator, for example `APP_MicroDcs__MessageWorkers=7`. Every error in the options ends the process at start (D-27).

| Name | Type | Default | Description |
|---|---|---|---|
| `MicroDcs:CloudEventSource` | string | none, required | `source` of events this application creates. |
| `MicroDcs:InstanceId` | string | `POD_ID` or a generated id | Instance id; part of the dedupe token. |
| `MicroDcs:Roles` | `Processor`, `Publisher`, `All` | `All` | Which parts this instance runs. |
| `MicroDcs:MessageExpiry` | time span | none | Expiry interval for created events. |
| `MicroDcs:ShutdownGracePeriod` | time span | 30 s | Time for in-flight work on shutdown. |
| `MicroDcs:OutgoingQueueMaxSize` | int | 1000 | Upper bound of any outgoing queue (D-28). |
| `MicroDcs:MessageWorkers` | int | 5 | Parallel workers per transport. |
| `MicroDcs:DedupeLease` | time span | 30 s | Lease of a dedupe claim; must exceed the slowest handler. |
| `MicroDcs:DedupeRetention` | time span | 10 min | How long a processed event stays a duplicate. |
| `MicroDcs:StartLeaseTtl` | time span | 30 s | Lifetime of the singleton start lease. |
| `MicroDcs:Bindings:{id}:Transport` | string | `mqtt` | Transport name; the transport must be registered. |
| `MicroDcs:Bindings:{id}:Prefix` | string | none, required | Address prefix; `/` separated segments of letters, digits, `_`, `-`. |
| `MicroDcs:Bindings:{id}:ScopeLevels` | int | 0 | Scope levels below the prefix. |
| `MicroDcs:Bindings:{id}:Discriminator` | string | none | Separates processors that share a prefix; the pair must be unique. |
| `MicroDcs:Bindings:{id}:ScopeCheck` | `Enforce`, `Off` | `Enforce` | Compare subject scope and address scope. |
| `MicroDcs:Bindings:{id}:Level0Subjects` | `Reject`, `Accept` | `Reject` | Subjects on addresses without a scope. |

The remaining binding fields (`ReplyBase`, `SharedGroup`, `OutgoingQueueSize`, `OutgoingTypeFilter`) are validated but used from S10 and S13 on.

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
| MDCS010 | Codec without a generated descriptor. |
| MDCS011 | Payload type is generic, a value type or nested. |
