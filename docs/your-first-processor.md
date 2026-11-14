# Your First Processor

This tutorial builds a small processor that answers a ping with a pong. It runs on MQTT against the local broker and
has a unit test without a broker. Afterwards you know the payload, processor and hosting basics; the reference is
[Processors](processors-dotnet.md).

## Prerequisites

* The .NET SDK from the repository's `global.json` (11.0.100).
* Docker, for the Mosquitto broker of the dev stack.
* A clone of the repository next to your project. The NuGet packages and a `dotnet new` template follow with S26; until
  then the project references the MicroDCS projects directly.

```text
workspace/
  microdcs/          # git clone https://github.com/aschamberger/microdcs
  microdcs-ping/     # this tutorial
    src/Ping/
    tests/Ping.Tests/
```

## Step 1: the project

`src/Ping/Ping.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net11.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <PublishAot>true</PublishAot>
    <InvariantGlobalization>true</InvariantGlobalization>
  </PropertyGroup>
  <ItemGroup>
    <ProjectReference Include="../../../microdcs/src/MicroDCS.Core/MicroDCS.Core.csproj" />
    <ProjectReference Include="../../../microdcs/src/MicroDCS.Transport.Mqtt/MicroDCS.Transport.Mqtt.csproj" />
    <ProjectReference Include="../../../microdcs/src/MicroDCS.Generators/MicroDCS.Generators.csproj"
                      OutputItemType="Analyzer" ReferenceOutputAssembly="false" />
  </ItemGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.Extensions.Hosting" Version="11.0.0" />
  </ItemGroup>
</Project>
```

The generator is referenced as an analyzer. It wires the processor at build time and reports mistakes while you type
(`MDCS001` to `MDCS012`).

## Step 2: the payloads

A payload is a record with `[CloudEventType]`, listed in a source-generated `JsonSerializerContext`. `Payloads.cs`:

```csharp
using System.Text.Json.Serialization;
using MicroDCS.Core.Authoring;

namespace Ping;

/// <summary>A ping request.</summary>
[CloudEventType("com.example.ping.v1")]
public sealed record PingRequest(string Message);

/// <summary>The answer to a ping.</summary>
[CloudEventType("com.example.pong.v1")]
public sealed record PongResponse(string Message, int Length);

[JsonSerializable(typeof(PingRequest))]
[JsonSerializable(typeof(PongResponse))]
internal sealed partial class PingJsonContext : JsonSerializerContext;
```

* The type id is the CloudEvent `type`; it selects the handler. Put the version at the end, so a changed payload can get
  a new type.
* `Schema = "https://…"` on the attribute sets `dataschema`. Publish the schema where receivers can read it; the
  repository generates its own schema documents in tests ([D-58](porting/decisions.md#d-58)).
* Without the `JsonSerializerContext` entry the build fails with `MDCS009`: there is no reflection-based serialization
  in a Native AOT binary.

## Step 3: the processor

`PingProcessor.cs`:

```csharp
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;
using Microsoft.Extensions.Logging;

namespace Ping;

[Processor(ProcessorBinding.Northbound, Id = "ping")]
public sealed partial class PingProcessor(ILogger<PingProcessor> logger)
{
    [Incoming]
    public PongResponse OnPing(PingRequest ping, [Scope] string scope)
    {
        logger.PingReceived(scope);
        return new PongResponse(ping.Message, ping.Message.Length);
    }
}

internal static partial class Log
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Ping received in scope {Scope}")]
    public static partial void PingReceived(this ILogger logger, string scope);
}
```

What happens here:

* `[Processor(ProcessorBinding.Northbound, Id = "ping")]`: a northbound processor subscribes to `commands` and publishes
  `data`, `events` and `metadata`. `Id` names the binding section in the configuration (`MicroDcs:Bindings:ping`).
* The class is `partial`; the generator adds the description of its handlers. There is no base class.
* `[Incoming]` handles the type of its first parameter. `[Scope]` receives the scope from the subject
  (`plant1.line2` for the subject `plant1.line2/cell1`). Other parameters can be `EventContext`, a `CancellationToken`
  or any registered service.
* The return value is the reply. It gets `causationid` set to the request's `id`, the request's `correlationid` (or a new
  one) and the request's `subject`. Return `null` for no reply, or `IEnumerable<T>` for several.
* Logging uses `[LoggerMessage]`. Log ids and sizes, never payload contents.

## Step 4: the host

`Program.cs`:

```csharp
using MicroDCS.Core.Hosting;
using MicroDCS.Transport.Mqtt;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Ping;

var builder = Host.CreateApplicationBuilder(args);
builder.Configuration.AddEnvironmentVariables("APP_");

builder.Services
    .AddMicroDcs(builder.Configuration)
    .AddProcessor<PingProcessor>();
builder.Services.AddMqttTransport(builder.Configuration);

await builder.Build().RunAsync();
```

Configuration is the `MicroDcs` section; environment variables use the prefix `APP_` and `__` as separator. The binding
places the processor in the address space: prefix `dev` and two scope levels give the subscription
`dev/+/+/commands` (and the shorter ones for fewer levels). An error in the options ends the start with a message
([Operations](operations.md#configuration-reference)).

## Step 5: run it

Start the broker and the processor:

```bash
cd ../microdcs && docker compose up -d --wait mosquitto && cd ../microdcs-ping/src/Ping

APP_MicroDcs__CloudEventSource=urn:example:ping \
APP_MicroDcs__Bindings__ping__Prefix=dev \
APP_MicroDcs__Bindings__ping__ScopeLevels=2 \
dotnet run
```

In a second terminal, watch the topics and send a ping as a CloudEvent in binary content mode (attributes as MQTT user
properties):

```bash
docker exec -it microdcs-mosquitto-1 mosquitto_sub -V mqttv5 -t 'dev/#' -v -F '%t %P %p'

docker exec microdcs-mosquitto-1 mosquitto_pub -V mqttv5 -t dev/plant1/line2/commands \
  -D publish user-property specversion 1.0 \
  -D publish user-property id ping-1 \
  -D publish user-property source urn:example:client \
  -D publish user-property type com.example.ping.v1 \
  -D publish user-property subject plant1.line2/cell1 \
  -D publish content-type application/json \
  -m '{"Message":"hello"}'
```

The subscriber shows the pong on the `events` address of the same scope, because the request carried no response topic:

```text
dev/plant1/line2/events specversion:1.0 id:ec6a… source:urn:example:ping type:com.example.pong.v1
  datacontenttype:application/json; charset=utf-8 subject:plant1.line2/cell1 … causationid:ping-1 {"Message":"hello","Length":5}
```

Things to try:

* Send the same `id` again. The pipeline recognises the duplicate and does not run the handler a second time once Valkey
  is configured (`APP_MicroDcs__Valkey__ConnectionString=localhost:6379` after `docker compose up -d --wait valkey`);
  without Valkey the dedupe store is in memory.
* Send the ping with `subject` `other.scope/cell1` to `dev/plant1/line2/commands`. The scope does not match the address
  and the message is dropped.
* Send an unknown `type`. Without a `[Fallback]` handler the message is dropped; a request with a response topic gets an
  error event `io.microdcs.error.v1`.
* Add `-D publish response-topic dev-client/inbox` to the ping: the pong goes there instead of `events`.

## Step 6: test it

`tests/Ping.Tests/Ping.Tests.csproj` references the processor project, `MicroDCS.Testing`, `xunit.v3.mtp-v2` and
`Shouldly` (with `<OutputType>Exe</OutputType>` for Microsoft.Testing.Platform). `ProcessorHarness` runs the real
lifecycle and pipeline on the in-memory transport with a fake clock:

```csharp
using MicroDCS.Core.Transport;
using MicroDCS.Testing;
using Ping;
using Shouldly;
using Xunit;

namespace Ping.Tests;

public sealed class PingProcessorTests
{
    [Fact]
    public async Task A_ping_is_answered_with_a_pong()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var harness = await ProcessorHarness.StartAsync<PingProcessor>(cancellationToken: ct);

        var result = await harness.SendAsync(new PingRequest("hello"), subject: "plant1.line2/cell1", ct);

        result.Outcome.ShouldBe(DeliveryOutcome.Ack);
        var pong = harness.Decode<PongResponse>(result.Published.ShouldHaveSingleItem());
        pong.ShouldBe(new PongResponse("hello", 5));
        result.Published[0].Envelope.Subject.ShouldBe("plant1.line2/cell1");
    }
}
```

`harness.Time` is a `FakeTimeProvider`: advance it to fire expiry timers without waiting.

## Going further

**Commands to equipment.** A southbound processor sends commands. An `[Outgoing(typeof(T))]` factory builds the
payload from a `CommandRequest` (scope, job id, action key, recipe parameters); the SFC engine calls it for a
`push_command` action. `[OnResponse]` handles the equipment's answer and may return an `ActionOutcome` that a recipe
transition names. `[OnExpired]` runs when a command expires without an answer. The binding then needs a `ReplyBase`
for the instance's reply address. See the greetings demo in
[`src/MicroDCS.App/Greetings`](https://github.com/aschamberger/microdcs/tree/main/src/MicroDCS.App/Greetings).

**Events from application code.** Inject `IEventPublisher` and call it from a handler, a lifecycle hook or a hosted
service. A full outgoing queue throws `OutboundQueueFullException` instead of blocking.

**Attributes instead of body fields.** A string property with `[CarriedAs("name")]` and `[JsonIgnore]` travels as a
CloudEvent extension attribute and is copied from a request to a reply that declares the same name.

**Lifecycle hooks.** `[OnInitialize]` runs before the transports start, `[OnStarted]` after; with `Singleton = true`
only one replica runs it (a lease in Valkey). `[OnShutdown]` runs after the transports stopped. The Machinery Jobs
processor publishes its retained metadata event this way.

**Native AOT.** `dotnet publish -c Release -r linux-x64` produces a single native binary. The trim and AOT analyzers run
on every build; a processor that needs reflection fails at build time, not in production.

**Next pages.** [Processors](processors-dotnet.md) for every attribute, parameter and diagnostic,
[MQTT transport](mqtt-transport.md) for topics and delivery, [SFC Engine](sfc-engine.md) for recipes, and
[Machinery Jobs](job-control.md#machinery-jobs-processor) for a production processor with Valkey state.
