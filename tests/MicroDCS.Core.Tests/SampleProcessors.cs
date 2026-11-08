using System.Text.Json.Serialization;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Tests;

[CloudEventType("test.hello.v1", Schema = "https://example.com/hello")]
public sealed record Hello(string Name)
{
    [CarriedAs("hiddenstr")]
    [JsonIgnore]
    public string? HiddenStr { get; init; }
}

[CloudEventType("test.bye.v1")]
public sealed record Bye(string Name);

[CloudEventType("test.ack.v1")]
public sealed record ByeAck(string Name);

[JsonSerializable(typeof(Hello))]
[JsonSerializable(typeof(Bye))]
[JsonSerializable(typeof(ByeAck))]
internal sealed partial class SampleContext : JsonSerializerContext;

public interface ILog
{
    void Add(string entry);
}

[Processor(ProcessorBinding.Southbound, Id = "sample")]
public sealed partial class SampleProcessor(ILog log)
{
    [Incoming]
    public IEnumerable<Hello> OnHello(Hello hello, [Scope] string scope)
    {
        log.Add("hello:" + hello.Name + ":" + scope);
        yield return hello with { };
        yield return new Hello("Alice");
    }

    [Incoming]
    public async Task<ByeAck?> OnBye(Bye bye, EventContext context, CancellationToken cancellationToken)
    {
        await Task.Yield();
        log.Add("bye:" + context.EventId);
        return bye.Name.Length == 0 ? null : new ByeAck(bye.Name);
    }

    [OnResponse]
    public void OnAck(ByeAck ack) => log.Add("ack:" + ack.Name);

    [Outgoing(typeof(Bye))]
    public static Bye CreateBye(CommandRequest request) => new(request.Scope);

    [Fallback]
    public void OnUnknown(EventEnvelope raw) => log.Add("fallback:" + raw.Type);

    [OnInitialize]
    public void Init() => log.Add("init");

    [OnStarted(Singleton = true)]
    public Task StartedAsync(CancellationToken cancellationToken)
    {
        log.Add("started");
        return Task.CompletedTask;
    }

    [OnShutdown]
    public ValueTask StopAsync()
    {
        log.Add("shutdown");
        return ValueTask.CompletedTask;
    }
}
