using BenchmarkDotNet.Attributes;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MicroDCS.Benchmarks;

/// <summary>
/// The envelope codec and the pipeline hot path on a transport that discards what it publishes (R-18). The dedupe store is a
/// no-op, so the numbers show the pipeline itself; Valkey round trips are measured by the integration environment, not here.
/// </summary>
[MemoryDiagnoser]
public class PipelineBenchmarks
{
    private ServiceProvider _provider = null!;
    private IHostedService _host = null!;
    private MessagePipeline _pipeline = null!;
    private CloudEventsEnvelopeCodec _codec = null!;
    private EnvelopeCarrier _carrier;
    private EventEnvelope _envelope = null!;
    private InboundMessage _message = null!;
    private int _counter;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MicroDcs:CloudEventSource"] = "urn:bench",
            ["MicroDcs:InstanceId"] = "bench",
            ["MicroDcs:Bindings:bench:Transport"] = "null",
            ["MicroDcs:Bindings:bench:Prefix"] = "bench",
            ["MicroDcs:Bindings:bench:ScopeLevels"] = "2",
        }).Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IDedupeStore, NullDedupeStore>();
        services.AddSingleton<ITransport, NullTransport>();
        services.AddMicroDcs(configuration).AddProcessor<BenchProcessor>();
        _provider = services.BuildServiceProvider();
        _host = _provider.GetServices<IHostedService>().Single();
        await _host.StartAsync(CancellationToken.None).ConfigureAwait(false);
        _pipeline = _provider.GetRequiredService<MessagePipeline>();

        _codec = new CloudEventsEnvelopeCodec();
        var carried = new Dictionary<string, string>();
        var payload = MicroDcsPayloads.MicroDCS_Benchmarks_Hello;
        _envelope = new EventEnvelope
        {
            Id = "bench-0",
            Source = "urn:bench:client",
            Type = payload.TypeId,
            DataContentType = payload.ContentType,
            Subject = "plant1.line2/res",
            Time = DateTimeOffset.UnixEpoch,
            CorrelationId = "c1",
            Data = payload.Encode(new Hello("Bob"), carried),
        };
        _carrier = _codec.Encode(_envelope);
        _message = new InboundMessage
        {
            BindingId = "bench",
            Kind = InboundKind.Request,
            Envelope = _envelope,
            Address = new LogicalAddress("bench", ["plant1", "line2"], null, Intent.Command),
        };
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        await _host.StopAsync(CancellationToken.None).ConfigureAwait(false);
        await _provider.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark]
    public bool DecodeEnvelope() => _codec.TryDecode(_carrier, out _, out _);

    [Benchmark]
    public EnvelopeCarrier EncodeEnvelope() => _codec.Encode(_envelope);

    [Benchmark]
    public async ValueTask<DeliveryOutcome> HandleHello()
    {
        // A new id per message, as on the wire; the rest of the envelope is reused.
        var message = _message with { Envelope = _envelope with { Id = "bench-" + Interlocked.Increment(ref _counter) } };
        return await _pipeline.HandleAsync(message, CancellationToken.None).ConfigureAwait(false);
    }

    private sealed class NullDedupeStore : IDedupeStore
    {
        public ValueTask<ClaimState> ClaimAsync(string source, string id, string token, CancellationToken cancellationToken) => ValueTask.FromResult(ClaimState.Claimed);

        public ValueTask MarkDoneAsync(string source, string id, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ReleaseAsync(string source, string id, string token, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<TimeSpan> LeaseRemainingAsync(string source, string id, CancellationToken cancellationToken) => ValueTask.FromResult(TimeSpan.Zero);
    }

    private sealed class NullTransport : ITransport
    {
        public string Name => "null";

        public DeliveryProfile Profile { get; } = new(DeliveryGuarantee.AtLeastOnce, NativeReply: true, NativeExpiry: false, StateTopics: false, SharedConsumption: false);

        public IAddressRenderer Addresses { get; } = new SlashAddressRenderer();

        public bool IsConnected => true;

        public ReplyAddress? GetReplyAddress(string bindingId, string instanceId) => new ReplyAddress("reply");

        public ValueTask StartAsync(IInboundHandler handler, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask StopReceivingAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask PublishAsync(OutboundMessage message, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask StopAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
