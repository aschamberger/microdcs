using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Time.Testing;

namespace MicroDCS.Testing;

/// <summary>What one send call of the <see cref="ProcessorHarness"/> produced.</summary>
/// <param name="Outcome">What the pipeline asked the transport to do with the delivery.</param>
/// <param name="Published">The messages published while handling it, oldest first.</param>
public sealed record HarnessResult(DeliveryOutcome Outcome, ImmutableArray<OutboundMessage> Published);

/// <summary>
/// Drives one processor through the real lifecycle and pipeline on the in-memory transport, with a fake clock. No broker
/// and no Valkey. Create it with <see cref="StartAsync{TProcessor}"/>.
/// </summary>
public sealed class ProcessorHarness : IAsyncDisposable
{
    private readonly ServiceProvider _provider;
    private readonly ProcessorDescriptor _descriptor;
    private readonly string _prefix;

    private ProcessorHarness(ServiceProvider provider, ProcessorDescriptor descriptor, string prefix)
    {
        _provider = provider;
        _descriptor = descriptor;
        _prefix = prefix;
        Transport = provider.GetRequiredService<InMemoryTransport>();
        Time = (FakeTimeProvider)provider.GetRequiredService<TimeProvider>();
    }

    /// <summary>The in-memory transport.</summary>
    public InMemoryTransport Transport { get; }

    /// <summary>The fake clock.</summary>
    public FakeTimeProvider Time { get; }

    /// <summary>The service provider, to resolve processor dependencies in assertions.</summary>
    public IServiceProvider Services => _provider;

    /// <summary>Builds the host services for a processor and runs the start lifecycle.</summary>
    /// <typeparam name="TProcessor">The processor under test.</typeparam>
    /// <param name="configure">Registers the processor's dependencies, fakes and filters.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The started harness.</returns>
    public static async Task<ProcessorHarness> StartAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TProcessor>(Action<IServiceCollection>? configure = null, CancellationToken cancellationToken = default)
        where TProcessor : class, IDescribedProcessor
    {
        var descriptor = TProcessor.Describe();
        const string Prefix = "test";
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MicroDcs:CloudEventSource"] = "urn:test:processor",
            ["MicroDcs:InstanceId"] = "test-instance",
            [$"MicroDcs:Bindings:{descriptor.BindingId}:Transport"] = "memory",
            [$"MicroDcs:Bindings:{descriptor.BindingId}:Prefix"] = Prefix,
            [$"MicroDcs:Bindings:{descriptor.BindingId}:ScopeLevels"] = "3",
        }).Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<TimeProvider>(new FakeTimeProvider(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)));
        services.AddMicroDcs(configuration).AddInMemoryTransport().AddProcessor<TProcessor>();
        configure?.Invoke(services);

        var provider = services.BuildServiceProvider();
        var harness = new ProcessorHarness(provider, descriptor, Prefix);
        foreach (var hosted in provider.GetServices<IHostedService>())
        {
            await hosted.StartAsync(cancellationToken).ConfigureAwait(false);
        }

        return harness;
    }

    /// <summary>Sends a payload as a request to the processor.</summary>
    /// <typeparam name="TPayload">The payload type; it needs a descriptor from a handler or <c>AddPayloads</c>.</typeparam>
    /// <param name="payload">The payload.</param>
    /// <param name="subject">The subject, for example <c>plant1.line2/resource</c>.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public Task<HarnessResult> SendAsync<TPayload>(TPayload payload, string? subject = null, CancellationToken cancellationToken = default)
        where TPayload : class => SendAsync(payload, subject, InboundKind.Request, replyTo: null, cancellationToken);

    /// <summary>Sends a payload as a response to a command this instance sent.</summary>
    /// <typeparam name="TPayload">The payload type.</typeparam>
    /// <param name="payload">The payload.</param>
    /// <param name="causationId">The id of the command it answers.</param>
    /// <param name="subject">The subject.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public Task<HarnessResult> SendResponseAsync<TPayload>(TPayload payload, string causationId, string? subject = null, CancellationToken cancellationToken = default)
        where TPayload : class => SendAsync(payload, subject, InboundKind.Response, replyTo: null, cancellationToken, causationId);

    /// <summary>Sends a request that carries a reply address, so replies go there.</summary>
    /// <typeparam name="TPayload">The payload type.</typeparam>
    /// <param name="payload">The payload.</param>
    /// <param name="replyTo">The reply address.</param>
    /// <param name="subject">The subject.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public Task<HarnessResult> SendWithReplyAsync<TPayload>(TPayload payload, ReplyAddress replyTo, string? subject = null, CancellationToken cancellationToken = default)
        where TPayload : class => SendAsync(payload, subject, InboundKind.Request, replyTo, cancellationToken);

    /// <summary>Sends a prebuilt envelope, for events without a handler or with attributes the payload does not model.</summary>
    /// <param name="envelope">The envelope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public Task<HarnessResult> SendEnvelopeAsync(EventEnvelope envelope, CancellationToken cancellationToken = default) =>
        DeliverAsync(envelope, InboundKind.Request, replyTo: null, cancellationToken);

    /// <summary>Sends a prebuilt envelope as a response, for example an error event from the peer.</summary>
    /// <param name="envelope">The envelope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result.</returns>
    public Task<HarnessResult> SendResponseEnvelopeAsync(EventEnvelope envelope, CancellationToken cancellationToken = default) =>
        DeliverAsync(envelope, InboundKind.Response, replyTo: null, cancellationToken);

    /// <summary>Decodes a published message into its payload.</summary>
    /// <typeparam name="TPayload">The payload type.</typeparam>
    /// <param name="message">The published message.</param>
    /// <returns>The payload.</returns>
    public TPayload Decode<TPayload>(OutboundMessage message)
        where TPayload : class
    {
        ArgumentNullException.ThrowIfNull(message);
        var registry = _provider.GetRequiredService<ProcessorRegistry>();
        if (!registry.TryGetDescriptor(typeof(TPayload), out var descriptor))
        {
            throw new InvalidOperationException($"No payload descriptor for '{typeof(TPayload).Name}'.");
        }

        return (TPayload)descriptor.Decode(message.Envelope.Data, message.Envelope.Extensions);
    }

    /// <summary>Runs the shutdown phases and releases the services.</summary>
    /// <returns>A task.</returns>
    public async ValueTask DisposeAsync()
    {
        foreach (var hosted in _provider.GetServices<IHostedService>().Reverse())
        {
            await hosted.StopAsync(CancellationToken.None).ConfigureAwait(false);
        }

        await _provider.DisposeAsync().ConfigureAwait(false);
    }

    private async Task<HarnessResult> SendAsync<TPayload>(TPayload payload, string? subject, InboundKind kind, ReplyAddress? replyTo, CancellationToken cancellationToken, string? causationId = null)
        where TPayload : class
    {
        var registry = _provider.GetRequiredService<ProcessorRegistry>();
        if (!registry.TryGetDescriptor(typeof(TPayload), out var descriptor))
        {
            throw new InvalidOperationException($"No payload descriptor for '{typeof(TPayload).Name}'. Register it with AddPayloads.");
        }

        var carried = new Dictionary<string, string>(StringComparer.Ordinal);
        var data = descriptor.Encode(payload, carried);
        var envelope = new EventEnvelope
        {
            Id = Guid.NewGuid().ToString(),
            Source = "urn:test:client",
            Type = descriptor.TypeId,
            DataContentType = descriptor.ContentType,
            DataSchema = descriptor.Schema,
            Subject = subject,
            CausationId = causationId,
            Time = Time.GetUtcNow(),
            Extensions = carried.ToImmutableDictionary(StringComparer.Ordinal),
            Data = data,
        };
        return await DeliverAsync(envelope, kind, replyTo, cancellationToken).ConfigureAwait(false);
    }

    private async Task<HarnessResult> DeliverAsync(EventEnvelope envelope, InboundKind kind, ReplyAddress? replyTo, CancellationToken cancellationToken)
    {
        var before = Transport.Published.Length;
        var scope = envelope.Subject is not null && LogicalAddress.TryGetScopePath(envelope.Subject, out var path) ? path : [];
        var intent = _descriptor.SubscribeIntents.IsDefaultOrEmpty ? Intent.Command : _descriptor.SubscribeIntents[0];
        var message = new InboundMessage
        {
            BindingId = _descriptor.BindingId,
            Kind = kind,
            Envelope = envelope,
            Address = new LogicalAddress(_prefix, scope, null, intent),
            ReplyTo = replyTo,
        };

        var outcome = await Transport.InjectAsync(message, cancellationToken).ConfigureAwait(false);
        return new HarnessResult(outcome, [.. Transport.Published.Skip(before)]);
    }
}
