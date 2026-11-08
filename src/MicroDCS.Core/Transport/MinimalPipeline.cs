using System.Collections.Immutable;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Transport;

/// <summary>
/// The smallest pipeline that drives the processors (D-37, T-1): claim, scope check, decode, filters and handler, replies,
/// mark done. Outgoing queues, expiry timers, the activity source and inspection arrive with S10; this class is replaced
/// there and keeps the order of steps, which the tests pin.
/// </summary>
public sealed class MinimalPipeline : IInboundHandler
{
    private readonly ProcessorRegistry _registry;
    private readonly IReadOnlyDictionary<string, ITransport> _transports;
    private readonly IDedupeStore _dedupe;
    private readonly IActionSink _actions;
    private readonly IEnumerable<IProcessorFilter> _filters;
    private readonly IServiceProvider _services;
    private readonly MicroDcsOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<MinimalPipeline> _logger;
    private int _inFlight;

    /// <summary>Creates the pipeline.</summary>
    /// <param name="registry">The processors.</param>
    /// <param name="transports">The transports by name, used to publish replies.</param>
    /// <param name="dedupe">The dedupe store.</param>
    /// <param name="actions">The SFC seam.</param>
    /// <param name="filters">The processor filters.</param>
    /// <param name="services">Provider handlers resolve their processor and services from.</param>
    /// <param name="options">The options.</param>
    /// <param name="time">The clock.</param>
    /// <param name="logger">The logger.</param>
    public MinimalPipeline(
        ProcessorRegistry registry,
        IEnumerable<ITransport> transports,
        IDedupeStore dedupe,
        IActionSink actions,
        IEnumerable<IProcessorFilter> filters,
        IServiceProvider services,
        IOptions<MicroDcsOptions> options,
        TimeProvider time,
        ILogger<MinimalPipeline> logger)
    {
        _registry = registry;
        _transports = transports.ToDictionary(t => t.Name, StringComparer.Ordinal);
        _dedupe = dedupe;
        _actions = actions;
        _filters = filters;
        _services = services;
        _options = options.Value;
        _time = time;
        _logger = logger;
    }

    /// <summary>The number of messages being processed. The host waits for zero during shutdown.</summary>
    public int InFlight => Volatile.Read(ref _inFlight);

    /// <inheritdoc />
    public async ValueTask<DeliveryOutcome> HandleAsync(InboundMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        Interlocked.Increment(ref _inFlight);
        try
        {
            return await ProcessAsync(message, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    private async ValueTask<DeliveryOutcome> ProcessAsync(InboundMessage message, CancellationToken cancellationToken)
    {
        var envelope = message.Envelope;
        if (!_registry.TryGetProcessor(message.BindingId, out var processor) || !_options.Bindings.TryGetValue(message.BindingId, out var binding))
        {
            Log.MessageDropped(_logger, envelope.Id, message.BindingId, "no processor for the binding");
            return DeliveryOutcome.Drop;
        }

        if (!ScopePolicy.Allows(binding.ScopeCheck, binding.Level0Subjects, message.Address, envelope.Subject))
        {
            Log.MessageDropped(_logger, envelope.Id, message.BindingId, "scope check failed");
            return DeliveryOutcome.Drop;
        }

        var transport = _transports.GetValueOrDefault(binding.Transport);
        var dedupe = transport?.Profile.Guarantee == DeliveryGuarantee.AtLeastOnce;
        if (dedupe && await ClaimAsync(envelope, cancellationToken).ConfigureAwait(false) == ClaimState.Done)
        {
            Log.DuplicateAcknowledged(_logger, envelope.Id, message.BindingId);
            return DeliveryOutcome.Ack;
        }

        var outcome = await DispatchAsync(message, processor, transport, cancellationToken).ConfigureAwait(false);
        if (dedupe && outcome != DeliveryOutcome.Fault)
        {
            // Before the ack: a crash in between redelivers, it never loses the message (T-3).
            await _dedupe.MarkDoneAsync(envelope.Source, envelope.Id, cancellationToken).ConfigureAwait(false);
        }

        return outcome;
    }

    private async ValueTask<ClaimState> ClaimAsync(EventEnvelope envelope, CancellationToken cancellationToken)
    {
        var token = _options.InstanceId + ":" + Guid.NewGuid().ToString("N");
        while (true)
        {
            var state = await _dedupe.ClaimAsync(envelope.Source, envelope.Id, token, cancellationToken).ConfigureAwait(false);
            if (state != ClaimState.Busy)
            {
                return state;
            }

            // Another worker holds the lease; wait for it to finish or lapse, at least 100 ms (T-20).
            var remaining = await _dedupe.LeaseRemainingAsync(envelope.Source, envelope.Id, cancellationToken).ConfigureAwait(false);
            var wait = remaining > TimeSpan.FromMilliseconds(100) ? remaining : TimeSpan.FromMilliseconds(100);
            await Task.Delay(wait, _time, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask<DeliveryOutcome> DispatchAsync(InboundMessage message, ProcessorDescriptor processor, ITransport? transport, CancellationToken cancellationToken)
    {
        var envelope = message.Envelope;
        var kind = message.Kind == InboundKind.Response ? HandlerKind.Response : HandlerKind.Incoming;
        var handler = processor.Handlers.FirstOrDefault(h => h.Kind == kind && h.TypeId == envelope.Type)
            ?? processor.Handlers.FirstOrDefault(h => h.Kind == HandlerKind.Fallback);
        if (handler is null)
        {
            Log.MessageDropped(_logger, envelope.Id, message.BindingId, "unknown event type");
            return DeliveryOutcome.Drop;
        }

        object input;
        try
        {
            input = handler.Payload is null ? envelope : handler.Payload.Decode(envelope.Data, envelope.Extensions);
        }
        catch (PayloadException)
        {
            Log.MessageDropped(_logger, envelope.Id, message.BindingId, "payload could not be decoded");
            return DeliveryOutcome.Drop;
        }

        var context = new EventContext
        {
            EventId = envelope.Id,
            Source = envelope.Source,
            Type = envelope.Type,
            Subject = envelope.Subject,
            CorrelationId = envelope.CorrelationId,
            CausationId = envelope.CausationId,
            Method = envelope.Method,
            BindingId = message.BindingId,
            Address = message.Address,
            InstanceId = _options.InstanceId ?? string.Empty,
            Envelope = envelope,
        };

        try
        {
            var replies = await RunFiltersAsync(handler, context, input, cancellationToken).ConfigureAwait(false);
            await _actions.EventReceivedAsync(context, cancellationToken).ConfigureAwait(false);
            await PublishRepliesAsync(message, replies, transport, cancellationToken).ConfigureAwait(false);
            return DeliveryOutcome.Ack;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Log.HandlerFaulted(_logger, ex, envelope.Id, envelope.Type, message.BindingId);
            return DeliveryOutcome.Fault;
        }
    }

    private ValueTask<IReadOnlyList<Reply>> RunFiltersAsync(HandlerDescriptor handler, EventContext context, object input, CancellationToken cancellationToken)
    {
        HandlerContinuation next = ct => handler.Invoke(_services, context, input, ct);
        foreach (var filter in _filters.Reverse())
        {
            var inner = next;
            next = ct => filter.InvokeAsync(context, inner, ct);
        }

        return next(cancellationToken);
    }

    private async ValueTask PublishRepliesAsync(InboundMessage request, IReadOnlyList<Reply> replies, ITransport? transport, CancellationToken cancellationToken)
    {
        if (replies.Count == 0 || transport is null)
        {
            return;
        }

        foreach (var reply in replies)
        {
            var envelope = BuildEnvelope(request.Envelope, reply);
            var outbound = new OutboundMessage
            {
                BindingId = request.BindingId,
                Envelope = envelope,
                SendTo = request.ReplyTo,
                Address = request.ReplyTo is null && request.Address is { } address ? address with { Intent = Intent.Event } : null,
                Retain = reply.Options?.Retain ?? false,
            };
            await transport.PublishAsync(outbound, cancellationToken).ConfigureAwait(false);
        }
    }

    private EventEnvelope BuildEnvelope(EventEnvelope request, Reply reply)
    {
        var options = reply.Options;
        var now = _time.GetUtcNow();

        // Correlation and causation follow the request (P-6); a prebuilt envelope keeps what it already has.
        if (reply.Payload is EventEnvelope prebuilt)
        {
            return prebuilt with
            {
                Id = string.IsNullOrEmpty(prebuilt.Id) ? Guid.NewGuid().ToString() : prebuilt.Id,
                Source = string.IsNullOrEmpty(prebuilt.Source) ? _options.CloudEventSource! : prebuilt.Source,
                Subject = prebuilt.Subject ?? request.Subject,
                CorrelationId = prebuilt.CorrelationId ?? request.CorrelationId ?? Guid.NewGuid().ToString(),
                CausationId = prebuilt.CausationId ?? request.Id,
                Time = prebuilt.Time ?? now,
                RecordedTime = prebuilt.RecordedTime ?? now,
            };
        }

        if (!_registry.TryGetDescriptor(reply.Payload.GetType(), out var descriptor))
        {
            throw new InvalidOperationException($"No payload descriptor for reply type '{reply.Payload.GetType().Name}'. Register it with AddPayloads.");
        }

        var carried = new Dictionary<string, string>(StringComparer.Ordinal);
        var data = descriptor.Encode(reply.Payload, carried);

        // E-5: attributes the request carried are copied to the response when the response type declares them and the handler left them unset.
        foreach (var name in descriptor.CarriedNames)
        {
            if (!carried.ContainsKey(name) && request.Extensions.TryGetValue(name, out var value))
            {
                carried[name] = value;
            }
        }

        return new EventEnvelope
        {
            Id = options?.EventId ?? Guid.NewGuid().ToString(),
            Source = _options.CloudEventSource!,
            Type = descriptor.TypeId,
            DataContentType = descriptor.ContentType,
            DataSchema = descriptor.Schema,
            Subject = options?.Subject ?? request.Subject,
            Time = now,
            RecordedTime = now,
            ExpiryInterval = _options.MessageExpiry is { } expiry && expiry > TimeSpan.Zero ? expiry : null,
            CorrelationId = options?.CorrelationId ?? request.CorrelationId ?? Guid.NewGuid().ToString(),
            CausationId = options?.CausationId ?? request.Id,
            ActionKey = options?.ActionKey,
            Method = options?.Method,
            Extensions = carried.ToImmutableDictionary(StringComparer.Ordinal),
            Data = data,
        };
    }
}
