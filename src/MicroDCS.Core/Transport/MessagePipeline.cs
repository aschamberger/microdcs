using System.Collections.Immutable;
using System.Diagnostics;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Transport;

/// <summary>
/// The one transport-neutral pipeline (A-1, D-37). Inbound order (T-1): claim, expiry-timer cancel, scope check, decode and
/// dispatch, replies, mark done, then the transport acknowledges. Outbound: application events go through a bounded queue per
/// binding (D-28); commands that expect a response start a local expiry timer (P-12, D-9). One activity source continues the
/// W3C trace context; logs carry identifiers and sizes, never payloads (R-10).
/// </summary>
public sealed class MessagePipeline : IInboundHandler, IEventPublisher, IDisposable
{
    /// <summary>The CloudEvents type of the event a pipeline sends back when it drops a request that has a reply address.</summary>
    public const string ErrorEventType = "io.microdcs.error.v1";

    private readonly ProcessorRegistry _registry;
    private readonly Dictionary<string, ITransport> _transports;
    private readonly IDedupeStore _dedupe;
    private readonly IActionSink _actions;
    private readonly IEnumerable<IProcessorFilter> _filters;
    private readonly IServiceProvider _services;
    private readonly MicroDcsOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly ILogger _security;
    private readonly EnvelopeFactory _envelopes;
    private readonly ExpiryTracker _expiry;
    private OutboundQueues? _queues;
    private int _inFlight;

    /// <summary>Creates the pipeline.</summary>
    /// <param name="registry">The processors.</param>
    /// <param name="transports">The transports by name, used to publish.</param>
    /// <param name="dedupe">The dedupe store.</param>
    /// <param name="actions">The SFC seam.</param>
    /// <param name="filters">The processor filters.</param>
    /// <param name="services">Provider handlers resolve their processor and services from.</param>
    /// <param name="options">The options.</param>
    /// <param name="time">The clock.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public MessagePipeline(
        ProcessorRegistry registry,
        IEnumerable<ITransport> transports,
        IDedupeStore dedupe,
        IActionSink actions,
        IEnumerable<IProcessorFilter> filters,
        IServiceProvider services,
        IOptions<MicroDcsOptions> options,
        TimeProvider time,
        ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _registry = registry;
        _transports = transports.ToDictionary(t => t.Name, StringComparer.Ordinal);
        _dedupe = dedupe;
        _actions = actions;
        _filters = filters;
        _services = services;
        _options = options.Value;
        _time = time;
        _logger = loggerFactory.CreateLogger<MessagePipeline>();
        _security = loggerFactory.CreateLogger(PipelineTelemetry.SecurityCategory);
        _envelopes = new EnvelopeFactory(registry, options, time);
        _expiry = new ExpiryTracker(time);
    }

    /// <summary>The number of messages being processed. The host waits for zero during shutdown.</summary>
    public int InFlight => Volatile.Read(ref _inFlight);

    /// <summary>The number of commands waiting for a response or their expiry.</summary>
    public int PendingExpiries => _expiry.Pending;

    /// <summary>Starts the outgoing queues and their pumps. Called once, after the transports started.</summary>
    public void StartPublishing()
    {
        var queues = new OutboundQueues(_options, _registry.Processors.Select(p => p.BindingId));
        queues.Start(PublishQueuedAsync, OnPumpFault);
        _queues = queues;
    }

    /// <summary>Shutdown phase 3: waits until the outgoing queues are empty. New events are refused afterwards.</summary>
    /// <param name="cancellationToken">Cancelled when the grace period ends.</param>
    /// <returns>A task.</returns>
    public ValueTask DrainOutgoingAsync(CancellationToken cancellationToken) =>
        _queues?.DrainAsync(cancellationToken) ?? ValueTask.CompletedTask;

    /// <summary>Shutdown phase 5: waits for the pending expiry timers; when the token is cancelled the rest is cancelled.</summary>
    /// <param name="cancellationToken">Cancelled when the grace period ends.</param>
    /// <returns>A task.</returns>
    public ValueTask WaitForExpiriesAsync(CancellationToken cancellationToken) => _expiry.WaitAsync(cancellationToken);

    /// <inheritdoc />
    public void Dispose()
    {
        _queues?.Dispose();
        _expiry.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask<DeliveryOutcome> HandleAsync(InboundMessage message, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        Interlocked.Increment(ref _inFlight);
        using var activity = StartActivity(message);
        try
        {
            var result = await ProcessAsync(message, cancellationToken).ConfigureAwait(false);
            PipelineTelemetry.Received.Add(1, new KeyValuePair<string, object?>("outcome", result.ToString()));
            if (result is Result.Dropped or Result.Faulted)
            {
                activity?.SetStatus(ActivityStatusCode.Error);
            }

            return result switch
            {
                Result.Faulted => DeliveryOutcome.Fault,
                Result.Dropped => DeliveryOutcome.Drop,
                _ => DeliveryOutcome.Ack,
            };
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    /// <inheritdoc />
    public ValueTask PublishAsync<TPayload>(TPayload payload, Intent intent, PublishOptions? options, CancellationToken cancellationToken)
        where TPayload : notnull
    {
        ArgumentNullException.ThrowIfNull(payload);
        var queues = _queues ?? throw new InvalidOperationException("The pipeline is not publishing yet: events can be published after the host started.");
        var targets = _registry.Processors
            .Where(p => (options?.BindingId is null || p.BindingId == options.BindingId)
                && !p.PublishIntents.IsDefaultOrEmpty && p.PublishIntents.Contains(intent))
            .Select(p => p.BindingId)
            .ToList();
        if (targets.Count == 0)
        {
            // T-23: publishing an intent no processor declares is an error, not a silent fallback.
            throw new InvalidOperationException($"No binding publishes the intent '{intent}'" + (options?.BindingId is { } id ? $" on binding '{id}'." : "."));
        }

        var envelope = _envelopes.Build(payload, options, request: null);
        foreach (var target in targets)
        {
            if (!queues.Accepts(target, envelope.Type))
            {
                Log.OutgoingFiltered(_logger, envelope.Id, envelope.Type, target);
                continue;
            }

            queues.Enqueue(target, CreateOutbound(target, envelope, intent, options?.Retain ?? false));
        }

        return ValueTask.CompletedTask;
    }

    private OutboundMessage CreateOutbound(string bindingId, EventEnvelope envelope, Intent intent, bool retain)
    {
        var binding = _options.Bindings[bindingId];
        var scope = ImmutableArray<string>.Empty;
        if (envelope.Subject is not null && !LogicalAddress.TryGetScopePath(envelope.Subject, out scope))
        {
            // D-38: an invalid scope segment makes the event unpublishable; it never falls back to an address without scope.
            throw new InvalidOperationException($"The subject of event '{envelope.Id}' has a scope segment that is not valid in an address (D-38).");
        }

        var transport = _transports.GetValueOrDefault(binding.Transport);
        return new OutboundMessage
        {
            BindingId = bindingId,
            Envelope = envelope,
            Address = new LogicalAddress(binding.Prefix!, scope, binding.Discriminator, intent),
            // T-9: a command carries this instance's reply address so the response comes back here (D-9).
            ReplyTo = intent == Intent.Command ? transport?.GetReplyAddress(bindingId, _options.InstanceId ?? string.Empty) : null,
            Retain = retain,
        };
    }

    private async ValueTask PublishQueuedAsync(OutboundMessage message, CancellationToken cancellationToken)
    {
        var binding = _options.Bindings[message.BindingId];
        var transport = _transports[binding.Transport];
        var envelope = message.Envelope;
        var expires = message.ReplyTo is not null && envelope.ExpiryInterval is { } interval && interval > TimeSpan.Zero;

        // The timer starts before the publish: a fast response must find it and cancel it.
        if (expires)
        {
            _expiry.Start(envelope.Id, envelope.ExpiryInterval!.Value, ct => HandleExpiredAsync(message, ct));
        }

        try
        {
            await transport.PublishAsync(message, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            if (expires)
            {
                _expiry.Cancel(envelope.Id);
            }

            throw;
        }

        PipelineTelemetry.Published.Add(1);
        Log.Published(_logger, envelope.Id, envelope.Type, message.BindingId);
    }

    private async ValueTask HandleExpiredAsync(OutboundMessage command, CancellationToken cancellationToken)
    {
        var envelope = command.Envelope;
        PipelineTelemetry.Expired.Add(1);
        Log.CommandExpired(_logger, envelope.Id, envelope.ExpiryInterval ?? TimeSpan.Zero);
        if (!_registry.TryGetProcessor(command.BindingId, out var processor))
        {
            return;
        }

        var handler = processor.Handlers.FirstOrDefault(h => h.Kind == HandlerKind.Expired && h.TypeId == envelope.Type);
        if (handler?.Payload is null)
        {
            return;
        }

        try
        {
            var input = handler.Payload.Decode(envelope.Data, envelope.Extensions);
            var replies = await handler.Invoke(_services, BuildContext(envelope, command.BindingId, command.Address), input, cancellationToken).ConfigureAwait(false);
            foreach (var reply in replies)
            {
                if (_queues is null or { IsClosed: true })
                {
                    // Shutdown phase 3 closed the queues before the timers ended (D-37): follow-up events are not published.
                    Log.ExpiryFollowUpDropped(_logger, envelope.Id);
                    break;
                }

                var follow = _envelopes.Build(reply.Payload, reply.Options, envelope);
                _queues?.Enqueue(command.BindingId, CreateOutbound(command.BindingId, follow, Intent.Event, reply.Options?.Retain ?? false));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // As in Python (P-12): a failing expiry handler is logged; it does not end the host.
            Log.ExpiryFaulted(_logger, ex, envelope.Id);
        }
    }

    private void OnPumpFault(string bindingId, Exception exception)
    {
        Log.PublishFaulted(_logger, exception, bindingId);
        _services.GetService<IHostApplicationLifetime>()?.StopApplication();
    }

    private static Activity? StartActivity(InboundMessage message)
    {
        var envelope = message.Envelope;
        ActivityContext parent = default;
        if (envelope.TraceParent is { } traceParent)
        {
            ActivityContext.TryParse(traceParent, envelope.TraceState, isRemote: true, out parent);
        }

        var activity = PipelineTelemetry.Source.StartActivity("process " + envelope.Type, ActivityKind.Consumer, parent);
        if (activity is not null)
        {
            activity.SetTag("messaging.operation.type", "process");
            activity.SetTag("messaging.message.id", envelope.Id);
            activity.SetTag("microdcs.binding", message.BindingId);
            activity.SetTag("cloudevents.type", envelope.Type);
        }

        return activity;
    }

    private async ValueTask<Result> ProcessAsync(InboundMessage message, CancellationToken cancellationToken)
    {
        var envelope = message.Envelope;
        if (!_registry.TryGetProcessor(message.BindingId, out var processor) || !_options.Bindings.TryGetValue(message.BindingId, out var binding))
        {
            Log.MessageDropped(_logger, envelope.Id, message.BindingId, "no processor for the binding");
            return Result.Dropped;
        }

        var transport = _transports.GetValueOrDefault(binding.Transport);
        var dedupe = transport?.Profile.Guarantee == DeliveryGuarantee.AtLeastOnce;
        if (dedupe && await ClaimAsync(envelope, cancellationToken).ConfigureAwait(false) == ClaimState.Done)
        {
            Log.DuplicateAcknowledged(_logger, envelope.Id, message.BindingId);
            return Result.Duplicate;
        }

        // A response (or anything that names a pending command as its cause) ends that command's timer (T-1).
        if (envelope.CausationId is { } causation)
        {
            _expiry.Cancel(causation);
        }

        Step step;
        if (!ScopePolicy.Allows(binding.ScopeCheck, binding.Level0Subjects, message.Address, envelope.Subject))
        {
            Log.ScopeRejected(_security, envelope.Id, message.BindingId);
            step = new Step(Result.Dropped, ErrorKind.PermissionDenied, "The subject does not match the address scope.");
        }
        else
        {
            step = await DispatchAsync(message, processor, transport, cancellationToken).ConfigureAwait(false);
        }

        if (step.Result == Result.Dropped && step.Kind is { } kind)
        {
            await ReplyWithErrorAsync(message, transport, kind, step.Text!, cancellationToken).ConfigureAwait(false);
        }

        if (step.Result == Result.Faulted)
        {
            StopUnlessRedeliverable(message.BindingId, transport);
            return Result.Faulted;
        }

        if (dedupe)
        {
            // Before the ack: a crash in between redelivers, it never loses the message (T-3).
            await _dedupe.MarkDoneAsync(envelope.Source, envelope.Id, cancellationToken).ConfigureAwait(false);
        }

        return step.Result;
    }

    private void StopUnlessRedeliverable(string bindingId, ITransport? transport)
    {
        // D-27, T-19: a fault ends the host unless the transport can nack and have the broker redeliver in this session.
        if (transport?.Profile.CanRedeliver == true)
        {
            return;
        }

        Log.StoppingOnFault(_logger, bindingId, transport?.Name ?? "none");
        _services.GetService<IHostApplicationLifetime>()?.StopApplication();
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

    private async ValueTask<Step> DispatchAsync(InboundMessage message, ProcessorDescriptor processor, ITransport? transport, CancellationToken cancellationToken)
    {
        var envelope = message.Envelope;
        var kind = message.Kind == InboundKind.Response ? HandlerKind.Response : HandlerKind.Incoming;
        var handler = processor.Handlers.FirstOrDefault(h => h.Kind == kind && h.TypeId == envelope.Type)
            ?? processor.Handlers.FirstOrDefault(h => h.Kind == HandlerKind.Fallback);
        if (handler is null)
        {
            Log.MessageDropped(_logger, envelope.Id, message.BindingId, "unknown event type");
            return new Step(Result.Dropped, ErrorKind.UnknownPayloadType, "No handler for the event type.");
        }

        object input;
        try
        {
            input = handler.Payload is null ? envelope : handler.Payload.Decode(envelope.Data, envelope.Extensions);
        }
        catch (PayloadException)
        {
            Log.MessageDropped(_logger, envelope.Id, message.BindingId, "payload could not be decoded");
            return new Step(Result.Dropped, ErrorKind.PayloadInvalid, "The payload could not be decoded.");
        }

        var context = BuildContext(envelope, message.BindingId, message.Address);
        try
        {
            var replies = await RunFiltersAsync(handler, context, input, cancellationToken).ConfigureAwait(false);
            await _actions.EventReceivedAsync(context, cancellationToken).ConfigureAwait(false);
            await PublishRepliesAsync(message, replies, transport, cancellationToken).ConfigureAwait(false);
            return new Step(Result.Processed);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Log.HandlerFaulted(_logger, ex, envelope.Id, envelope.Type, message.BindingId);
            return new Step(Result.Faulted);
        }
    }

    private EventContext BuildContext(EventEnvelope envelope, string bindingId, LogicalAddress? address) => new()
    {
        EventId = envelope.Id,
        Source = envelope.Source,
        Type = envelope.Type,
        Subject = envelope.Subject,
        CorrelationId = envelope.CorrelationId,
        CausationId = envelope.CausationId,
        Method = envelope.Method,
        BindingId = bindingId,
        Address = address,
        InstanceId = _options.InstanceId ?? string.Empty,
        Envelope = envelope,
    };

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
            var envelope = _envelopes.Build(reply.Payload, reply.Options, request.Envelope);
            await SendReplyAsync(request, transport, envelope, reply.Options?.Retain ?? false, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask ReplyWithErrorAsync(InboundMessage request, ITransport? transport, ErrorKind kind, string text, CancellationToken cancellationToken)
    {
        // Only a request that asked for an answer gets one. Best effort: the message is dropped either way.
        if (transport is null || request.ReplyTo is null || request.Kind != InboundKind.Request)
        {
            return;
        }

        var now = _time.GetUtcNow();
        var envelope = new EventEnvelope
        {
            Id = Guid.NewGuid().ToString(),
            Source = _options.CloudEventSource!,
            Type = ErrorEventType,
            Subject = request.Envelope.Subject,
            Time = now,
            RecordedTime = now,
            CorrelationId = request.Envelope.CorrelationId ?? request.Envelope.Id,
            CausationId = request.Envelope.Id,
            Error = new EnvelopeError(kind, text),
        };
        try
        {
            await SendReplyAsync(request, transport, envelope, retain: false, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Log.PublishFaulted(_logger, ex, request.BindingId);
        }
    }

    private static async ValueTask SendReplyAsync(InboundMessage request, ITransport transport, EventEnvelope envelope, bool retain, CancellationToken cancellationToken)
    {
        var outbound = new OutboundMessage
        {
            BindingId = request.BindingId,
            Envelope = envelope,
            SendTo = request.ReplyTo,
            Address = request.ReplyTo is null && request.Address is { } address ? address with { Intent = Intent.Event } : null,
            Retain = retain,
        };
        await transport.PublishAsync(outbound, cancellationToken).ConfigureAwait(false);
        PipelineTelemetry.Published.Add(1);
    }

    private enum Result
    {
        Processed,
        Duplicate,
        Dropped,
        Faulted,
    }

    private readonly record struct Step(Result Result, ErrorKind? Kind = null, string? Text = null);
}
