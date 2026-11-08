using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Authoring;

/// <summary>Overrides for a published event. Everything not set comes from the pipeline (source, id, expiry, correlation).</summary>
public sealed record PublishOptions
{
    /// <summary>The subject; its scope also selects the address.</summary>
    public string? Subject { get; init; }

    /// <summary>The correlation id; a new one is created when absent.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>The causation id.</summary>
    public string? CausationId { get; init; }

    /// <summary>Stable key of a logical command.</summary>
    public string? ActionKey { get; init; }

    /// <summary>Publishes on this binding only. By default the event goes to every binding whose processor publishes the intent and whose outgoing type filter accepts the type.</summary>
    public string? BindingId { get; init; }

    /// <summary>Overrides the event id (stable command id for re-dispatch, D-9).</summary>
    public string? EventId { get; init; }

    /// <summary>The method hint (<c>PUT</c> or <c>DELETE</c>).</summary>
    public string? Method { get; init; }

    /// <summary>Keep as last value where the transport has a per-message retain flag (MQTT). Other transports ignore it with a warning; use <c>IStateTopicPort</c> for durable last values (D-47).</summary>
    public bool Retain { get; init; }
}

/// <summary>Publishes application-initiated events (J-2, J-3). Replaces <c>publish_event</c> and <c>trigger_outgoing_event</c>.</summary>
public interface IEventPublisher
{
    /// <summary>Enqueues an event on the outgoing queue of the processor's binding.</summary>
    /// <typeparam name="TPayload">The payload type; it has <see cref="CloudEventTypeAttribute"/>.</typeparam>
    /// <param name="payload">The payload.</param>
    /// <param name="intent">The intent (selects the address).</param>
    /// <param name="options">Overrides.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task that completes when the event is queued.</returns>
    /// <exception cref="OutboundQueueFullException">The bounded queue is full (D-28).</exception>
    ValueTask PublishAsync<TPayload>(TPayload payload, Intent intent, PublishOptions? options, CancellationToken cancellationToken)
        where TPayload : notnull;
}

/// <summary>The bounded outgoing queue of a binding is full. The enqueue fails instead of blocking silently (D-28).</summary>
public sealed class OutboundQueueFullException : InvalidOperationException
{
    /// <summary>Creates the exception.</summary>
    /// <param name="bindingId">The binding.</param>
    public OutboundQueueFullException(string bindingId)
        : base($"The outgoing queue of binding '{bindingId}' is full. Raise the queue size or apply backpressure.")
    {
    }

    /// <summary>Creates the exception.</summary>
    public OutboundQueueFullException()
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">The message.</param>
    /// <param name="innerException">The cause.</param>
    public OutboundQueueFullException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
