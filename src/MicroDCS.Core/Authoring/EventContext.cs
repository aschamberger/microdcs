using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;

namespace MicroDCS.Core.Authoring;

/// <summary>What a handler may know about the event it handles. No transport names or types (A-2).</summary>
public sealed record EventContext
{
    /// <summary>The event id.</summary>
    public required string EventId { get; init; }

    /// <summary>The event source.</summary>
    public required string Source { get; init; }

    /// <summary>The event type.</summary>
    public required string Type { get; init; }

    /// <summary>The subject.</summary>
    public string? Subject { get; init; }

    /// <summary>The correlation id.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>The id of the event that caused this one.</summary>
    public string? CausationId { get; init; }

    /// <summary>The method hint (<c>PUT</c> when absent).</summary>
    public string? Method { get; init; }

    /// <summary>The binding that received the event.</summary>
    public required string BindingId { get; init; }

    /// <summary>The logical address the event arrived on.</summary>
    public LogicalAddress? Address { get; init; }

    /// <summary>The id of this application instance.</summary>
    public required string InstanceId { get; init; }

    /// <summary>The envelope as received, for handlers that need attributes not listed here.</summary>
    public required EventEnvelope Envelope { get; init; }
}
