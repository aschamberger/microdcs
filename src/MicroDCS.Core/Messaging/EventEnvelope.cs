using System.Collections.Frozen;

namespace MicroDCS.Core.Messaging;

/// <summary>
/// The transport-neutral CloudEvent as the pipeline and processors see it (D-14). It is a plain record; the
/// CloudEvents SDK is used only behind <see cref="IEnvelopeCodec"/>. No <c>object</c> data: the payload is bytes.
/// </summary>
public sealed record EventEnvelope
{
    /// <summary>Identifies the event. Unique per <see cref="Source"/>; a re-sent duplicate keeps the id. Not required to be a UUID (A-6).</summary>
    public required string Id { get; init; }

    /// <summary>The context in which the event happened (URI-reference).</summary>
    public required string Source { get; init; }

    /// <summary>The CloudEvent type, the key for handler dispatch.</summary>
    public required string Type { get; init; }

    /// <summary>The media type of <see cref="Data"/>.</summary>
    public string? DataContentType { get; init; }

    /// <summary>The schema the data adheres to.</summary>
    public string? DataSchema { get; init; }

    /// <summary>Subject in the producer's context. Grammar: <c>scope[/resource]</c>, scope is a dotted path (see <see cref="MicroDCS.Core.Transport.LogicalAddress"/>).</summary>
    public string? Subject { get; init; }

    /// <summary>When the occurrence happened.</summary>
    public DateTimeOffset? Time { get; init; }

    /// <summary>When the event was recorded by its producer.</summary>
    public DateTimeOffset? RecordedTime { get; init; }

    /// <summary>W3C <c>traceparent</c>.</summary>
    public string? TraceParent { get; init; }

    /// <summary>W3C <c>tracestate</c>.</summary>
    public string? TraceState { get; init; }

    /// <summary>The event is no longer useful after this instant.</summary>
    public DateTimeOffset? ExpiryTime { get; init; }

    /// <summary>The event is no longer useful after this interval, relative to <see cref="Time"/> or <see cref="RecordedTime"/>.</summary>
    public TimeSpan? ExpiryInterval { get; init; }

    /// <summary>Groups events of one logical flow.</summary>
    public string? CorrelationId { get; init; }

    /// <summary>The <see cref="Id"/> of the event that caused this one.</summary>
    public string? CausationId { get; init; }

    /// <summary>HTTP-style method hint: <c>PUT</c> (default when absent) or <c>DELETE</c>.</summary>
    public string? Method { get; init; }

    /// <summary>Stable key of a logical command, identical on every re-delivery (<c>{job}:{action}</c>).</summary>
    public string? ActionKey { get; init; }

    /// <summary>Error description when the event reports a failure.</summary>
    public EnvelopeError? Error { get; init; }

    /// <summary>Extension attributes that are not part of the model, including payload fields carried as attributes (E-4). Names never collide with standard attributes (E-3).</summary>
    public IReadOnlyDictionary<string, string> Extensions { get; init; } = FrozenDictionary<string, string>.Empty;

    /// <summary>The encoded payload.</summary>
    public ReadOnlyMemory<byte> Data { get; init; }
}

/// <summary>Machine readable error kinds on <see cref="EnvelopeError"/>.</summary>
public enum ErrorKind
{
    /// <summary>Unknown error.</summary>
    Unknown,

    /// <summary>No handler for the event type.</summary>
    UnknownPayloadType,

    /// <summary>The content type is not supported.</summary>
    UnsupportedContentType,

    /// <summary>The payload could not be decoded or is invalid.</summary>
    PayloadInvalid,

    /// <summary>A required attribute is missing.</summary>
    MissingProperty,

    /// <summary>An attribute value is invalid.</summary>
    PropertyInvalid,

    /// <summary>The system is in a state that does not allow the operation.</summary>
    StateInvalid,

    /// <summary>The operation timed out.</summary>
    Timeout,

    /// <summary>The scope check rejected the message.</summary>
    PermissionDenied,

    /// <summary>The operation was cancelled.</summary>
    Cancelled,

    /// <summary>A resource is exhausted.</summary>
    ResourceExhausted,

    /// <summary>A dependency is not available.</summary>
    ServiceUnavailable,

    /// <summary>The feature is not implemented.</summary>
    NotImplemented,

    /// <summary>An internal logic error.</summary>
    InternalLogicError,
}

/// <summary>Error attributes of an event: kind, message and context (timeouts, retry counts). Messages never contain payload values.</summary>
/// <param name="Kind">The machine readable kind.</param>
/// <param name="Message">A human readable explanation.</param>
/// <param name="Context">Further key/value pairs.</param>
public sealed record EnvelopeError(ErrorKind Kind, string Message, IReadOnlyDictionary<string, string>? Context = null);
