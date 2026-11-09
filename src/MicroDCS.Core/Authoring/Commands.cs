using System.Collections.Frozen;
using System.Text.Json;

namespace MicroDCS.Core.Authoring;

/// <summary>Input of an <see cref="OutgoingAttribute"/> factory: what a caller that knows only a type id can provide (P-13).</summary>
public sealed record CommandRequest
{
    /// <summary>The scope the command is for.</summary>
    public required string Scope { get; init; }

    /// <summary>The job the command belongs to, if any.</summary>
    public string? JobId { get; init; }

    /// <summary>Stable key of the logical command (<c>{job}:{action}</c>); becomes <c>mdcsactionkey</c>.</summary>
    public required string ActionKey { get; init; }

    /// <summary>The event id of this dispatch. Stable across re-dispatch only if the caller keeps it (D-9).</summary>
    public required string CommandId { get; init; }

    /// <summary>The correlation id of the flow (the job).</summary>
    public string? CorrelationId { get; init; }

    /// <summary>Free-form parameters from the recipe action.</summary>
    public IReadOnlyDictionary<string, JsonElement> Parameters { get; init; } = FrozenDictionary<string, JsonElement>.Empty;

    /// <summary>The wire expiry of this dispatch; <see langword="null"/> uses <c>MicroDcs:MessageExpiry</c> (D-18).</summary>
    public TimeSpan? ExpiryInterval { get; init; }
}

/// <summary>Dispatches a command by CloudEvent type id to the southbound processor that declares an <see cref="OutgoingAttribute"/> factory for it. Replaces the SFC engine's lookup of processors and payload classes.</summary>
public interface IOutgoingCommands
{
    /// <summary>Whether a processor can produce the type.</summary>
    /// <param name="typeId">The CloudEvent type id.</param>
    /// <returns><see langword="true"/> when a factory is registered.</returns>
    bool CanDispatch(string typeId);

    /// <summary>Builds the payload through the factory and publishes it as a command with the request's ids.</summary>
    /// <param name="typeId">The CloudEvent type id.</param>
    /// <param name="request">The request.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task that completes when the command is queued.</returns>
    /// <exception cref="OutboundQueueFullException">The outgoing queue is full (D-28).</exception>
    ValueTask DispatchAsync(string typeId, CommandRequest request, CancellationToken cancellationToken);
}

/// <summary>
/// The result a response handler reports for the command it answers (D-44): return it from an <see cref="OnResponseAttribute"/> handler, alone
/// or inside <see cref="Replies"/>. It is not published. A handler that returns none reports <see cref="DefaultName"/>.
/// </summary>
/// <param name="Name">The outcome name, one the <see cref="OutgoingAttribute"/> factory of the command declares.</param>
public sealed record ActionOutcome(string Name)
{
    /// <summary>The outcome of a response whose handler reports none.</summary>
    public const string DefaultName = "done";

    /// <summary>The reserved outcome of a reported failure; every action type has it (D-44).</summary>
    public const string ErrorName = "error";

    /// <summary>The outcome of a response whose handler reports none.</summary>
    public static ActionOutcome Done { get; } = new(DefaultName);

    /// <summary>The error text of a failure; stored with the action as data, never used for a decision.</summary>
    public string? Error { get; init; }

    /// <summary>Whether this is the reserved failure outcome.</summary>
    public bool IsFailure => string.Equals(Name, ErrorName, StringComparison.Ordinal);

    /// <summary>Creates a failure, which a recipe can route with the condition <c>error</c>.</summary>
    /// <param name="error">What went wrong; names and identifiers only.</param>
    /// <returns>The outcome.</returns>
    public static ActionOutcome Failed(string? error = null) => new(ErrorName) { Error = error };
}

/// <summary>A response to a command, as the pipeline reports it to the <see cref="IActionSink"/>.</summary>
public sealed record ActionResponse
{
    /// <summary>The id of the response event; repeated deliveries of one response carry the same id.</summary>
    public required string EventId { get; init; }

    /// <summary>The id of the command this answers (the response's causation id).</summary>
    public required string CommandId { get; init; }

    /// <summary>The action key if the responder echoed <c>mdcsactionkey</c>.</summary>
    public string? ActionKey { get; init; }

    /// <summary>The outcome.</summary>
    public required ActionOutcome Outcome { get; init; }
}

/// <summary>
/// The seam between the pipeline and the SFC (P-17). The pipeline reports responses and incoming events here; the SFC host implements it
/// (S19). Without an SFC the default implementation does nothing. Command expiry is not reported: the deadlines in the SFC state decide (D-18).
/// </summary>
public interface IActionSink
{
    /// <summary>A response handler ran for a command with this id. A failure leaves the message unacknowledged, so the response is delivered again.</summary>
    /// <param name="response">The response.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask CompleteAsync(ActionResponse response, CancellationToken cancellationToken);

    /// <summary>An incoming event may complete a waiting pull-event action (P-17).</summary>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask EventReceivedAsync(EventContext context, CancellationToken cancellationToken);
}
