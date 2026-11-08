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
/// The seam between the pipeline and the SFC (P-17). Southbound response handlers report command outcomes here; the
/// SFC host implements it (S19). Without an SFC the default implementation does nothing.
/// </summary>
public interface IActionSink
{
    /// <summary>A response to the command with this id arrived.</summary>
    /// <param name="commandId">The id (the response's causation id).</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask CompleteAsync(string commandId, CancellationToken cancellationToken);

    /// <summary>The command with this id expired without a response.</summary>
    /// <param name="commandId">The id of the expired command.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask FailAsync(string commandId, CancellationToken cancellationToken);

    /// <summary>An incoming event may complete a waiting pull-event action (P-17).</summary>
    /// <param name="context">The event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask EventReceivedAsync(EventContext context, CancellationToken cancellationToken);

    /// <summary>A scope was seen for the first time (P-17).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask ScopeDiscoveredAsync(string scope, CancellationToken cancellationToken);
}
