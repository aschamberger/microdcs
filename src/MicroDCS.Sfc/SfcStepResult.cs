using System.Text.Json.Serialization;

namespace MicroDCS.Sfc;

/// <summary>What the host reports to the northbound state machine; a verdict, never a policy (D-17).</summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "kind")]
[JsonDerivedType(typeof(SfcDefer), "defer")]
[JsonDerivedType(typeof(SfcComplete), "complete")]
[JsonDerivedType(typeof(SfcFault), "fault")]
[JsonDerivedType(typeof(SfcCancelled), "cancelled")]
public abstract record SfcContinuation;

/// <summary>The job waits for more events.</summary>
public sealed record SfcDefer : SfcContinuation;

/// <summary>All steps are done.</summary>
public sealed record SfcComplete : SfcContinuation
{
    /// <summary>Gets the outcome of the job; <c>completed</c>.</summary>
    [JsonPropertyName("outcome")]
    public required string Outcome { get; init; }
}

/// <summary>The job cannot go on. The host decides whether to abort or re-dispatch.</summary>
public sealed record SfcFault : SfcContinuation
{
    /// <summary>Gets the code: <c>timeout</c>, <c>error</c>, <c>no_transition</c> or <c>propagation_limit</c>.</summary>
    [JsonPropertyName("code")]
    public required string Code { get; init; }

    /// <summary>Gets the message; names and identifiers only.</summary>
    [JsonPropertyName("message")]
    public required string Message { get; init; }
}

/// <summary>The job ended on request (additive to the three continuations of D-17).</summary>
public sealed record SfcCancelled : SfcContinuation;

/// <summary>What the core did with an event.</summary>
public enum SfcDisposition
{
    /// <summary>The event changed the state.</summary>
    Applied,

    /// <summary>The event was a duplicate or late and changed nothing (D-17).</summary>
    Absorbed,

    /// <summary>The event contradicts the state (unknown action, wrong type, never dispatched) and was refused, never matched to a sibling.</summary>
    Rejected,
}

/// <summary>The result of one step: the next state, the continuation and the commands to apply in order.</summary>
public sealed record SfcStepResult
{
    /// <summary>Gets the next state (the given one when the event was absorbed or rejected).</summary>
    [JsonPropertyName("state")]
    public required SfcState State { get; init; }

    /// <summary>Gets the continuation.</summary>
    [JsonPropertyName("continuation")]
    public required SfcContinuation Continuation { get; init; }

    /// <summary>Gets the commands, in order.</summary>
    [JsonPropertyName("commands")]
    public IReadOnlyList<SfcCommand> Commands { get; init; } = [];

    /// <summary>Gets what the core did with the event.</summary>
    [JsonPropertyName("disposition")]
    public SfcDisposition Disposition { get; init; }

    /// <summary>Gets the reason of an absorbed or rejected event; names only.</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; init; }
}

/// <summary>The host's view at the time of the call (the fourth input of the step function).</summary>
public sealed record SfcSnapshot
{
    /// <summary>The attempt limit when the recipe does not set one (<c>Sfc:MaxAttempts</c>, D-44).</summary>
    public const int DefaultMaxAttemptsValue = 3;

    /// <summary>Gets a snapshot with the default attempt limit and no live work.</summary>
    public static SfcSnapshot Default { get; } = new();

    /// <summary>Gets the attempt limit for push commands whose action sets none.</summary>
    [JsonPropertyName("defaultMaxAttempts")]
    public int DefaultMaxAttempts { get; init; } = DefaultMaxAttemptsValue;

    /// <summary>Gets the action keys whose work the host's ledger still holds (outbox entries not yet executed).</summary>
    [JsonPropertyName("liveWork")]
    public IReadOnlyList<string> LiveWork { get; init; } = [];
}
