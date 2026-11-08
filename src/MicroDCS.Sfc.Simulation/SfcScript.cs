namespace MicroDCS.Sfc.Simulation;

/// <summary>How scripted equipment answers one attempt of an action.</summary>
public enum SfcReplyKind
{
    /// <summary>Answers with an outcome.</summary>
    Outcome,

    /// <summary>Reports a failure (the reserved outcome <c>error</c>).</summary>
    Error,

    /// <summary>Does not answer; the deadline expires.</summary>
    Silent,
}

/// <summary>One scripted answer.</summary>
/// <param name="Kind">What the equipment does.</param>
/// <param name="Outcome">The outcome name for <see cref="SfcReplyKind.Outcome"/>.</param>
/// <param name="Error">The error text for <see cref="SfcReplyKind.Error"/>.</param>
/// <param name="DelayMs">The virtual time the equipment needs.</param>
public sealed record SfcReply(SfcReplyKind Kind, string? Outcome = null, string? Error = null, long DelayMs = 0)
{
    /// <summary>Creates an answer with an outcome.</summary>
    /// <param name="outcome">The outcome name.</param>
    /// <param name="delayMs">The delay in virtual milliseconds.</param>
    /// <returns>The reply.</returns>
    public static SfcReply Ok(string outcome = "ok", long delayMs = 0) => new(SfcReplyKind.Outcome, outcome, null, delayMs);

    /// <summary>Creates a failure answer.</summary>
    /// <param name="error">The error text.</param>
    /// <param name="delayMs">The delay in virtual milliseconds.</param>
    /// <returns>The reply.</returns>
    public static SfcReply Fail(string error = "failed", long delayMs = 0) => new(SfcReplyKind.Error, null, error, delayMs);

    /// <summary>Creates a missing answer.</summary>
    /// <returns>The reply.</returns>
    public static SfcReply Silence() => new(SfcReplyKind.Silent);
}

/// <summary>
/// What the simulated equipment does: per action a list of answers, one per time the equipment is asked (re-dispatches and loop iterations count; the last one repeats), a default for the
/// rest, and events that arrive from outside at fixed times.
/// </summary>
public sealed class SfcScript
{
    private readonly Dictionary<string, IReadOnlyList<SfcReply>> _replies = new(StringComparer.Ordinal);
    private readonly List<SfcEvent> _injected = [];

    /// <summary>Gets or sets the answer for actions without a script; the default is <c>ok</c> without delay.</summary>
    public SfcReply Default { get; set; } = SfcReply.Ok();

    /// <summary>Gets the events injected from outside, with their own times.</summary>
    public IReadOnlyList<SfcEvent> Injected => _injected;

    /// <summary>Scripts the answers of an action, one per time the equipment is asked.</summary>
    /// <param name="action">The action name.</param>
    /// <param name="replies">The answers.</param>
    /// <returns>This script.</returns>
    public SfcScript For(string action, params SfcReply[] replies)
    {
        ArgumentException.ThrowIfNullOrEmpty(action);
        _replies[action] = replies;
        return this;
    }

    /// <summary>Adds an event that arrives from outside at <see cref="SfcEvent.At"/>.</summary>
    /// <param name="input">The event.</param>
    /// <returns>This script.</returns>
    public SfcScript Inject(SfcEvent input)
    {
        ArgumentNullException.ThrowIfNull(input);
        _injected.Add(input);
        return this;
    }

    /// <summary>Gets the reply for the n-th time the equipment is asked about an action.</summary>
    /// <param name="action">The action name.</param>
    /// <param name="ordinal">How often the equipment was asked, starting at 1.</param>
    /// <returns>The scripted reply, the last one if the list is shorter, or <see cref="Default"/>.</returns>
    public SfcReply ReplyFor(string action, int ordinal) =>
        _replies.TryGetValue(action, out IReadOnlyList<SfcReply>? list) && list.Count > 0
            ? list[Math.Min(Math.Max(ordinal, 1), list.Count) - 1]
            : Default;
}
