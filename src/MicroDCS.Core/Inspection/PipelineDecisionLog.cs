using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Inspection;

/// <summary>One recorded pipeline decision. Identifiers, type and timing only, never the payload (R-10).</summary>
/// <param name="Sequence">Running number, starts at 1.</param>
/// <param name="At">When the decision was made.</param>
/// <param name="BindingId">The binding.</param>
/// <param name="EventId">The CloudEvent id.</param>
/// <param name="Source">The CloudEvent source.</param>
/// <param name="Type">The CloudEvent type.</param>
/// <param name="Outcome"><c>Processed</c>, <c>Duplicate</c>, <c>Dropped</c> or <c>Faulted</c>.</param>
/// <param name="Duration">Processing time.</param>
public sealed record PipelineDecision(long Sequence, DateTimeOffset At, string BindingId, string EventId, string Source, string Type, string Outcome, TimeSpan Duration);

/// <summary>
/// Fixed-size ring buffer of the last pipeline decisions (D-19). Its size is zero while the management listener is off, and
/// recording then does nothing, so inspection costs nothing on the hot path.
/// </summary>
public sealed class PipelineDecisionLog
{
    private readonly PipelineDecision?[] _slots;
    private long _count;

    /// <summary>Initializes a new instance of the <see cref="PipelineDecisionLog"/> class from the options.</summary>
    /// <param name="options">The core options.</param>
    public PipelineDecisionLog(IOptions<MicroDcsOptions> options)
        : this(CapacityFrom(options))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="PipelineDecisionLog"/> class.</summary>
    /// <param name="capacity">Number of decisions kept; zero keeps none.</param>
    public PipelineDecisionLog(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        _slots = new PipelineDecision?[capacity];
    }

    /// <summary>The number of decisions kept at most.</summary>
    public int Capacity => _slots.Length;

    /// <summary>Records a decision; does nothing when the capacity is zero.</summary>
    /// <param name="bindingId">The binding.</param>
    /// <param name="envelope">The envelope (only identifiers are kept).</param>
    /// <param name="outcome">The outcome name.</param>
    /// <param name="duration">Processing time.</param>
    /// <param name="at">The time.</param>
    public void Record(string bindingId, EventEnvelope envelope, string outcome, TimeSpan duration, DateTimeOffset at)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        if (_slots.Length == 0)
        {
            return;
        }

        var sequence = Interlocked.Increment(ref _count);
        Volatile.Write(ref _slots[(sequence - 1) % _slots.Length], new PipelineDecision(sequence, at, bindingId, envelope.Id, envelope.Source, envelope.Type, outcome, duration));
    }

    /// <summary>Returns the newest decisions first.</summary>
    /// <param name="max">Maximum number.</param>
    /// <returns>The decisions.</returns>
    public IReadOnlyList<PipelineDecision> Newest(int max)
    {
        var result = new List<PipelineDecision>();
        for (var i = 0; i < _slots.Length; i++)
        {
            if (Volatile.Read(ref _slots[i]) is { } decision)
            {
                result.Add(decision);
            }
        }

        return [.. result.OrderByDescending(static d => d.Sequence).Take(max)];
    }

    private static int CapacityFrom(IOptions<MicroDcsOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var management = options.Value.Management;
        return management.Enabled ? management.DecisionBufferSize : 0;
    }
}
