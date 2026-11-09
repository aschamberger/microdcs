namespace MicroDCS.Sfc.Simulation;

/// <summary>Why a run stopped.</summary>
public enum SfcRunEnd
{
    /// <summary>The job completed.</summary>
    Completed,

    /// <summary>The job ended with a fault verdict.</summary>
    Faulted,

    /// <summary>The job was cancelled.</summary>
    Cancelled,

    /// <summary>The job waits and nothing is scheduled: an event from outside is needed.</summary>
    Idle,

    /// <summary>The virtual time limit was reached.</summary>
    TimeLimit,

    /// <summary>The evolution limit was reached, for example by a loop that answers at once.</summary>
    EvolutionLimit,
}

/// <summary>
/// An in-memory host for one job: a virtual clock, scripted equipment and a transcript. It is not durable and not distributed;
/// it exists to test the core against a host, to dry-run recipes and to replay a bug report (state plus events).
/// </summary>
public sealed class SfcSimulation
{
    private readonly SfcGraph _graph;
    private readonly IRecipeInterpreter _interpreter;
    private readonly SfcSnapshot _snapshot;
    private readonly SfcScript _script;
    private readonly List<(long Due, long Order, SfcEvent Event, string? CommandId)> _scheduled = [];
    private readonly List<SfcCommand> _commands = [];
    private readonly Dictionary<string, int> _asked = new(StringComparer.Ordinal);
    private long _order;

    /// <summary>Initializes a new instance of the <see cref="SfcSimulation"/> class.</summary>
    /// <param name="graph">The graph.</param>
    /// <param name="script">What the equipment does; defaults to answering <c>ok</c> at once.</param>
    /// <param name="snapshot">The host snapshot; defaults to <see cref="SfcSnapshot.Default"/>.</param>
    /// <param name="jobId">The job id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="interpreter">The interpreter; defaults to <see cref="SfcInterpreter"/>.</param>
    public SfcSimulation(SfcGraph graph, SfcScript? script = null, SfcSnapshot? snapshot = null, string jobId = "job-1", string scope = "sim", IRecipeInterpreter? interpreter = null)
    {
        ArgumentNullException.ThrowIfNull(graph);
        _graph = graph;
        _script = script ?? new SfcScript();
        _snapshot = snapshot ?? SfcSnapshot.Default;
        _interpreter = interpreter ?? new SfcInterpreter();
        State = SfcState.Create(graph, jobId, scope);
        foreach (SfcEvent injected in _script.Injected)
        {
            Schedule(injected.At, injected, null);
        }
    }

    /// <summary>Gets the virtual time in epoch milliseconds.</summary>
    public long Now { get; private set; }

    /// <summary>Gets the state after the last evolution.</summary>
    public SfcState State { get; private set; }

    /// <summary>Gets the continuation of the last evolution.</summary>
    public SfcContinuation Continuation { get; private set; } = new SfcDefer();

    /// <summary>Gets the transcript.</summary>
    public SfcTranscript Transcript { get; } = new();

    /// <summary>Gets every command the core returned, in order.</summary>
    public IReadOnlyList<SfcCommand> Commands => _commands;

    /// <summary>Moves the virtual clock forward.</summary>
    /// <param name="milliseconds">The time to add.</param>
    public void Advance(long milliseconds) => Now += milliseconds;

    /// <summary>Evolves the job by one event at the current virtual time and schedules the scripted answers to its commands.</summary>
    /// <param name="input">The event; its <see cref="SfcEvent.At"/> is the time the core sees.</param>
    /// <returns>The result of the core.</returns>
    public SfcStepResult Send(SfcEvent input)
    {
        ArgumentNullException.ThrowIfNull(input);
        SfcStepResult result = _interpreter.Evolve(_graph, State, _snapshot, input);
        Transcript.Add(input.At, input, result);
        State = result.State;
        if (result.Disposition == SfcDisposition.Applied)
        {
            Continuation = result.Continuation;
        }

        _commands.AddRange(result.Commands);
        foreach (SfcCommand command in result.Commands)
        {
            Apply(command);
        }

        return result;
    }

    /// <summary>Starts the job if needed and runs until it ends, waits for outside events or the time limit passes.</summary>
    /// <param name="maxVirtualMilliseconds">The limit in virtual time after the start.</param>
    /// <param name="maxEvolutions">The limit of evolutions in this run; stops loops that answer at once and never advance the clock.</param>
    /// <returns>Why the run stopped.</returns>
    public SfcRunEnd Run(long maxVirtualMilliseconds = 3_600_000, int maxEvolutions = 10_000)
    {
        long limit = Now + maxVirtualMilliseconds;
        int evolutions = 0;
        if (State.Status == SfcJobStatus.NotStarted)
        {
            Send(new SfcStart { At = Now });
        }

        while (!State.IsEnded)
        {
            if (++evolutions > maxEvolutions)
            {
                return SfcRunEnd.EvolutionLimit;
            }

            (long Due, long Order, SfcEvent Event, string? CommandId)? next = _scheduled.Count == 0 ? null : _scheduled.MinBy(static s => (s.Due, s.Order));
            long? deadline = State.NextDeadline;
            if (next is null && deadline is null)
            {
                return SfcRunEnd.Idle;
            }

            // An answer that arrives at the deadline wins over the tick.
            bool answerFirst = next is not null && (deadline is null || next.Value.Due <= deadline);
            long due = answerFirst ? next!.Value.Due : deadline!.Value;
            if (due > limit)
            {
                return SfcRunEnd.TimeLimit;
            }

            Now = Math.Max(Now, due);
            if (answerFirst)
            {
                _scheduled.Remove(next!.Value);
                Send(next.Value.Event with { At = Now });
            }
            else
            {
                Send(new SfcTick { At = Now });
            }
        }

        return State.Status switch
        {
            SfcJobStatus.Completed => SfcRunEnd.Completed,
            SfcJobStatus.Faulted => SfcRunEnd.Faulted,
            _ => SfcRunEnd.Cancelled,
        };
    }

    private void Apply(SfcCommand command)
    {
        switch (command)
        {
            case CancelWork cancel:
                _scheduled.RemoveAll(s => s.CommandId == cancel.CommandId);
                break;
            case StartWork start:
                _asked[start.Action] = _asked.GetValueOrDefault(start.Action) + 1;
                SfcReply reply = _script.ReplyFor(start.Action, _asked[start.Action]);
                if (reply.Kind == SfcReplyKind.Silent)
                {
                    break;
                }

                SfcEvent response = (start.Work, reply.Kind) switch
                {
                    (SfcWorkKind.PushCommand, SfcReplyKind.Error) => new SfcActionFailed { At = 0, ActionKey = start.ActionKey, Error = reply.Error, Round = start.Round },
                    (SfcWorkKind.PushCommand, _) => new SfcActionCompleted { At = 0, ActionKey = start.ActionKey, Outcome = reply.Outcome ?? "ok", TypeId = start.TypeId, Round = start.Round },
                    (_, SfcReplyKind.Error) => MatchingEvent(start, SfcOutcome.Error),
                    _ => MatchingEvent(start, reply.Outcome ?? "ok"),
                };
                Schedule(Now + reply.DelayMs, response, start.CommandId);
                break;
            default:
                break;
        }
    }

    /// <summary>A scripted reply to a pull event answers that action, so it carries what the action's selector asks for.</summary>
    private SfcEventReceived MatchingEvent(StartWork start, string outcome)
    {
        SfcSelector? selector = _graph.FindAction(start.Action)?.Selector;
        return new SfcEventReceived
        {
            At = 0,
            TypeId = start.TypeId,
            Outcome = outcome,
            Subject = selector?.Subject?.Replace(SfcSelector.JobIdPlaceholder, State.JobId, StringComparison.Ordinal),
            CorrelationId = selector?.CorrelationId?.Replace(SfcSelector.JobIdPlaceholder, State.JobId, StringComparison.Ordinal),
        };
    }

    private void Schedule(long due, SfcEvent input, string? commandId) => _scheduled.Add((due, _order++, input, commandId));
}
