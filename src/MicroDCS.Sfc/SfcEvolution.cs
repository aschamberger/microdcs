namespace MicroDCS.Sfc;

/// <summary>One evolution step: a mutable working copy of the state, frozen into the result at the end.</summary>
internal sealed class SfcEvolution
{
    private readonly SfcGraph _graph;
    private readonly SfcState _original;
    private readonly SfcSnapshot _snapshot;
    private readonly long _at;
    private readonly List<string> _active;
    private readonly SortedDictionary<string, SfcActionState> _actions;
    private readonly List<SfcCommand> _commands = [];
    private SfcJobStatus _status;
    private string? _current;
    private long _sequence;
    private long? _startedAt;
    private string? _faultCode;
    private string? _faultMessage;
    private SfcContinuation _continuation = new SfcDefer();
    private int _entries;

    internal SfcEvolution(SfcGraph graph, SfcState state, SfcSnapshot snapshot, long at)
    {
        _graph = graph;
        _original = state;
        _snapshot = snapshot;
        _at = at;
        _active = [.. state.ActiveSteps];
        _actions = new SortedDictionary<string, SfcActionState>(state.Actions.ToDictionary(static p => p.Key, static p => p.Value, StringComparer.Ordinal), StringComparer.Ordinal);
        _status = state.Status;
        _current = state.Current;
        _sequence = state.Sequence;
        _faultCode = state.FaultCode;
        _faultMessage = state.FaultMessage;
    }

    private bool IsRunning => _status == SfcJobStatus.Running;

    internal SfcStepResult Run(SfcEvent input) => input switch
    {
        SfcStart => Start(),
        SfcActionCompleted c => Respond(c.ActionKey, c.Outcome, null, c.TypeId, c.Round),
        SfcActionFailed f => Respond(f.ActionKey, SfcOutcome.Error, f.Error, null, f.Round),
        SfcEventReceived e => Receive(e),
        SfcTick t => Tick(t),
        SfcResume => Resume(),
        SfcCancel c => Cancel(c),
        _ => Refuse(SfcDisposition.Rejected, "Unknown event kind."),
    };

    private SfcStepResult Start()
    {
        if (_status != SfcJobStatus.NotStarted)
        {
            return Refuse(SfcDisposition.Absorbed, "The job was started already.");
        }

        _startedAt = _at;
        _status = SfcJobStatus.Running;
        _current = _graph.InitialStep;
        EnterStep(_graph.InitialStep);
        Settle();
        return Applied();
    }

    private SfcStepResult Respond(string actionKey, string outcome, string? error, string? typeId, int? round)
    {
        if (!IsRunning)
        {
            return Refuse(SfcDisposition.Absorbed, "The job is not running; late responses are absorbed.");
        }

        if (!ActionKey.TryParse(actionKey, out string jobId, out string name)
            || !string.Equals(jobId, _original.JobId, StringComparison.Ordinal)
            || !_actions.TryGetValue(name, out SfcActionState? action))
        {
            return Refuse(SfcDisposition.Rejected, "The action key does not belong to this job.");
        }

        if (typeId is not null && !string.Equals(typeId, action.TypeId, StringComparison.Ordinal))
        {
            return Refuse(SfcDisposition.Rejected, $"The response type contradicts action '{name}'.");
        }

        if (round is int answered && answered != action.Round)
        {
            return Refuse(SfcDisposition.Absorbed, $"The response belongs to an earlier pass through the step of action '{name}'.");
        }

        if (action.IsDone)
        {
            return Refuse(SfcDisposition.Absorbed, $"Action '{name}' is closed; the response is a duplicate.");
        }

        if (action.Status == SfcActionStatus.Pending)
        {
            return Refuse(SfcDisposition.Rejected, $"Action '{name}' was never dispatched.");
        }

        Finish(action, outcome, error);
        Settle();
        return Applied();
    }

    private SfcStepResult Receive(SfcEventReceived received)
    {
        if (!IsRunning)
        {
            return Refuse(SfcDisposition.Absorbed, "The job is not running.");
        }

        SfcActionBlock? block = _graph.Recipe.Actions.FirstOrDefault(a =>
            _actions[a.Name].Status == SfcActionStatus.Waiting
            && string.Equals(a.TypeId, received.TypeId, StringComparison.Ordinal)
            && (a.Selector is null || a.Selector.Matches(_original.JobId, received.Subject, received.CorrelationId)));
        if (block is null)
        {
            return Refuse(SfcDisposition.Absorbed, "No action of this job waits for this event.");
        }

        Finish(_actions[block.Name], received.Outcome, received.Error);
        Settle();
        return Applied();
    }

    private SfcStepResult Tick(SfcTick tick)
    {
        if (!IsRunning)
        {
            return Refuse(SfcDisposition.Absorbed, "The job is not running.");
        }

        bool changed = false;
        if (tick.OutageMs > 0)
        {
            foreach (SfcActionState open in _actions.Values.Where(static a => a.IsOpen && a.Deadline is not null).ToList())
            {
                _actions[open.Name] = open with { Deadline = open.Deadline + tick.OutageMs };
                changed = true;
            }
        }

        foreach (SfcActionBlock assoc in _graph.Recipe.Actions)
        {
            SfcActionState action = _actions[assoc.Name];
            if (IsRunning && action.IsOpen && action.Deadline <= _at)
            {
                Expire(assoc, action);
                changed = true;
            }
        }

        Settle();
        return changed ? Applied() : Refuse(SfcDisposition.Absorbed, "No deadline is due.");
    }

    private SfcStepResult Resume()
    {
        if (!IsRunning)
        {
            return Refuse(SfcDisposition.Absorbed, "The job is not running.");
        }

        bool changed = false;
        foreach (SfcActionBlock assoc in _graph.Recipe.Actions.Where(a => _active.Contains(a.Step)))
        {
            SfcActionState action = _actions[assoc.Name];
            if (!IsRunning || _snapshot.LiveWork.Contains(ActionKey.Create(_original.JobId, assoc.Name)))
            {
                continue;
            }

            switch (action.Status)
            {
                case SfcActionStatus.Pending:
                    Begin(assoc, action.Attempt);
                    changed = true;
                    break;
                case SfcActionStatus.Dispatched:
                    Redispatch(assoc, action);
                    changed = true;
                    break;
                case SfcActionStatus.Waiting:
                    EmitAwait(assoc, action);
                    changed = true;
                    break;
                default:
                    break;
            }
        }

        Settle();
        return changed ? Applied() : Refuse(SfcDisposition.Absorbed, "All open work is live.");
    }

    private SfcStepResult Cancel(SfcCancel cancel)
    {
        if (!IsRunning)
        {
            return Refuse(SfcDisposition.Absorbed, "The job is not running.");
        }

        CancelOpenWork(cancel.Reason ?? "cancelled");
        _status = SfcJobStatus.Cancelled;
        _active.Clear();
        _continuation = new SfcCancelled();
        return Applied();
    }

    // ── Steps, divergences, routing ────────────────────────────────────────

    private void Settle()
    {
        while (IsRunning)
        {
            string? done = _active.FirstOrDefault(IsStepDone);
            if (done is null)
            {
                return;
            }

            CompleteStep(done);
        }
    }

    private bool IsStepDone(string step) => _graph.ActionsOf(step).All(a => _actions[a.Name].IsDone);

    private void EnterStep(string step)
    {
        if (++_entries > SfcInterpreter.MaxStepEntriesPerEvolution)
        {
            Fault("propagation_limit", $"More than {SfcInterpreter.MaxStepEntriesPerEvolution} steps were entered in one evolution.");
            return;
        }

        _active.Add(step);
        foreach (SfcActionBlock assoc in _graph.ActionsOf(step))
        {
            _actions[assoc.Name] = new SfcActionState { Name = assoc.Name, TypeId = assoc.TypeId, Round = _actions[assoc.Name].Round + 1 };
            Begin(assoc, 0);
        }
    }

    private void CompleteStep(string step)
    {
        (HashSet<string> outcomes, string? failed) = Collect([step]);
        _active.Remove(step);
        if (_graph.SequenceOf(step) is SfcSequencePosition position)
        {
            if (failed is not null)
            {
                Fault(failed, $"An action of step '{step}' in divergence '{position.Divergence.Name}' ended with '{failed}'; a sequence has no transitions to route it.");
                return;
            }

            IReadOnlyList<string> sequence = position.Divergence.Sequences[position.Sequence];
            if (position.Index + 1 < sequence.Count)
            {
                EnterStep(sequence[position.Index + 1]);
            }
            else if (_active.Count == 0)
            {
                Converge(position.Divergence, position.Divergence.Kind == SfcNames.Selection ? position.Sequence : null);
            }

            return;
        }

        Route(step, _graph.Outgoing(step), outcomes, failed);
    }

    /// <summary>
    /// Clears the transitions after the convergence. A selection divergence runs one sequence, so only its outcomes count: the
    /// other sequences may hold the outcomes of an earlier pass of a loop.
    /// </summary>
    private void Converge(SfcDivergence divergence, int? takenSequence)
    {
        IEnumerable<string> steps = takenSequence is int taken ? divergence.Sequences[taken] : divergence.Sequences.SelectMany(static p => p);
        (HashSet<string> outcomes, _) = Collect(steps);
        Route(divergence.Name, _graph.ConvergenceTransitions(divergence), outcomes, null);
    }

    private void Route(string node, IReadOnlyList<SfcTransition> candidates, HashSet<string> outcomes, string? failed)
    {
        if (candidates.Count == 0)
        {
            if (failed is not null)
            {
                Fault(failed, $"'{node}' ended with '{failed}' and has no transition.");
            }
            else
            {
                Complete();
            }

            return;
        }

        SfcTransition? match = Match(candidates, outcomes, failed);
        if (match is null)
        {
            Fault(failed ?? "no_transition", $"No transition of '{node}' matches the outcomes [{string.Join(", ", outcomes.Order(StringComparer.Ordinal))}].");
            return;
        }

        MoveTo(match.Target, outcomes, failed);
    }

    private void MoveTo(string target, HashSet<string> outcomes, string? failed)
    {
        SfcDivergence? divergence = _graph.FindDivergence(target);
        _current = target;
        if (divergence is null)
        {
            EnterStep(target);
            return;
        }

        if (divergence.Kind == SfcNames.Simultaneous)
        {
            // The filter is lazy: a fault while entering one sequence stops the others.
            foreach (string first in divergence.Sequences.Select(static p => p[0]).Where(_ => IsRunning))
            {
                EnterStep(first);
            }

            return;
        }

        SfcTransition? entry = Match(_graph.SelectionTransitions(divergence), outcomes, failed);
        if (entry is null)
        {
            Fault("no_transition", $"No entry transition of selection divergence '{divergence.Name}' matches.");
            return;
        }

        EnterStep(entry.Target);
    }

    /// <summary>The first transition by priority whose condition matches. The default matches only when no action failed (D-44).</summary>
    private static SfcTransition? Match(IReadOnlyList<SfcTransition> candidates, HashSet<string> outcomes, string? failed) =>
        candidates.FirstOrDefault(t => t.Condition == SfcOutcome.Default ? failed is null : outcomes.Contains(t.Condition));

    private (HashSet<string> Outcomes, string? Failed) Collect(IEnumerable<string> steps)
    {
        List<SfcActionState> done = [.. steps.SelectMany(s => _graph.ActionsOf(s)).Select(a => _actions[a.Name])];
        var outcomes = done.Select(static a => a.Outcome).OfType<string>().ToHashSet(StringComparer.Ordinal);
        string? failed = done.FirstOrDefault(static a => a.Status == SfcActionStatus.Failed)?.Outcome;

        return (outcomes, failed);
    }

    private void Complete()
    {
        _status = SfcJobStatus.Completed;
        _active.Clear();
        _continuation = new SfcComplete { Outcome = "completed" };
    }

    private void Fault(string code, string message)
    {
        CancelOpenWork("teardown");
        _status = SfcJobStatus.Faulted;
        _active.Clear();
        _faultCode = code;
        _faultMessage = message;
        _continuation = new SfcFault { Code = code, Message = message };
    }

    // ── Actions, attempts, deadlines ────────────────────────────────────

    private int MaxAttempts(SfcActionBlock assoc) => assoc.MaxAttempts ?? _snapshot.DefaultMaxAttempts;

    private void Begin(SfcActionBlock assoc, int previousAttempt)
    {
        if (assoc.Interaction == SfcNames.PushCommand)
        {
            Dispatch(assoc, previousAttempt + 1);
        }
        else
        {
            Await(assoc);
        }
    }

    private void Dispatch(SfcActionBlock assoc, int attempt)
    {
        string commandId = NextCommandId(assoc);
        long deadline = _at + (assoc.TimeoutSeconds * 1000L);
        _actions[assoc.Name] = _actions[assoc.Name] with
        {
            Status = SfcActionStatus.Dispatched,
            Attempt = attempt,
            CommandId = commandId,
            Deadline = deadline,
            Outcome = null,
            Error = null,
        };
        _commands.Add(NewStartWork(assoc, SfcWorkKind.PushCommand, commandId, attempt, deadline));
    }

    private void Await(SfcActionBlock assoc)
    {
        string commandId = NextCommandId(assoc);
        long deadline = _at + (assoc.TimeoutSeconds * 1000L);
        _actions[assoc.Name] = _actions[assoc.Name] with
        {
            Status = SfcActionStatus.Waiting,
            Attempt = 1,
            CommandId = commandId,
            Deadline = deadline,
            Outcome = null,
            Error = null,
        };
        _commands.Add(NewStartWork(assoc, SfcWorkKind.AwaitEvent, commandId, 1, deadline));
    }

    private void EmitAwait(SfcActionBlock assoc, SfcActionState action) =>
        _commands.Add(NewStartWork(assoc, SfcWorkKind.AwaitEvent, action.CommandId ?? NextCommandId(assoc), action.Attempt, action.Deadline ?? _at));

    private StartWork NewStartWork(SfcActionBlock assoc, SfcWorkKind kind, string commandId, int attempt, long deadline) => new()
    {
        ActionKey = ActionKey.Create(_original.JobId, assoc.Name),
        CommandId = commandId,
        Work = kind,
        Action = assoc.Name,
        TypeId = assoc.TypeId,
        Attempt = attempt,
        Round = _actions[assoc.Name].Round,
        Deadline = deadline,
        Parameters = assoc.Parameters,
    };

    private string NextCommandId(SfcActionBlock assoc) => $"{ActionKey.Create(_original.JobId, assoc.Name)}#{++_sequence}";

    private void Expire(SfcActionBlock assoc, SfcActionState action)
    {
        if (assoc.Interaction == SfcNames.PushCommand)
        {
            Redispatch(assoc, action);
        }
        else
        {
            CancelExpired(assoc, action);
            Finish(action, SfcOutcome.Timeout, null);
        }
    }

    private void Redispatch(SfcActionBlock assoc, SfcActionState action)
    {
        if (action.Attempt >= MaxAttempts(assoc))
        {
            CancelExpired(assoc, action);
            Finish(action, SfcOutcome.Timeout, null);
            return;
        }

        _commands.Add(new CancelWork
        {
            ActionKey = ActionKey.Create(_original.JobId, assoc.Name),
            CommandId = action.CommandId ?? string.Empty,
            Reason = "redispatch",
        });
        Dispatch(assoc, action.Attempt + 1);
    }

    /// <summary>The last attempt expired: withdraw it so the host drops the registration of the command or the awaited event.</summary>
    private void CancelExpired(SfcActionBlock assoc, SfcActionState action) =>
        _commands.Add(new CancelWork
        {
            ActionKey = ActionKey.Create(_original.JobId, assoc.Name),
            CommandId = action.CommandId ?? string.Empty,
            Reason = "timeout",
        });

    private void Finish(SfcActionState action, string outcome, string? error)
    {
        _actions[action.Name] = action with
        {
            Status = SfcOutcome.IsReserved(outcome) ? SfcActionStatus.Failed : SfcActionStatus.Completed,
            Outcome = outcome,
            Error = error is { Length: > 1024 } ? error[..1024] : error,
            Deadline = null,
        };
    }

    private void CancelOpenWork(string reason)
    {
        foreach (SfcActionState open in _actions.Values.Where(static a => a.IsOpen))
        {
            _commands.Add(new CancelWork
            {
                ActionKey = ActionKey.Create(_original.JobId, open.Name),
                CommandId = open.CommandId ?? string.Empty,
                Reason = reason,
            });
        }
    }

    // ── Results ─────────────────────────────────────────────────────────

    private SfcStepResult Applied() => new()
    {
        State = _original with
        {
            Status = _status,
            Current = _current,
            ActiveSteps = [.. _active],
            Actions = _actions,
            Sequence = _sequence,
            StartedAt = _startedAt ?? _original.StartedAt,
            FaultCode = _faultCode,
            FaultMessage = _faultMessage,
        },
        Continuation = _continuation,
        Commands = _commands,
        Disposition = SfcDisposition.Applied,
    };

    private SfcStepResult Refuse(SfcDisposition disposition, string reason) => new()
    {
        State = _original,
        Continuation = new SfcDefer(),
        Disposition = disposition,
        Reason = reason,
    };
}
