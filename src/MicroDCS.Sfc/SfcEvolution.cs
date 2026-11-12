namespace MicroDCS.Sfc;

/// <summary>One evolution step: a mutable working copy of the state, frozen into the result at the end.</summary>
internal sealed class SfcEvolution
{
    /// <summary>Ends the command id of a hold command, so that its answer is told apart from the answer to the work it holds (D-57).</summary>
    internal const string HoldIdSuffix = ":hold";

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
    private long? _heldAt;
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
        _heldAt = state.HeldAt;
    }

    private bool IsRunning => _status == SfcJobStatus.Running;

    private bool IsHeld => IsRunning && _heldAt is not null;

    internal SfcStepResult Run(SfcEvent input) => input switch
    {
        SfcStart => Start(),
        SfcActionCompleted c => Respond(c.ActionKey, c.Outcome, null, c.TypeId, c.Round, c.CommandId),
        SfcActionFailed f => Respond(f.ActionKey, SfcOutcome.Error, f.Error, null, f.Round, f.CommandId),
        SfcEventReceived e => Receive(e),
        SfcTick t => Tick(t),
        SfcResume => Resume(),
        SfcCancel c => Cancel(c),
        SfcHold => Hold(),
        SfcRelease => Release(),
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

    private SfcStepResult Respond(string actionKey, string outcome, string? error, string? typeId, int? round, string? commandId)
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

        SfcActionBlock assoc = _graph.FindAction(name)!; // every action of the state comes from the recipe the graph was built from
        if (typeId is not null && !IsTypeOf(assoc, action, typeId, IsHoldCommandId(commandId)))
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

        if (IsHoldCommandId(commandId))
        {
            bool current = string.Equals(commandId, action.HoldCommandId, StringComparison.Ordinal);
            bool held = outcome == SfcOutcome.Held && error is null;
            if (current && action.Hold == SfcHoldStatus.Holding)
            {
                HoldAnswered(assoc, action, held);
            }
            else if (current && held && action.Hold != SfcHoldStatus.Held)
            {
                // The hold expired, but the equipment did hold the work: only a restart gets it going again, the work's own command was sent already.
                HoldAnswered(assoc, action, held: true);
            }
            else
            {
                return Refuse(SfcDisposition.Absorbed, $"The hold of action '{name}' is no longer open; the answer is late.");
            }

            Settle();
            return Applied();
        }

        // After a 'held' answer the restart owns the work (D-57): an answer to the held attempt, or to the work before the restart, is late.
        if (action.Hold == SfcHoldStatus.Held)
        {
            return Refuse(SfcDisposition.Absorbed, $"The work of action '{name}' is held; its restart answers for it.");
        }

        if (action.Restarted && commandId is not null && !string.Equals(commandId, action.CommandId, StringComparison.Ordinal))
        {
            return Refuse(SfcDisposition.Absorbed, $"Action '{name}' was restarted; only the restart answers for it.");
        }

        WithdrawHold(action, "superseded");
        Finish(_actions[name], outcome, error);
        Settle();
        return Applied();
    }

    /// <summary>The type an answer may have: the hold type for a hold command, the restart type once restarted, else the action type.</summary>
    private static bool IsTypeOf(SfcActionBlock assoc, SfcActionState action, string typeId, bool holdAnswer) => holdAnswer
        ? string.Equals(typeId, assoc.Hold?.TypeId, StringComparison.Ordinal)
        : string.Equals(typeId, assoc.TypeId, StringComparison.Ordinal) || (action.Restarted && string.Equals(typeId, assoc.Restart?.TypeId, StringComparison.Ordinal));

    private static bool IsHoldCommandId(string? commandId) => commandId is not null && commandId.EndsWith(HoldIdSuffix, StringComparison.Ordinal);

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
            // Deadlines do not expire during a hold; the release moves them by its length, which covers any outage inside the hold too. Hold commands run
            // against the clock while held, so an outage moves them in both cases.
            foreach (SfcActionState open in _actions.Values.Where(a => (!IsHeld && a.IsOpen && a.Deadline is not null) || a.Hold == SfcHoldStatus.Holding).ToList())
            {
                _actions[open.Name] = open with
                {
                    Deadline = IsHeld ? open.Deadline : open.Deadline + tick.OutageMs,
                    HoldDeadline = open.Hold == SfcHoldStatus.Holding ? open.HoldDeadline + tick.OutageMs : open.HoldDeadline,
                };
                changed = true;
            }
        }

        foreach (SfcActionBlock assoc in _graph.Recipe.Actions)
        {
            SfcActionState action = _actions[assoc.Name];
            if (IsRunning && action.Hold == SfcHoldStatus.Holding && action.HoldDeadline <= _at)
            {
                HoldAnswered(assoc, action, held: false, reason: "timeout");
                changed = true;
                action = _actions[assoc.Name];
            }

            if (IsRunning && !IsHeld && action.IsOpen && action.Deadline <= _at)
            {
                Expire(assoc, action);
                changed = true;
            }
        }

        if (IsHeld && !changed)
        {
            return Refuse(SfcDisposition.Absorbed, "The job is held.");
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

        if (IsHeld)
        {
            return Refuse(SfcDisposition.Absorbed, "The job is held; the release issues its open work.");
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
                case SfcActionStatus.Dispatched when action.Hold == SfcHoldStatus.Holding:
                    // The answer to the hold, or its deadline, decides what happens to this work (D-57).
                    break;
                case SfcActionStatus.Dispatched when action.Restarted:
                    // A restart is sent once (D-57): issue it again with its command id, the host sends each id once.
                    ReissueOpen(assoc, action);
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
        _heldAt = null;
        _active.Clear();
        _continuation = new SfcCancelled();
        return Applied();
    }

    private SfcStepResult Hold()
    {
        if (!IsRunning)
        {
            return Refuse(SfcDisposition.Absorbed, "The job is not running.");
        }

        if (IsHeld)
        {
            return Refuse(SfcDisposition.Absorbed, "The job is held already.");
        }

        _heldAt = _at;

        // Hold logic (D-57): the running work of a push command that declares it is told to hold. Other work runs to its end, as in S20a.
        foreach (SfcActionBlock assoc in _graph.Recipe.Actions.Where(static a => a.Hold is not null))
        {
            SfcActionState action = _actions[assoc.Name];
            if (action.Status == SfcActionStatus.Dispatched && action.Hold is not (SfcHoldStatus.Holding or SfcHoldStatus.Held))
            {
                string holdId = NextCommandId(assoc) + HoldIdSuffix;
                long holdDeadline = _at + (assoc.Hold!.TimeoutSeconds * 1000L); // the filter of the loop selects actions with a hold command
                _actions[assoc.Name] = action with { Hold = SfcHoldStatus.Holding, HoldCommandId = holdId, HoldDeadline = holdDeadline };
                _commands.Add(NewStartWork(assoc, SfcWorkKind.HoldCommand, holdId, action.Attempt, holdDeadline));
            }
        }

        return Applied();
    }

    /// <summary>
    /// Ends a hold. Open deadlines move by its length (the mechanism of the outage rule, D-18), so the hold costs no attempt. Every open push attempt is
    /// issued again with its command id: the host skipped the ones that reached it during the hold, and sends each command id once (D-52).
    /// </summary>
    private SfcStepResult Release()
    {
        if (!IsHeld)
        {
            return Refuse(SfcDisposition.Absorbed, IsRunning ? "The job is not held." : "The job is not running.");
        }

        long held = Math.Max(0, _at - (_heldAt ?? _at));
        _heldAt = null;
        foreach (SfcActionBlock assoc in _graph.Recipe.Actions)
        {
            SfcActionState action = _actions[assoc.Name];
            if (!action.IsOpen || action.Deadline is not long deadline)
            {
                continue;
            }

            action = action with { Deadline = deadline + held };
            _actions[assoc.Name] = action;
            if (action.Status == SfcActionStatus.Dispatched && action.Hold != SfcHoldStatus.Holding)
            {
                // Work that could not be held runs on: issue its command again (D-52). A hold still out decides when it is answered (D-57).
                _actions[assoc.Name] = action with { Hold = null };
                ReissueOpen(assoc, _actions[assoc.Name]);
            }
        }

        foreach (SfcActionBlock assoc in _graph.Recipe.Actions.Where(a => _actions[a.Name].Hold == SfcHoldStatus.Held))
        {
            Restart(assoc, _actions[assoc.Name]);
        }

        Settle();
        return Applied();
    }

    /// <summary>
    /// Records the answer to a hold command (D-57). <c>held</c> ends the attempt: its command is withdrawn and the release sends the restart, at once
    /// when the job was released before the answer. Anything else leaves the work running to its end, as without hold logic.
    /// </summary>
    private void HoldAnswered(SfcActionBlock assoc, SfcActionState action, bool held, string reason = "not_held")
    {
        string actionKey = ActionKey.Create(_original.JobId, assoc.Name);
        if (held)
        {
            _commands.Add(new CancelWork { ActionKey = actionKey, CommandId = action.CommandId ?? string.Empty, Reason = "held" });
            _actions[assoc.Name] = action with { Hold = SfcHoldStatus.Held, HoldDeadline = null, Deadline = null };
            if (!IsHeld)
            {
                Restart(assoc, _actions[assoc.Name]);
            }

            return;
        }

        if (reason == "timeout")
        {
            _commands.Add(new CancelWork { ActionKey = actionKey, CommandId = action.HoldCommandId ?? string.Empty, Reason = reason });
        }

        _actions[assoc.Name] = action with { Hold = IsHeld ? SfcHoldStatus.NotHeld : null, HoldDeadline = null };
        if (!IsHeld)
        {
            // Released before the answer: the release left this command to the answer, so it is issued now. The host sends each command id once.
            ReissueOpen(assoc, _actions[assoc.Name]);
        }
    }

    /// <summary>Withdraws a hold that is still out, because its work ended on its own.</summary>
    private void WithdrawHold(SfcActionState action, string reason)
    {
        if (action.Hold != SfcHoldStatus.Holding)
        {
            return;
        }

        _commands.Add(new CancelWork { ActionKey = ActionKey.Create(_original.JobId, action.Name), CommandId = action.HoldCommandId ?? string.Empty, Reason = reason });
        _actions[action.Name] = action with { Hold = null, HoldDeadline = null };
    }

    /// <summary>Sends the restart of held work (D-57): once, without using an attempt, with its own deadline. Its response completes the action.</summary>
    private void Restart(SfcActionBlock assoc, SfcActionState action)
    {
        string commandId = NextCommandId(assoc);
        long deadline = _at + (assoc.Restart!.TimeoutSeconds * 1000L); // only held work restarts, and the build allows a hold only together with a restart (SFC019)
        _actions[assoc.Name] = action with
        {
            Status = SfcActionStatus.Dispatched,
            CommandId = commandId,
            Deadline = deadline,
            Hold = null,
            HoldCommandId = null,
            HoldDeadline = null,
            Restarted = true,
        };
        _commands.Add(NewStartWork(assoc, SfcWorkKind.RestartCommand, commandId, action.Attempt, deadline));
    }

    private void ReissueOpen(SfcActionBlock assoc, SfcActionState action) =>
        _commands.Add(NewStartWork(assoc, action.Restarted ? SfcWorkKind.RestartCommand : SfcWorkKind.PushCommand, action.CommandId ?? NextCommandId(assoc), action.Attempt, action.Deadline ?? _at));

    // ── Steps, divergences, routing ────────────────────────────────────────

    /// <summary>Clears what can be cleared. A held job records outcomes but stays at its step boundary until the release.</summary>
    private void Settle()
    {
        while (IsRunning && !IsHeld)
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
        // Hold and restart work exists only for actions that declare both commands (SFC019), so the null-forgiving operators below hold.
        TypeId = kind switch
        {
            SfcWorkKind.HoldCommand => assoc.Hold!.TypeId,
            SfcWorkKind.RestartCommand => assoc.Restart!.TypeId,
            _ => assoc.TypeId,
        },
        Attempt = attempt,
        Round = _actions[assoc.Name].Round,
        Deadline = deadline,
        Parameters = kind switch
        {
            SfcWorkKind.HoldCommand => assoc.Hold!.Parameters,
            SfcWorkKind.RestartCommand => assoc.Restart!.Parameters,
            _ => assoc.Parameters,
        },
    };

    private string NextCommandId(SfcActionBlock assoc) => $"{ActionKey.Create(_original.JobId, assoc.Name)}#{++_sequence}";

    private void Expire(SfcActionBlock assoc, SfcActionState action)
    {
        WithdrawHold(action, "superseded");
        action = _actions[assoc.Name];
        // A pull event times out at once; a restart is sent once (D-57), so its expiry is the end of the action too.
        if (assoc.Interaction == SfcNames.PushCommand && !action.Restarted)
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
            Hold = null,
            HoldDeadline = null,
        };
    }

    private void CancelOpenWork(string reason)
    {
        foreach (SfcActionState holding in _actions.Values.Where(static a => a.Hold == SfcHoldStatus.Holding))
        {
            _commands.Add(new CancelWork
            {
                ActionKey = ActionKey.Create(_original.JobId, holding.Name),
                CommandId = holding.HoldCommandId ?? string.Empty,
                Reason = reason,
            });
        }

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
            // A write in this version can add fields of this version, so it carries this version's minor number.
            FormatMinor = Math.Max(_original.FormatMinor, SfcState.CurrentFormatMinor),
            Status = _status,
            Current = _current,
            ActiveSteps = [.. _active],
            Actions = _actions,
            Sequence = _sequence,
            StartedAt = _startedAt ?? _original.StartedAt,
            HeldAt = _status == SfcJobStatus.Running ? _heldAt : null,
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
