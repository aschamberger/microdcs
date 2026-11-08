using System.Collections.Frozen;

namespace MicroDCS.JobControl.StateMachine;

/// <summary>
/// The pure job order state machine (D-34): no I/O, no locks and no store, so a compare-and-set can call it. V2 rules come from the
/// table generated from the NodeSet plus the supplement below; V1 rules are the V2 rules narrowed to the V1 commands, with Stop as a
/// removal.
/// </summary>
public static class JobStateMachine
{
    private const string Receiver = "ISA95JobOrderReceiverObjectType";
    private const string Ended = "ISA95EndedStateMachineType";
    private const string Interrupted = "ISA95InterruptedStateMachineType";
    private const string Prepare = "ISA95PrepareStateMachineType";
    private const string NotAllowedToStart = "NotAllowedToStart";
    private const string AllowedToStart = "AllowedToStart";
    private const string Running = "Running";
    private const string InterruptedState = "Interrupted";
    private const string EndedState = "Ended";
    private const string Aborted = "Aborted";

    private static readonly FrozenDictionary<(string Machine, string From, string To), NodeSetTransition> _byStates =
        JobControlV2Transitions.All.ToFrozenDictionary(t => (t.Machine, t.From, t.To));

    private static readonly FrozenDictionary<(string From, string Cause), NodeSetTransition> _byCause =
        JobControlV2Transitions.All
            .Where(t => t.Machine == Receiver && t.Cause is not null)
            .ToFrozenDictionary(t => (t.From, t.Cause!));

    /// <summary>Applies an operation.</summary>
    /// <param name="version">The specification version whose rules apply.</param>
    /// <param name="state">The current state.</param>
    /// <param name="operation">The operation.</param>
    /// <returns>The result; a rejection leaves the state unchanged.</returns>
    public static JobTransition Apply(JobControlVersion version, JobOrderState state, JobOrderOperation operation) =>
        version == JobControlVersion.V1 ? ApplyV1(state, operation) : ApplyV2(state, operation);

    private static JobTransition ApplyV1(JobOrderState state, JobOrderOperation operation)
    {
        if (!IsV1Operation(operation))
        {
            return Reject(state, JobReturnStatus.InvalidCommand);
        }

        var result = operation == JobOrderOperation.Stop && state != JobOrderState.Initial ? StopAndRemove(state) : ApplyV2(state, operation);

        // V1 has no distinct "invalid status" outcome for a command: it reports the command as not valid (upstream ToV1ReturnStatus).
        return result.Status.HasFlag(JobReturnStatus.InvalidStatus)
            ? Reject(state, (result.Status & ~JobReturnStatus.InvalidStatus) | JobReturnStatus.InvalidCommand)
            : result;
    }

    private static bool IsV1Operation(JobOrderOperation operation) => operation is
        JobOrderOperation.Store or JobOrderOperation.StoreAndStart or JobOrderOperation.Start or JobOrderOperation.Update or
        JobOrderOperation.Stop or JobOrderOperation.Cancel or JobOrderOperation.Clear or
        JobOrderOperation.BeginExecution or JobOrderOperation.Hold or JobOrderOperation.Complete or JobOrderOperation.Close;

    private static JobTransition StopAndRemove(JobOrderState state) =>
        state is JobOrderState.Running or JobOrderState.Held or JobOrderState.Suspended
            ? new JobTransition(JobReturnStatus.Success, JobOrderState.Removed, JobOrderEffect.CatalogRemoved)
            : Reject(state, JobReturnStatus.InvalidStatus);

    private static JobTransition ApplyV2(JobOrderState state, JobOrderOperation operation)
    {
        if (operation is JobOrderOperation.Store or JobOrderOperation.StoreAndStart)
        {
            var stored = operation == JobOrderOperation.Store ? JobOrderState.NotAllowedToStartReady : JobOrderState.AllowedToStartReady;
            return state == JobOrderState.Initial
                ? new JobTransition(JobReturnStatus.Success, stored, JobOrderEffect.StatusEvent)
                : Reject(state, JobReturnStatus.UnableToAccept);
        }

        if (state == JobOrderState.Initial)
        {
            return Reject(state, JobReturnStatus.UnknownJobOrderId);
        }

        return operation switch
        {
            JobOrderOperation.Cancel => Remove(state, IsPrepare(state)),
            JobOrderOperation.Clear => Remove(state, state is JobOrderState.Completed or JobOrderState.Closed or JobOrderState.Aborted),
            // Only the cause-less row: Interrupted to Running is Resume, not the start of execution.
            JobOrderOperation.BeginExecution => Top(state) == AllowedToStart ? Receive(state, operation, Running) : Reject(state, JobReturnStatus.InvalidStatus),
            JobOrderOperation.Hold => Receive(state, operation, InterruptedState),
            JobOrderOperation.Complete => Receive(state, operation, EndedState),
            JobOrderOperation.Close => Sub(state, Ended, "Closed", JobOrderState.Closed),
            JobOrderOperation.PrepareToWaiting => SubPrepare(state, "Waiting"),
            JobOrderOperation.PrepareToReady => SubPrepare(state, "Ready"),
            JobOrderOperation.PrepareToLoaded => SubPrepare(state, "Loaded"),
            JobOrderOperation.InterruptToHeld => Sub(state, Interrupted, "Held", JobOrderState.Held),
            JobOrderOperation.InterruptToSuspended => Sub(state, Interrupted, "Suspended", JobOrderState.Suspended),
            _ => ByCauseOrReject(state, operation),
        };
    }

    private static JobTransition Remove(JobOrderState state, bool allowed) =>
        allowed ? new JobTransition(JobReturnStatus.Success, JobOrderState.Removed, JobOrderEffect.CatalogRemoved) : Reject(state, JobReturnStatus.InvalidStatus);

    private static JobTransition ByCauseOrReject(JobOrderState state, JobOrderOperation operation) =>
        Top(state) is { } from && _byCause.TryGetValue((from, operation.ToString()), out var transition)
            ? Accept(state, operation, transition)
            : Reject(state, JobReturnStatus.InvalidStatus);

    private static JobTransition Receive(JobOrderState state, JobOrderOperation operation, string to) =>
        Top(state) is { } from && _byStates.TryGetValue((Receiver, from, to), out var transition)
            ? Accept(state, operation, transition)
            : Reject(state, JobReturnStatus.InvalidStatus);

    private static JobTransition Accept(JobOrderState state, JobOrderOperation operation, NodeSetTransition transition) =>
        new(JobReturnStatus.Success, Target(state, operation, transition.To), EffectOf(transition));

    private static JobTransition Sub(JobOrderState state, string machine, string to, JobOrderState target) =>
        SubName(state) is { } from && _byStates.TryGetValue((machine, from, to), out var transition)
            ? new JobTransition(JobReturnStatus.Success, target, EffectOf(transition))
            : Reject(state, JobReturnStatus.InvalidStatus);

    private static JobTransition SubPrepare(JobOrderState state, string to)
    {
        var target = (Top(state), to) switch
        {
            (NotAllowedToStart, "Waiting") => JobOrderState.NotAllowedToStartWaiting,
            (NotAllowedToStart, "Ready") => JobOrderState.NotAllowedToStartReady,
            (NotAllowedToStart, "Loaded") => JobOrderState.NotAllowedToStartLoaded,
            (AllowedToStart, "Waiting") => JobOrderState.AllowedToStartWaiting,
            (AllowedToStart, "Ready") => JobOrderState.AllowedToStartReady,
            (AllowedToStart, "Loaded") => JobOrderState.AllowedToStartLoaded,
            _ => state,
        };

        return Sub(state, Prepare, to, target);
    }

    private static JobOrderState Target(JobOrderState state, JobOrderOperation operation, string to) => to switch
    {
        // Update is a self-transition and keeps the sub-state; every other entry takes the initial sub-state.
        NotAllowedToStart => operation == JobOrderOperation.Update ? state : JobOrderState.NotAllowedToStartReady,
        AllowedToStart => operation == JobOrderOperation.Update ? state : JobOrderState.AllowedToStartReady,
        Running => JobOrderState.Running,
        InterruptedState => operation == JobOrderOperation.Hold ? JobOrderState.Held : JobOrderState.Suspended,
        EndedState => JobOrderState.Completed,
        Aborted => JobOrderState.Aborted,
        _ => throw new InvalidOperationException($"Transition table holds an unknown target state '{to}'."),
    };

    private static JobOrderEffect EffectOf(NodeSetTransition transition) => transition.Effect is null ? JobOrderEffect.None : JobOrderEffect.StatusEvent;

    private static JobTransition Reject(JobOrderState state, JobReturnStatus status) => new(status, state, JobOrderEffect.None);

    private static bool IsPrepare(JobOrderState state) => Top(state) is NotAllowedToStart or AllowedToStart;

    private static string? Top(JobOrderState state) => state switch
    {
        JobOrderState.NotAllowedToStartWaiting or JobOrderState.NotAllowedToStartReady or JobOrderState.NotAllowedToStartLoaded => NotAllowedToStart,
        JobOrderState.AllowedToStartWaiting or JobOrderState.AllowedToStartReady or JobOrderState.AllowedToStartLoaded => AllowedToStart,
        JobOrderState.Running => Running,
        JobOrderState.Held or JobOrderState.Suspended => InterruptedState,
        JobOrderState.Completed or JobOrderState.Closed => EndedState,
        JobOrderState.Aborted => Aborted,
        _ => null,
    };

    private static string? SubName(JobOrderState state) => state switch
    {
        JobOrderState.NotAllowedToStartWaiting or JobOrderState.AllowedToStartWaiting => "Waiting",
        JobOrderState.NotAllowedToStartReady or JobOrderState.AllowedToStartReady => "Ready",
        JobOrderState.NotAllowedToStartLoaded or JobOrderState.AllowedToStartLoaded => "Loaded",
        JobOrderState.Held => "Held",
        JobOrderState.Suspended => "Suspended",
        JobOrderState.Completed => "Completed",
        JobOrderState.Closed => "Closed",
        _ => null,
    };
}
