using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.StateMachine;
using Opc.Ua;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.JobControl.Conversions;

/// <summary>
/// The Job Control V2 state array of OPC 10031-4 (7.3.2) for a neutral state: a top-level entry and, for a state with a sub-state, an
/// entry whose browse path targets the sub-state machine. Decoding reads the browse path and the numbers, never the text.
/// </summary>
public static class JobStateArray
{
    // The sub-state machines are objects of the V2 model, so their browse names carry its namespace.
    private static readonly ushort _namespaceIndex = (ushort)JobControlContext.Shared.NamespaceUris.GetIndex(V2.Namespaces.ISA95JobControlV2);

    private const uint NotAllowedToStart = 1;
    private const uint AllowedToStart = 2;
    private const uint Running = 3;
    private const uint Interrupted = 4;
    private const uint Ended = 5;
    private const uint Aborted = 6;

    /// <summary>Builds the V2 state array.</summary>
    /// <param name="state">The neutral state.</param>
    /// <returns>The state array; empty for a state V2 cannot show (<see cref="JobOrderState.Initial"/>, <see cref="JobOrderState.Removed"/>).</returns>
    public static ArrayOf<V2.ISA95StateDataType> ToV2(JobOrderState state) => state switch
    {
        JobOrderState.NotAllowedToStartWaiting => Array(NotAllowedToStart, nameof(NotAllowedToStart), V2.BrowseNames.NotAllowedToStartSubstates, 1, "Waiting"),
        JobOrderState.NotAllowedToStartReady => Array(NotAllowedToStart, nameof(NotAllowedToStart), V2.BrowseNames.NotAllowedToStartSubstates, 2, "Ready"),
        JobOrderState.NotAllowedToStartLoaded => Array(NotAllowedToStart, nameof(NotAllowedToStart), V2.BrowseNames.NotAllowedToStartSubstates, 3, "Loaded"),
        JobOrderState.AllowedToStartWaiting => Array(AllowedToStart, nameof(AllowedToStart), V2.BrowseNames.AllowedToStartSubstates, 1, "Waiting"),
        JobOrderState.AllowedToStartReady => Array(AllowedToStart, nameof(AllowedToStart), V2.BrowseNames.AllowedToStartSubstates, 2, "Ready"),
        JobOrderState.AllowedToStartLoaded => Array(AllowedToStart, nameof(AllowedToStart), V2.BrowseNames.AllowedToStartSubstates, 3, "Loaded"),
        JobOrderState.Running or JobOrderState.V1Loaded => [TopLevel(Running, nameof(Running))],
        JobOrderState.Held => Array(Interrupted, nameof(Interrupted), V2.BrowseNames.InterruptedSubstates, 1, "Held"),
        JobOrderState.Suspended => Array(Interrupted, nameof(Interrupted), V2.BrowseNames.InterruptedSubstates, 2, "Suspended"),
        JobOrderState.Completed => Array(Ended, nameof(Ended), V2.BrowseNames.EndedSubstates, 1, "Completed"),
        JobOrderState.Closed => Array(Ended, nameof(Ended), V2.BrowseNames.EndedSubstates, 2, "Closed"),
        JobOrderState.Aborted or JobOrderState.V1Error => [TopLevel(Aborted, nameof(Aborted))],
        _ => [],
    };

    /// <summary>Reads a V2 state array. A top-level entry without a sub-state takes Ready, Held or Completed.</summary>
    /// <param name="state">The state array.</param>
    /// <returns>The neutral state; <see cref="JobOrderState.Initial"/> for an empty array or an unknown top-level number.</returns>
    public static JobOrderState FromV2(ArrayOf<V2.ISA95StateDataType> state)
    {
        var (top, machine, sub) = Parse(state);
        if (machine is not null && TryFromSubstate(machine, sub, top, out var resolved))
        {
            return resolved;
        }

        return FromTopLevel(top);
    }

    /// <summary>Tests a state against a V2 state query. A query with only a top-level entry matches every sub-state of it.</summary>
    /// <param name="candidate">The neutral state.</param>
    /// <param name="query">The state array of the query.</param>
    /// <returns><see langword="true"/> if the state matches.</returns>
    public static bool Matches(JobOrderState candidate, ArrayOf<V2.ISA95StateDataType> query)
    {
        var (top, machine, sub) = Parse(query);
        if (machine is not null)
        {
            return TryFromSubstate(machine, sub, top, out var resolved) && candidate == resolved;
        }

        return top != 0 && ToV2(candidate) is { Count: > 0 } encoded && encoded[0].StateNumber == top;
    }

    private static (uint Top, string? Machine, uint Sub) Parse(ArrayOf<V2.ISA95StateDataType> state)
    {
        uint top = 0;
        string? machine = null;
        uint sub = 0;
        foreach (var entry in state)
        {
            if (SubstateMachine(entry.BrowsePath) is { } name)
            {
                machine = name;
                sub = entry.StateNumber;
            }
            else
            {
                top = entry.StateNumber;
            }
        }

        return (top, machine, sub);
    }

    private static JobOrderState FromTopLevel(uint number) => number switch
    {
        NotAllowedToStart => JobOrderState.NotAllowedToStartReady,
        AllowedToStart => JobOrderState.AllowedToStartReady,
        Running => JobOrderState.Running,
        Interrupted => JobOrderState.Held,
        Ended => JobOrderState.Completed,
        Aborted => JobOrderState.Aborted,
        _ => JobOrderState.Initial,
    };

    private static bool TryFromSubstate(string machine, uint number, uint top, out JobOrderState state)
    {
        (uint owner, state) = (machine, number) switch
        {
            (V2.BrowseNames.NotAllowedToStartSubstates, 1) => (NotAllowedToStart, JobOrderState.NotAllowedToStartWaiting),
            (V2.BrowseNames.NotAllowedToStartSubstates, 2) => (NotAllowedToStart, JobOrderState.NotAllowedToStartReady),
            (V2.BrowseNames.NotAllowedToStartSubstates, 3) => (NotAllowedToStart, JobOrderState.NotAllowedToStartLoaded),
            (V2.BrowseNames.AllowedToStartSubstates, 1) => (AllowedToStart, JobOrderState.AllowedToStartWaiting),
            (V2.BrowseNames.AllowedToStartSubstates, 2) => (AllowedToStart, JobOrderState.AllowedToStartReady),
            (V2.BrowseNames.AllowedToStartSubstates, 3) => (AllowedToStart, JobOrderState.AllowedToStartLoaded),
            (V2.BrowseNames.InterruptedSubstates, 1) => (Interrupted, JobOrderState.Held),
            (V2.BrowseNames.InterruptedSubstates, 2) => (Interrupted, JobOrderState.Suspended),
            (V2.BrowseNames.EndedSubstates, 1) => (Ended, JobOrderState.Completed),
            (V2.BrowseNames.EndedSubstates, 2) => (Ended, JobOrderState.Closed),
            _ => (0u, JobOrderState.Initial),
        };

        // A query or array may name only the sub-state; when it names both, they must belong together.
        return owner != 0 && (top == 0 || top == owner);
    }

    private static ArrayOf<V2.ISA95StateDataType> Array(uint top, string topText, string machine, uint sub, string subText) =>
    [
        TopLevel(top, topText),
        new V2.ISA95StateDataType
        {
            BrowsePath = new RelativePath(ReferenceTypeIds.HasSubStateMachine, new QualifiedName(machine, _namespaceIndex)),
            StateText = new LocalizedText(subText),
            StateNumber = sub,
        },
    ];

    private static V2.ISA95StateDataType TopLevel(uint number, string text) => new()
    {
        BrowsePath = new RelativePath(),
        StateText = new LocalizedText(text),
        StateNumber = number,
    };

    private static string? SubstateMachine(RelativePath? browsePath) =>
        browsePath is null || browsePath.Elements.Count == 0 ? null : browsePath.Elements[0]?.TargetName.Name;
}
