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

        return machine is null ? FromTopLevel(top) : FromSubstate(machine, sub, top);
    }

    /// <summary>Tests a state against a V2 state query. A query with only a top-level entry matches every sub-state of it.</summary>
    /// <param name="candidate">The neutral state.</param>
    /// <param name="query">The state array of the query.</param>
    /// <returns><see langword="true"/> if the state matches.</returns>
    public static bool Matches(JobOrderState candidate, ArrayOf<V2.ISA95StateDataType> query)
    {
        var hasSubstate = false;
        uint top = 0;
        foreach (var entry in query)
        {
            if (SubstateMachine(entry.BrowsePath) is not null)
            {
                hasSubstate = true;
            }
            else if (entry.StateNumber is >= NotAllowedToStart and <= Aborted)
            {
                top = entry.StateNumber;
            }
        }

        if (hasSubstate)
        {
            return candidate == FromV2(query);
        }

        return top != 0 && ToV2(candidate) is { Count: > 0 } encoded && encoded[0].StateNumber == top;
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

    private static JobOrderState FromSubstate(string machine, uint number, uint top) => (machine, number) switch
    {
        (V2.BrowseNames.NotAllowedToStartSubstates, 1) => JobOrderState.NotAllowedToStartWaiting,
        (V2.BrowseNames.NotAllowedToStartSubstates, 2) => JobOrderState.NotAllowedToStartReady,
        (V2.BrowseNames.NotAllowedToStartSubstates, 3) => JobOrderState.NotAllowedToStartLoaded,
        (V2.BrowseNames.AllowedToStartSubstates, 1) => JobOrderState.AllowedToStartWaiting,
        (V2.BrowseNames.AllowedToStartSubstates, 2) => JobOrderState.AllowedToStartReady,
        (V2.BrowseNames.AllowedToStartSubstates, 3) => JobOrderState.AllowedToStartLoaded,
        (V2.BrowseNames.InterruptedSubstates, 1) => JobOrderState.Held,
        (V2.BrowseNames.InterruptedSubstates, 2) => JobOrderState.Suspended,
        (V2.BrowseNames.EndedSubstates, 1) => JobOrderState.Completed,
        (V2.BrowseNames.EndedSubstates, 2) => JobOrderState.Closed,
        _ => FromTopLevel(top),
    };

    private static ArrayOf<V2.ISA95StateDataType> Array(uint top, string topText, string machine, uint sub, string subText) =>
    [
        TopLevel(top, topText),
        new V2.ISA95StateDataType
        {
            BrowsePath = new RelativePath(ReferenceTypeIds.HasSubStateMachine, new QualifiedName(machine)),
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
