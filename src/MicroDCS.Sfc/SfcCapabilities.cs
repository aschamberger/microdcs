namespace MicroDCS.Sfc;

/// <summary>Features a station supports; a recipe that needs more is refused at build time (D-44).</summary>
[Flags]
public enum SfcFeature
{
    /// <summary>No feature.</summary>
    None = 0,

    /// <summary>Push-command actions.</summary>
    PushCommand = 1,

    /// <summary>Pull-event actions.</summary>
    PullEvent = 2,

    /// <summary>Simultaneous divergences.</summary>
    SimultaneousDivergence = 4,

    /// <summary>Selection divergences.</summary>
    SelectionDivergence = 8,

    /// <summary>Hold and restart commands of push-command actions (D-57): a pause tells the equipment to hold its work.</summary>
    EquipmentHold = 16,
}

/// <summary>What a station can run: features, accepted qualifiers and the outcomes its processors declare.</summary>
/// <param name="Features">The supported features.</param>
/// <param name="Outcomes">The declared outcome names per action type id; the reserved outcomes are always valid.</param>
/// <param name="EventOutcomes">The declared outcome names per event type a pull-event action can wait for; absent means <paramref name="Outcomes"/> also answers for pull events.</param>
public sealed record SfcCapabilities(
    SfcFeature Features,
    IReadOnlyDictionary<string, IReadOnlySet<string>> Outcomes,
    IReadOnlyDictionary<string, IReadOnlySet<string>>? EventOutcomes = null)
{
    /// <summary>The only accepted action qualifier (non-stored, D-44).</summary>
    public const string AcceptedQualifier = "N";

    /// <summary>Tells whether a qualifier is supported.</summary>
    /// <param name="qualifier">The qualifier.</param>
    /// <returns><see langword="true"/> for <see cref="SfcActionQualifier.NonStored"/> only.</returns>
    public static bool Supports(SfcActionQualifier qualifier) => qualifier == SfcActionQualifier.NonStored;

    /// <summary>Tells whether a qualifier is accepted.</summary>
    /// <param name="qualifier">The qualifier from the recipe.</param>
    /// <returns><see langword="true"/> for <see cref="AcceptedQualifier"/> only.</returns>
    public static bool AcceptsQualifier(string? qualifier) =>
        string.Equals(qualifier, AcceptedQualifier, StringComparison.Ordinal);

    /// <summary>Tells whether an outcome name is valid for an action type.</summary>
    /// <param name="typeId">The action's CloudEvent type id.</param>
    /// <param name="outcome">The outcome name.</param>
    /// <returns><see langword="true"/> for a reserved outcome or one the type declares.</returns>
    public bool IsOutcomeValid(string typeId, string outcome) =>
        SfcOutcome.IsReserved(outcome)
        || (Outcomes.TryGetValue(typeId, out IReadOnlySet<string>? declared) && declared.Contains(outcome))
        || (EventOutcomes is not null && EventOutcomes.TryGetValue(typeId, out IReadOnlySet<string>? declaredByEvent) && declaredByEvent.Contains(outcome));

    /// <summary>Tells whether a processor of this station handles the type of an action.</summary>
    /// <param name="typeId">The action's CloudEvent type id.</param>
    /// <param name="interaction">The interaction of the action.</param>
    /// <returns><see langword="true"/> when an event handler (pull) or a command factory (push) exists.</returns>
    public bool Handles(string typeId, string interaction) =>
        interaction == SfcNames.PullEvent && EventOutcomes is not null ? EventOutcomes.ContainsKey(typeId) : Outcomes.ContainsKey(typeId);
}
