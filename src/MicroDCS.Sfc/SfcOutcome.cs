namespace MicroDCS.Sfc;

/// <summary>Outcome names (D-17, D-44). A transition condition names an outcome or <see cref="Default"/>.</summary>
public static class SfcOutcome
{
    /// <summary>The condition of a default transition; matches every outcome.</summary>
    public const string Default = "true";

    /// <summary>Reserved: the attempts of an action are used up.</summary>
    public const string Timeout = "timeout";

    /// <summary>Reserved: the response handler reported a failure.</summary>
    public const string Error = "error";

    /// <summary>
    /// The outcome a hold command answers when the equipment stopped the work of the action and can restart it (D-57). Not reserved: the hold type
    /// declares it. Any other answer, an error or a timeout means the work could not be held.
    /// </summary>
    public const string Held = "held";

    /// <summary>Tells whether a name is one of the reserved outcomes, which every action type has.</summary>
    /// <param name="name">The outcome name.</param>
    /// <returns><see langword="true"/> for <see cref="Timeout"/> and <see cref="Error"/>.</returns>
    public static bool IsReserved(string name) =>
        string.Equals(name, Timeout, StringComparison.Ordinal) || string.Equals(name, Error, StringComparison.Ordinal);
}
