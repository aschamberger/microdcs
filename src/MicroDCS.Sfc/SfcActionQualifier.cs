namespace MicroDCS.Sfc;

/// <summary>
/// The action qualifiers of IEC 61131-3 (SFC action blocks). Recipes carry the short form (<c>N</c>, <c>SD</c>, ...); only
/// <see cref="NonStored"/> is supported so far (D-44).
/// </summary>
public enum SfcActionQualifier
{
    /// <summary><c>N</c>: non-stored; the action runs while the step is active.</summary>
    NonStored,

    /// <summary><c>R</c>: overriding reset; ends a stored action.</summary>
    OverridingReset,

    /// <summary><c>S</c>: set (stored); the action starts with the step and stays until reset.</summary>
    Set,

    /// <summary><c>L</c>: time limited; runs while the step is active, at most for a duration.</summary>
    TimeLimited,

    /// <summary><c>D</c>: time delayed; starts a duration after the step became active.</summary>
    TimeDelayed,

    /// <summary><c>P</c>: pulse.</summary>
    Pulse,

    /// <summary><c>SD</c>: stored and time delayed.</summary>
    StoredAndTimeDelayed,

    /// <summary><c>DS</c>: delayed and stored.</summary>
    DelayedAndStored,

    /// <summary><c>SL</c>: stored and time limited.</summary>
    StoredAndTimeLimited,

    /// <summary><c>P1</c>: pulse on the rising edge of the step (activation).</summary>
    PulseRisingEdge,

    /// <summary><c>P0</c>: pulse on the falling edge of the step (deactivation).</summary>
    PulseFallingEdge,
}

/// <summary>Conversion between <see cref="SfcActionQualifier"/> and the short form used in recipes.</summary>
public static class SfcActionQualifiers
{
    private static readonly (SfcActionQualifier Qualifier, string Code)[] _codes =
    [
        (SfcActionQualifier.NonStored, "N"),
        (SfcActionQualifier.OverridingReset, "R"),
        (SfcActionQualifier.Set, "S"),
        (SfcActionQualifier.TimeLimited, "L"),
        (SfcActionQualifier.TimeDelayed, "D"),
        (SfcActionQualifier.Pulse, "P"),
        (SfcActionQualifier.StoredAndTimeDelayed, "SD"),
        (SfcActionQualifier.DelayedAndStored, "DS"),
        (SfcActionQualifier.StoredAndTimeLimited, "SL"),
        (SfcActionQualifier.PulseRisingEdge, "P1"),
        (SfcActionQualifier.PulseFallingEdge, "P0"),
    ];

    /// <summary>Gets the short forms of all qualifiers, in the order of the standard's table.</summary>
    public static IReadOnlyList<string> Codes { get; } = [.. _codes.Select(static c => c.Code)];

    /// <summary>Parses the short form (case-sensitive).</summary>
    /// <param name="code">The short form from the recipe.</param>
    /// <param name="qualifier">The qualifier when known.</param>
    /// <returns><see langword="true"/> when the code is one of <see cref="Codes"/>.</returns>
    public static bool TryParse(string? code, out SfcActionQualifier qualifier)
    {
        foreach ((SfcActionQualifier candidate, string text) in _codes)
        {
            if (string.Equals(code, text, StringComparison.Ordinal))
            {
                qualifier = candidate;
                return true;
            }
        }

        qualifier = default;
        return false;
    }

    /// <summary>Gets the short form of a qualifier.</summary>
    /// <param name="qualifier">The qualifier.</param>
    /// <returns>The short form, such as <c>SD</c>.</returns>
    public static string ToCode(SfcActionQualifier qualifier) =>
        _codes.First(c => c.Qualifier == qualifier).Code;
}
