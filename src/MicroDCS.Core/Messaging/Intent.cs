namespace MicroDCS.Core.Messaging;

/// <summary>What a message means. The intent selects the address segment and the allowed direction (D-3).</summary>
public enum Intent
{
    /// <summary>Data being transmitted.</summary>
    Data,

    /// <summary>Something that happened.</summary>
    Event,

    /// <summary>A command to execute.</summary>
    Command,

    /// <summary>Metadata about the system or other messages.</summary>
    Meta,
}

/// <summary>Which side of the equipment boundary a processor serves.</summary>
public enum ProcessorBinding
{
    /// <summary>Subscribes to commands, publishes data, events and metadata.</summary>
    Northbound,

    /// <summary>Subscribes to data, events and metadata, publishes commands.</summary>
    Southbound,
}

/// <summary>Wire names of intents. The names are part of the address model (A-3), not of a transport.</summary>
public static class IntentNames
{
    /// <summary>Returns the address segment of an intent: <c>data</c>, <c>events</c>, <c>commands</c> or <c>metadata</c>.</summary>
    /// <param name="intent">The intent.</param>
    /// <returns>The segment.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The value is not a defined intent.</exception>
    public static string ToSegment(Intent intent) => intent switch
    {
        Intent.Data => "data",
        Intent.Event => "events",
        Intent.Command => "commands",
        Intent.Meta => "metadata",
        _ => throw new ArgumentOutOfRangeException(nameof(intent), intent, "Unknown intent."),
    };

    /// <summary>Parses an address segment back to an intent.</summary>
    /// <param name="segment">The segment.</param>
    /// <param name="intent">The intent when the segment is known.</param>
    /// <returns><see langword="true"/> when the segment names an intent.</returns>
    public static bool TryParse(string segment, out Intent intent)
    {
        (var found, intent) = segment switch
        {
            "data" => (true, Intent.Data),
            "events" => (true, Intent.Event),
            "commands" => (true, Intent.Command),
            "metadata" => (true, Intent.Meta),
            _ => (false, default(Intent)),
        };
        return found;
    }
}
