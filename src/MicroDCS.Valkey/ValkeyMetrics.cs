using System.Diagnostics.Metrics;

namespace MicroDCS.Valkey;

/// <summary>Meter of the Valkey layer.</summary>
public static class ValkeyMetrics
{
    /// <summary>The meter name; add it to the OpenTelemetry metrics configuration.</summary>
    public const string MeterName = "MicroDCS.Valkey";

    private static readonly Meter _meter = new(MeterName);

    internal static readonly Counter<long> WriteRetries = _meter.CreateCounter<long>("microdcs.valkey.write.retries", description: "Writes repeated after an unknown outcome");

    internal static readonly Counter<long> WritesAlreadyApplied = _meter.CreateCounter<long>("microdcs.valkey.write.already_applied", description: "Writes found applied by the operation id after an unknown outcome");

    internal static readonly Counter<long> StreamEntriesLost = _meter.CreateCounter<long>("microdcs.valkey.stream.entries_lost", description: "Pending stream entries deleted before they were processed");

    internal static readonly Counter<long> StreamEntriesTrimmed = _meter.CreateCounter<long>("microdcs.valkey.stream.entries_trimmed", description: "Acknowledged stream entries trimmed");

    internal static readonly Counter<long> PollErrors = _meter.CreateCounter<long>("microdcs.valkey.poll.errors", description: "Failed poller steps");
}
