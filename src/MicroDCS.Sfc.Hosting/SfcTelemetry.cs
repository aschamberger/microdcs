using System.Diagnostics.Metrics;

namespace MicroDCS.Sfc.Hosting;

/// <summary>Meter of the SFC host.</summary>
public static class SfcTelemetry
{
    /// <summary>The meter name; add it to the OpenTelemetry metrics configuration.</summary>
    public const string MeterName = "MicroDCS.Sfc";

    private static readonly Meter _meter = new(MeterName);

    internal static readonly Counter<long> Dispatched = _meter.CreateCounter<long>("microdcs.sfc.dispatched", description: "Push commands queued");

    internal static readonly Counter<long> DispatchDropped = _meter.CreateCounter<long>("microdcs.sfc.dispatch.dropped", description: "Push commands not queued because the outgoing queue was full");

    internal static readonly Counter<long> Dead = _meter.CreateCounter<long>("microdcs.sfc.work.dead", description: "Work entries moved to the dead stream");

    internal static readonly Counter<long> Outages = _meter.CreateCounter<long>("microdcs.sfc.outages", description: "Platform outages found by the sweep");
}
