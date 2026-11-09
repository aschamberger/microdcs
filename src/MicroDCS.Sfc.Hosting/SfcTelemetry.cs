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

    internal static readonly Counter<long> EventsMatched = _meter.CreateCounter<long>("microdcs.sfc.events.matched", description: "Incoming events that completed a pull-event action");

    internal static readonly Counter<long> EventsUnmatched = _meter.CreateCounter<long>("microdcs.sfc.events.unmatched", description: "Incoming events of a waited-for type that no job took");

    internal static readonly Counter<long> Outages = _meter.CreateCounter<long>("microdcs.sfc.outages", description: "Platform outages found by the sweep");
}
