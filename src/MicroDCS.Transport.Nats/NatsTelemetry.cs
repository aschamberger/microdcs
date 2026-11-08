using System.Diagnostics.Metrics;
using MicroDCS.Core.Transport;

namespace MicroDCS.Transport.Nats;

/// <summary>Counters of the NATS transport on the shared <c>MicroDCS</c> meter (D-37).</summary>
internal static class NatsTelemetry
{
    private static readonly Meter _meter = new(PipelineTelemetry.Name);

    public static readonly Counter<long> Dropped = _meter.CreateCounter<long>("microdcs.nats.dropped", "{message}", "NATS messages dropped by the transport before the pipeline (undecodable, no matching subscription, invalid subject).");

    public static readonly Counter<long> Reconnects = _meter.CreateCounter<long>("microdcs.nats.reconnects", "{connection}", "Successful NATS reconnects.");

    public static readonly Counter<long> DeadLettered = _meter.CreateCounter<long>("microdcs.nats.poisoned", "{message}", "NATS messages terminated after the maximum number of delivery attempts.");
}
