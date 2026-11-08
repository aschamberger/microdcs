using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace MicroDCS.Core.Transport;

/// <summary>
/// The single <see cref="ActivitySource"/> and <see cref="Meter"/> of the pipeline (D-37, A-1). Transports do not create their
/// own spans for message processing; they hand the message to the pipeline, which continues the W3C trace context carried in
/// <c>traceparent</c> and <c>tracestate</c>.
/// </summary>
public static class PipelineTelemetry
{
    /// <summary>Name of the activity source and the meter.</summary>
    public const string Name = "MicroDCS";

    /// <summary>Name of the logger category for security relevant rejections (R-11).</summary>
    public const string SecurityCategory = "MicroDCS.Security";

    internal static readonly ActivitySource Source = new(Name);

    private static readonly Meter _meter = new(Name);

    internal static readonly Counter<long> Received = _meter.CreateCounter<long>("microdcs.messages.received", "{message}", "Messages handled by the pipeline, by outcome.");

    internal static readonly Counter<long> Published = _meter.CreateCounter<long>("microdcs.messages.published", "{message}", "Messages handed to a transport.");

    internal static readonly Counter<long> QueueRejected = _meter.CreateCounter<long>("microdcs.outbound.rejected", "{message}", "Outgoing events refused because the binding queue was full (D-28).");

    internal static readonly Counter<long> Expired = _meter.CreateCounter<long>("microdcs.commands.expired", "{command}", "Commands that expired without a response.");
}
