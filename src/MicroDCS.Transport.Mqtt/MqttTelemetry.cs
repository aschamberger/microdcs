using System.Diagnostics.Metrics;
using MicroDCS.Core.Transport;

namespace MicroDCS.Transport.Mqtt;

/// <summary>Counters of the MQTT transport on the shared <c>MicroDCS</c> meter (D-37).</summary>
internal static class MqttTelemetry
{
    private static readonly Meter _meter = new(PipelineTelemetry.Name);

    public static readonly Counter<long> Dropped = _meter.CreateCounter<long>("microdcs.mqtt.dropped", "{message}", "MQTT messages dropped by the transport before the pipeline (undecodable, no matching subscription, invalid topic).");

    public static readonly Counter<long> Reconnects = _meter.CreateCounter<long>("microdcs.mqtt.reconnects", "{connection}", "Successful MQTT reconnects.");
}
