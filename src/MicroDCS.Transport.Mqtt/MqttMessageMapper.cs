using System.Buffers;
using System.Text;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MQTTnet;
using MQTTnet.Packets;
using MQTTnet.Protocol;

namespace MicroDCS.Transport.Mqtt;

/// <summary>
/// Maps between the envelope carrier and MQTT 5 publish properties (binary content mode): attributes are user properties,
/// <c>datacontenttype</c> is the content type, the data is the payload. Pure, so it is tested without a broker.
/// </summary>
internal static class MqttMessageMapper
{
    /// <summary>Largest value of the MQTT message expiry interval property (four byte integer).</summary>
    private const uint MaxExpirySeconds = uint.MaxValue;

    /// <summary>Builds the MQTT message for an outbound message.</summary>
    /// <param name="codec">The envelope codec.</param>
    /// <param name="message">The outbound message.</param>
    /// <param name="topic">The rendered topic.</param>
    /// <param name="qos">The quality of service.</param>
    /// <returns>The MQTT message.</returns>
    public static MqttApplicationMessage ToMqtt(IEnvelopeCodec codec, OutboundMessage message, string topic, MqttQualityOfServiceLevel qos)
    {
        var envelope = message.Envelope;
        var carrier = codec.Encode(envelope);
        var builder = new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(new ReadOnlySequence<byte>(carrier.Data))
            .WithQualityOfServiceLevel(qos)
            .WithRetainFlag(message.Retain);

        if (carrier.ContentType is not null)
        {
            builder.WithContentType(carrier.ContentType);
        }

        if (envelope.ExpiryInterval is { } interval && interval > TimeSpan.Zero)
        {
            builder.WithMessageExpiryInterval(ToSeconds(interval));
        }

        if (message.ReplyTo is { } reply)
        {
            builder.WithResponseTopic(reply.Value);
        }

        // A response carries the id of its request as causation id; correlation data follows the same key as the expiry timer (D-37).
        var correlation = envelope.CausationId ?? envelope.Id;
        if (message.ReplyTo is not null || envelope.CausationId is not null)
        {
            builder.WithCorrelationData(Encoding.UTF8.GetBytes(correlation));
        }

        foreach (var (name, value) in carrier.Attributes)
        {
            builder.WithUserProperty(name, Encoding.UTF8.GetBytes(value).AsMemory());
        }

        return builder.Build();
    }

    /// <summary>Builds the carrier of a received MQTT message.</summary>
    /// <param name="message">The MQTT message.</param>
    /// <returns>The carrier.</returns>
    public static EnvelopeCarrier ToCarrier(MqttApplicationMessage message)
    {
        var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
        if (message.UserProperties is { } properties)
        {
            foreach (var property in properties)
            {
                // MQTT allows repeated names; the first one wins so a later duplicate cannot override an attribute.
                attributes.TryAdd(property.Name, property.ReadValueAsString());
            }
        }

        return new EnvelopeCarrier(attributes, message.ContentType, message.Payload.ToArray());
    }

    /// <summary>Applies the remaining MQTT message expiry when the sender did not set <c>expiryinterval</c> (Python parity).</summary>
    /// <param name="envelope">The decoded envelope.</param>
    /// <param name="message">The MQTT message.</param>
    /// <returns>The envelope, with the expiry interval filled in when it was absent.</returns>
    public static EventEnvelope WithMqttExpiry(EventEnvelope envelope, MqttApplicationMessage message)
    {
        if (envelope.ExpiryInterval is null && message.MessageExpiryInterval > 0)
        {
            return envelope with { ExpiryInterval = TimeSpan.FromSeconds(message.MessageExpiryInterval) };
        }

        return envelope;
    }

    /// <summary>Converts a duration to whole seconds, rounding up, at least one.</summary>
    /// <param name="interval">The interval.</param>
    /// <returns>The seconds.</returns>
    public static uint ToSeconds(TimeSpan interval) =>
        (uint)Math.Min(MaxExpirySeconds, Math.Max(1d, Math.Ceiling(interval.TotalSeconds)));
}
