namespace MicroDCS.Core.Messaging;

/// <summary>
/// The transport carrier of an event in binary content mode: attributes as strings (MQTT user properties, NATS
/// headers, AMQP application properties), the content type and the data.
/// </summary>
/// <param name="Attributes">Attribute name to value, including extension attributes.</param>
/// <param name="ContentType">The transport content type, if any.</param>
/// <param name="Data">The data.</param>
public readonly record struct EnvelopeCarrier(
    IReadOnlyDictionary<string, string> Attributes,
    string? ContentType,
    ReadOnlyMemory<byte> Data);

/// <summary>
/// The only place that touches the CloudEvents SDK (D-14): converts between the carrier and <see cref="EventEnvelope"/>.
/// A MicroDCS binding per transport builds the carrier; the codec owns attribute names, parsing and the collision guard (E-3).
/// </summary>
public interface IEnvelopeCodec
{
    /// <summary>Decodes a carrier. Unknown attributes become <see cref="EventEnvelope.Extensions"/>; extensions named like a standard attribute are dropped.</summary>
    /// <param name="carrier">The carrier.</param>
    /// <param name="envelope">The envelope when decoding succeeded.</param>
    /// <param name="failure">The reason when it did not. Never contains attribute values.</param>
    /// <returns><see langword="true"/> on success.</returns>
    bool TryDecode(in EnvelopeCarrier carrier, out EventEnvelope envelope, out EnvelopeError? failure);

    /// <summary>Encodes an envelope for a transport that uses binary content mode.</summary>
    /// <param name="envelope">The envelope.</param>
    /// <returns>The carrier.</returns>
    EnvelopeCarrier Encode(EventEnvelope envelope);
}
