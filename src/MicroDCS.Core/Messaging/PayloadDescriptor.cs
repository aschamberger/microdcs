namespace MicroDCS.Core.Messaging;

/// <summary>How a payload type is encoded (D-34 for job models).</summary>
public enum PayloadCodec
{
    /// <summary>System.Text.Json with a generated <c>JsonTypeInfo</c>.</summary>
    Json,

    /// <summary>OPC UA JSON, verbose (job models, D-34).</summary>
    OpcUaJson,

    /// <summary>Raw bytes.</summary>
    Binary,
}

/// <summary>
/// Describes one payload type: CloudEvent type id, schema, codec and the (de)serialization functions. The source
/// generator emits one per <c>[CloudEventType]</c> record (S08); handwritten descriptors are allowed.
/// </summary>
public interface IPayloadDescriptor
{
    /// <summary>The CloudEvent type id.</summary>
    string TypeId { get; }

    /// <summary>The schema URI written to <c>dataschema</c>, if any.</summary>
    string? Schema { get; }

    /// <summary>The content type written for the encoded payload.</summary>
    string ContentType { get; }

    /// <summary>The payload CLR type.</summary>
    Type PayloadType { get; }

    /// <summary>Decodes the payload. Throws <see cref="PayloadException"/> without echoing values.</summary>
    /// <param name="data">The data.</param>
    /// <param name="extensions">Extension attributes carrying payload fields (E-4).</param>
    /// <returns>The payload object.</returns>
    object Decode(ReadOnlyMemory<byte> data, IReadOnlyDictionary<string, string> extensions);

    /// <summary>Encodes the payload and collects the fields carried as attributes.</summary>
    /// <param name="payload">The payload object.</param>
    /// <param name="carried">Receives attributes to add to the envelope.</param>
    /// <returns>The encoded data.</returns>
    ReadOnlyMemory<byte> Encode(object payload, IDictionary<string, string> carried);
}

/// <summary>A payload could not be decoded or encoded. The message names the field, never its value.</summary>
public sealed class PayloadException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="message">The message, free of payload values.</param>
    public PayloadException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception.</summary>
    /// <param name="message">The message, free of payload values.</param>
    /// <param name="inner">The cause.</param>
    public PayloadException(string message, Exception inner)
        : base(message, inner)
    {
    }

    /// <summary>Creates the exception.</summary>
    public PayloadException()
    {
    }
}
