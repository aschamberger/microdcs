using MicroDCS.Core.Messaging;
using Opc.Ua;

namespace MicroDCS.JobControl.Codec;

/// <summary><see cref="IPayloadDescriptor"/> for generated job types, encoded as OPC UA JSON verbose (<see cref="PayloadCodec.OpcUaJson"/>).</summary>
/// <typeparam name="T">The generated type.</typeparam>
/// <param name="typeId">The CloudEvent type id.</param>
/// <param name="schema">The schema URI written to <c>dataschema</c>, if any.</param>
public sealed class OpcUaJsonPayloadDescriptor<T>(string typeId, string? schema) : IPayloadDescriptor
    where T : class, IEncodeable, new()
{
    /// <inheritdoc />
    public string TypeId { get; } = typeId;

    /// <inheritdoc />
    public string? Schema { get; } = schema;

    /// <inheritdoc />
    public string ContentType => JsonPayloadDescriptor<T>.JsonContentType;

    /// <inheritdoc />
    public Type PayloadType => typeof(T);

    /// <inheritdoc />
    public IReadOnlyCollection<string> CarriedNames => [];

    /// <inheritdoc />
    public object Decode(ReadOnlyMemory<byte> data, IReadOnlyDictionary<string, string> extensions) => OpcUaJsonCodec.Decode<T>(data);

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Encode(object payload, IDictionary<string, string> carried)
    {
        if (payload is not T value)
        {
            throw new PayloadException($"Payload is not of type '{typeof(T).Name}'.");
        }

        return OpcUaJsonCodec.Encode(value);
    }
}
