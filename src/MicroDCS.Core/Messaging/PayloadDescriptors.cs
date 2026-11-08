using System.Collections.Immutable;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace MicroDCS.Core.Messaging;

/// <summary>A payload property carried as a CloudEvent extension attribute (E-4).</summary>
/// <typeparam name="T">The payload type.</typeparam>
/// <param name="Name">The attribute name.</param>
/// <param name="Get">Reads the value; <see langword="null"/> means not set.</param>
/// <param name="Set">Returns a copy of the payload with the value set (the generator emits a <c>with</c> expression).</param>
public sealed record CarriedField<T>(string Name, Func<T, string?> Get, Func<T, string, T> Set)
    where T : class;

/// <summary>
/// <see cref="IPayloadDescriptor"/> for JSON payloads: System.Text.Json with a source-generated
/// <see cref="JsonTypeInfo{T}"/>, never the reflection overloads (AOT). The generator emits instances of this class.
/// </summary>
/// <typeparam name="T">The payload type.</typeparam>
public sealed class JsonPayloadDescriptor<T> : IPayloadDescriptor
    where T : class
{
    /// <summary>The content type written for JSON payloads.</summary>
    public const string JsonContentType = "application/json; charset=utf-8";

    private readonly JsonTypeInfo<T> _typeInfo;
    private readonly ImmutableArray<CarriedField<T>> _carried;

    /// <summary>Creates the descriptor.</summary>
    /// <param name="typeId">The CloudEvent type id.</param>
    /// <param name="schema">The schema URI, if any.</param>
    /// <param name="typeInfo">The generated type info.</param>
    /// <param name="carried">The properties carried as attributes.</param>
    public JsonPayloadDescriptor(string typeId, string? schema, JsonTypeInfo<T> typeInfo, ImmutableArray<CarriedField<T>> carried = default)
    {
        TypeId = typeId;
        Schema = schema;
        _typeInfo = typeInfo;
        _carried = carried.IsDefault ? [] : carried;
        CarriedNames = [.. _carried.Select(c => c.Name)];
    }

    /// <inheritdoc />
    public string TypeId { get; }

    /// <inheritdoc />
    public string? Schema { get; }

    /// <inheritdoc />
    public string ContentType => JsonContentType;

    /// <inheritdoc />
    public Type PayloadType => typeof(T);

    /// <inheritdoc />
    public IReadOnlyCollection<string> CarriedNames { get; }

    /// <inheritdoc />
    public object Decode(ReadOnlyMemory<byte> data, IReadOnlyDictionary<string, string> extensions)
    {
        ArgumentNullException.ThrowIfNull(extensions);

        T? payload;
        try
        {
            payload = JsonSerializer.Deserialize(data.Span, _typeInfo);
        }
        catch (JsonException ex)
        {
            // JsonException messages can quote values; keep only the path and the type.
            throw new PayloadException($"Payload of type '{TypeId}' is invalid at '{ex.Path}'.");
        }

        if (payload is null)
        {
            throw new PayloadException($"Payload of type '{TypeId}' is empty.");
        }

        foreach (var field in _carried)
        {
            if (extensions.TryGetValue(field.Name, out var value))
            {
                payload = field.Set(payload, value);
            }
        }

        return payload;
    }

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Encode(object payload, IDictionary<string, string> carried)
    {
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentNullException.ThrowIfNull(carried);

        if (payload is not T typed)
        {
            throw new PayloadException($"Payload is not of type '{TypeId}'.");
        }

        foreach (var field in _carried)
        {
            if (field.Get(typed) is { } value)
            {
                carried[field.Name] = value;
            }
        }

        return JsonSerializer.SerializeToUtf8Bytes(typed, _typeInfo);
    }
}

/// <summary>A payload that is raw bytes (<see cref="PayloadCodec.Binary"/>).</summary>
/// <param name="Data">The bytes.</param>
public sealed record BinaryPayload(ReadOnlyMemory<byte> Data);

/// <summary><see cref="IPayloadDescriptor"/> for <see cref="BinaryPayload"/>: the bytes pass through unchanged.</summary>
public sealed class BinaryPayloadDescriptor : IPayloadDescriptor
{
    /// <summary>Creates the descriptor.</summary>
    /// <param name="typeId">The CloudEvent type id.</param>
    /// <param name="schema">The schema URI, if any.</param>
    /// <param name="contentType">The content type; <c>application/octet-stream</c> by default.</param>
    public BinaryPayloadDescriptor(string typeId, string? schema = null, string contentType = "application/octet-stream")
    {
        TypeId = typeId;
        Schema = schema;
        ContentType = contentType;
    }

    /// <inheritdoc />
    public string TypeId { get; }

    /// <inheritdoc />
    public string? Schema { get; }

    /// <inheritdoc />
    public string ContentType { get; }

    /// <inheritdoc />
    public Type PayloadType => typeof(BinaryPayload);

    /// <inheritdoc />
    public IReadOnlyCollection<string> CarriedNames => [];

    /// <inheritdoc />
    public object Decode(ReadOnlyMemory<byte> data, IReadOnlyDictionary<string, string> extensions) => new BinaryPayload(data);

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Encode(object payload, IDictionary<string, string> carried) =>
        payload is BinaryPayload binary ? binary.Data : throw new PayloadException($"Payload is not of type '{TypeId}'.");
}
