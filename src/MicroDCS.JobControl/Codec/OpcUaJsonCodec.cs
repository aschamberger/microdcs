using System.Buffers;
using System.Text.Json;
using MicroDCS.Core.Messaging;
using Opc.Ua;

namespace MicroDCS.JobControl.Codec;

/// <summary>
/// OPC UA JSON, verbose, for the generated job types (D-34). It hides three traps of the stack: the root is written with
/// <see cref="IEncodeable.Encode"/> rather than <c>WriteEncodeable(null, ...)</c>, a decoded type keeps defaults for missing fields
/// (required fields are checked separately, see <see cref="JobControlValidation"/>), and hand-built V2 objects need their
/// <c>EncodingMask</c> bits, which the conversions set.
/// </summary>
public static class OpcUaJsonCodec
{
    /// <summary>Encodes a value.</summary>
    /// <typeparam name="T">The generated type.</typeparam>
    /// <param name="value">The value.</param>
    /// <returns>The UTF-8 JSON.</returns>
    /// <exception cref="PayloadException">The value cannot be encoded; the message holds no payload values.</exception>
    public static byte[] Encode<T>(T value)
        where T : class, IEncodeable
    {
        ArgumentNullException.ThrowIfNull(value);

        try
        {
            var buffer = new ArrayBufferWriter<byte>();
            using (var encoder = new JsonEncoder(buffer, JobControlContext.Shared, JsonEncoderOptions.Verbose))
            {
                value.Encode(encoder);
            }

            return buffer.WrittenSpan.ToArray();
        }
        catch (ServiceResultException)
        {
            throw new PayloadException($"Payload of type '{typeof(T).Name}' cannot be encoded as OPC UA JSON.");
        }
    }

    /// <summary>Decodes a value. Unknown fields are ignored; missing fields keep their defaults.</summary>
    /// <typeparam name="T">The generated type.</typeparam>
    /// <param name="data">The UTF-8 JSON.</param>
    /// <returns>The value.</returns>
    /// <exception cref="PayloadException">The payload is not valid; the message holds no payload values.</exception>
    public static T Decode<T>(ReadOnlyMemory<byte> data)
        where T : class, IEncodeable, new()
    {
        try
        {
            using var decoder = new JsonDecoder(new ReadOnlySequence<byte>(data), JobControlContext.Shared, new JsonDecoderOptions());
            var value = new T();
            value.Decode(decoder);
            return value;
        }
        catch (Exception ex) when (ex is ServiceResultException or JsonException or FormatException or InvalidOperationException)
        {
            // The messages of these exceptions can quote field values (D-36).
            throw new PayloadException($"Payload of type '{typeof(T).Name}' is not valid OPC UA JSON.");
        }
    }
}
