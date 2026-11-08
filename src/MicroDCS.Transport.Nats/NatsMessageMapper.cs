using System.Globalization;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using NATS.Client.Core;

namespace MicroDCS.Transport.Nats;

/// <summary>
/// Maps between the envelope carrier and NATS headers (binary content mode): attributes are headers under their own name,
/// <c>datacontenttype</c> is <c>Content-Type</c>, the data is the payload. Pure, so it is tested without a server.
/// Attribute names are lowercase letters and digits, so they never collide with the <c>Nats-*</c> or <c>Mdcs-*</c> headers.
/// </summary>
internal static class NatsMessageMapper
{
    /// <summary>Header that carries the per-instance reply subject of a command (a JetStream message has no usable reply field: it holds the acknowledgement subject).</summary>
    public const string ReplyToHeader = "Mdcs-Reply-To";

    /// <summary>JetStream publisher-side duplicate detection (a retry after a lost acknowledgement does not store twice).</summary>
    public const string MessageIdHeader = "Nats-Msg-Id";

    /// <summary>Per-message time to live, enforced by the server (2.11 and later, stream option <c>AllowMsgTTL</c>).</summary>
    public const string TtlHeader = "Nats-TTL";

    private const string ContentTypeHeader = "Content-Type";

    /// <summary>Builds the headers and payload of an outbound message.</summary>
    /// <param name="codec">The envelope codec.</param>
    /// <param name="message">The outbound message.</param>
    /// <param name="subject">The rendered subject, part of the message id.</param>
    /// <param name="durable">Whether the message goes to a stream (message id and TTL apply) or to a core subject (reply).</param>
    /// <returns>The headers and the data.</returns>
    public static (NatsHeaders Headers, ReadOnlyMemory<byte> Data) ToNats(IEnvelopeCodec codec, OutboundMessage message, string subject, bool durable)
    {
        var envelope = message.Envelope;
        var carrier = codec.Encode(envelope);
        var headers = new NatsHeaders();
        foreach (var (name, value) in carrier.Attributes)
        {
            headers[name] = value;
        }

        if (carrier.ContentType is not null)
        {
            headers[ContentTypeHeader] = carrier.ContentType;
        }

        if (message.ReplyTo is { } reply)
        {
            headers[ReplyToHeader] = reply.Value;
        }

        if (durable)
        {
            headers[MessageIdHeader] = subject + ":" + envelope.Source + ":" + envelope.Id;
            if (envelope.ExpiryInterval is { } interval && interval > TimeSpan.Zero)
            {
                headers[TtlHeader] = ToSeconds(interval).ToString(CultureInfo.InvariantCulture);
            }
        }

        return (headers, carrier.Data);
    }

    /// <summary>Builds the carrier of a received message.</summary>
    /// <param name="headers">The headers, or <see langword="null"/> when the message has none.</param>
    /// <param name="data">The payload.</param>
    /// <returns>The carrier.</returns>
    public static EnvelopeCarrier ToCarrier(NatsHeaders? headers, byte[]? data)
    {
        var attributes = new Dictionary<string, string>(StringComparer.Ordinal);
        string? contentType = null;
        if (headers is not null)
        {
            foreach (var (name, values) in headers)
            {
                if (string.Equals(name, ContentTypeHeader, StringComparison.OrdinalIgnoreCase))
                {
                    contentType = values.FirstOrDefault();
                }
                else if (!IsTransportHeader(name) && values.FirstOrDefault() is { } value)
                {
                    // Repeated values: the first one wins so a later duplicate cannot override an attribute.
                    attributes.TryAdd(name, value);
                }
            }
        }

        return new EnvelopeCarrier(attributes, contentType, data ?? []);
    }

    /// <summary>Reads the reply subject from the headers.</summary>
    /// <param name="headers">The headers.</param>
    /// <returns>The reply address, or <see langword="null"/>.</returns>
    public static ReplyAddress? GetReplyTo(NatsHeaders? headers) =>
        headers is not null && headers.TryGetValue(ReplyToHeader, out var values) && values.FirstOrDefault() is { Length: > 0 } value
            ? new ReplyAddress(value)
            : null;

    /// <summary>Applies the time to live of the message when the sender did not set <c>expiryinterval</c>.</summary>
    /// <param name="envelope">The decoded envelope.</param>
    /// <param name="headers">The headers.</param>
    /// <returns>The envelope, with the expiry interval filled in when it was absent.</returns>
    public static EventEnvelope WithNatsExpiry(EventEnvelope envelope, NatsHeaders? headers)
    {
        if (envelope.ExpiryInterval is null
            && headers is not null
            && headers.TryGetValue(TtlHeader, out var values)
            && long.TryParse(values.FirstOrDefault(), NumberStyles.None, CultureInfo.InvariantCulture, out var seconds)
            && seconds > 0)
        {
            return envelope with { ExpiryInterval = TimeSpan.FromSeconds(seconds) };
        }

        return envelope;
    }

    /// <summary>Converts a duration to whole seconds, rounding up, at least one.</summary>
    /// <param name="interval">The interval.</param>
    /// <returns>The seconds.</returns>
    public static long ToSeconds(TimeSpan interval) => (long)Math.Max(1d, Math.Ceiling(interval.TotalSeconds));

    private static bool IsTransportHeader(string name) =>
        name.StartsWith("Nats-", StringComparison.OrdinalIgnoreCase) || name.StartsWith("Mdcs-", StringComparison.OrdinalIgnoreCase);
}
