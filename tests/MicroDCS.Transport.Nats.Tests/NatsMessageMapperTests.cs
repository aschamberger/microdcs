using System.Text;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using NATS.Client.Core;

namespace MicroDCS.Transport.Nats.Tests;

public sealed class NatsMessageMapperTests
{
    private static readonly CloudEventsEnvelopeCodec _codec = new();

    private static EventEnvelope Envelope(Func<EventEnvelope, EventEnvelope>? change = null)
    {
        var envelope = new EventEnvelope
        {
            Id = "7f1c",
            Source = "urn:test",
            Type = "io.microdcs.test.v1",
            DataContentType = "application/json",
            Subject = "site.area",
            Data = Encoding.UTF8.GetBytes("{\"a\":1}"),
        };
        return change?.Invoke(envelope) ?? envelope;
    }

    private static OutboundMessage Outbound(EventEnvelope envelope, ReplyAddress? replyTo = null) => new()
    {
        BindingId = "b",
        Envelope = envelope,
        Address = new LogicalAddress("dcs", ["site", "area"], null, Intent.Command),
        ReplyTo = replyTo,
    };

    [Fact]
    public void Attributes_become_headers_and_data_the_payload()
    {
        var (headers, data) = NatsMessageMapper.ToNats(_codec, Outbound(Envelope()), "dcs.site.area.commands", durable: true);

        headers["id"].ToString().ShouldBe("7f1c");
        headers["type"].ToString().ShouldBe("io.microdcs.test.v1");
        headers["subject"].ToString().ShouldBe("site.area");
        headers["Content-Type"].ToString().ShouldBe("application/json");
        Encoding.UTF8.GetString(data.Span).ShouldBe("{\"a\":1}");
    }

    [Fact]
    public void A_stream_message_carries_a_message_id_from_subject_source_and_id()
    {
        var (headers, _) = NatsMessageMapper.ToNats(_codec, Outbound(Envelope()), "dcs.site.area.commands", durable: true);

        headers[NatsMessageMapper.MessageIdHeader].ToString().ShouldBe("dcs.site.area.commands:urn:test:7f1c");
    }

    [Fact]
    public void A_core_message_has_neither_message_id_nor_time_to_live()
    {
        var envelope = Envelope(e => e with { ExpiryInterval = TimeSpan.FromSeconds(30) });
        var (headers, _) = NatsMessageMapper.ToNats(_codec, Outbound(envelope), "dcs.reply.i1", durable: false);

        headers.ContainsKey(NatsMessageMapper.MessageIdHeader).ShouldBeFalse();
        headers.ContainsKey(NatsMessageMapper.TtlHeader).ShouldBeFalse();
    }

    [Fact]
    public void The_expiry_interval_becomes_the_message_time_to_live_in_whole_seconds_rounded_up()
    {
        var envelope = Envelope(e => e with { ExpiryInterval = TimeSpan.FromMilliseconds(1500) });
        var (headers, _) = NatsMessageMapper.ToNats(_codec, Outbound(envelope), "dcs.site.area.commands", durable: true);

        headers[NatsMessageMapper.TtlHeader].ToString().ShouldBe("2");
    }

    [Fact]
    public void The_reply_address_travels_in_a_header()
    {
        var (headers, _) = NatsMessageMapper.ToNats(_codec, Outbound(Envelope(), new ReplyAddress("dcs-reply.i1")), "dcs.site.area.commands", durable: true);

        NatsMessageMapper.GetReplyTo(headers).ShouldBe(new ReplyAddress("dcs-reply.i1"));
    }

    [Fact]
    public void A_message_without_the_reply_header_has_no_reply_address()
    {
        NatsMessageMapper.GetReplyTo(new NatsHeaders()).ShouldBeNull();
        NatsMessageMapper.GetReplyTo(null).ShouldBeNull();
    }

    [Fact]
    public void A_sent_message_decodes_back_to_the_same_envelope_without_transport_headers()
    {
        var original = Envelope(e => e with { ExpiryInterval = TimeSpan.FromSeconds(30) });
        var (headers, data) = NatsMessageMapper.ToNats(_codec, Outbound(original, new ReplyAddress("r.i1")), "dcs.site.area.commands", durable: true);

        var carrier = NatsMessageMapper.ToCarrier(headers, data.ToArray());
        carrier.Attributes.Keys.ShouldNotContain(k => k.StartsWith("Nats-", StringComparison.OrdinalIgnoreCase) || k.StartsWith("Mdcs-", StringComparison.OrdinalIgnoreCase));
        _codec.TryDecode(carrier, out var decoded, out var failure).ShouldBeTrue(failure?.Kind.ToString());

        decoded.Id.ShouldBe("7f1c");
        decoded.Type.ShouldBe("io.microdcs.test.v1");
        decoded.Subject.ShouldBe("site.area");
        decoded.Data.ToArray().ShouldBe(original.Data.ToArray());
        decoded.ExpiryInterval.ShouldBe(TimeSpan.FromSeconds(30));
    }

    [Fact]
    public void The_first_value_of_a_repeated_header_wins()
    {
        var headers = new NatsHeaders { ["id"] = new Microsoft.Extensions.Primitives.StringValues(["first", "second"]) };

        NatsMessageMapper.ToCarrier(headers, []).Attributes["id"].ShouldBe("first");
    }

    [Fact]
    public void The_server_time_to_live_fills_an_absent_expiry_interval()
    {
        var headers = new NatsHeaders { [NatsMessageMapper.TtlHeader] = "45" };

        NatsMessageMapper.WithNatsExpiry(Envelope(), headers).ExpiryInterval.ShouldBe(TimeSpan.FromSeconds(45));
    }

    [Fact]
    public void An_expiry_interval_set_by_the_sender_is_kept()
    {
        var headers = new NatsHeaders { [NatsMessageMapper.TtlHeader] = "45" };
        var envelope = Envelope(e => e with { ExpiryInterval = TimeSpan.FromSeconds(10) });

        NatsMessageMapper.WithNatsExpiry(envelope, headers).ExpiryInterval.ShouldBe(TimeSpan.FromSeconds(10));
    }

    [Theory]
    [InlineData("never")]
    [InlineData("-1")]
    [InlineData("1m")]
    public void A_time_to_live_that_is_not_a_positive_number_of_seconds_is_ignored(string value)
    {
        var headers = new NatsHeaders { [NatsMessageMapper.TtlHeader] = value };

        NatsMessageMapper.WithNatsExpiry(Envelope(), headers).ExpiryInterval.ShouldBeNull();
    }
}
