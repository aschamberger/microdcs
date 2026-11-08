using System.Buffers;
using System.Text;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MicroDCS.Transport.Mqtt;
using MQTTnet;
using MQTTnet.Protocol;

namespace MicroDCS.Transport.Mqtt.Tests;

public sealed class MqttMessageMapperTests
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

    private static OutboundMessage Outbound(EventEnvelope envelope, ReplyAddress? replyTo = null, bool retain = false) => new()
    {
        BindingId = "b",
        Envelope = envelope,
        Address = new LogicalAddress("dcs", ["site", "area"], null, Intent.Command),
        ReplyTo = replyTo,
        Retain = retain,
    };

    [Fact]
    public void Attributes_become_user_properties_and_data_the_payload()
    {
        var message = MqttMessageMapper.ToMqtt(_codec, Outbound(Envelope()), "dcs/site/area/commands", MqttQualityOfServiceLevel.AtLeastOnce);

        message.Topic.ShouldBe("dcs/site/area/commands");
        message.ContentType.ShouldBe("application/json");
        Encoding.UTF8.GetString(message.Payload).ShouldBe("{\"a\":1}");
        message.QualityOfServiceLevel.ShouldBe(MqttQualityOfServiceLevel.AtLeastOnce);
        message.UserProperties.ShouldNotBeNull().Select(p => p.Name).ShouldContain("type");
    }

    [Fact]
    public void A_command_carries_response_topic_and_correlation_data()
    {
        var envelope = Envelope(e => e with { ExpiryInterval = TimeSpan.FromMilliseconds(1500) });

        var message = MqttMessageMapper.ToMqtt(_codec, Outbound(envelope, new ReplyAddress("dcs/replies/i1")), "t", MqttQualityOfServiceLevel.AtLeastOnce);

        message.ResponseTopic.ShouldBe("dcs/replies/i1");
        Encoding.UTF8.GetString(message.CorrelationData).ShouldBe("7f1c");
        message.MessageExpiryInterval.ShouldBe(2u);
    }

    [Fact]
    public void A_response_correlates_to_its_request()
    {
        var envelope = Envelope(e => e with { CausationId = "req-1" });

        var message = MqttMessageMapper.ToMqtt(_codec, Outbound(envelope), "t", MqttQualityOfServiceLevel.AtLeastOnce);

        Encoding.UTF8.GetString(message.CorrelationData).ShouldBe("req-1");
        message.ResponseTopic.ShouldBeNull();
    }

    [Fact]
    public void An_event_without_reply_has_no_response_properties()
    {
        var message = MqttMessageMapper.ToMqtt(_codec, Outbound(Envelope()), "t", MqttQualityOfServiceLevel.AtMostOnce);

        message.ResponseTopic.ShouldBeNull();
        message.CorrelationData.ShouldBeNull();
        message.MessageExpiryInterval.ShouldBe(0u);
        message.Retain.ShouldBeFalse();
    }

    [Fact]
    public void Retain_is_carried()
    {
        MqttMessageMapper.ToMqtt(_codec, Outbound(Envelope(), retain: true), "t", MqttQualityOfServiceLevel.AtLeastOnce).Retain.ShouldBeTrue();
    }

    [Fact]
    public void Round_trip_keeps_the_envelope()
    {
        var envelope = Envelope(e => e with { CorrelationId = "c1", ActionKey = "job:act", Extensions = new Dictionary<string, string> { ["site"] = "x" } });
        var mqtt = MqttMessageMapper.ToMqtt(_codec, Outbound(envelope), "t", MqttQualityOfServiceLevel.AtLeastOnce);

        _codec.TryDecode(MqttMessageMapper.ToCarrier(mqtt), out var decoded, out var failure).ShouldBeTrue(failure?.Message);

        decoded.Id.ShouldBe("7f1c");
        decoded.CorrelationId.ShouldBe("c1");
        decoded.ActionKey.ShouldBe("job:act");
        decoded.Extensions["site"].ShouldBe("x");
        decoded.Data.ToArray().ShouldBe(envelope.Data.ToArray());
    }

    [Fact]
    public void The_first_of_repeated_user_properties_wins()
    {
        var mqtt = new MqttApplicationMessageBuilder()
            .WithTopic("t")
            .WithUserProperty("type", Encoding.UTF8.GetBytes("first").AsMemory())
            .WithUserProperty("type", Encoding.UTF8.GetBytes("second").AsMemory())
            .Build();

        MqttMessageMapper.ToCarrier(mqtt).Attributes["type"].ShouldBe("first");
    }

    [Fact]
    public void Remaining_broker_expiry_fills_a_missing_interval()
    {
        var mqtt = new MqttApplicationMessageBuilder().WithTopic("t").WithMessageExpiryInterval(30).Build();

        MqttMessageMapper.WithMqttExpiry(Envelope(), mqtt).ExpiryInterval.ShouldBe(TimeSpan.FromSeconds(30));
        MqttMessageMapper.WithMqttExpiry(Envelope(e => e with { ExpiryInterval = TimeSpan.FromSeconds(5) }), mqtt).ExpiryInterval.ShouldBe(TimeSpan.FromSeconds(5));
    }

    [Theory]
    [InlineData(0.1, 1u)]
    [InlineData(1.0, 1u)]
    [InlineData(1.01, 2u)]
    [InlineData(3600, 3600u)]
    public void Expiry_is_rounded_up_to_whole_seconds(double seconds, uint expected)
    {
        MqttMessageMapper.ToSeconds(TimeSpan.FromSeconds(seconds)).ShouldBe(expected);
    }
}
