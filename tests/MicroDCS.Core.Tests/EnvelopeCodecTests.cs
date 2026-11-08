using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Tests;

public sealed class EnvelopeCodecTests
{
    private readonly CloudEventsEnvelopeCodec _codec = new();

    private static EventEnvelope Sample() => new()
    {
        Id = "e1",
        Source = "urn:test:src",
        Type = "com.example.hello.v1",
        DataContentType = "application/json; charset=utf-8",
        DataSchema = "https://example.com/schema",
        Subject = "plant1.line2/res",
        Time = new DateTimeOffset(2026, 11, 8, 10, 0, 0, TimeSpan.Zero),
        RecordedTime = new DateTimeOffset(2026, 11, 8, 10, 0, 1, TimeSpan.Zero),
        ExpiryInterval = TimeSpan.FromSeconds(30),
        CorrelationId = "c1",
        CausationId = "e0",
        Method = "DELETE",
        ActionKey = "job1:a1",
        TraceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        Extensions = new Dictionary<string, string> { ["hiddenstr"] = "secret" },
        Data = new byte[] { 1, 2, 3 },
    };

    [Fact]
    public void Roundtrip_keeps_all_attributes()
    {
        var carrier = _codec.Encode(Sample());

        carrier.Attributes["specversion"].ShouldBe("1.0");
        _codec.TryDecode(carrier, out var decoded, out var failure).ShouldBeTrue();
        failure.ShouldBeNull();

        var expected = Sample();
        decoded.ShouldBeEquivalentTo(expected with { Extensions = decoded.Extensions, Data = decoded.Data });
        decoded.Extensions["hiddenstr"].ShouldBe("secret");
        decoded.Data.ToArray().ShouldBe(new byte[] { 1, 2, 3 });
    }

    [Fact]
    public void Extension_named_like_a_standard_attribute_is_dropped_on_decode()
    {
        var carrier = _codec.Encode(Sample());
        var attributes = new Dictionary<string, string>(carrier.Attributes) { ["custom"] = "x" };
        var injected = new EnvelopeCarrier(attributes, carrier.ContentType, carrier.Data);

        _codec.TryDecode(injected, out var decoded, out _).ShouldBeTrue();

        decoded.Extensions.Keys.Order().ShouldBe(["custom", "hiddenstr"]);
    }

    [Fact]
    public void Extension_named_like_a_standard_attribute_is_not_written_on_encode()
    {
        var envelope = Sample() with { Extensions = new Dictionary<string, string> { ["id"] = "evil", ["correlationid"] = "evil" } };

        var carrier = _codec.Encode(envelope);

        carrier.Attributes["id"].ShouldBe("e1");
        carrier.Attributes["correlationid"].ShouldBe("c1");
    }

    [Theory]
    [InlineData("id")]
    [InlineData("source")]
    [InlineData("type")]
    public void Missing_required_attribute_is_reported_by_name(string name)
    {
        var attributes = new Dictionary<string, string>(_codec.Encode(Sample()).Attributes);
        attributes.Remove(name);

        _codec.TryDecode(new EnvelopeCarrier(attributes, null, default), out _, out var failure).ShouldBeFalse();

        failure!.Kind.ShouldBe(ErrorKind.MissingProperty);
        failure.Message.ShouldContain(name);
    }

    [Fact]
    public void Invalid_value_is_reported_without_the_value()
    {
        var attributes = new Dictionary<string, string>(_codec.Encode(Sample()).Attributes) { ["time"] = "not-a-time-SECRET" };

        _codec.TryDecode(new EnvelopeCarrier(attributes, null, default), out _, out var failure).ShouldBeFalse();

        failure!.Kind.ShouldBe(ErrorKind.PropertyInvalid);
        failure.Message.ShouldContain("time");
        failure.Message.ShouldNotContain("SECRET");
    }

    [Fact]
    public void Error_roundtrips_as_key_value_text()
    {
        var error = new EnvelopeError(ErrorKind.Timeout, "too slow", new Dictionary<string, string> { ["a"] = "1", ["b"] = "2" });

        var carrier = _codec.Encode(Sample() with { Error = error });

        carrier.Attributes["mdcserrorcontext"].ShouldBe("a=1,b=2");
        _codec.TryDecode(carrier, out var decoded, out _).ShouldBeTrue();
        decoded.Error!.Kind.ShouldBe(ErrorKind.Timeout);
        decoded.Error.Message.ShouldBe("too slow");
        decoded.Error.Context!["b"].ShouldBe("2");
    }

    [Fact]
    public void Error_without_message_gets_the_default_message()
    {
        var carrier = _codec.Encode(Sample() with { Error = new EnvelopeError(ErrorKind.Unknown, "") });

        carrier.Attributes["mdcserrormessage"].ShouldBe("Unknown error occurred.");
    }

    [Fact]
    public void Error_context_with_separator_is_rejected_on_encode()
    {
        var error = new EnvelopeError(ErrorKind.Unknown, "m", new Dictionary<string, string> { ["a"] = "x,y" });

        Should.Throw<ArgumentException>(() => _codec.Encode(Sample() with { Error = error }));
    }
}
