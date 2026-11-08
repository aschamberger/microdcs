using System.Collections.Immutable;
using System.Text.Json.Serialization;
using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Tests;

public sealed record SampleHello(string Name)
{
    // A carried property must also be [JsonIgnore]; the S08 analyzer will enforce the pair.
    [JsonIgnore]
    public string? HiddenStr { get; init; }
}

[JsonSerializable(typeof(SampleHello))]
internal sealed partial class SampleJsonContext : JsonSerializerContext;

public sealed class PayloadDescriptorTests
{
    private static JsonPayloadDescriptor<SampleHello> Descriptor() => new(
        "hello.v1",
        null,
        SampleJsonContext.Default.SampleHello,
        [new CarriedField<SampleHello>("hiddenstr", p => p.HiddenStr, (p, v) => p with { HiddenStr = v })]);

    [Fact]
    public void Carried_field_travels_as_attribute_and_is_restored()
    {
        var carried = new Dictionary<string, string>();
        var data = Descriptor().Encode(new SampleHello("Bob") { HiddenStr = "h" }, carried);

        carried["hiddenstr"].ShouldBe("h");
        System.Text.Encoding.UTF8.GetString(data.Span).ShouldNotContain("hiddenstr", Case.Insensitive);

        var decoded = (SampleHello)Descriptor().Decode(data, carried);
        decoded.Name.ShouldBe("Bob");
        decoded.HiddenStr.ShouldBe("h");
    }

    [Fact]
    public void Invalid_json_names_the_type_not_the_value()
    {
        var ex = Should.Throw<PayloadException>(() => Descriptor().Decode("""{"name": 12345SECRET}"""u8.ToArray(), new Dictionary<string, string>()));

        ex.Message.ShouldContain("hello.v1");
        ex.Message.ShouldNotContain("SECRET");
    }

    [Fact]
    public void Null_payload_is_rejected()
    {
        Should.Throw<PayloadException>(() => Descriptor().Decode("null"u8.ToArray(), new Dictionary<string, string>()));
    }

    [Fact]
    public void Binary_passes_bytes_through()
    {
        var descriptor = new BinaryPayloadDescriptor("raw.v1");
        var payload = (BinaryPayload)descriptor.Decode(new byte[] { 9 }, new Dictionary<string, string>());

        descriptor.Encode(payload, new Dictionary<string, string>()).ToArray().ShouldBe([(byte)9]);
        descriptor.ContentType.ShouldBe("application/octet-stream");
    }
}
