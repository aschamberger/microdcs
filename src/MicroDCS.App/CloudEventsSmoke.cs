using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using CloudNative.CloudEvents;
using CloudNative.CloudEvents.SystemTextJson;

namespace MicroDCS.App;

/// <summary>
/// Native AOT smoke check for the upstream CloudEvents SDK (D-14): round-trips events through
/// <see cref="JsonEventFormatter"/> in structured and binary mode and compares every attribute
/// and the data. Run with <c>microdcs smoke</c>; exit code 0 means every case passed.
/// </summary>
/// <remarks>
/// With <c>PublishAot</c> the reflection-based System.Text.Json path is disabled, so the formatter
/// needs options whose <see cref="JsonSerializerOptions.TypeInfoResolver"/> is source generated
/// (<see cref="SmokeJsonContext"/>). The default constructor is checked to fail, so an upstream
/// change in that behaviour shows up here.
/// </remarks>
internal static class CloudEventsSmoke
{
    private static readonly CloudEventAttribute _testExtension =
        CloudEventAttribute.CreateExtension("mdcssmoke", CloudEventAttributeType.String);

    private static readonly CloudEventAttribute[] _extensions = [_testExtension];

    private static readonly DateTimeOffset _time = new(2026, 11, 7, 12, 0, 0, TimeSpan.Zero);

    private static readonly byte[] _binaryData = [0x00, 0x01, 0x7F, 0x80, 0xFE, 0xFF];

    private static readonly JsonSerializerOptions _options = new() { TypeInfoResolver = SmokeJsonContext.Default };

    /// <summary>Runs every case, writes one line per case and returns the process exit code.</summary>
    public static int Run(TextWriter output)
    {
        (string Name, Action Check)[] cases =
        [
            ("structured-jsonelement", StructuredJsonElement),
            ("structured-bytes", StructuredBytes),
            ("binary-jsonelement", BinaryJsonElement),
            ("binary-bytes", BinaryBytes),
            ("structured-sourcegen-typed", StructuredSourceGeneratedType),
            ("default-options-jsonelement-fails", DefaultOptionsJsonElementFails),
        ];

        var failed = 0;
        foreach (var (name, check) in cases)
        {
            try
            {
                check();
                output.WriteLine($"ok   {name}");
            }
            // The smoke run reports every failure kind (including trimming failures such as
            // MissingMethodException) per case instead of stopping at the first one.
#pragma warning disable CA1031 // Justification: see the comment above; the exit code reports the failure.
            catch (Exception ex)
#pragma warning restore CA1031
            {
                failed++;
                output.WriteLine($"FAIL {name}: {ex.GetType().FullName}: {ex.Message}");
            }
        }

        output.WriteLine(failed == 0 ? "cloudevents smoke: all cases passed" : $"cloudevents smoke: {failed} case(s) failed");
        return failed == 0 ? 0 : 1;
    }

    private static void StructuredJsonElement()
    {
        using var doc = JsonDocument.Parse("""{"greeting":"hello","count":3,"nested":{"ok":true}}""");
        var original = NewEvent(doc.RootElement.Clone(), "application/json");
        var formatter = new JsonEventFormatter(_options, default);

        var body = formatter.EncodeStructuredModeMessage(original, out var contentType);
        var decoded = formatter.DecodeStructuredModeMessage(body, contentType, _extensions);

        AssertAttributes(original, decoded);
        AssertJsonEqual(doc.RootElement, ExpectType<JsonElement>(decoded.Data));
    }

    private static void StructuredBytes()
    {
        var original = NewEvent(_binaryData, "application/octet-stream");
        var formatter = new JsonEventFormatter(_options, default);

        var body = formatter.EncodeStructuredModeMessage(original, out var contentType);
        Expect(Encoding.UTF8.GetString(body.Span).Contains("\"data_base64\"", StringComparison.Ordinal), "byte[] data is encoded as data_base64");
        var decoded = formatter.DecodeStructuredModeMessage(body, contentType, _extensions);

        AssertAttributes(original, decoded);
        Expect(ExpectType<byte[]>(decoded.Data).AsSpan().SequenceEqual(_binaryData), "byte[] data round-trips");
    }

    private static void BinaryJsonElement()
    {
        using var doc = JsonDocument.Parse("""[1,"two",{"three":3.0}]""");
        var original = NewEvent(doc.RootElement.Clone(), "application/json");
        var formatter = new JsonEventFormatter(_options, default);

        var data = formatter.EncodeBinaryModeEventData(original);
        var decoded = CopyAttributes(original);
        formatter.DecodeBinaryModeEventData(data, decoded);

        AssertJsonEqual(doc.RootElement, ExpectType<JsonElement>(decoded.Data));
    }

    private static void BinaryBytes()
    {
        var original = NewEvent(_binaryData, "application/octet-stream");
        var formatter = new JsonEventFormatter(_options, default);

        var data = formatter.EncodeBinaryModeEventData(original);
        var decoded = CopyAttributes(original);
        formatter.DecodeBinaryModeEventData(data, decoded);

        Expect(ExpectType<byte[]>(decoded.Data).AsSpan().SequenceEqual(_binaryData), "byte[] data round-trips");
    }

    private static void StructuredSourceGeneratedType()
    {
        var formatter = new JsonEventFormatter<SmokePayload>(_options, default);
        var payload = new SmokePayload("job-42", 7, [1.5, 2.5]);
        var original = NewEvent(payload, "application/json");

        var body = formatter.EncodeStructuredModeMessage(original, out var contentType);
        var decoded = formatter.DecodeStructuredModeMessage(body, contentType, _extensions);

        AssertAttributes(original, decoded);
        var roundTripped = ExpectType<SmokePayload>(decoded.Data);
        Expect(roundTripped.JobOrderId == payload.JobOrderId && roundTripped.Count == payload.Count
            && roundTripped.Values.SequenceEqual(payload.Values), "typed data round-trips");
    }

    private static void DefaultOptionsJsonElementFails()
    {
        using var doc = JsonDocument.Parse("""{"a":1}""");
        var original = NewEvent(doc.RootElement.Clone(), "application/json");
        try
        {
            new JsonEventFormatter().EncodeStructuredModeMessage(original, out _);
        }
        catch (InvalidOperationException)
        {
            return;
        }
        throw new InvalidOperationException("the default JsonEventFormatter serialized JsonElement data; reflection JSON is enabled or upstream changed");
    }

    private static CloudEvent NewEvent(object data, string dataContentType)
    {
        var cloudEvent = new CloudEvent(CloudEventsSpecVersion.V1_0, _extensions)
        {
            Id = "smoke-1",
            Source = new Uri("urn:microdcs:smoke"),
            Type = "dev.microdcs.smoke",
            Subject = "aot",
            Time = _time,
            DataContentType = dataContentType,
            Data = data,
        };
        cloudEvent[_testExtension] = "extension-value";
        return cloudEvent;
    }

    private static CloudEvent CopyAttributes(CloudEvent source)
    {
        var copy = new CloudEvent(CloudEventsSpecVersion.V1_0, _extensions);
        foreach (var (attribute, value) in source.GetPopulatedAttributes())
        {
            copy[attribute] = value;
        }
        return copy;
    }

    private static void AssertAttributes(CloudEvent expected, CloudEvent actual)
    {
        Expect(actual.SpecVersion == expected.SpecVersion, "specversion");
        Expect(actual.Id == expected.Id, "id");
        Expect(actual.Source == expected.Source, "source");
        Expect(actual.Type == expected.Type, "type");
        Expect(actual.Subject == expected.Subject, "subject");
        Expect(actual.Time == expected.Time, "time");
        Expect(actual.DataContentType == expected.DataContentType, "datacontenttype");
        Expect(actual[_testExtension] is "extension-value", "extension attribute");
        Expect(actual.IsValid, "decoded event is valid");
    }

    private static void AssertJsonEqual(JsonElement expected, JsonElement actual) =>
        Expect(JsonElement.DeepEquals(expected, actual), "JSON data round-trips");

    private static T ExpectType<T>(object? value) =>
        value is T typed ? typed : throw new InvalidOperationException($"data is {value?.GetType().FullName ?? "null"}, expected {typeof(T).FullName}");

    private static void Expect(bool condition, string what)
    {
        if (!condition)
        {
            throw new InvalidOperationException($"check failed: {what}");
        }
    }
}

/// <summary>Typed payload for the source-generated JSON case.</summary>
internal sealed record SmokePayload(string JobOrderId, int Count, double[] Values);

/// <summary>Source-generated serializer metadata for the smoke payloads.</summary>
[JsonSerializable(typeof(SmokePayload))]
[JsonSerializable(typeof(JsonElement))]
internal sealed partial class SmokeJsonContext : JsonSerializerContext;
