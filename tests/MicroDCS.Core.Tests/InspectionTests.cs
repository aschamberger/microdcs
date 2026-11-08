using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Tests;

public sealed class InspectionTests
{
    private static EventEnvelope Envelope(string id) => new() { Id = id, Source = "urn:s", Type = "t.v1" };

    [Fact]
    public void RingBufferKeepsTheNewestDecisionsFirst()
    {
        var log = new PipelineDecisionLog(3);
        for (var i = 1; i <= 5; i++)
        {
            log.Record("b", Envelope("e" + i), "Processed", TimeSpan.Zero, DateTimeOffset.UnixEpoch);
        }

        log.Newest(10).Select(d => d.EventId).ShouldBe(["e5", "e4", "e3"]);
        log.Newest(2).Select(d => d.Sequence).ShouldBe([5L, 4L]);
    }

    [Fact]
    public void ZeroCapacityRecordsNothing()
    {
        var log = new PipelineDecisionLog(0);

        log.Record("b", Envelope("e1"), "Processed", TimeSpan.Zero, DateTimeOffset.UnixEpoch);

        log.Newest(10).ShouldBeEmpty();
    }

    [Fact]
    public void RedactionReplacesOnlyPayloadValues()
    {
        var page = new InspectionPage([InspectionEntries.Create("k", "1", ("a", "visible", Sensitivity.Identifier), ("b", "hidden", Sensitivity.Payload))], null);

        var redacted = Redaction.Apply(page, includePayloads: false).Entries.Single().Fields;

        redacted["a"].Text.ShouldBe("visible");
        redacted["b"].Text.ShouldBe("<redacted 6 chars>");
    }

    private static string[] Errors(Action<ManagementOptions> configure)
    {
        var options = new MicroDcsOptions { CloudEventSource = "urn:t" };
        configure(options.Management);
        var result = new MicroDcsOptionsValidator().Validate(null, options);
        return result.Failures?.ToArray() ?? [];
    }

    [Fact]
    public void DefaultManagementOptionsAreValidAndOff()
    {
        Errors(_ => { }).ShouldBeEmpty();
        new ManagementOptions().Enabled.ShouldBeFalse();
    }

    [Fact]
    public void FlagsNeedTheListener() =>
        Errors(m => m.McpEnabled = true).ShouldContain(e => e.Contains("need Enabled", StringComparison.Ordinal));

    [Fact]
    public void WriteGroupNeedsMcp() =>
        Errors(m =>
        {
            m.Enabled = true;
            m.WriteEnabled = true;
        }).ShouldContain(e => e.Contains("WriteEnabled needs McpEnabled", StringComparison.Ordinal));

    [Fact]
    public void NonLoopbackNeedsABearerTokenFile()
    {
        Errors(m =>
        {
            m.Enabled = true;
            m.Url = "http://0.0.0.0:8081";
        }).ShouldContain(e => e.Contains("BearerTokenFile", StringComparison.Ordinal));

        Errors(m =>
        {
            m.Enabled = true;
            m.Url = "http://0.0.0.0:8081";
            m.BearerTokenFile = "/run/secrets/token";
        }).ShouldBeEmpty();
    }

    [Fact]
    public void LoopbackNeedsNoToken() =>
        Errors(m =>
        {
            m.Enabled = true;
            m.Url = "http://localhost:8081";
        }).ShouldBeEmpty();
}
