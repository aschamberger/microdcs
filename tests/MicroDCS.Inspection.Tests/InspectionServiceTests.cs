using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;

namespace MicroDCS.Inspection.Tests;

public sealed class InspectionServiceTests
{
    private static readonly InspectionEntry _payloads = InspectionEntries.Create(
        "doc",
        "k",
        ("id", "j1", Sensitivity.Identifier),
        ("body", "{\"secret\":1}", Sensitivity.Payload));

    private static InspectionService Create(FakeInspector inspector, FakeTimeProvider? time = null, Action<ManagementOptions>? configure = null)
    {
        var options = new MicroDcsOptions();
        configure?.Invoke(options.Management);
        return new InspectionService([inspector], Options.Create(options), time ?? new FakeTimeProvider(), NullLoggerFactory.Instance);
    }

    [Fact]
    public async Task PayloadsAreRedactedByDefault()
    {
        var service = Create(new FakeInspector("x", _payloads));

        var page = await service.QueryAsync("test", "t", "x", new InspectionQuery(), TestContext.Current.CancellationToken);

        var fields = page.Entries.Single().Fields;
        fields["id"].Text.ShouldBe("j1");
        fields["body"].Text.ShouldStartWith("<redacted");
        fields["body"].Text.ShouldNotContain("secret");
    }

    [Fact]
    public async Task PayloadsAreShownWhenEnabled()
    {
        var service = Create(new FakeInspector("x", _payloads), configure: m => m.IncludePayloads = true);

        var page = await service.QueryAsync("test", "t", "x", new InspectionQuery(), TestContext.Current.CancellationToken);

        page.Entries.Single().Fields["body"].Text.ShouldBe("{\"secret\":1}");
    }

    [Fact]
    public async Task LimitIsCappedAndResultsAreTruncated()
    {
        var entries = Enumerable.Range(0, 10).Select(i => InspectionEntries.Create("e", i.ToString(System.Globalization.CultureInfo.InvariantCulture))).ToArray();
        var inspector = new FakeInspector("x", entries);
        var service = Create(inspector, configure: m => m.MaxResults = 3);

        var page = await service.QueryAsync("test", "t", "x", new InspectionQuery(Limit: 1000), TestContext.Current.CancellationToken);

        inspector.LastQuery!.Limit.ShouldBe(3);
        page.Entries.Count.ShouldBe(3);
    }

    [Fact]
    public async Task LongValuesAreCut()
    {
        var long1 = InspectionEntries.Create("e", "k", ("v", new string('a', InspectionService.MaxValueLength + 50), Sensitivity.Public));
        var service = Create(new FakeInspector("x", long1));

        var page = await service.QueryAsync("test", "t", "x", new InspectionQuery(), TestContext.Current.CancellationToken);

        page.Entries.Single().Fields["v"].Text.Length.ShouldBe(InspectionService.MaxValueLength + 3);
    }

    [Fact]
    public async Task UnknownInspectorIsRefusedWithTheKnownNames()
    {
        var service = Create(new FakeInspector("x"));

        var ex = await Should.ThrowAsync<InspectionRefusedException>(async () =>
            await service.QueryAsync("test", "t", "nope", new InspectionQuery(), TestContext.Current.CancellationToken));

        ex.Message.ShouldContain("nope");
        ex.Message.ShouldContain("x");
    }

    [Fact]
    public async Task RateLimitRefusesUntilTheWindowPasses()
    {
        var time = new FakeTimeProvider();
        var service = Create(new FakeInspector("x"), time, m => m.RequestsPerMinute = 2);
        var ct = TestContext.Current.CancellationToken;

        await service.QueryAsync("test", "t", "x", new InspectionQuery(), ct);
        await service.QueryAsync("test", "t", "x", new InspectionQuery(), ct);
        await Should.ThrowAsync<InspectionRefusedException>(async () => await service.QueryAsync("test", "t", "x", new InspectionQuery(), ct));

        time.Advance(TimeSpan.FromMinutes(1));
        await service.QueryAsync("test", "t", "x", new InspectionQuery(), ct);
    }

    [Fact]
    public async Task EachCallerHasItsOwnRateLimitWindow()
    {
        var service = Create(new FakeInspector("x"), new FakeTimeProvider(), m => m.RequestsPerMinute = 1);
        var ct = TestContext.Current.CancellationToken;

        await service.QueryAsync("rest", "t", "x", new InspectionQuery(), ct);
        var refused = await Should.ThrowAsync<InspectionRefusedException>(async () => await service.QueryAsync("rest", "t", "x", new InspectionQuery(), ct));
        refused.RateLimited.ShouldBeTrue();

        await service.QueryAsync("mcp", "t", "x", new InspectionQuery(), ct);
    }
}
