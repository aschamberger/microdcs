using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;

namespace MicroDCS.Inspection.Tests;

public sealed class McpContractTests
{
    private static async Task<McpClient> ConnectAsync(TestApp app, CancellationToken ct) =>
        await McpClient.CreateAsync(new HttpClientTransport(new HttpClientTransportOptions { Endpoint = app.McpEndpoint! }), cancellationToken: ct);

    private static string Text(CallToolResult result) => result.Content.OfType<TextContentBlock>().Single().Text;

    [Fact]
    public async Task ReadToolsAreListedAndTheWriteGroupIsOffByDefault()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync([], ct);
        await using var client = await ConnectAsync(app, ct);

        var names = (await client.ListToolsAsync(cancellationToken: ct)).Select(t => t.Name).ToList();

        names.ShouldBe(
            ["app_status", "config_effective", "dedupe_lookup", "inspect", "inspectors_list", "pipeline_recent", "processors_list", "stream_pending", "valkey_document", "valkey_keys"],
            ignoreOrder: true);
    }

    [Fact]
    public async Task WriteGroupAppearsWhenEnabled()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync(new() { ["MicroDcs:Management:WriteEnabled"] = "true" }, ct);
        await using var client = await ConnectAsync(app, ct);

        (await client.ListToolsAsync(cancellationToken: ct)).Select(t => t.Name).ShouldContain("inject_event");
    }

    [Fact]
    public async Task AppStatusAndProcessorsDescribeTheRunningApplication()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync([], ct);
        await using var client = await ConnectAsync(app, ct);

        var status = Text(await client.CallToolAsync("app_status", cancellationToken: ct));
        var processors = Text(await client.CallToolAsync("processors_list", cancellationToken: ct));

        status.ShouldContain("memory (AtLeastOnce)");
        processors.ShouldContain("PingProcessor");
        processors.ShouldContain("test.ping.v1");
    }

    [Fact]
    public async Task InjectedEventShowsUpInTheDecisionBufferWithoutItsPayload()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync(new() { ["MicroDcs:Management:WriteEnabled"] = "true" }, ct);
        await using var client = await ConnectAsync(app, ct);

        var outcome = await client.CallToolAsync(
            "inject_event",
            new Dictionary<string, object?> { ["binding"] = "ping", ["type"] = "test.ping.v1", ["subject"] = "plant1/x", ["dataJson"] = "{\"text\":\"top-secret\"}" },
            cancellationToken: ct);
        var recent = Text(await client.CallToolAsync("pipeline_recent", cancellationToken: ct));

        Text(outcome).ShouldBe("Ack");
        recent.ShouldContain("test.ping.v1");
        recent.ShouldContain("Processed");
        recent.ShouldNotContain("top-secret");
    }

    [Fact]
    public async Task InjectIntoUnknownBindingIsRefusedWithAReadableMessage()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync(new() { ["MicroDcs:Management:WriteEnabled"] = "true" }, ct);
        await using var client = await ConnectAsync(app, ct);

        var result = await client.CallToolAsync(
            "inject_event",
            new Dictionary<string, object?> { ["binding"] = "nope", ["type"] = "t", ["subject"] = "a/b" },
            cancellationToken: ct);

        result.IsError.ShouldBe(true);
        Text(result).ShouldContain("Unknown binding");
    }

    [Fact]
    public async Task ValkeyToolsExplainThatTheInspectorIsMissing()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync([], ct);
        await using var client = await ConnectAsync(app, ct);

        var result = await client.CallToolAsync("valkey_keys", new Dictionary<string, object?> { ["kind"] = "scopes" }, cancellationToken: ct);

        result.IsError.ShouldBe(true);
        Text(result).ShouldContain("Unknown inspector 'valkey'");
    }

    [Fact]
    public async Task DisabledManagementOpensNoPort()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync(new() { ["MicroDcs:Management:Enabled"] = "false", ["MicroDcs:Management:McpEnabled"] = "false" }, ct);

        app.McpEndpoint.ShouldBeNull();
    }
}
