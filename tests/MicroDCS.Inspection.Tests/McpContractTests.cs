using MicroDCS.Maintenance;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using NSubstitute;

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
            ["app_status", "config_effective", "dedupe_lookup", "inspect", "inspectors_list", "job_order", "job_orders", "pipeline_recent", "processors_list", "publisher_status", "sfc_dryrun", "sfc_explain", "stream_pending", "valkey_document", "valkey_keys"],
            ignoreOrder: true);
    }

    private const string Recipe = """{"Steps":[{"Name":"A","Initial":true},{"Name":"B"}],"Transitions":[{"Source":"A","Target":"B","Condition":"true"}],"Actions":[{"Name":"heat","Step":"A","Qualifier":"N","Interaction":"push_command","TypeId":"t.heat","TimeoutSeconds":10},{"Name":"cool","Step":"B","Qualifier":"N","Interaction":"push_command","TypeId":"t.cool","TimeoutSeconds":10}]}""";

    [Fact]
    public async Task SfcToolsExplainAndDryRunARecipeWithoutRunningAnything()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync([], ct);
        await using var client = await ConnectAsync(app, ct);

        var explained = Text(await client.CallToolAsync("sfc_explain", new Dictionary<string, object?> { ["recipe"] = Recipe }, cancellationToken: ct));
        var dryRun = Text(await client.CallToolAsync(
            "sfc_dryrun",
            new Dictionary<string, object?> { ["recipe"] = Recipe, ["script"] = """{"replies":{"heat":["silent","ok@2000"]}}""" },
            cancellationToken: ct));

        explained.ShouldContain("initial step 'A'");
        explained.ShouldContain("action 'heat': push_command t.heat, timeout 10s");
        dryRun.ShouldContain("run ended: Completed");
        dryRun.ShouldContain("start_work push job-1:heat type=t.heat attempt=2");
        dryRun.ShouldContain("cancel_work job-1:heat");
    }

    [Fact]
    public async Task SfcToolsShareTheRequestRateLimitOfTheOtherTools()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync(new() { ["MicroDcs:Management:RequestsPerMinute"] = "2" }, ct);
        await using var client = await ConnectAsync(app, ct);
        var arguments = new Dictionary<string, object?> { ["recipe"] = Recipe };

        var first = await client.CallToolAsync("sfc_explain", arguments, cancellationToken: ct);
        var second = await client.CallToolAsync("sfc_dryrun", arguments, cancellationToken: ct);
        var third = await client.CallToolAsync("sfc_explain", arguments, cancellationToken: ct);

        first.IsError.ShouldNotBe(true);
        second.IsError.ShouldNotBe(true);
        third.IsError.ShouldBe(true);
    }

    [Fact]
    public async Task SfcToolsRefuseInvalidInputWithAReadableMessage()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync([], ct);
        await using var client = await ConnectAsync(app, ct);
        const string NoInitial = """{"Steps":[{"Name":"A"}],"Transitions":[],"Actions":[]}""";

        var broken = await client.CallToolAsync("sfc_explain", new Dictionary<string, object?> { ["recipe"] = NoInitial }, cancellationToken: ct);
        var badScript = await client.CallToolAsync("sfc_dryrun", new Dictionary<string, object?> { ["recipe"] = Recipe, ["script"] = "{\"nope\":1}" }, cancellationToken: ct);

        broken.IsError.ShouldBe(true);
        Text(broken).ShouldContain("SFC001");
        badScript.IsError.ShouldBe(true);
        Text(badScript).ShouldContain("Unknown script field 'nope'");
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

    private static Task<TestApp> StartWithMaintenanceAsync(IMaintenanceOperations operations, CancellationToken ct) =>
        TestApp.StartAsync([], ct, services =>
        {
            services.AddSingleton(operations);
            services.AddSingleton<IOptions<MaintenanceOptions>>(Options.Create(new MaintenanceOptions { Enabled = true }));
        });

    [Fact]
    public async Task MaintenanceToolsAreOffUnlessEnabled()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await TestApp.StartAsync([], ct);
        await using var client = await ConnectAsync(app, ct);

        (await client.ListToolsAsync(cancellationToken: ct)).Select(t => t.Name).ShouldNotContain(static n => n.StartsWith("maint_", StringComparison.Ordinal));
    }

    [Fact]
    public async Task MaintenanceToolsAppearWhenEnabledAndPlansShowTheTokenAndWarnings()
    {
        var ct = TestContext.Current.CancellationToken;
        var operations = Substitute.For<IMaintenanceOperations>();
        operations.PlanPurgeScopeAsync("mcp", "plant.a", false, Arg.Any<CancellationToken>()).Returns(new MaintenancePlan(
            "tok123", "maint_purge_scope", "scope=plant.a", "Removes everything.",
            [new PlanItem("joborder", "joborder:o1", "delete", "hash, 5 fields, version 1")], ["Bypasses the state machine."], "dig", DateTimeOffset.UnixEpoch));
        await using var app = await StartWithMaintenanceAsync(operations, ct);
        await using var client = await ConnectAsync(app, ct);

        var names = (await client.ListToolsAsync(cancellationToken: ct)).Select(t => t.Name).Where(static n => n.StartsWith("maint_", StringComparison.Ordinal)).ToList();
        var text = Text(await client.CallToolAsync("maint_purge_scope_plan", new Dictionary<string, object?> { ["scope"] = "plant.a" }, cancellationToken: ct));

        names.ShouldBe(["maint_apply", "maint_dedupe_release_plan", "maint_job_delete_plan", "maint_publisher_reset_plan", "maint_purge_scope_plan", "maint_restore_plan", "maint_retained_clear_plan", "maint_snapshots_list"], ignoreOrder: true);
        text.ShouldContain("tok123");
        text.ShouldContain("WARNING: Bypasses the state machine.");
        text.ShouldContain("Nothing has changed yet");
    }

    [Fact]
    public async Task ARefusedMaintenanceCallIsAToolErrorWithTheReason()
    {
        var ct = TestContext.Current.CancellationToken;
        var operations = Substitute.For<IMaintenanceOperations>();
        operations.ApplyAsync("mcp", "bad", Arg.Any<CancellationToken>()).Returns<MaintenanceResult>(_ => throw new MaintenanceRefusedException("Unknown or already used plan token."));
        await using var app = await StartWithMaintenanceAsync(operations, ct);
        await using var client = await ConnectAsync(app, ct);

        var result = await client.CallToolAsync("maint_apply", new Dictionary<string, object?> { ["token"] = "bad" }, cancellationToken: ct);

        result.IsError.ShouldBe(true);
        Text(result).ShouldContain("already used");
    }
}
