using MicroDCS.App.Greetings;
using MicroDCS.Core.Hosting;
using MicroDCS.Inspection;
using MicroDCS.Mcp;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MicroDCS.App;

/// <summary>
/// Native AOT smoke check of the inspection surface (S11): the management listener (Kestrel) on a free loopback port, the MCP
/// endpoint with source-generated tool registration, a read tool, the write tool going through the pipeline, readiness with
/// the registered checks and the REST API (S21).
/// </summary>
internal static class InspectionSmoke
{
    private const string Initialize = """{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"1"}}}""";
    private const string ListTools = """{"jsonrpc":"2.0","id":2,"method":"tools/list"}""";
    private const string AppStatus = """{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"app_status","arguments":{}}}""";
    private const string Inject = """{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"inject_event","arguments":{"binding":"greetings","type":"com.github.aschamberger.microdcs.greetings.hello.v1","subject":"a.b/x","dataJson":"{\"name\":\"Bob\"}"}}}""";
    private const string Recent = """{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"pipeline_recent","arguments":{}}}""";

    public static async Task<int> RunAsync(TextWriter output)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MicroDcs:CloudEventSource"] = "urn:smoke",
            ["MicroDcs:Bindings:greetings:Transport"] = "memory",
            ["MicroDcs:Bindings:greetings:Prefix"] = "smoke",
            ["MicroDcs:Bindings:greetings:ScopeLevels"] = "2",
            ["MicroDcs:Management:Enabled"] = "true",
            ["MicroDcs:Management:ApiEnabled"] = "true",
            ["MicroDcs:Management:McpEnabled"] = "true",
            ["MicroDcs:Management:WriteEnabled"] = "true",
            ["MicroDcs:Management:Url"] = "http://127.0.0.1:0",
        }).Build();

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMicroDcs(configuration).AddInMemoryTransport().AddProcessor<GreetingsProcessor>();
        services.AddMicroDcsManagement().AddMicroDcsMcp();
        var provider = services.BuildServiceProvider();
        await using var scope = provider.ConfigureAwait(false);
        var hosted = provider.GetServices<IHostedService>().ToList();
        foreach (var service in hosted)
        {
            await service.StartAsync(CancellationToken.None).ConfigureAwait(false);
        }

        try
        {
            var address = provider.GetRequiredService<ManagementServer>().GetAddresses() is [var first, ..] ? first : null;
            if (address is null)
            {
                await output.WriteLineAsync("FAIL inspection-mcp (no listener)").ConfigureAwait(false);
                return 1;
            }

            using var client = new HttpClient { BaseAddress = new Uri(address) };
            client.DefaultRequestHeaders.Accept.ParseAdd("application/json");
            client.DefaultRequestHeaders.Accept.ParseAdd("text/event-stream");
            var health = await client.GetAsync("/health/ready").ConfigureAwait(false);
            _ = await Post(client, Initialize).ConfigureAwait(false);
            var tools = await Post(client, ListTools).ConfigureAwait(false);
            var status = await Post(client, AppStatus).ConfigureAwait(false);
            var injected = await Post(client, Inject).ConfigureAwait(false);
            var recent = await Post(client, Recent).ConfigureAwait(false);

            // REST (S21): the embedded OpenAPI document, and the source-generated problem JSON (no jobs processor here).
            var openApi = await client.GetStringAsync("/api/v1/openapi.json").ConfigureAwait(false);
            using var jobs = await client.GetAsync("/api/v1/jobs?scope=a").ConfigureAwait(false);
            var problem = await jobs.Content.ReadAsStringAsync().ConfigureAwait(false);

            var ok = health.IsSuccessStatusCode
                && tools.Contains("inject_event", StringComparison.Ordinal)
                && status.Contains("memory (AtLeastOnce)", StringComparison.Ordinal)
                && injected.Contains("Ack", StringComparison.Ordinal)
                && recent.Contains("Processed", StringComparison.Ordinal)
                && openApi.Contains("/api/v1/jobs/{id}", StringComparison.Ordinal)
                && jobs.StatusCode == System.Net.HttpStatusCode.NotFound
                && problem.Contains("\"status\":404", StringComparison.Ordinal);
            await output.WriteLineAsync(ok ? "ok   inspection-mcp" : "FAIL inspection-mcp").ConfigureAwait(false);
            return ok ? 0 : 1;
        }
        finally
        {
            foreach (var service in Enumerable.Reverse(hosted))
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private static async Task<string> Post(HttpClient client, string json)
    {
        using var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
        using var response = await client.PostAsync("/mcp", content).ConfigureAwait(false);
        return await response.Content.ReadAsStringAsync().ConfigureAwait(false);
    }
}
