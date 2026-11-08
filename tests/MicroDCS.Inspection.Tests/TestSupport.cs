using System.Text.Json.Serialization;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Messaging;
using MicroDCS.Inspection;
using MicroDCS.Mcp;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace MicroDCS.Inspection.Tests;

[CloudEventType("test.ping.v1")]
public sealed record Ping(string Text);

[JsonSerializable(typeof(Ping))]
internal sealed partial class PingContext : JsonSerializerContext;

[Processor(ProcessorBinding.Southbound, Id = "ping")]
public sealed partial class PingProcessor
{
    public string? Last { get; private set; }

    [Incoming]
    public void OnPing(Ping ping) => Last = ping.Text;
}

/// <summary>An inspector with fixed entries.</summary>
internal sealed class FakeInspector(string name, params InspectionEntry[] entries) : IInspector
{
    public string Name => name;

    public InspectionQuery? LastQuery { get; private set; }

    public ValueTask<InspectionPage> InspectAsync(InspectionQuery query, CancellationToken cancellationToken)
    {
        LastQuery = query;
        return ValueTask.FromResult(new InspectionPage(entries, null));
    }
}

/// <summary>A module that maps one endpoint, so the listener can be tested without MCP.</summary>
internal sealed class PingModule : IManagementModule
{
    public bool IsEnabled(IServiceProvider application) => true;

    public void ConfigureServices(IServiceCollection management, IServiceProvider application)
    {
    }

    public void Map(IEndpointRouteBuilder endpoints) =>
        endpoints.MapGet("/ping", context => context.Response.WriteAsync("pong"));
}

/// <summary>A running application with the processor, the in-memory transport, the management listener and MCP.</summary>
internal sealed class TestApp : IAsyncDisposable
{
    private readonly ServiceProvider _provider;
    private readonly List<IHostedService> _started = [];

    private TestApp(ServiceProvider provider) => _provider = provider;

    public IServiceProvider Services => _provider;

    public Uri? McpEndpoint { get; private set; }

    public static async Task<TestApp> StartAsync(Dictionary<string, string?> settings, CancellationToken cancellationToken)
    {
        var values = new Dictionary<string, string?>
        {
            ["MicroDcs:CloudEventSource"] = "urn:test",
            ["MicroDcs:Bindings:ping:Transport"] = "memory",
            ["MicroDcs:Bindings:ping:Prefix"] = "test",
            ["MicroDcs:Bindings:ping:ScopeLevels"] = "1",
            ["MicroDcs:Management:Enabled"] = "true",
            ["MicroDcs:Management:McpEnabled"] = "true",
            ["MicroDcs:Management:Url"] = "http://127.0.0.1:0",
        };
        foreach (var (key, value) in settings)
        {
            values[key] = value;
        }

        var configuration = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMicroDcs(configuration).AddInMemoryTransport().AddProcessor<PingProcessor>();
        services.AddMicroDcsManagement().AddMicroDcsMcp();
        var app = new TestApp(services.BuildServiceProvider());
        foreach (var service in app._provider.GetServices<IHostedService>())
        {
            await service.StartAsync(cancellationToken);
            app._started.Add(service);
        }

        var server = app._provider.GetRequiredService<ManagementServer>();
        app.McpEndpoint = server.GetAddresses() is [var address, ..] ? new Uri(address + "/mcp") : null;
        return app;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var service in Enumerable.Reverse(_started))
        {
            await service.StopAsync(CancellationToken.None);
        }

        await _provider.DisposeAsync();
    }
}
