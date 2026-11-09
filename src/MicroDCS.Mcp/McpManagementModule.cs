using MicroDCS.Core.Hosting;
using MicroDCS.Inspection;
using MicroDCS.Maintenance;
using MicroDCS.Sfc.Hosting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace MicroDCS.Mcp;

/// <summary>Serves the MCP tools under <c>/mcp</c> on the management listener when <c>MicroDcs:Management:McpEnabled</c> is on.</summary>
internal sealed class McpManagementModule : IManagementModule
{
    public bool IsEnabled(IServiceProvider application) =>
        application.GetRequiredService<IOptions<MicroDcsOptions>>().Value.Management.McpEnabled;

    public void ConfigureServices(IServiceCollection management, IServiceProvider application)
    {
        var builder = management.AddMcpServer()
            .WithHttpTransport(static options => options.Stateless = true)
            .WithTools<InspectionTools>(McpJsonContext.ToolOptions)
            .WithTools<SfcTools>(McpJsonContext.ToolOptions);
        if (application.GetRequiredService<IOptions<MicroDcsOptions>>().Value.Management.WriteEnabled)
        {
            management.AddSingleton(application.GetRequiredService<EventInjector>());
            builder.WithTools<WriteTools>(McpJsonContext.ToolOptions);
            if (application.GetService<ISfcOperations>() is { } sfc)
            {
                management.AddSingleton(sfc);
                builder.WithTools<SfcWriteTools>(McpJsonContext.ToolOptions);
            }
        }

        if (MaintenanceEnabled(application))
        {
            // Resolved here so that a missing registration ends the start (D-27).
            management.AddSingleton(application.GetRequiredService<IMaintenanceOperations>());
            builder.WithTools<MaintenanceTools>(McpJsonContext.ToolOptions);
        }
    }

    public void Map(IEndpointRouteBuilder endpoints) => endpoints.MapMcp("/mcp");

    private static bool MaintenanceEnabled(IServiceProvider application) =>
        application.GetService<IOptions<MaintenanceOptions>>()?.Value.Enabled == true;
}

/// <summary>Registration of the MCP adapter (explicit, D-39). Dev and test applications only.</summary>
public static class McpServiceCollectionExtensions
{
    /// <summary>Adds the MCP module to the management listener. It serves nothing unless <c>MicroDcs:Management:McpEnabled</c> is true.</summary>
    /// <param name="services">The services.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMicroDcsMcp(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IManagementModule, McpManagementModule>());
        return services;
    }
}
