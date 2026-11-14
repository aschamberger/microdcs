using MicroDCS.Core.Hosting;
using MicroDCS.Inspection;
using MicroDCS.Maintenance;
using MicroDCS.Sfc.Hosting;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
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

/// <summary>
/// D-19 (revised S23a): the read tools may run in production, behind the bearer token off loopback. The write group (<c>inject_event</c>, the SFC
/// operator actions) runs real handlers and commands, so <c>WriteEnabled</c> is refused outside the environments Development and Test, like the
/// maintenance group (D-45). An application without a host environment counts as Production.
/// </summary>
internal sealed class McpOptionsValidator(IServiceProvider services) : IValidateOptions<MicroDcsOptions>
{
    public ValidateOptionsResult Validate(string? name, MicroDcsOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!options.Management.WriteEnabled)
        {
            return ValidateOptionsResult.Success;
        }

        var environment = services.GetService<IHostEnvironment>()?.EnvironmentName ?? Environments.Production;
        return IsAllowedEnvironment(environment)
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail($"MicroDcs:Management:WriteEnabled is only allowed in the environments Development and Test, not '{environment}'.");
    }

    internal static bool IsAllowedEnvironment(string environmentName) =>
        string.Equals(environmentName, Environments.Development, StringComparison.OrdinalIgnoreCase)
        || string.Equals(environmentName, "Test", StringComparison.OrdinalIgnoreCase);
}

/// <summary>Registration of the MCP adapter (explicit, D-39). The read tools may run in production; the write group only in Development and Test.</summary>
public static class McpServiceCollectionExtensions
{
    /// <summary>Adds the MCP module to the management listener. It serves nothing unless <c>MicroDcs:Management:McpEnabled</c> is true.</summary>
    /// <param name="services">The services.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMicroDcsMcp(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IManagementModule, McpManagementModule>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<MicroDcsOptions>, McpOptionsValidator>());
        return services;
    }
}
