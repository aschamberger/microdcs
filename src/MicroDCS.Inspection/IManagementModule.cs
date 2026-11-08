using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace MicroDCS.Inspection;

/// <summary>
/// An adapter on the management listener, for example MCP. The listener runs its own small web host; a module adds services to
/// it and maps its endpoints. Modules are registered explicitly (D-39) and only start when the listener is enabled.
/// </summary>
public interface IManagementModule
{
    /// <summary>Whether the module is on, from the application options.</summary>
    /// <param name="application">The application services.</param>
    /// <returns><see langword="true"/> when its endpoints should be served.</returns>
    bool IsEnabled(IServiceProvider application);

    /// <summary>Adds the services of the module to the management host. <see cref="InspectionService"/> is available there.</summary>
    /// <param name="management">The management host services.</param>
    /// <param name="application">The application services.</param>
    void ConfigureServices(IServiceCollection management, IServiceProvider application);

    /// <summary>Maps the endpoints.</summary>
    /// <param name="endpoints">The routes of the management host.</param>
    void Map(IEndpointRouteBuilder endpoints);
}
