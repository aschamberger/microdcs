using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MicroDCS.Inspection;

/// <summary>Registration of the management listener (explicit, D-39).</summary>
public static class ManagementServiceCollectionExtensions
{
    /// <summary>
    /// Adds the inspection service, the event injector, the management listener and its REST API module (<c>ApiEnabled</c>). Nothing listens unless
    /// <c>MicroDcs:Management:Enabled</c> is true. Call after <c>AddMicroDcs</c> so the listener starts after the core.
    /// </summary>
    /// <param name="services">The services.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMicroDcsManagement(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<InspectionService>();
        services.TryAddSingleton<EventInjector>();
        services.TryAddSingleton<ManagementServer>();
        services.AddHostedService(static sp => sp.GetRequiredService<ManagementServer>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IManagementModule, RestApiModule>());
        return services;
    }
}
