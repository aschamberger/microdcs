using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace MicroDCS.Maintenance;

/// <summary>Registration of the maintenance operations (explicit, D-39). Dev and test applications only.</summary>
public static class MaintenanceServiceCollectionExtensions
{
    /// <summary>
    /// Adds <see cref="IMaintenanceOperations"/> and its options from <c>MicroDcs:Maintenance</c>. Call after <c>AddMicroDcsValkey</c>.
    /// Enabling it in any environment other than Development and Test fails the host start (D-27).
    /// </summary>
    /// <param name="services">The services.</param>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMicroDcsMaintenance(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);
        services.AddOptions<MaintenanceOptions>().Bind(configuration.GetSection(MaintenanceOptions.SectionName)).ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<MaintenanceOptions>, MaintenanceOptionsValidator>());
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IMaintenanceOperations, ValkeyMaintenance>();
        return services;
    }
}
