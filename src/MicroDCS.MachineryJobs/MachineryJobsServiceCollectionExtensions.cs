using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MicroDCS.MachineryJobs;

/// <summary>Registers the Machinery Jobs processor.</summary>
public static class MachineryJobsServiceCollectionExtensions
{
    /// <summary>Registers the processor, its Valkey store, the acceptance options and the inspector. Needs <c>AddMicroDcsValkey</c>.</summary>
    /// <param name="services">The services.</param>
    /// <param name="configuration">The application configuration (the section <c>MicroDcs:JobAcceptance</c> is read).</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMachineryJobs(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<JobAcceptanceOptions>().Bind(configuration.GetSection(JobAcceptanceOptions.SectionName));
        services.TryAddSingleton<IJobStore, ValkeyJobStore>();
        services.TryAddSingleton<JobAcceptanceCheck>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IInspector, JobsInspector>());
        services.AddProcessor<MachineryJobsProcessor>();
        services.AddPayloads([.. MachineryJobsProcessor.PublishedPayloads]);
        return services;
    }
}
