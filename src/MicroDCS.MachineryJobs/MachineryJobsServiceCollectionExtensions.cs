using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Transport;
using MicroDCS.MachineryJobs.Publishing;
using MicroDCS.Valkey.Streams;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MicroDCS.MachineryJobs;

/// <summary>Registers the Machinery Jobs processor.</summary>
public static class MachineryJobsServiceCollectionExtensions
{
    /// <summary>Registers the processor, its Valkey store, the acceptance options, the job order publisher and the inspectors. Needs <c>AddMicroDcsValkey</c>.</summary>
    /// <param name="services">The services.</param>
    /// <param name="configuration">The application configuration (the sections <c>MicroDcs:JobAcceptance</c> and <c>MicroDcs:JobPublisher</c> are read).</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMachineryJobs(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<JobAcceptanceOptions>().Bind(configuration.GetSection(JobAcceptanceOptions.SectionName));
        services.TryAddSingleton<IJobStore, ValkeyJobStore>();
        services.TryAddSingleton<JobAcceptanceCheck>();
        services.TryAddSingleton<JobExecution>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IInspector, JobsInspector>());
        services.AddOptions<JobPublisherOptions>()
            .Bind(configuration.GetSection(JobPublisherOptions.SectionName))
            .Validate(static o => o.IsValid(), "MicroDcs:JobPublisher: RetainedTtl must be at least 2 s, ReadCount positive, LeaseTtl at least 3 s.")
            .ValidateOnStart();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IJobPublisherStore, ValkeyJobPublisherStore>();
        services.TryAddSingleton<JobStateTopics>();
        services.TryAddSingleton<IStateTopicCatalog>(static sp => sp.GetRequiredService<JobStateTopics>());
        services.TryAddSingleton<JobOrderPublisher>();
        services.AddSingleton<IStreamConsumer>(static sp => sp.GetRequiredService<JobOrderPublisher>());
        services.AddHostedService(static sp => sp.GetRequiredService<JobOrderPublisher>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IInspector, JobPublisherInspector>());
        services.AddProcessor<MachineryJobsProcessor>();
        services.AddPayloads([.. MachineryJobsProcessor.PublishedPayloads]);
        return services;
    }
}
