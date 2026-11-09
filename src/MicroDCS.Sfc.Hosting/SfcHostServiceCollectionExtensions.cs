using MicroDCS.Core.Authoring;
using MicroDCS.Core.Inspection;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Streams;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace MicroDCS.Sfc.Hosting;

/// <summary>Registers the SFC host.</summary>
public static class SfcHostServiceCollectionExtensions
{
    /// <summary>Adds the SFC host: the interpreter, the work consumer and the due-index sweep in the poller, the action sink, the inspector and the operator actions. Needs <c>AddMicroDcsValkey</c> and <c>AddMachineryJobs</c>; runs only under the <c>Processor</c> role.</summary>
    /// <param name="services">The services.</param>
    /// <param name="configuration">The application configuration (the section <c>MicroDcs:Sfc</c> is read).</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddSfcHost(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<SfcHostOptions>()
            .Bind(configuration.GetSection(SfcHostOptions.SectionName))
            .Validate(static o => o.IsValid(), "MicroDcs:Sfc: durations, MaxAttempts, MaxDeliveries, ReadCount and SweepLimit must be positive, and OutageThreshold above SweepInterval.")
            .Validate<IOptions<ValkeyOptions>>(static (o, valkey) => o.OutageThreshold > valkey.Value.PollInterval, "MicroDcs:Sfc:OutageThreshold must be above MicroDcs:Valkey:PollInterval, which paces the sweep.")
            .ValidateOnStart();
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IRecipeInterpreter, SfcInterpreter>();
        services.TryAddSingleton<SfcPendingCommands>();
        services.TryAddSingleton<SfcGraphs>();
        services.TryAddSingleton<SfcJobRunner>();
        services.TryAddSingleton<ISfcOperations, SfcOperations>();
        services.Replace(ServiceDescriptor.Singleton<IActionSink, SfcActionSink>());
        services.AddSingleton<IStreamConsumer, SfcWorkConsumer>();
        services.AddSingleton<IPollerTask, SfcDueSweep>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IInspector, SfcInspector>());
        return services;
    }
}
