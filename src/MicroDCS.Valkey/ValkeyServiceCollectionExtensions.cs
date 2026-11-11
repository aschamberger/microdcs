using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Transport;
using MicroDCS.Valkey.Documents;
using MicroDCS.Valkey.Sfc;
using MicroDCS.Valkey.Streams;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MicroDCS.Valkey;

/// <summary>Registration of the Valkey layer (explicit, D-39).</summary>
public static class ValkeyServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Valkey layer: options from <c>MicroDcs:Valkey</c>, the connection, the DAOs, the streams helper, the poller, and the Valkey
    /// implementations of <see cref="IDedupeStore"/> and <see cref="IStartLease"/> (they replace the in-memory defaults of
    /// <c>AddMicroDcs</c>, in either call order). The connection is made at host start; a failure ends the process (D-27).
    /// </summary>
    /// <param name="services">The services.</param>
    /// <param name="configuration">The application configuration (the section <c>MicroDcs:Valkey</c> is read).</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMicroDcsValkey(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<ValkeyOptions>().Bind(configuration.GetSection(ValkeyOptions.SectionName));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<ValkeyOptions>, ValkeyOptionsValidator>());
        services.TryAddSingleton(TimeProvider.System);

        // Registered first so that the connection is made, and fails fast, before other hosted services start.
        services.AddHostedService<ValkeyStartupCheck>();
        services.TryAddSingleton<IConnectionMultiplexer>(static sp =>
        {
            var options = sp.GetRequiredService<IOptions<ValkeyOptions>>().Value;
            var configurationOptions = ValkeyConnection.CreateConfiguration(options);

            // A singleton factory cannot await; the connect happens once, at host start, and a failure ends the process (D-27).
            var multiplexer = ConnectionMultiplexer.Connect(configurationOptions);
            var logger = sp.GetRequiredService<ILogger<ValkeyContext>>();
            Log.Connected(logger, options.KeyPrefix);
            return multiplexer;
        });

        services.TryAddSingleton<ValkeyContext>();
        services.TryAddSingleton<JobOrderDao>();
        services.TryAddSingleton<JobResponseDao>();
        services.TryAddSingleton<WorkMasterDao>();
        services.TryAddSingleton<ReferenceListDao>();
        services.TryAddSingleton<JobAcceptanceDao>();
        services.TryAddSingleton<SfcStateDao>();
        services.TryAddSingleton<ValkeyStreams>();
        services.TryAddSingleton<ValkeyIndexMaintenance>();
        services.TryAddSingleton<ValkeyHealthCheck>();
        services.AddMicroDcsHealthCheck<ValkeyHealthCheck>(ValkeyHealthCheck.Name);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IInspector, ValkeyInspector>());
        services.TryAddSingleton<ValkeyPoller>();
        services.AddHostedService(static sp => sp.GetRequiredService<ValkeyPoller>());

        services.Replace(ServiceDescriptor.Singleton<IDedupeStore>(static sp =>
        {
            var core = sp.GetRequiredService<IOptions<MicroDcsOptions>>().Value;
            return new ValkeyDedupeStore(sp.GetRequiredService<ValkeyContext>(), core.DedupeLease, core.DedupeRetention);
        }));
        services.Replace(ServiceDescriptor.Singleton<IStartLease, ValkeyStartLease>());
        return services;
    }

    private sealed class ValkeyStartupCheck(IConnectionMultiplexer multiplexer) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken) =>
            multiplexer.GetDatabase().PingAsync().WaitAsync(cancellationToken);

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
