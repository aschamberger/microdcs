using System.Diagnostics.CodeAnalysis;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Inspection;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Hosting;

/// <summary>Registration is explicit (D-39): nothing is found by scanning.</summary>
public static class MicroDcsServiceCollectionExtensions
{
    /// <summary>Adds the core: options bound from the <c>MicroDcs</c> section, the pipeline and the lifecycle. Defaults for the dedupe store and the start lease are in memory; the Valkey layer replaces them.</summary>
    /// <param name="services">The services.</param>
    /// <param name="configuration">The application configuration (the section <c>MicroDcs</c> is read).</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMicroDcs(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<MicroDcsOptions>().Bind(configuration.GetSection(MicroDcsOptions.SectionName));
        services.PostConfigure<MicroDcsOptions>(static o =>
            o.InstanceId ??= Environment.GetEnvironmentVariable("POD_ID") ?? Guid.NewGuid().ToString("N"));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<MicroDcsOptions>, MicroDcsOptionsValidator>());

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IActionSink, NullActionSink>();
        services.TryAddSingleton<IStartLease, AlwaysGrantedStartLease>();
        services.TryAddSingleton<IDedupeStore>(static sp =>
        {
            var options = sp.GetRequiredService<IOptions<MicroDcsOptions>>().Value;
            return new InMemoryDedupeStore(sp.GetRequiredService<TimeProvider>(), options.DedupeLease, options.DedupeRetention);
        });
        services.TryAddSingleton(static sp => new ProcessorRegistry(
            sp.GetServices<ProcessorRegistration>().Select(r => r.Descriptor),
            sp.GetServices<IPayloadDescriptor>()));
        services.TryAddSingleton<PipelineDecisionLog>();
        services.TryAddSingleton<MessagePipeline>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IInspector, PipelineInspector>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IInspector, ProcessorsInspector>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IInspector, AppInspector>());
        services.TryAddSingleton<IInboundHandler>(static sp => sp.GetRequiredService<MessagePipeline>());
        services.TryAddSingleton<IEventPublisher>(static sp => sp.GetRequiredService<MessagePipeline>());
        services.TryAddSingleton<IOutgoingCommands>(static sp => sp.GetRequiredService<MessagePipeline>());
        services.AddHostedService<MicroDcsHostedService>();

        // Readiness (D-54): /health/ready on the management listener asks every registered check.
        services.TryAddSingleton<TransportsHealthCheck>();
        services.AddMicroDcsHealthCheck<TransportsHealthCheck>(TransportsHealthCheck.Name);
        return services;
    }

    /// <summary>
    /// Adds a readiness check that resolves <typeparamref name="TCheck"/> from the container (register it first). Idempotent: a
    /// second call with the same name adds nothing, so calling a registration method twice does not break the health service with a
    /// duplicate name.
    /// </summary>
    /// <typeparam name="TCheck">The registered check.</typeparam>
    /// <param name="services">The services.</param>
    /// <param name="name">The check name, shown by <c>/health/ready</c> when it fails.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMicroDcsHealthCheck<TCheck>(this IServiceCollection services, string name)
        where TCheck : class, IHealthCheck
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrEmpty(name);
        services.AddHealthChecks();
        services.Configure<HealthCheckServiceOptions>(options =>
        {
            if (!options.Registrations.Any(r => r.Name == name))
            {
                options.Registrations.Add(new HealthCheckRegistration(name, static sp => sp.GetRequiredService<TCheck>(), failureStatus: null, tags: null));
            }
        });
        return services;
    }

    /// <summary>Registers a generated processor (singleton) and its descriptor.</summary>
    /// <typeparam name="TProcessor">The processor class marked with <see cref="ProcessorAttribute"/>.</typeparam>
    /// <param name="services">The services.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddProcessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TProcessor>(this IServiceCollection services)
        where TProcessor : class, IDescribedProcessor
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddSingleton<TProcessor>();
        services.AddSingleton(new ProcessorRegistration(TProcessor.Describe()));
        return services;
    }

    /// <summary>Registers payload descriptors that no handler names, for example payloads of a <see cref="Replies"/> builder.</summary>
    /// <param name="services">The services.</param>
    /// <param name="descriptors">The descriptors.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddPayloads(this IServiceCollection services, params IPayloadDescriptor[] descriptors)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(descriptors);
        foreach (var descriptor in descriptors)
        {
            services.AddSingleton(descriptor);
        }

        return services;
    }

    /// <summary>Registers the in-memory transport (tests, harness, demos).</summary>
    /// <param name="services">The services.</param>
    /// <param name="guarantee">The delivery guarantee it declares.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddInMemoryTransport(this IServiceCollection services, DeliveryGuarantee guarantee = DeliveryGuarantee.AtLeastOnce)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddSingleton(new InMemoryTransport(guarantee));
        services.AddSingleton<ITransport>(static sp => sp.GetRequiredService<InMemoryTransport>());
        return services;
    }
}

/// <summary>Carries a processor descriptor through the container.</summary>
/// <param name="Descriptor">The descriptor.</param>
public sealed record ProcessorRegistration(ProcessorDescriptor Descriptor);
