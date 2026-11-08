using MicroDCS.Core.Inspection;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace MicroDCS.Transport.Nats;

/// <summary>Registration of the NATS transport (explicit, D-39).</summary>
public static class NatsServiceCollectionExtensions
{
    /// <summary>
    /// Adds the NATS transport: <see cref="ITransport"/>, <see cref="IStateTopicPort"/>, the <c>transport.nats</c> inspector and the
    /// options from <c>MicroDcs:Nats</c>. Bindings select it with <c>Transport = nats</c>. Call after <c>AddMicroDcs</c>.
    /// </summary>
    /// <param name="services">The services.</param>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddNatsTransport(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<NatsOptions>().Bind(configuration.GetSection(NatsOptions.SectionName)).ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<NatsOptions>, NatsOptionsValidator>());
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IEnvelopeCodec, CloudEventsEnvelopeCodec>();
        services.AddSingleton<NatsTransport>();
        services.AddSingleton<ITransport>(static sp => sp.GetRequiredService<NatsTransport>());
        services.AddSingleton<IStateTopicPort>(static sp => sp.GetRequiredService<NatsTransport>());
        services.AddSingleton<IInspector, NatsInspector>();
        return services;
    }
}
