using MicroDCS.Core.Inspection;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace MicroDCS.Transport.Mqtt;

/// <summary>Registration of the MQTT transport (explicit, D-39).</summary>
public static class MqttServiceCollectionExtensions
{
    /// <summary>
    /// Adds the MQTT transport: <see cref="ITransport"/>, <see cref="IStateTopicPort"/>, the <c>transport.mqtt</c> inspector and the
    /// options from <c>MicroDcs:Mqtt</c>. Bindings select it with <c>Transport = mqtt</c>. Call after <c>AddMicroDcs</c>.
    /// </summary>
    /// <param name="services">The services.</param>
    /// <param name="configuration">The application configuration.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMqttTransport(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.AddOptions<MqttOptions>().Bind(configuration.GetSection(MqttOptions.SectionName)).ValidateOnStart();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<MqttOptions>, MqttOptionsValidator>());
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IEnvelopeCodec, CloudEventsEnvelopeCodec>();
        services.AddSingleton<MqttTransport>();
        services.AddSingleton<ITransport>(static sp => sp.GetRequiredService<MqttTransport>());
        services.AddSingleton<IStateTopicPort>(static sp => sp.GetRequiredService<MqttTransport>());
        services.AddSingleton<IInspector, MqttInspector>();
        return services;
    }
}
