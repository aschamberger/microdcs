using MicroDCS.Core.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace MicroDCS.Telemetry;

/// <summary>
/// Registration of OpenTelemetry over OTLP (D-54). Exporters, endpoints, headers and protocol come from the standard
/// <c>OTEL_*</c> settings of the OpenTelemetry SDK. A signal is exported when <c>OTEL_EXPORTER_OTLP_ENDPOINT</c> or its own
/// endpoint (for example <c>OTEL_EXPORTER_OTLP_TRACES_ENDPOINT</c>) is set; without any endpoint the SDK is not added at all.
/// </summary>
public static class TelemetryServiceCollectionExtensions
{
    /// <summary>The service name when <c>OTEL_SERVICE_NAME</c> is not set.</summary>
    public const string DefaultServiceName = "microdcs";

    /// <summary>Every MicroDCS activity source and meter (<c>MicroDCS</c>, <c>MicroDCS.Valkey</c>, <c>MicroDCS.Sfc</c> and any later one).</summary>
    public const string MicroDcsPattern = "MicroDCS*";

    /// <summary>The activity source and meter of NATS.Net.</summary>
    public const string NatsName = "NATS.Net";

    /// <summary>The activity source of ASP.NET Core (the management listener); health probes are not recorded.</summary>
    public const string AspNetCoreSource = "Microsoft.AspNetCore";

    internal const string Endpoint = "OTEL_EXPORTER_OTLP_ENDPOINT";
    internal const string TracesEndpoint = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
    internal const string MetricsEndpoint = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT";
    internal const string LogsEndpoint = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";

    private static readonly string[] _meters =
    [
        MicroDcsPattern,
        NatsName,
        "System.Runtime",
        "Microsoft.AspNetCore.Hosting",
        "Microsoft.AspNetCore.Server.Kestrel",
    ];

    /// <summary>
    /// Adds tracing, metrics and logging with the OTLP exporter for the signals that have an endpoint. The resource carries
    /// <c>service.name</c> (<c>OTEL_SERVICE_NAME</c>, default <see cref="DefaultServiceName"/>) and <c>service.instance.id</c>
    /// (the instance id the application uses, <see cref="MicroDcsOptions.InstanceId"/>: configured, <c>POD_ID</c> or generated).
    /// Call it before <c>AddMicroDcs</c>: hosted services start in order, and the providers must exist before the transports deliver.
    /// </summary>
    /// <param name="services">The services.</param>
    /// <param name="configuration">The configuration; the host's environment variables are part of it.</param>
    /// <returns>The services.</returns>
    public static IServiceCollection AddMicroDcsTelemetry(this IServiceCollection services, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        var signals = Signals.From(configuration);
        if (!signals.Any)
        {
            return services;
        }

        var name = configuration["OTEL_SERVICE_NAME"];
        var builder = services.AddOpenTelemetry().ConfigureResource(resource => resource
            .AddService(string.IsNullOrWhiteSpace(name) ? DefaultServiceName : name, autoGenerateServiceInstanceId: false)
            .AddDetector(static sp => new InstanceDetector(sp.GetService<IOptions<MicroDcsOptions>>()?.Value.InstanceId)));
        if (signals.Traces)
        {
            builder.WithTracing(static tracing => tracing.AddSource(MicroDcsPattern, NatsName, AspNetCoreSource).AddOtlpExporter());
        }

        if (signals.Metrics)
        {
            builder.WithMetrics(static metrics => metrics.AddMeter(_meters).AddOtlpExporter());
        }

        if (signals.Logs)
        {
            builder.WithLogging(static logging => logging.AddOtlpExporter());
        }

        return services;
    }

    /// <summary>
    /// <c>service.instance.id</c> from the options after <c>PostConfigure</c>, so telemetry names the same instance as the reply
    /// addresses (D-9), also when only <c>POD_ID</c> is set.
    /// </summary>
    internal sealed class InstanceDetector(string? instanceId) : IResourceDetector
    {
        public Resource Detect() => string.IsNullOrWhiteSpace(instanceId)
            ? Resource.Empty
            : new Resource([new KeyValuePair<string, object>("service.instance.id", instanceId)]);
    }

    /// <summary>Which signals have an OTLP endpoint.</summary>
    internal readonly record struct Signals(bool Traces, bool Metrics, bool Logs)
    {
        public bool Any => Traces || Metrics || Logs;

        public static Signals From(IConfiguration configuration)
        {
            var all = IsSet(configuration, Endpoint);
            return new Signals(
                all || IsSet(configuration, TracesEndpoint),
                all || IsSet(configuration, MetricsEndpoint),
                all || IsSet(configuration, LogsEndpoint));
        }

        private static bool IsSet(IConfiguration configuration, string key) => !string.IsNullOrWhiteSpace(configuration[key]);
    }
}
