using MicroDCS.App.Greetings;
using MicroDCS.Core.Hosting;
using MicroDCS.Inspection;
using MicroDCS.MachineryJobs;
using MicroDCS.Maintenance;
using MicroDCS.Mcp;
using MicroDCS.Sfc.Hosting;
using MicroDCS.Telemetry;
using MicroDCS.Transport.Mqtt;
using MicroDCS.Transport.Nats;
using MicroDCS.Valkey;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

namespace MicroDCS.App;

/// <summary>
/// <c>microdcs serve</c>: the processors whose bindings are configured, on the transport each binding names (<c>mqtt</c>, <c>nats</c> or <c>memory</c>), with Valkey
/// (and the maintenance operations) when <c>MicroDcs:Valkey:ConnectionString</c> is set the management listener when <c>MicroDcs:Management:Enabled</c> is true, and OpenTelemetry over OTLP when an OTLP endpoint is configured.
/// The greetings demo runs unless only the Machinery Jobs binding is configured; that binding needs Valkey and otherwise ends the start (D-27), and it also runs the recipes of its jobs through the SFC host.
/// Configuration comes from environment variables with the prefix <c>APP_</c> and <c>__</c> as separator (D-39).
/// </summary>
internal static class ServeCommand
{
    public static async Task RunAsync(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);
        builder.Configuration.AddEnvironmentVariables("APP_");

        // OTLP export when an OTEL_EXPORTER_OTLP_*ENDPOINT variable is set (D-54); the host reads unprefixed environment variables too.
        // Before AddMicroDcs: hosted services start in order, and the providers must listen before the transports deliver.
        builder.Services.AddMicroDcsTelemetry(builder.Configuration);
        builder.Services.AddMicroDcs(builder.Configuration);
        var bindings = builder.Configuration.GetSection("MicroDcs:Bindings");
        var machineryJobs = bindings.GetSection(MachineryJobsProcessor.BindingId).Exists();
        if (!machineryJobs || bindings.GetSection("greetings").Exists())
        {
            builder.Services.AddProcessor<GreetingsProcessor>();
        }

        // Transports are registered by what the bindings name (D-39); without a named transport the in-memory one runs.
        var transports = builder.Configuration.GetSection("MicroDcs:Bindings").GetChildren()
            .Select(static b => b["Transport"] ?? MqttOptions.TransportName)
            .ToHashSet(StringComparer.Ordinal);
        if (transports.Contains(MqttOptions.TransportName))
        {
            builder.Services.AddMqttTransport(builder.Configuration);
        }

        if (transports.Contains(NatsOptions.TransportName))
        {
            builder.Services.AddNatsTransport(builder.Configuration);
        }

        if (transports.Contains("memory") || transports.Count == 0)
        {
            builder.Services.AddInMemoryTransport();
        }

        if (!string.IsNullOrEmpty(builder.Configuration["MicroDcs:Valkey:ConnectionString"]))
        {
            builder.Services.AddMicroDcsValkey(builder.Configuration);

            // The maintenance operations work on Valkey state and start only in Development or Test (D-45).
            builder.Services.AddMicroDcsMaintenance(builder.Configuration);
            if (machineryJobs)
            {
                builder.Services.AddMachineryJobs(builder.Configuration);

                // Runs only under the Processor role (D-51); MicroDcs:Sfc:Enabled turns it off for a station without recipes.
                builder.Services.AddSfcHost(builder.Configuration);
            }
        }
        else if (machineryJobs)
        {
            throw new InvalidOperationException("The Machinery Jobs binding needs Valkey: set MicroDcs:Valkey:ConnectionString.");
        }

        builder.Services.AddMicroDcsManagement().AddMicroDcsMcp();
        using var host = builder.Build();
        await host.RunAsync().ConfigureAwait(false);
    }
}
