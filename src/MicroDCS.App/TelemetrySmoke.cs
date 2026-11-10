using System.Diagnostics;
using System.Diagnostics.Metrics;
using MicroDCS.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace MicroDCS.App;

/// <summary>
/// Native AOT smoke check of the OpenTelemetry wiring (S21, D-54): the SDK with the OTLP exporters builds, a source and a meter
/// under the <c>MicroDCS*</c> pattern are listened to. The endpoint refuses connections; a failed export is not an error here.
/// </summary>
internal static class TelemetrySmoke
{
    public static async Task<int> RunAsync(TextWriter output)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://127.0.0.1:9",
            ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf",
            ["OTEL_EXPORTER_OTLP_TIMEOUT"] = "500",
        }).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMicroDcsTelemetry(configuration);
        var provider = services.BuildServiceProvider();
        await using var scope = provider.ConfigureAwait(false);

        var tracing = provider.GetService<TracerProvider>();
        var metrics = provider.GetService<MeterProvider>();
        using var source = new ActivitySource("MicroDCS.Smoke");
        using var meter = new Meter("MicroDCS.Smoke");
        meter.CreateCounter<long>("microdcs.smoke").Add(1);
        using var activity = source.StartActivity("smoke");

        var ok = tracing is not null && metrics is not null && activity is not null;
        await output.WriteLineAsync(ok ? "ok   telemetry" : "FAIL telemetry").ConfigureAwait(false);
        return ok ? 0 : 1;
    }
}
