using MicroDCS.Core.Hosting;
using MicroDCS.Telemetry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace MicroDCS.App.Tests;

public sealed class TelemetryTests
{
    private static IConfiguration Configuration(params (string Key, string Value)[] values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values.Select(static v => new KeyValuePair<string, string?>(v.Key, v.Value))).Build();

    [Fact]
    public void WithoutAnEndpointTheSdkIsNotAdded()
    {
        var services = new ServiceCollection();

        services.AddMicroDcsTelemetry(Configuration());

        using var provider = services.BuildServiceProvider();
        provider.GetService<TracerProvider>().ShouldBeNull();
        provider.GetService<MeterProvider>().ShouldBeNull();
    }

    [Fact]
    public void TheCommonEndpointTurnsOnEverySignal()
    {
        var signals = TelemetryServiceCollectionExtensions.Signals.From(Configuration((TelemetryServiceCollectionExtensions.Endpoint, "http://collector:4317")));

        signals.ShouldBe(new TelemetryServiceCollectionExtensions.Signals(true, true, true));
    }

    [Fact]
    public void ASignalEndpointTurnsOnOnlyThatSignal()
    {
        var configuration = Configuration((TelemetryServiceCollectionExtensions.MetricsEndpoint, "http://collector:4318/v1/metrics"));
        var services = new ServiceCollection();

        services.AddMicroDcsTelemetry(configuration);

        TelemetryServiceCollectionExtensions.Signals.From(configuration).ShouldBe(new TelemetryServiceCollectionExtensions.Signals(false, true, false));
        using var provider = services.BuildServiceProvider();
        provider.GetService<MeterProvider>().ShouldNotBeNull();
        provider.GetService<TracerProvider>().ShouldBeNull();
    }

    [Fact]
    public void TheResourceNamesTheInstanceTheApplicationUses()
    {
        // The id comes from the options after PostConfigure (POD_ID or generated), not from the raw configuration key.
        var services = new ServiceCollection();
        services.Configure<MicroDcsOptions>(static o => o.InstanceId = "pod-7");
        services.AddMicroDcsTelemetry(Configuration((TelemetryServiceCollectionExtensions.TracesEndpoint, "http://collector:4317")));

        using var provider = services.BuildServiceProvider();
        var resource = provider.GetRequiredService<TracerProvider>().GetResource();

        resource.Attributes.ShouldContain(new KeyValuePair<string, object>("service.instance.id", "pod-7"));
        resource.Attributes.ShouldContain(new KeyValuePair<string, object>("service.name", TelemetryServiceCollectionExtensions.DefaultServiceName));
    }
}
