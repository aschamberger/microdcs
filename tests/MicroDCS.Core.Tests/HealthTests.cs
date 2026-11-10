using MicroDCS.Core.Hosting;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using NSubstitute;

namespace MicroDCS.Core.Tests;

public sealed class HealthTests
{
    private static ITransport Transport(string name, bool connected)
    {
        var transport = Substitute.For<ITransport>();
        transport.Name.Returns(name);
        transport.IsConnected.Returns(connected);
        return transport;
    }

    [Fact]
    public async Task TransportsCheck_OneTransportDown_IsUnhealthyAndNamesIt()
    {
        var check = new TransportsHealthCheck([Transport("mqtt", true), Transport("nats", false)], Options.Create(new MicroDcsOptions()));

        var result = await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken);

        result.Status.ShouldBe(HealthStatus.Unhealthy);
        result.Description.ShouldBe("not connected: nats");
    }

    [Fact]
    public async Task TransportsCheck_AllConnected_IsHealthy()
    {
        var check = new TransportsHealthCheck([Transport("mqtt", true)], Options.Create(new MicroDcsOptions()));

        (await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken)).Status.ShouldBe(HealthStatus.Healthy);
    }

    [Fact]
    public async Task TransportsCheck_NoRole_IsHealthyWithoutStartedTransports()
    {
        var check = new TransportsHealthCheck([Transport("mqtt", false)], Options.Create(new MicroDcsOptions { Roles = InstanceRoles.None }));

        (await check.CheckHealthAsync(new HealthCheckContext(), TestContext.Current.CancellationToken)).Status.ShouldBe(HealthStatus.Healthy);
    }

    [Fact]
    public async Task AddMicroDcs_CalledTwice_RegistersTheTransportsCheckOnce()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["MicroDcs:CloudEventSource"] = "urn:test" }).Build();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMicroDcs(configuration);
        services.AddMicroDcs(configuration);
        await using var provider = services.BuildServiceProvider();

        var report = await provider.GetRequiredService<HealthCheckService>().CheckHealthAsync(TestContext.Current.CancellationToken);

        report.Entries.Keys.ShouldBe([TransportsHealthCheck.Name]);
    }
}
