using MicroDCS.Valkey;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using StackExchange.Redis;

namespace MicroDCS.Maintenance.Tests;

public sealed class GuardTests
{
    private static ValkeyMaintenance Create(string environment, bool enabled = true)
    {
        var multiplexer = Substitute.For<IConnectionMultiplexer>();
        var context = new ValkeyContext(multiplexer, Options.Create(new ValkeyOptions { KeyPrefix = "p" }), TimeProvider.System, NullLogger<ValkeyContext>.Instance);
        return new ValkeyMaintenance(context, Options.Create(new MaintenanceOptions { Enabled = enabled }), new FakeEnvironment(environment), TimeProvider.System, NullLoggerFactory.Instance);
    }

    [Theory]
    [InlineData("Production")]
    [InlineData("Staging")]
    public async Task EveryOperationRefusesOutsideDevelopmentAndTest(string environment)
    {
        var ct = TestContext.Current.CancellationToken;
        var operations = Create(environment);

        (await Should.ThrowAsync<MaintenanceRefusedException>(async () => await operations.PlanDedupeReleaseAsync("t", "s", "1", ct))).Message.ShouldContain("Development and Test");
        await Should.ThrowAsync<MaintenanceRefusedException>(async () => await operations.PlanPurgeScopeAsync("t", "a.b", false, ct));
        await Should.ThrowAsync<MaintenanceRefusedException>(async () => await operations.PlanRestoreAsync("t", "x", false, ct));
        await Should.ThrowAsync<MaintenanceRefusedException>(async () => await operations.ApplyAsync("t", "token", ct));
        await Should.ThrowAsync<MaintenanceRefusedException>(async () => await operations.ListSnapshotsAsync("t", ct));
    }

    [Fact]
    public async Task ADisabledGroupRefuses()
    {
        var ct = TestContext.Current.CancellationToken;

        (await Should.ThrowAsync<MaintenanceRefusedException>(async () => await Create("Development", enabled: false).ListSnapshotsAsync("t", ct)))
            .Message.ShouldContain("not enabled");
    }
}
