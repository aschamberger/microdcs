using Microsoft.Extensions.Time.Testing;

namespace MicroDCS.Maintenance.Tests;

public sealed class PlanStoreTests
{
    private static PendingPlan Pending(FakeTimeProvider time, string token, TimeSpan lifetime) => new()
    {
        Plan = new MaintenancePlan(token, "maint_x", "a", "s", [], [], "d", time.GetUtcNow() + lifetime),
        Targets = [],
    };

    [Fact]
    public void ATokenWorksOnce()
    {
        var time = new FakeTimeProvider();
        var store = new PlanStore(time);
        store.Add(Pending(time, "t1", TimeSpan.FromMinutes(5)));

        store.Take("t1").Plan.Token.ShouldBe("t1");
        Should.Throw<MaintenanceRefusedException>(() => store.Take("t1")).Message.ShouldContain("already used");
    }

    [Fact]
    public void AnExpiredTokenIsRefused()
    {
        var time = new FakeTimeProvider();
        var store = new PlanStore(time);
        store.Add(Pending(time, "t1", TimeSpan.FromMinutes(5)));
        time.Advance(TimeSpan.FromMinutes(6));

        Should.Throw<MaintenanceRefusedException>(() => store.Take("t1")).Message.ShouldContain("expired");
    }

    [Fact]
    public void AnUnknownTokenIsRefused() =>
        Should.Throw<MaintenanceRefusedException>(() => new PlanStore(new FakeTimeProvider()).Take("nope"));

    [Fact]
    public void TokensAreRandom() =>
        Enumerable.Range(0, 20).Select(static _ => PlanStore.NewToken()).Distinct().Count().ShouldBe(20);
}
