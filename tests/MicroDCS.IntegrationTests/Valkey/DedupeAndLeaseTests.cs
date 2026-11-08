using MicroDCS.Core.Transport;
using MicroDCS.Valkey;

namespace MicroDCS.IntegrationTests.Valkey;

[Trait("Category", "Integration")]
public sealed class DedupeAndLeaseTests
{
    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task ClaimFollowsTheLeaseSemantics()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = new ValkeyDedupeStore(env.Context, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(5));

        (await store.ClaimAsync("src", "1", "a", ct)).ShouldBe(ClaimState.Claimed);
        (await store.ClaimAsync("src", "1", "a", ct)).ShouldBe(ClaimState.Claimed, "same token: a retry after a lost reply");
        (await store.ClaimAsync("src", "1", "b", ct)).ShouldBe(ClaimState.Busy);
        (await store.LeaseRemainingAsync("src", "1", ct)).ShouldBeGreaterThan(TimeSpan.FromSeconds(20));

        await store.MarkDoneAsync("src", "1", ct);
        (await store.ClaimAsync("src", "1", "b", ct)).ShouldBe(ClaimState.Done);
        (await store.LeaseRemainingAsync("src", "never", ct)).ShouldBe(TimeSpan.Zero);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task RV4TheDoneMarkerHasNoLeaseLeft()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = new ValkeyDedupeStore(env.Context, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(10));

        await store.ClaimAsync("src", "3", "a", ct);
        await store.MarkDoneAsync("src", "3", ct);

        // The key keeps the 10 min retention, but a waiting duplicate must not sleep for it.
        (await store.LeaseRemainingAsync("src", "3", ct)).ShouldBe(TimeSpan.Zero);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task ReleaseGivesUpOnlyTheOwnLease()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = new ValkeyDedupeStore(env.Context, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(10));

        await store.ClaimAsync("src", "4", "a", ct);
        await store.ReleaseAsync("src", "4", "b", ct);
        (await store.ClaimAsync("src", "4", "b", ct)).ShouldBe(ClaimState.Busy, "another token does not release");

        await store.ReleaseAsync("src", "4", "a", ct);
        (await store.ClaimAsync("src", "4", "b", ct)).ShouldBe(ClaimState.Claimed);

        await store.MarkDoneAsync("src", "4", ct);
        await store.ReleaseAsync("src", "4", "b", ct);
        (await store.ClaimAsync("src", "4", "c", ct)).ShouldBe(ClaimState.Done, "a done marker is never released");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task ALapsedLeaseCanBeClaimedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = new ValkeyDedupeStore(env.Context, TimeSpan.FromMilliseconds(200), TimeSpan.FromMinutes(5));

        (await store.ClaimAsync("src", "2", "a", ct)).ShouldBe(ClaimState.Claimed);
        await Task.Delay(400, ct);
        (await store.ClaimAsync("src", "2", "b", ct)).ShouldBe(ClaimState.Claimed);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task OnlyOneInstanceGetsTheStartLease()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var lease = new ValkeyStartLease(env.Context);

        (await lease.TryAcquireAsync("processor-1", TimeSpan.FromSeconds(30), ct)).ShouldBeTrue();
        (await lease.TryAcquireAsync("processor-1", TimeSpan.FromSeconds(30), ct)).ShouldBeFalse();
        (await lease.TryAcquireAsync("processor-2", TimeSpan.FromSeconds(30), ct)).ShouldBeTrue();
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task HealthCheckPings()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var result = await new ValkeyHealthCheck(env.Context).CheckHealthAsync(new(), ct);
        result.Status.ShouldBe(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Healthy);
    }
}
