using MicroDCS.IntegrationTests.Valkey;
using MicroDCS.MachineryJobs.Publishing;
using MicroDCS.Valkey.Documents;

namespace MicroDCS.IntegrationTests.MachineryJobs;

/// <summary>The publisher's cursors, sequence, lease and change reads against Valkey.</summary>
[Trait("Category", "Integration")]
public sealed class JobPublisherStoreTests
{
    private const string Scope = "site.area";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static bool Configured => TestEndpoints.ValkeyConfigured;

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(JobPublisherStoreTests), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task The_lease_has_one_holder_and_is_renewed_and_released()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = new ValkeyJobPublisherStore(env.Context);
        var ttl = TimeSpan.FromSeconds(30);

        (await store.AcquireLeaseAsync("a", ttl, Ct)).ShouldBeTrue();
        (await store.AcquireLeaseAsync("a", ttl, Ct)).ShouldBeTrue();
        (await store.AcquireLeaseAsync("b", ttl, Ct)).ShouldBeFalse();
        (await store.GetLeaseHolderAsync(Ct)).ShouldBe("a");

        await store.ReleaseLeaseAsync("b", Ct);
        (await store.GetLeaseHolderAsync(Ct)).ShouldBe("a");

        await store.ReleaseLeaseAsync("a", Ct);
        (await store.AcquireLeaseAsync("b", ttl, Ct)).ShouldBeTrue();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(JobPublisherStoreTests), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task A_lease_that_is_not_renewed_runs_out()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = new ValkeyJobPublisherStore(env.Context);

        (await store.AcquireLeaseAsync("a", TimeSpan.FromMilliseconds(100), Ct)).ShouldBeTrue();
        await Task.Delay(300, Ct);

        (await store.AcquireLeaseAsync("b", TimeSpan.FromSeconds(30), Ct)).ShouldBeTrue();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(JobPublisherStoreTests), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task Changes_are_read_after_the_cursor()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = new ValkeyJobPublisherStore(env.Context);
        var orders = new JobOrderDao(env.Context);
        await orders.SaveAsync("o1", Scope, "{}", 1, "Store", 0, cancellationToken: Ct);
        await orders.SaveAsync("o1", Scope, "{}", 1, "Start", 1, cancellationToken: Ct);

        var all = await store.ReadChangesAsync(Scope, null, 10, Ct);

        all.Select(static c => c.ChangeType).ShouldBe(["Store", "Start"]);
        all[0].JobOrderId.ShouldBe("o1");
        (await store.FirstChangeIdAsync(Scope, Ct)).ShouldBe(all[0].Id);
        (await store.LastChangeIdAsync(Scope, Ct)).ShouldBe(all[1].Id);
        (await store.ReadChangesAsync(Scope, all[0].Id, 10, Ct)).ShouldHaveSingleItem().ChangeType.ShouldBe("Start");
        (await store.ReadChangesAsync(Scope, all[1].Id, 10, Ct)).ShouldBeEmpty();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(JobPublisherStoreTests), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task Cursors_and_sequences_are_kept_per_scope()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = new ValkeyJobPublisherStore(env.Context);

        (await store.GetCursorAsync(Scope, Ct)).ShouldBeNull();
        await store.SaveCursorAsync(Scope, "5-1", Ct);
        (await store.GetCursorAsync(Scope, Ct)).ShouldBe("5-1");
        (await store.GetCursorAsync("other", Ct)).ShouldBeNull();

        (await store.GetSequenceAsync(Scope, Ct)).ShouldBe(0);
        (await store.NextSequenceAsync(Scope, Ct)).ShouldBe(1);
        (await store.NextSequenceAsync(Scope, Ct)).ShouldBe(2);
        (await store.GetSequenceAsync(Scope, Ct)).ShouldBe(2);
    }
}
