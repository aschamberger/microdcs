using MicroDCS.Valkey;
using MicroDCS.Valkey.Documents;
using MicroDCS.Valkey.Sfc;
using StackExchange.Redis;

namespace MicroDCS.IntegrationTests.Valkey;

[Trait("Category", "Integration")]
public sealed class SfcAndIndexTests
{
    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task CompareAndSetStoresStateCommandsDeadlineAndActiveSetAtomically()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var dao = new SfcStateDao(env.Context);
        var command = new SfcWorkEntry([new("kind", "command"), new("action", "job-1:fill")]);

        (await dao.CompareAndSetAsync("job-1", "s", "state-1", 0, 5_000, true, [command], cancellationToken: ct)).ShouldBe(new WriteResult(true, 1));

        (await dao.GetAsync("job-1", ct))!.Document.ShouldBe("state-1");
        (await dao.ListActiveAsync("s", ct)).ShouldBe(["job-1"]);
        (await dao.DueAsync("s", 4_999, cancellationToken: ct)).ShouldBeEmpty();
        (await dao.DueAsync("s", 5_000, cancellationToken: ct)).ShouldBe(["job-1"]);
        var work = await env.Database.StreamRangeAsync(env.Keys.SfcWork("s"));
        work.Length.ShouldBe(1);
        work[0].Values.ToDictionary(v => v.Name.ToString(), v => v.Value.ToString())["action"].ShouldBe("job-1:fill");
        (await env.Database.SetContainsAsync(env.Keys.Scopes(), "s")).ShouldBeTrue();

        // Another instance moved first: no state change, no extra work entry.
        (await dao.CompareAndSetAsync("job-1", "s", "stale", 0, null, false, [command], cancellationToken: ct)).ShouldBe(new WriteResult(false, 1));
        (await env.Database.StreamLengthAsync(env.Keys.SfcWork("s"))).ShouldBe(1);

        (await dao.CompareAndSetAsync("job-1", "s", "state-2", 1, null, false, [], cancellationToken: ct)).ShouldBe(new WriteResult(true, 2));
        (await dao.ListActiveAsync("s", ct)).ShouldBeEmpty();
        (await dao.DueAsync("s", long.MaxValue / 2, cancellationToken: ct)).ShouldBeEmpty();

        await dao.DeleteAsync("job-1", ct);
        (await dao.GetAsync("job-1", ct)).ShouldBeNull();
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task ALostCompareAndSetReplyDoesNotAppendTheCommandsTwice()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var key = env.Keys.SfcState("job-2");
        var calls = 0;

        var result = await env.Context.WriteAsync(key, "op-x", async _ =>
        {
            calls++;
            await env.Database.ScriptEvaluateAsync(
                SfcStateDao.CompareAndSetScript,
                [key, env.Keys.SfcWork("s"), env.Keys.SfcDue("s"), env.Keys.SfcActive("s"), env.Keys.Scopes(), env.Keys.Nudge("sfc")],
                [-1, "op-x", "job-2", "s", "doc", string.Empty, "1", 1, 2, "kind", "command"]);
            throw new RedisTimeoutException(CommandFlags.None, "reply lost", CommandStatus.Sent);
        }, ct);

        result.Written.ShouldBeTrue();
        calls.ShouldBe(1);
        (await env.Database.StreamLengthAsync(env.Keys.SfcWork("s"))).ShouldBe(1);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task HeartbeatAndRecipesRoundTrip()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var dao = new SfcStateDao(env.Context);

        (await dao.GetHeartbeatAsync(ct)).ShouldBeNull();
        await dao.SetHeartbeatAsync(1234, ct);
        (await dao.GetHeartbeatAsync(ct)).ShouldBe(1234);

        (await dao.PutRecipeAsync("abc", "recipe-1", ct)).ShouldBeTrue();
        (await dao.PutRecipeAsync("abc", "recipe-2", ct)).ShouldBeFalse();
        (await dao.GetRecipeAsync("abc", ct)).ShouldBe("recipe-1");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task IndexesMatchAScanAndAreRebuiltAfterDamage()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var orders = new JobOrderDao(env.Context);
        var responses = new JobResponseDao(env.Context);
        var masters = new WorkMasterDao(env.Context);
        var maintenance = new ValkeyIndexMaintenance(env.Context);

        for (var i = 0; i < 6; i++)
        {
            var scope = i % 2 == 0 ? "a" : "b";
            await orders.SaveAsync($"o{i}", scope, "{}", i, "Create", cancellationToken: ct);
            await responses.SaveAsync($"r{i}", scope, "{}", i * 10, $"o{i}", i % 3 == 0 ? "Ready" : "Running", cancellationToken: ct);
            await masters.SaveAsync($"w{i}", scope, "{}", cancellationToken: ct);
        }

        await responses.SaveAsync("r1", "b", "{}", 10, "o1", "Ended", cancellationToken: ct);
        await orders.DeleteAsync("o2", ct);
        await responses.DeleteAsync("r3", ct);
        await masters.DeleteAsync("w4", ct);

        (await maintenance.VerifyAsync(ct)).ShouldBeEmpty();

        // Damage: a missing list, a foreign member, a wrong state index and a stale by-order entry.
        await env.Database.KeyDeleteAsync(env.Keys.JobOrderList("a"));
        await env.Database.SetAddAsync(env.Keys.JobResponseByState("a", "Ready"), "ghost");
        await env.Database.SetRemoveAsync(env.Keys.JobResponseByState("b", "Ended"), "r1");
        await env.Database.StringSetAsync(env.Keys.JobResponseByOrder("o99"), "r99");
        await env.Database.SetRemoveAsync(env.Keys.Scopes(), "a");

        var differences = await maintenance.VerifyAsync(ct);
        differences.Select(d => (d.Kind, d.Member)).ShouldContain(("missing", "o0"));
        differences.Select(d => (d.Kind, d.Member)).ShouldContain(("extra", "ghost"));
        differences.Select(d => (d.Kind, d.Member)).ShouldContain(("missing", "r1"));
        differences.Select(d => (d.Kind, d.Member)).ShouldContain(("extra", "r99"));
        differences.Select(d => (d.Kind, d.Member)).ShouldContain(("missing", "a"));

        (await maintenance.RebuildAsync(ct)).ShouldBeGreaterThan(0);
        (await maintenance.VerifyAsync(ct)).ShouldBeEmpty();
        (await orders.ListAsync("a", cancellationToken: ct)).ShouldBe(["o0", "o4"]);
        (await responses.GetByJobOrderAsync("o99", ct)).ShouldBeNull();
    }
}
