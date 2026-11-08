using MicroDCS.Valkey;
using MicroDCS.Valkey.Documents;
using StackExchange.Redis;

namespace MicroDCS.IntegrationTests.Valkey;

[Trait("Category", "Integration")]
public sealed class DocumentTests
{
    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task JobOrderRoundTripListChangeEntryAndScopeRegistry()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var dao = new JobOrderDao(env.Context);

        (await dao.SaveAsync("j2", "plant/a", "{\"n\":2}", 5, "Create", 0, cancellationToken: ct)).ShouldBe(new WriteResult(true, 1));
        (await dao.SaveAsync("j1", "plant/a", "{\"n\":1}", 1, "Create", 0, cancellationToken: ct)).Written.ShouldBeTrue();

        var stored = (await dao.GetAsync("j2", ct)).ShouldNotBeNull();
        (stored.Scope, stored.Version, stored.Document, stored.Score).ShouldBe(("plant/a", 1L, "{\"n\":2}", 5.0));
        (await dao.ListAsync("plant/a", cancellationToken: ct)).ShouldBe(["j1", "j2"]);
        (await dao.ListAsync("plant/a", 1, 1, ct)).ShouldBe(["j2"]);
        (await env.Database.SetMembersAsync(env.Keys.Scopes())).Select(v => v.ToString()).ShouldBe(["plant/a"]);

        var changes = await env.Database.StreamRangeAsync(env.Keys.Changes("plant/a"));
        changes.Length.ShouldBe(2);
        changes[0].Values.ToDictionary(v => v.Name.ToString(), v => v.Value.ToString())["job_order_id"].ShouldBe("j2");

        await dao.DeleteAsync("j2", ct);
        (await dao.GetAsync("j2", ct)).ShouldBeNull();
        (await dao.ListAsync("plant/a", cancellationToken: ct)).ShouldBe(["j1"]);
        await dao.DeleteAsync("j2", ct);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task WritesAreGuardedByTheVersion()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var dao = new JobOrderDao(env.Context);

        (await dao.SaveAsync("j", "s", "v1", 0, "Create", 0, cancellationToken: ct)).Written.ShouldBeTrue();
        (await dao.SaveAsync("j", "s", "stale", 0, "Update", 0, cancellationToken: ct)).ShouldBe(new WriteResult(false, 1));
        (await dao.SaveAsync("j", "s", "v2", 0, "Update", 1, cancellationToken: ct)).ShouldBe(new WriteResult(true, 2));
        (await dao.SaveAsync("j", "s", "v3", 0, "Update", cancellationToken: ct)).ShouldBe(new WriteResult(true, 3));
        (await dao.GetAsync("j", ct))!.Document.ShouldBe("v3");

        await Should.ThrowAsync<InvalidOperationException>(() => dao.SaveAsync("j", "other-scope", "x", 0, "Update", cancellationToken: ct));
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task ALostReplyIsRecognisedByTheOperationIdAndNotAppliedTwice()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var key = env.Keys.JobOrder("j");
        var calls = 0;

        var result = await env.Context.WriteAsync(key, "op-1", async _ =>
        {
            calls++;
            var reply = await env.Database.ScriptEvaluateAsync(
                DocumentScripts.PutJobOrder,
                [key, env.Keys.JobOrderList("s"), env.Keys.Changes("s"), env.Keys.Scopes(), env.Keys.Nudge("changes")],
                [-1, "op-1", "j", "s", "1", "doc", "Create", "ts", "j", 100, string.Empty, string.Empty]);
            reply.Length.ShouldBe(3);

            // The script ran; the reply is lost.
            throw new RedisConnectionException(ConnectionFailureType.SocketClosed, CommandFlags.None, "reply lost", null, CommandStatus.Unknown);
        }, ct);

        result.ShouldBe(new WriteResult(true, 1));
        calls.ShouldBe(1, "the write must not run a second time");
        (await env.Database.StreamLengthAsync(env.Keys.Changes("s"))).ShouldBe(1);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task AWriteThatDidNotHappenIsRepeatedAfterAConnectionError()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var dao = new JobOrderDao(env.Context);
        var key = env.Keys.JobOrder("j");
        var calls = 0;

        var result = await env.Context.WriteAsync(key, "op-2", async _ =>
        {
            if (++calls == 1)
            {
                throw new RedisTimeoutException(CommandFlags.None, "timeout before the script ran", CommandStatus.Unknown);
            }

            return await env.Database.ScriptEvaluateAsync(
                DocumentScripts.PutJobOrder,
                [key, env.Keys.JobOrderList("s"), env.Keys.Changes("s"), env.Keys.Scopes(), env.Keys.Nudge("changes")],
                [-1, "op-2", "j", "s", "1", "doc", "Create", "ts", "j", 100, string.Empty, string.Empty]);
        }, ct);

        result.ShouldBe(new WriteResult(true, 1));
        calls.ShouldBe(2);
        (await dao.GetAsync("j", ct))!.Operation.ShouldBe("op-2");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task WritesSurviveAKilledConnection()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var dao = new JobOrderDao(env.Context);
        (await dao.SaveAsync("j", "s", "v1", 0, "Create", cancellationToken: ct)).Written.ShouldBeTrue();

        // Close every client connection (including ours) from the server side.
        var server = env.Multiplexer.GetServers()[0];
        await server.ExecuteAsync("CLIENT", "KILL", "TYPE", "normal", "SKIPME", "no");

        (await dao.SaveAsync("j", "s", "v2", 0, "Update", cancellationToken: ct)).Written.ShouldBeTrue();
        var stored = (await dao.GetAsync("j", ct)).ShouldNotBeNull();
        (stored.Document, stored.Version).ShouldBe(("v2", 2L));
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task JobResponseIndexesFollowTheDocument()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var dao = new JobResponseDao(env.Context);
        var ready = KeySchema.NormalizeState(["AllowedToStart", "Ready"]);
        ready.ShouldBe("AllowedToStart_Ready");

        await dao.SaveAsync("r1", "s", "{}", 10, "order-1", ready, 0, cancellationToken: ct);
        await dao.SaveAsync("r2", "s", "{}", 20, "order-2", ready, 0, cancellationToken: ct);

        (await dao.GetByJobOrderAsync("order-1", ct))!.Id.ShouldBe("r1");
        (await dao.GetByJobOrderAsync("missing", ct)).ShouldBeNull();
        (await Collect(dao.IdsByStateAsync("s", ready, cancellationToken: ct))).ShouldBe(["r1", "r2"], ignoreOrder: true);
        (await Collect(dao.IdsByStateAsync("s", string.Empty, cancellationToken: ct))).ShouldBeEmpty();

        await dao.SaveAsync("r1", "s", "{\"x\":1}", 10, "order-1", "Running", 1, cancellationToken: ct);
        (await Collect(dao.IdsByStateAsync("s", ready, cancellationToken: ct))).ShouldBe(["r2"]);
        (await Collect(dao.IdsByStateAsync("s", "Running", cancellationToken: ct))).ShouldBe(["r1"]);
        (await dao.ListAsync("s", cancellationToken: ct)).ShouldBe(["r1", "r2"]);

        await dao.DeleteAsync("r1", ct);
        (await dao.GetByJobOrderAsync("order-1", ct)).ShouldBeNull();
        (await Collect(dao.IdsByStateAsync("s", "Running", cancellationToken: ct))).ShouldBeEmpty();
        (await dao.ListAsync("s", cancellationToken: ct)).ShouldBe(["r2"]);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task WorkMastersReferenceListsAndAcceptance()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var masters = new WorkMasterDao(env.Context);

        await masters.SaveAsync("wm2", "s", "{}", 0, cancellationToken: ct);
        await masters.SaveAsync("wm1", "s", "{}", cancellationToken: ct);
        (await masters.ListAsync("s", ct)).ShouldBe(["wm1", "wm2"]);
        (await masters.IsMemberAsync("wm1", "s", ct)).ShouldBeTrue();
        await masters.DeleteAsync("wm1", ct);
        (await masters.IsMemberAsync("wm1", "s", ct)).ShouldBeFalse();
        (await masters.GetAsync("wm1", ct)).ShouldBeNull();

        var lists = new ReferenceListDao(env.Context);
        await lists.AddAsync("equipment", "e1", "s", ct);
        (await lists.IsMemberAsync("equipment", "e1", "s", ct)).ShouldBeTrue();
        (await lists.IsMemberAsync("personnel", "e1", "s", ct)).ShouldBeFalse();
        await lists.RemoveAsync("equipment", "e1", "s", ct);
        (await lists.IsMemberAsync("equipment", "e1", "s", ct)).ShouldBeFalse();
        await Should.ThrowAsync<ArgumentException>(() => lists.AddAsync("nonsense", "x", "s", ct));

        var acceptance = new JobAcceptanceDao(env.Context);
        (await acceptance.GetAsync("s", ct)).ShouldBeNull();
        await acceptance.SaveAsync("s", 7, ct);
        (await acceptance.GetAsync("s", ct)).ShouldBe(7);
        await acceptance.DeleteAsync("s", ct);
        (await acceptance.GetAsync("s", ct)).ShouldBeNull();
    }

    private static async Task<List<string>> Collect(IAsyncEnumerable<string> source)
    {
        var all = new List<string>();
        await foreach (var item in source)
        {
            all.Add(item);
        }

        return all;
    }
}
