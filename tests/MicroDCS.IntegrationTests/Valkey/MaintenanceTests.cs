using MicroDCS.Maintenance;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Documents;
using MicroDCS.Valkey.Sfc;
using MicroDCS.Valkey.Streams;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MicroDCS.IntegrationTests.Valkey;

[Trait("Category", "Integration")]
public sealed class MaintenanceTests
{
    private const string Scope = "plant/a";

    private sealed class Environment(string name) : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = name;

        public string ApplicationName { get; set; } = "test";

        public string ContentRootPath { get; set; } = AppContext.BaseDirectory;

        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }

    private sealed class Rig : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "mdcs-it-" + Guid.NewGuid().ToString("N")[..8]);

        public Rig(ValkeyTestEnvironment env, Action<MaintenanceOptions>? configure = null, string environment = "Test")
        {
            Env = env;
            var options = new MaintenanceOptions { Enabled = true, SnapshotDirectory = _directory };
            configure?.Invoke(options);
            Operations = new ValkeyMaintenance(env.Context, Options.Create(options), new Environment(environment), TimeProvider.System, NullLoggerFactory.Instance);
            Directory = _directory;
        }

        public ValkeyTestEnvironment Env { get; }

        public ValkeyMaintenance Operations { get; }

        public string Directory { get; }

        public void Dispose()
        {
            if (System.IO.Directory.Exists(_directory))
            {
                System.IO.Directory.Delete(_directory, recursive: true);
            }
        }
    }

    /// <summary>Every key of the test prefix with its content, to compare states.</summary>
    private static async Task<SortedDictionary<string, string>> DumpAsync(ValkeyTestEnvironment env, CancellationToken ct)
    {
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);
        foreach (var key in env.Multiplexer.GetServers()[0].Keys(pattern: env.Context.Options.KeyPrefix + ":*"))
        {
            var image = await KeyImage.CaptureAsync(env.Database, new Target(key.ToString()), ct);
            result[key.ToString()] = image!.Canonical();
        }

        return result;
    }

    private static async Task SeedScopeAsync(ValkeyTestEnvironment env, CancellationToken ct)
    {
        var ctx = env.Context;
        await new JobOrderDao(ctx).SaveAsync("o1", Scope, "{\"n\":1}", 1, "Create", 0, cancellationToken: ct);
        await new JobOrderDao(ctx).SaveAsync("o2", Scope, "{\"n\":2}", 2, "Create", 0, cancellationToken: ct);
        await new JobResponseDao(ctx).SaveAsync("r1", Scope, "{\"r\":1}", 10, "o1", "AllowedToStart_Ready", 0, cancellationToken: ct);
        await new WorkMasterDao(ctx).SaveAsync("w1", Scope, "{\"w\":1}", 0, cancellationToken: ct);
        await new ReferenceListDao(ctx).AddAsync("equipment", "e1", Scope, ct);
        await new JobAcceptanceDao(ctx).SaveAsync(Scope, 5, ct);
        await new SfcStateDao(ctx).CompareAndSetAsync("o1", Scope, "{\"sfc\":1}", 0, 123456, true, [new SfcWorkEntry([new("action", "start")])], cancellationToken: ct);
        await env.Database.HashSetAsync(env.Keys.PublisherCursors(), Scope, "1-0");
        await env.Database.HashSetAsync(env.Keys.PublisherCursors(), "plant/other", "2-0");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task DedupeReleaseAllowsTheEventAgainAndRestoreBringsTheEntryBack()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        using var rig = new Rig(env);
        var store = new ValkeyDedupeStore(env.Context, TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(5));
        await store.ClaimAsync("src", "1", "a", ct);
        await store.MarkDoneAsync("src", "1", ct);

        var plan = await rig.Operations.PlanDedupeReleaseAsync("test", "src", "1", ct);
        (await store.ClaimAsync("src", "1", "b", ct)).ShouldBe(MicroDCS.Core.Transport.ClaimState.Done, "a plan changes nothing");
        var result = await rig.Operations.ApplyAsync("test", plan.Token, ct);

        result.SnapshotId.ShouldNotBeNull();
        (await store.ClaimAsync("src", "1", "b", ct)).ShouldBe(MicroDCS.Core.Transport.ClaimState.Claimed);
        await env.Database.KeyDeleteAsync(env.Keys.CloudEventDedupe("src", "1"));
        var restore = await rig.Operations.PlanRestoreAsync("test", result.SnapshotId, false, ct);
        await rig.Operations.ApplyAsync("test", restore.Token, ct);
        (await store.ClaimAsync("src", "1", "c", ct)).ShouldBe(MicroDCS.Core.Transport.ClaimState.Done);
        (await env.Database.KeyTimeToLiveAsync(env.Keys.CloudEventDedupe("src", "1")))!.Value.ShouldBeGreaterThan(TimeSpan.FromMinutes(1), "the time to live comes back");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task PurgeScopeThenRestoreGivesBackTheExactState()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        using var rig = new Rig(env);
        await SeedScopeAsync(env, ct);
        await env.Database.StringSetAsync(env.Keys.CloudEventDedupe("other", "x"), "done");
        var before = await DumpAsync(env, ct);

        var plan = await rig.Operations.PlanPurgeScopeAsync("test", Scope, false, ct);

        plan.Items.Select(static i => i.Kind).ShouldContain("joborder");
        plan.Items.Select(static i => i.Kind).ShouldContain("jobresponse");
        plan.Items.Select(static i => i.Kind).ShouldContain("sfc");
        plan.Items.ShouldContain(static i => i.Key.StartsWith("scopes[", StringComparison.Ordinal));
        plan.Items.ShouldContain(static i => i.Key.StartsWith("publisher:cursors[", StringComparison.Ordinal));
        (await DumpAsync(env, ct)).ShouldBe(before, "planning changes nothing");

        var applied = await rig.Operations.ApplyAsync("test", plan.Token, ct);

        var after = await DumpAsync(env, ct);
        after.Keys.ShouldBe([env.Keys.CloudEventDedupe("other", "x"), env.Keys.PublisherCursors()], ignoreOrder: true);
        (await env.Database.HashKeysAsync(env.Keys.PublisherCursors())).Select(static v => v.ToString()).ShouldBe(["plant/other"]);
        (await env.Database.SetLengthAsync(env.Keys.Scopes())).ShouldBe(0);

        var restore = await rig.Operations.PlanRestoreAsync("test", applied.SnapshotId!, false, ct);
        var restored = await rig.Operations.ApplyAsync("test", restore.Token, ct);

        restored.Changed.ShouldBe(plan.Items.Count);
        var final = await DumpAsync(env, ct);
        // Stream entries are added again with new ids; everything else is identical.
        var workKey = env.Keys.SfcWork(Scope);
        var changesKey = env.Keys.Changes(Scope);
        restored.Notes.ShouldContain(static n => n.Contains("stream entries", StringComparison.Ordinal));
        final.Where(p => p.Key != workKey && p.Key != changesKey).ShouldBe(before.Where(p => p.Key != workKey && p.Key != changesKey));
        (await env.Database.StreamLengthAsync(workKey)).ShouldBe(1);
        (await env.Database.StreamLengthAsync(changesKey)).ShouldBe(3);
        (await new JobOrderDao(env.Context).GetAsync("o1", ct))!.Version.ShouldBe(1);
        (await env.Database.SortedSetLengthAsync(env.Keys.SfcDue(Scope))).ShouldBe(1);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task ApplyFailsWhenATargetChangedSinceThePlan()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        using var rig = new Rig(env);
        await SeedScopeAsync(env, ct);
        var plan = await rig.Operations.PlanPurgeScopeAsync("test", Scope, false, ct);
        await new JobOrderDao(env.Context).SaveAsync("o1", Scope, "{\"n\":99}", 1, "Update", 1, cancellationToken: ct);
        var before = await DumpAsync(env, ct);

        var ex = await Should.ThrowAsync<MaintenanceRefusedException>(async () => await rig.Operations.ApplyAsync("test", plan.Token, ct));

        ex.Message.ShouldContain("changed since the plan");
        (await DumpAsync(env, ct)).ShouldBe(before);
        (await rig.Operations.ListSnapshotsAsync("test", ct)).ShouldBeEmpty("no snapshot for a refused apply");
        await Should.ThrowAsync<MaintenanceRefusedException>(async () => await rig.Operations.ApplyAsync("test", plan.Token, ct));
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task RestoreRefusesToReplaceDifferentContentUnlessOverwriteIsPlanned()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        using var rig = new Rig(env);
        var dao = new JobOrderDao(env.Context);
        await dao.SaveAsync("o1", Scope, "{\"n\":1}", 1, "Create", 0, cancellationToken: ct);
        var purged = await rig.Operations.ApplyAsync("test", (await rig.Operations.PlanPurgeScopeAsync("test", Scope, false, ct)).Token, ct);
        await dao.SaveAsync("o1", Scope, "{\"n\":\"new\"}", 1, "Create", 0, cancellationToken: ct);

        var ex = await Should.ThrowAsync<MaintenanceRefusedException>(async () => await rig.Operations.PlanRestoreAsync("test", purged.SnapshotId!, false, ct));
        ex.Message.ShouldContain("overwrite");

        var plan = await rig.Operations.PlanRestoreAsync("test", purged.SnapshotId!, true, ct);
        plan.Items.ShouldContain(static i => i.Action == "overwrite");
        var applied = await rig.Operations.ApplyAsync("test", plan.Token, ct);

        applied.SnapshotId.ShouldNotBeNull("the replaced content is snapshotted first");
        (await dao.GetAsync("o1", ct))!.Document.ShouldBe("{\"n\":1}");
        (await rig.Operations.ListSnapshotsAsync("test", ct)).Count.ShouldBe(2);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task APlanAboveTheCapNeedsAllowLarge()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        using var rig = new Rig(env, static o => o.ItemCap = 5);
        await SeedScopeAsync(env, ct);

        var ex = await Should.ThrowAsync<MaintenanceRefusedException>(async () => await rig.Operations.PlanPurgeScopeAsync("test", Scope, false, ct));

        ex.Message.ShouldContain("allow_large");
        (await rig.Operations.PlanPurgeScopeAsync("test", Scope, true, ct)).Items.Count.ShouldBeGreaterThan(5);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task ApplyIsRefusedAndNothingChangesWhenTheSnapshotCannotBeWritten()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var blocker = Path.Combine(Path.GetTempPath(), "mdcs-it-file-" + Guid.NewGuid().ToString("N")[..8]);
        await File.WriteAllTextAsync(blocker, "x", ct);
        try
        {
            using var rig = new Rig(env, o => o.SnapshotDirectory = Path.Combine(blocker, "snapshots"));
            await SeedScopeAsync(env, ct);
            var before = await DumpAsync(env, ct);
            var plan = await rig.Operations.PlanPurgeScopeAsync("test", Scope, false, ct);

            await Should.ThrowAsync<MaintenanceRefusedException>(async () => await rig.Operations.ApplyAsync("test", plan.Token, ct));

            (await DumpAsync(env, ct)).ShouldBe(before);
        }
        finally
        {
            File.Delete(blocker);
        }
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task TheMarkerKeyGuardsAMisPointedConnection()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        using var rig = new Rig(env, static o => o.RequireEnvironmentMarker = true);

        var ex = await Should.ThrowAsync<MaintenanceRefusedException>(async () => await rig.Operations.ListSnapshotsAsync("test", ct));
        ex.Message.ShouldContain("marker");

        await env.Database.StringSetAsync(env.Keys.EnvironmentMarker(), "Production");
        await Should.ThrowAsync<MaintenanceRefusedException>(async () => await rig.Operations.ListSnapshotsAsync("test", ct));

        await env.Database.StringSetAsync(env.Keys.EnvironmentMarker(), "Test");
        (await rig.Operations.ListSnapshotsAsync("test", ct)).ShouldBeEmpty();
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = ValkeyTestEnvironment.SkipReason)]
    public async Task NothingStoredMeansNothingToPlan()
    {
        var ct = TestContext.Current.CancellationToken;
        using var env = await ValkeyTestEnvironment.CreateAsync();
        using var rig = new Rig(env);

        await Should.ThrowAsync<MaintenanceRefusedException>(async () => await rig.Operations.PlanPurgeScopeAsync("test", "plant/none", false, ct));
        await Should.ThrowAsync<MaintenanceRefusedException>(async () => await rig.Operations.PlanDedupeReleaseAsync("test", "src", "none", ct));
    }
}
