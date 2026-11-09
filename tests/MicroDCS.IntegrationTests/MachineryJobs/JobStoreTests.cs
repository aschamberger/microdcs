using MicroDCS.Core.Inspection;
using MicroDCS.IntegrationTests.Valkey;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.MachineryJobs;
using MicroDCS.Valkey.Documents;
using StackExchange.Redis;

namespace MicroDCS.IntegrationTests.MachineryJobs;

/// <summary>The job store and inspector against Valkey (S17).</summary>
[Trait("Category", "Integration")]
public sealed class JobStoreTests
{
    private const string Scope = "site.area";
    private const string Skip = ValkeyTestEnvironment.SkipReason;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static ValkeyJobStore Store(ValkeyTestEnvironment env) =>
        new(new JobOrderDao(env.Context), new JobResponseDao(env.Context), new WorkMasterDao(env.Context), new ReferenceListDao(env.Context), new JobAcceptanceDao(env.Context));

    private static JobOrderWrite Write(JobOrderState state, long expected, bool start = false, string change = "Store", string operation = "op") =>
        new(new JobOrder { Id = "job-1", Priority = 5 }, state, Scope, change, expected, operation, start ? JobRecipeWork.Start : JobRecipeWork.None);

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task An_order_round_trips_with_its_sub_state_and_version()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);

        (await store.SaveOrderAsync(Write(JobOrderState.AllowedToStartLoaded, 0), Ct)).ShouldBeTrue();

        var stored = (await store.GetOrderAsync("job-1", Ct)).ShouldNotBeNull();
        stored.Order.Priority.ShouldBe((short)5);
        stored.State.ShouldBe(JobOrderState.AllowedToStartLoaded);
        stored.Scope.ShouldBe(Scope);
        stored.Version.ShouldBe(1);
        stored.Operation.ShouldBe("op");
        (await store.CountOrdersAsync(Scope, Ct)).ShouldBe(1);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_stale_version_is_not_written()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);
        await store.SaveOrderAsync(Write(JobOrderState.NotAllowedToStartReady, 0), Ct);

        (await store.SaveOrderAsync(Write(JobOrderState.Running, 0), Ct)).ShouldBeFalse();

        (await store.GetOrderAsync("job-1", Ct))!.State.ShouldBe(JobOrderState.NotAllowedToStartReady);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_recipe_start_writes_the_work_entry_with_the_order()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);

        await store.SaveOrderAsync(Write(JobOrderState.AllowedToStartReady, 0, start: true), Ct);

        var entries = await env.Database.StreamRangeAsync(env.Keys.SfcWork(Scope));
        var entry = entries.ShouldHaveSingleItem();
        ((string?)entry["job_id"]).ShouldBe("job-1");
        ((string?)entry["action"]).ShouldBe("start");
        ((string?)entry["scope"]).ShouldBe(Scope);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_save_without_a_start_writes_no_work_entry()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();

        await Store(env).SaveOrderAsync(Write(JobOrderState.NotAllowedToStartReady, 0), Ct);

        (await env.Database.StreamLengthAsync(env.Keys.SfcWork(Scope))).ShouldBe(0);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_removal_is_version_guarded_and_leaves_a_change_entry()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);
        await store.SaveOrderAsync(Write(JobOrderState.NotAllowedToStartReady, 0), Ct);

        (await store.RemoveOrderAsync("job-1", Scope, "Cancel", 5, "op-2", Ct)).ShouldBeFalse();
        (await store.GetOrderAsync("job-1", Ct)).ShouldNotBeNull();

        (await store.RemoveOrderAsync("job-1", Scope, "Cancel", 1, "op-2", Ct)).ShouldBeTrue();

        (await store.GetOrderAsync("job-1", Ct)).ShouldBeNull();
        (await store.CountOrdersAsync(Scope, Ct)).ShouldBe(0);
        var changes = await env.Database.StreamRangeAsync(env.Keys.Changes(Scope));
        changes.Select(static c => (string?)c["change_type"]).ShouldBe(["Store", "Cancel"]);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task A_removal_can_be_recognised_by_its_operation()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);
        await store.SaveOrderAsync(Write(JobOrderState.NotAllowedToStartReady, 0), Ct);
        await store.RemoveOrderAsync("job-1", Scope, "Cancel", 1, "op-2", Ct);

        (await store.WasRemovedByAsync("job-1", Scope, "op-2", Ct)).ShouldBeTrue();
        (await store.WasRemovedByAsync("job-1", Scope, "op-3", Ct)).ShouldBeFalse();
        (await store.WasRemovedByAsync("job-1", "site.other", "op-2", Ct)).ShouldBeFalse();
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task The_state_filter_finds_orders_beyond_the_first_page()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);
        var inspector = new JobsInspector(store, env.Context);
        for (var i = 0; i < 205; i++)
        {
            var state = i == 204 ? JobOrderState.Running : JobOrderState.Aborted;
            await store.SaveOrderAsync(new JobOrderWrite(new JobOrder { Id = $"job-{i}", Priority = (short)i }, state, Scope, "Store", 0, "op", JobRecipeWork.None), Ct);
        }

        var running = await inspector.InspectAsync(new InspectionQuery("jobs.orders", Arguments: new Dictionary<string, string> { ["scope"] = Scope, ["state"] = "Running" }), Ct);

        running.Entries.ShouldHaveSingleItem().Key.ShouldBe("job-204");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task Recent_changes_list_the_newest_first()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);
        var inspector = new JobsInspector(store, env.Context);
        await store.SaveOrderAsync(Write(JobOrderState.NotAllowedToStartReady, 0, change: "Store"), Ct);
        await store.SaveOrderAsync(Write(JobOrderState.AllowedToStartReady, 1, change: "Start"), Ct);

        var order = (await inspector.InspectAsync(new InspectionQuery("jobs.order", Arguments: new Dictionary<string, string> { ["id"] = "job-1" }), Ct)).Entries.ShouldHaveSingleItem();

        order.Fields["changes"].Text.IndexOf("Start", StringComparison.Ordinal).ShouldBeLessThan(order.Fields["changes"].Text.IndexOf("Store", StringComparison.Ordinal));
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task Responses_are_found_by_order_and_by_state()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);
        await store.SaveResponseAsync(new JobResponse { Id = "r-1", JobOrderId = "job-1", State = JobOrderState.AllowedToStartLoaded }, Scope, Ct);
        await store.SaveResponseAsync(new JobResponse { Id = "r-2", JobOrderId = "job-2", State = JobOrderState.AllowedToStartReady }, Scope, Ct);
        await store.SaveResponseAsync(new JobResponse { Id = "r-3", JobOrderId = "job-3", State = JobOrderState.Running }, Scope, Ct);

        (await store.GetResponseByOrderAsync("job-1", Ct)).ShouldNotBeNull().Response.Id.ShouldBe("r-1");
        var found = await store.GetResponsesByStateAsync(Scope, [JobOrderState.AllowedToStartLoaded, JobOrderState.AllowedToStartReady], 10, Ct);
        found.Select(static r => r.Id).Order().ShouldBe(["r-1", "r-2"]);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task The_by_state_query_stops_at_the_limit()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);
        for (var i = 0; i < 5; i++)
        {
            await store.SaveResponseAsync(new JobResponse { Id = $"r-{i}", JobOrderId = $"job-{i}", State = JobOrderState.Running }, Scope, Ct);
        }

        (await store.GetResponsesByStateAsync(Scope, [JobOrderState.Running], 2, Ct)).Count.ShouldBe(2);
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task Work_masters_stay_in_their_scope()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);
        var master = new JobWorkMaster { Id = "wm", DataSchema = "s", Data = "recipe" };

        (await store.SaveWorkMasterAsync(master, Scope, Ct)).ShouldBeTrue();
        (await store.SaveWorkMasterAsync(master, "site.other", Ct)).ShouldBeFalse();
        await store.DeleteWorkMasterAsync("wm", "site.other", Ct);

        (await store.IsWorkMasterAsync("wm", Scope, Ct)).ShouldBeTrue();
        await store.DeleteWorkMasterAsync("wm", Scope, Ct);
        (await store.IsWorkMasterAsync("wm", Scope, Ct)).ShouldBeFalse();
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task References_and_limits_round_trip()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);

        foreach (var kind in Enum.GetValues<JobReferenceKind>())
        {
            await store.AddReferenceAsync(kind, "id", Scope, Ct);
            (await store.IsReferenceAsync(kind, "id", Scope, Ct)).ShouldBeTrue();
            await store.RemoveReferenceAsync(kind, "id", Scope, Ct);
            (await store.IsReferenceAsync(kind, "id", Scope, Ct)).ShouldBeFalse();
        }

        await store.SetAcceptanceLimitAsync(Scope, 4, Ct);
        (await store.GetAcceptanceLimitAsync(Scope, Ct)).ShouldBe(4);
        await store.DeleteAcceptanceLimitAsync(Scope, Ct);
        (await store.GetAcceptanceLimitAsync(Scope, Ct)).ShouldBeNull();
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task The_inspector_lists_orders_and_shows_the_changes_of_one()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var store = Store(env);
        var inspector = new JobsInspector(store, env.Context);
        await store.SaveOrderAsync(Write(JobOrderState.Running, 0), Ct);
        await store.SaveOrderAsync(new JobOrderWrite(new JobOrder { Id = "job-2" }, JobOrderState.Aborted, Scope, "Store", 0, "op", JobRecipeWork.None), Ct);
        await store.SaveResponseAsync(new JobResponse { Id = "r-1", JobOrderId = "job-1", State = JobOrderState.Running }, Scope, Ct);

        var orders = await inspector.InspectAsync(new InspectionQuery("jobs.orders", Arguments: new Dictionary<string, string> { ["scope"] = Scope }), Ct);
        orders.Entries.Select(static e => e.Key).Order().ShouldBe(["job-1", "job-2"]);

        var running = await inspector.InspectAsync(new InspectionQuery("jobs.orders", Arguments: new Dictionary<string, string> { ["scope"] = Scope, ["state"] = "running" }), Ct);
        running.Entries.ShouldHaveSingleItem().Key.ShouldBe("job-1");

        var order = (await inspector.InspectAsync(new InspectionQuery("jobs.order", Arguments: new Dictionary<string, string> { ["id"] = "job-1" }), Ct)).Entries.ShouldHaveSingleItem();
        order.Fields["state"].Text.ShouldBe("Running");
        order.Fields["responseId"].Text.ShouldBe("r-1");
        order.Fields["changes"].Text.ShouldContain("Store");
    }

    [Fact(SkipUnless = nameof(TestEndpoints.ValkeyConfigured), SkipType = typeof(TestEndpoints), Skip = Skip)]
    public async Task The_inspector_has_nothing_for_an_unknown_id()
    {
        using var env = await ValkeyTestEnvironment.CreateAsync();
        var inspector = new JobsInspector(Store(env), env.Context);

        (await inspector.InspectAsync(new InspectionQuery("jobs.order", Arguments: new Dictionary<string, string> { ["id"] = "nope" }), Ct)).Entries.ShouldBeEmpty();
    }
}
