using System.Globalization;
using MicroDCS.App.Greetings;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using MicroDCS.IntegrationTests.Mqtt;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.MachineryJobs;
using MicroDCS.Sfc;
using MicroDCS.Sfc.Hosting;
using MicroDCS.Transport.Mqtt;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Sfc;
using MicroDCS.Valkey.Streams;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MQTTnet;

namespace MicroDCS.IntegrationTests.Sfc;

/// <summary>The SFC host end to end: a recipe of push actions over MQTT and Valkey, with a test client as the equipment (S19).</summary>
[Trait("Category", "Integration")]
public sealed class SfcHostTests
{
    private const string Skip = $"{TestEndpoints.MqttVariable} or {TestEndpoints.ValkeyVariable} is not set";
    private const string Scope = "site.area";
    private const string ByeType = "com.github.aschamberger.microdcs.greetings.bye.v1";
    private const string HelloType = "com.github.aschamberger.microdcs.greetings.hello.v1";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public static bool Configured => TestEndpoints.MqttConfigured && TestEndpoints.ValkeyConfigured;

    private static string Recipe(int timeoutSeconds = 30, int maxAttempts = 3) => $$$"""
        {"Steps":[{"Name":"s1","Initial":true},{"Name":"s2"}],
         "Transitions":[{"Source":"s1","Target":"s2","Condition":"true","Priority":0}],
         "Actions":[{"Name":"a1","Step":"s1","Qualifier":"N","Interaction":"push_command","TypeId":"{{{ByeType}}}","TimeoutSeconds":{{{timeoutSeconds}}},"MaxAttempts":{{{maxAttempts}}},"Parameters":{}},
                    {"Name":"a2","Step":"s2","Qualifier":"N","Interaction":"push_command","TypeId":"{{{ByeType}}}","TimeoutSeconds":{{{timeoutSeconds}}},"MaxAttempts":{{{maxAttempts}}},"Parameters":{}}]}
        """;

    private static async Task EventuallyAsync(Func<Task<bool>> condition, string what)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        while (!await condition())
        {
            timeout.IsCancellationRequested.ShouldBeFalse($"Timed out waiting: {what}");
            await Task.Delay(25, CancellationToken.None);
        }
    }

    private sealed class Rig : IAsyncDisposable
    {
        private readonly IHost _host;

        private Rig(IHost host, MqttTestClient equipment, string prefix)
        {
            _host = host;
            Equipment = equipment;
            Prefix = prefix;
        }

        public MqttTestClient Equipment { get; }

        public string Prefix { get; }

        public IServiceProvider Services => _host.Services;

        public IJobStore Jobs => Services.GetRequiredService<IJobStore>();

        public SfcStateDao States => Services.GetRequiredService<SfcStateDao>();

        public ValkeyContext Context => Services.GetRequiredService<ValkeyContext>();

        public static async Task<Rig> StartAsync(Dictionary<string, string?>? extra = null)
        {
            var (broker, port) = TestEndpoints.Mqtt!.Value;
            var (valkey, valkeyPort) = TestEndpoints.Valkey!.Value;
            var prefix = "sf" + Guid.NewGuid().ToString("N")[..10];
            var settings = new Dictionary<string, string?>
            {
                ["MicroDcs:InstanceId"] = "i" + prefix,
                ["MicroDcs:CloudEventSource"] = "urn:it:sfc",
                [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:Transport"] = "mqtt",
                [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:Prefix"] = prefix + "-mes",
                [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:ScopeLevels"] = "2",
                [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:ReplyBase"] = prefix + "-mes-reply",
                ["MicroDcs:Bindings:greetings:Transport"] = "mqtt",
                ["MicroDcs:Bindings:greetings:Prefix"] = prefix,
                ["MicroDcs:Bindings:greetings:ScopeLevels"] = "2",
                ["MicroDcs:Bindings:greetings:ReplyBase"] = prefix + "-reply",
                ["MicroDcs:Mqtt:Hostname"] = broker,
                ["MicroDcs:Mqtt:Port"] = port.ToString(CultureInfo.InvariantCulture),
                ["MicroDcs:Mqtt:TlsCaFile"] = "/nonexistent/ca.crt",
                ["MicroDcs:Mqtt:SatTokenFile"] = "/nonexistent/sat",
                ["MicroDcs:Valkey:ConnectionString"] = $"{valkey}:{valkeyPort}",
                ["MicroDcs:Valkey:KeyPrefix"] = prefix,
                ["MicroDcs:Valkey:PollInterval"] = "00:00:00.100",
                ["MicroDcs:Sfc:SweepInterval"] = "00:00:00.100",
                ["MicroDcs:Sfc:ExpiryGrace"] = "00:00:00",
            };
            foreach (var (name, value) in extra ?? [])
            {
                settings[name] = value;
            }

            var builder = Host.CreateApplicationBuilder();
            builder.Configuration.AddInMemoryCollection(settings);
            builder.Services.AddMicroDcs(builder.Configuration).AddMqttTransport(builder.Configuration);
            builder.Services.AddProcessor<GreetingsProcessor>();
            builder.Services.AddMicroDcsValkey(builder.Configuration);
            builder.Services.AddMachineryJobs(builder.Configuration);
            builder.Services.AddSfcHost(builder.Configuration);
            var host = builder.Build();

            var equipment = await MqttTestClient.ConnectAsync(null, Ct);
            await equipment.SubscribeAsync($"{prefix}/site/area/commands", Ct);
            await host.StartAsync(Ct);
            return new Rig(host, equipment, prefix);
        }

        public async Task StartJobAsync(string job, string? recipe, string workMaster = "wm-1")
        {
            if (recipe is not null)
            {
                (await Jobs.SaveWorkMasterAsync(new JobWorkMaster { Id = workMaster, DataSchema = SfcInterpreter.RecipeDataSchema, Data = recipe }, Scope, Ct)).ShouldBeTrue();
            }

            var order = new JobOrder { Id = job, WorkMasters = [new JobWorkMaster { Id = workMaster }] };
            (await Jobs.SaveOrderAsync(new JobOrderWrite(order, JobOrderState.AllowedToStartReady, Scope, "StoreAndStart", 0, "op-start-" + job, JobRecipeWork.Start), Ct)).ShouldBeTrue();
        }

        public async Task<SfcState?> StateAsync(string job)
        {
            var document = await States.GetAsync(job, Ct);
            return document is null ? null : SfcJobRunner.Parse(document);
        }

        public async Task<JobOrderState> OrderStateAsync(string job) => (await Jobs.GetOrderAsync(job, Ct))?.State ?? JobOrderState.Removed;

        public Task AnswerAsync(MqttApplicationMessage command) =>
            Equipment.PublishEventAsync(command.ResponseTopic, Guid.NewGuid().ToString(), HelloType, Scope, "{\"Name\":\"Plant\"}", null, Ct, causationId: MqttTestClient.Property(command, "id"));

        public async ValueTask DisposeAsync()
        {
            await _host.StopAsync(CancellationToken.None);
            _host.Dispose();
            await Equipment.DisposeAsync();
        }
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task ARecipeOfPushActionsRunsToTheEndAndCompletesTheJob()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("job-1", Recipe());

        var first = await rig.Equipment.ReceiveAsync(Ct);
        MqttTestClient.Property(first, "type").ShouldBe(ByeType);
        MqttTestClient.Property(first, "mdcsactionkey").ShouldBe("job-1:a1");
        first.ResponseTopic.ShouldNotBeNullOrEmpty();
        first.MessageExpiryInterval.ShouldBeGreaterThan(0u);
        await EventuallyAsync(async () => await rig.OrderStateAsync("job-1") == JobOrderState.Running, "the job to run");

        await rig.AnswerAsync(first);
        var second = await rig.Equipment.ReceiveAsync(Ct);
        MqttTestClient.Property(second, "mdcsactionkey").ShouldBe("job-1:a2");
        await rig.AnswerAsync(second);

        await EventuallyAsync(async () => (await rig.StateAsync("job-1"))?.Status == SfcJobStatus.Completed, "the recipe to end");
        await EventuallyAsync(async () => await rig.OrderStateAsync("job-1") == JobOrderState.Completed, "the job to complete");
        var finished = (await rig.Jobs.GetResponseByOrderAsync("job-1", Ct)).ShouldNotBeNull().Response;
        finished.Id.ShouldBe("job-1-response");
        ((DateTime)finished.StartTime).Year.ShouldBeGreaterThan(2000);
        (await rig.States.ListActiveAsync(Scope, Ct)).ShouldBeEmpty();
        (await rig.States.ListDueAsync(Scope, 10, Ct)).ShouldBeEmpty();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AnUnansweredCommandIsDispatchedAgainThenAbortsTheJob()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("job-2", Recipe(timeoutSeconds: 1, maxAttempts: 2));

        var first = await rig.Equipment.ReceiveAsync(Ct);
        var second = await rig.Equipment.ReceiveAsync(Ct);

        MqttTestClient.Property(second, "mdcsactionkey").ShouldBe(MqttTestClient.Property(first, "mdcsactionkey"));
        MqttTestClient.Property(second, "id").ShouldNotBe(MqttTestClient.Property(first, "id"));
        await EventuallyAsync(async () => await rig.OrderStateAsync("job-2") == JobOrderState.Aborted, "the job to abort");
        var state = await rig.StateAsync("job-2");
        state.ShouldNotBeNull().Status.ShouldBe(SfcJobStatus.Faulted);
        state.FaultCode.ShouldBe(SfcOutcome.Timeout);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AnAbortOfTheJobEndsItsRecipe()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("job-3", Recipe());
        await rig.Equipment.ReceiveAsync(Ct);
        await EventuallyAsync(async () => await rig.OrderStateAsync("job-3") == JobOrderState.Running, "the job to run");

        var order = (await rig.Jobs.GetOrderAsync("job-3", Ct)).ShouldNotBeNull();
        (await rig.Jobs.SaveOrderAsync(new JobOrderWrite(order.Order, JobOrderState.Aborted, Scope, "Abort", order.Version, "op-abort", JobRecipeWork.Cancel), Ct)).ShouldBeTrue();

        await EventuallyAsync(async () => (await rig.StateAsync("job-3"))?.Status == SfcJobStatus.Cancelled, "the recipe to end");
        (await rig.States.ListDueAsync(Scope, 10, Ct)).ShouldBeEmpty();
        (await rig.OrderStateAsync("job-3")).ShouldBe(JobOrderState.Aborted);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AJobWithoutAUsableRecipeIsAbortedInsteadOfLeftWaiting()
    {
        await using var rig = await Rig.StartAsync();

        await rig.StartJobAsync("job-4", recipe: null, workMaster: "absent");

        await EventuallyAsync(async () => await rig.OrderStateAsync("job-4") == JobOrderState.Aborted, "the job to abort");
        (await rig.StateAsync("job-4")).ShouldBeNull();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AStartEntryOfARevokedOrderStartsNothing()
    {
        await using var rig = await Rig.StartAsync();
        var order = new JobOrder { Id = "job-5", WorkMasters = [new JobWorkMaster { Id = "wm-1" }] };
        (await rig.Jobs.SaveOrderAsync(new JobOrderWrite(order, JobOrderState.NotAllowedToStartReady, Scope, "RevokeStart", 0, "op", JobRecipeWork.Start), Ct)).ShouldBeTrue();

        var database = rig.Context.Multiplexer.GetDatabase();
        var stream = rig.Context.Keys.SfcWork(Scope);
        await EventuallyAsync(
            async () => (await database.StreamGroupInfoAsync(stream)) is [{ PendingMessageCount: 0, LastDeliveredId: not "0-0" }],
            "the entry to be consumed");

        (await rig.StateAsync("job-5")).ShouldBeNull();
        (await rig.OrderStateAsync("job-5")).ShouldBe(JobOrderState.NotAllowedToStartReady);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AWorkEntryThatKeepsFailingGoesToTheDeadStream()
    {
        await using var rig = await Rig.StartAsync(new() { ["MicroDcs:Sfc:AutoclaimIdle"] = "00:00:00.050", ["MicroDcs:Sfc:MaxDeliveries"] = "1" });
        var state = new SfcState { JobId = "job-6", Scope = Scope, RecipeHash = "not-stored", Status = SfcJobStatus.Running };
        (await rig.States.CompareAndSetAsync("job-6", Scope, SfcJobRunner.Serialize(state), 0, null, true, [], cancellationToken: Ct)).Written.ShouldBeTrue();
        var keys = rig.Context.Keys;
        await rig.Services.GetRequiredService<ValkeyStreams>().AppendAsync(keys.SfcWork(Scope), "sfc", Scope, SfcWork.Entry("job-6", Scope, SfcWork.Tick, (SfcWork.OutageField, "0")), Ct);

        var database = rig.Context.Multiplexer.GetDatabase();
        await EventuallyAsync(async () => await database.StreamLengthAsync(keys.SfcDead(Scope)) == 1, "the entry to go dead");

        var dead = (await database.StreamRangeAsync(keys.SfcDead(Scope))).ShouldHaveSingleItem();
        dead["action"].ToString().ShouldBe(SfcWork.Tick);
        dead["dead_reason"].ToString().ShouldContain("delivered");
        (await database.StreamPendingAsync(keys.SfcWork(Scope), SfcWork.Group)).PendingMessageCount.ShouldBe(0);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AnInstanceWithoutTheProcessorRoleRunsNoRecipe()
    {
        await using var rig = await Rig.StartAsync(new() { ["MicroDcs:Roles"] = nameof(InstanceRoles.Publisher) });
        await rig.StartJobAsync("job-7", Recipe());

        var database = rig.Context.Multiplexer.GetDatabase();
        var poller = rig.Services.GetRequiredService<ValkeyPoller>();
        await EventuallyAsync(() => Task.FromResult(poller.Scopes.Count > 0), "the scope to be known");
        await Task.Delay(300, CancellationToken.None); // Long enough for several polls; the assertion is that nothing happens.

        (await database.StreamLengthAsync(rig.Context.Keys.SfcWork(Scope))).ShouldBe(1);
        (await rig.StateAsync("job-7")).ShouldBeNull();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task TheInspectorShowsAJobItsDeadlineAndTheWork()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("job-8", Recipe());
        await rig.Equipment.ReceiveAsync(Ct);
        var inspector = rig.Services.GetServices<IInspector>().Single(static i => i.Name == "sfc");

        var job = (await inspector.InspectAsync(new InspectionQuery("sfc.job", Arguments: new Dictionary<string, string> { ["id"] = "job-8" }), Ct)).Entries.ShouldHaveSingleItem();
        var due = await inspector.InspectAsync(new InspectionQuery("sfc.due", Arguments: new Dictionary<string, string> { ["scope"] = Scope }), Ct);
        var work = await inspector.InspectAsync(new InspectionQuery("sfc.work", Arguments: new Dictionary<string, string> { ["scope"] = Scope }), Ct);

        job.Fields["status"].Text.ShouldBe(nameof(SfcJobStatus.Running));
        job.Fields["actions"].Text.ShouldContain("a1 Dispatched");
        due.Entries.ShouldHaveSingleItem().Key.ShouldBe("job-8");
        work.Entries[0].Fields["instancePendingCommands"].Text.ShouldBe("1");
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task OperatorActionsGoThroughTheWorkStream()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("job-9", Recipe());
        var first = await rig.Equipment.ReceiveAsync(Ct);
        await EventuallyAsync(async () => await rig.OrderStateAsync("job-9") == JobOrderState.Running, "the job to run");
        var operations = rig.Services.GetRequiredService<ISfcOperations>();

        (await operations.RedispatchAsync("job-9", "a1", Ct)).ShouldBeTrue();
        var again = await rig.Equipment.ReceiveAsync(Ct);
        MqttTestClient.Property(again, "mdcsactionkey").ShouldBe("job-9:a1");
        MqttTestClient.Property(again, "id").ShouldNotBe(MqttTestClient.Property(first, "id"));

        (await operations.AbortAsync("job-9", "test", Ct)).ShouldBeTrue();
        await EventuallyAsync(async () => await rig.OrderStateAsync("job-9") == JobOrderState.Aborted, "the job to abort");
        (await operations.TickAsync("unknown", Ct)).ShouldBeFalse();
    }

    private const string LoopRecipe = $$$"""
        {"Steps":[{"Name":"s1","Initial":true}],
         "Transitions":[{"Source":"s1","Target":"s1","Condition":"done","Priority":0}],
         "Actions":[{"Name":"a1","Step":"s1","Qualifier":"N","Interaction":"push_command","TypeId":"{{{ByeType}}}","TimeoutSeconds":30,"Parameters":{}}]}
        """;

    private static ActionResponse Response(string commandId, string actionKey) =>
        new() { EventId = Guid.NewGuid().ToString(), CommandId = commandId, ActionKey = actionKey, Outcome = ActionOutcome.Done };

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task ACommandIsSentEvenWhenItsTimeoutIsShorterThanTheGrace()
    {
        await using var rig = await Rig.StartAsync(new() { ["MicroDcs:Sfc:ExpiryGrace"] = "00:00:05" });

        await rig.StartJobAsync("job-10", Recipe(timeoutSeconds: 1, maxAttempts: 1));

        MqttTestClient.Property(await rig.Equipment.ReceiveAsync(Ct), "mdcsactionkey").ShouldBe("job-10:a1");
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task ATickRepeatedAfterItsWriteDoesNotMoveDeadlinesTwice()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("job-11", Recipe(timeoutSeconds: 60));
        await rig.Equipment.ReceiveAsync(Ct);
        var before = (await rig.StateAsync("job-11")).ShouldNotBeNull().NextDeadline.ShouldNotBeNull();
        var runner = rig.Services.GetRequiredService<SfcJobRunner>();
        var tick = new SfcTick { At = before - 30_000, OutageMs = 5000 };

        (await runner.RunAsync("job-11", tick, "1-0", null, Ct)).Status.ShouldBe(SfcRunStatus.Applied);
        (await runner.RunAsync("job-11", tick, "1-0", null, Ct)).Status.ShouldBe(SfcRunStatus.Unchanged);

        (await rig.StateAsync("job-11")).ShouldNotBeNull().NextDeadline.ShouldBe(before + 5000);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task ALateAnswerToAnEarlierPassOfALoopIsAbsorbed()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("job-12", LoopRecipe);
        var first = await rig.Equipment.ReceiveAsync(Ct);
        await rig.AnswerAsync(first);
        await rig.Equipment.ReceiveAsync(Ct);
        await EventuallyAsync(async () => (await rig.StateAsync("job-12"))?.Actions["a1"].Round == 2, "the second pass");
        var sink = rig.Services.GetRequiredService<IActionSink>();

        await sink.CompleteAsync(Response(MqttTestClient.Property(first, "id"), "job-12:a1"), Ct);

        var action = (await rig.StateAsync("job-12")).ShouldNotBeNull().Actions["a1"];
        action.Round.ShouldBe(2);
        action.Status.ShouldBe(SfcActionStatus.Dispatched);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AResponseThatCannotBeAppliedDoesNotFaultThePipeline()
    {
        await using var rig = await Rig.StartAsync();
        var state = new SfcState { JobId = "job-13", Scope = Scope, RecipeHash = "not-stored", Status = SfcJobStatus.Running };
        (await rig.States.CompareAndSetAsync("job-13", Scope, SfcJobRunner.Serialize(state), 0, null, true, [], cancellationToken: Ct)).Written.ShouldBeTrue();

        await rig.Services.GetRequiredService<IActionSink>().CompleteAsync(Response("cmd", "job-13:a1"), Ct);

        (await rig.States.GetAsync("job-13", Ct)).ShouldNotBeNull().Version.ShouldBe(1);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task ARecipeThatStartedAgainstAnAbortedOrderIsEnded()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("job-14", Recipe());
        await rig.Equipment.ReceiveAsync(Ct);
        await EventuallyAsync(async () => await rig.OrderStateAsync("job-14") == JobOrderState.Running, "the job to run");
        var order = (await rig.Jobs.GetOrderAsync("job-14", Ct)).ShouldNotBeNull();
        (await rig.Jobs.SaveOrderAsync(new JobOrderWrite(order.Order, JobOrderState.Aborted, Scope, "Abort", order.Version, "op-abort", JobRecipeWork.None), Ct)).ShouldBeTrue();
        var keys = rig.Context.Keys;

        await rig.Services.GetRequiredService<ValkeyStreams>().AppendAsync(keys.SfcWork(Scope), "sfc", Scope, SfcWork.Entry("job-14", Scope, SfcWork.Job, (SfcWork.OperationsField, nameof(JobOrderOperation.BeginExecution))), Ct);

        await EventuallyAsync(async () => (await rig.StateAsync("job-14"))?.Status == SfcJobStatus.Cancelled, "the recipe to end");
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AnOrderThatWaitedLongerThanItsTimeoutStillGetsItsFirstAttempt()
    {
        await using var rig = await Rig.StartAsync();
        (await rig.Jobs.SaveWorkMasterAsync(new JobWorkMaster { Id = "wm-1", DataSchema = SfcInterpreter.RecipeDataSchema, Data = Recipe(timeoutSeconds: 1, maxAttempts: 1) }, Scope, Ct)).ShouldBeTrue();
        var order = new JobOrder { Id = "job-15", WorkMasters = [new JobWorkMaster { Id = "wm-1" }] };
        (await rig.Jobs.SaveOrderAsync(new JobOrderWrite(order, JobOrderState.AllowedToStartReady, Scope, "StoreAndStart", 0, "op", JobRecipeWork.None), Ct)).ShouldBeTrue();

        // A start entry from an hour ago, as if no host had been running.
        var hourAgo = await rig.States.ServerTimeAsync(Ct) - 3_600_000;
        var database = rig.Context.Multiplexer.GetDatabase();
        await database.StreamAddAsync(rig.Context.Keys.SfcWork(Scope), [new("job_id", "job-15"), new("action", SfcWork.Start), new("scope", Scope)], messageId: $"{hourAgo}-0");
        await rig.Services.GetRequiredService<ValkeyStreams>().NudgeAsync("sfc", Scope, Ct);

        MqttTestClient.Property(await rig.Equipment.ReceiveAsync(Ct), "mdcsactionkey").ShouldBe("job-15:a1");
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task PendingEntriesCanBeReadForOneConsumer()
    {
        await using var rig = await Rig.StartAsync(new() { ["MicroDcs:Sfc:Enabled"] = "false" });
        var streams = rig.Services.GetRequiredService<ValkeyStreams>();
        var stream = rig.Context.Keys.SfcWork(Scope);
        await streams.EnsureGroupAsync(stream, SfcWork.Group, Ct);
        var fields = SfcWork.Entry("j", Scope, SfcWork.Tick);
        await streams.AppendAsync(stream, "sfc", Scope, fields, Ct);
        await streams.ReadAsync(stream, SfcWork.Group, "other", 1, Ct);
        await streams.AppendAsync(stream, "sfc", Scope, fields, Ct);
        await streams.ReadAsync(stream, SfcWork.Group, "mine", 1, Ct);

        var mine = await streams.PendingAsync(stream, SfcWork.Group, "-", "+", 1, "mine", Ct);

        mine.ShouldHaveSingleItem().ConsumerName.ToString().ShouldBe("mine");
    }
}
