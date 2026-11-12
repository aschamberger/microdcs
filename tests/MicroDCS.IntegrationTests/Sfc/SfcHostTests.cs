using System.Globalization;
using System.Text;
using MicroDCS.App.Greetings;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Inspection;
using MicroDCS.IntegrationTests.Mqtt;
using MicroDCS.JobControl.Codec;
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
using Opc.Ua;

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

        public async Task ScanAsync()
        {
            foreach (var consumer in Services.GetServices<IStreamConsumer>().Where(static c => c.Kind == SfcWork.NudgeKind))
            {
                await consumer.ScopeDiscoveredAsync(Scope, Ct);
            }
        }

        public async ValueTask DisposeAsync()
        {
            // A short grace period: shutdown phase 5 (D-37) waits for the expiry timers of open commands until the grace period
            // ends, and these recipes leave 30 s commands open. No test here asserts the shutdown, so a cancelled grace period
            // (timers cancelled, the abort path of each phase) is enough and saves 30 s per such test (S22a).
            using var grace = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            try
            {
                await _host.StopAsync(grace.Token);
            }
            finally
            {
                _host.Dispose();
                await Equipment.DisposeAsync();
            }
        }
    }

    private static string PullRecipe(string? selector = null, int timeoutSeconds = 30) => $$$"""
        {"Steps":[{"Name":"s1","Initial":true}],"Transitions":[],
         "Actions":[{"Name":"p1","Step":"s1","Qualifier":"N","Interaction":"pull_event","TypeId":"{{{HelloType}}}","TimeoutSeconds":{{{timeoutSeconds}}}{{{(selector is null ? string.Empty : ",\"Selector\":" + selector)}}}}]}
        """;

    private static Task PublishHelloAsync(Rig rig, string id, string subject = Scope, string? correlationId = null) =>
        rig.Equipment.PublishEventAsync($"{rig.Prefix}/site/area/events", id, HelloType, subject, "{\"Name\":\"Plant\"}", rig.Prefix + "-sink/hello", Ct, correlationId: correlationId);

    private static Task WaitingAsync(Rig rig, string job) =>
        EventuallyAsync(async () => (await rig.StateAsync(job))?.IsWaitingFor(HelloType) == true, $"{job} to wait");

    private static Task CompletedAsync(Rig rig, string job) =>
        EventuallyAsync(async () => (await rig.StateAsync(job))?.Status == SfcJobStatus.Completed, $"{job} to complete");

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AnEventCompletesTheOldestWaitOnly()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("pull-1", PullRecipe());
        await WaitingAsync(rig, "pull-1");
        await Task.Delay(5, CancellationToken.None);
        await rig.StartJobAsync("pull-2", null);
        await WaitingAsync(rig, "pull-2");

        await PublishHelloAsync(rig, "ev-1");
        await CompletedAsync(rig, "pull-1");
        (await rig.StateAsync("pull-2")).ShouldNotBeNull().IsWaitingFor(HelloType).ShouldBeTrue();

        await PublishHelloAsync(rig, "ev-2");
        await CompletedAsync(rig, "pull-2");
        (await rig.States.ListWaitingAsync(Scope, HelloType, 10, Ct)).ShouldBeEmpty();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task ASelectorOnTheJobIdPicksTheJobWhateverTheOrder()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("sel-1", PullRecipe("""{"CorrelationId":"{jobId}"}"""));
        await WaitingAsync(rig, "sel-1");
        await rig.StartJobAsync("sel-2", null);
        await WaitingAsync(rig, "sel-2");

        await PublishHelloAsync(rig, "ev-1", correlationId: "unrelated");
        await PublishHelloAsync(rig, "ev-2", correlationId: "sel-2");

        await CompletedAsync(rig, "sel-2");
        var first = (await rig.StateAsync("sel-1")).ShouldNotBeNull();
        first.IsWaitingFor(HelloType).ShouldBeTrue();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AnAppliedEventIsNotOfferedToAnotherJobWhenRedelivered()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("claim-1", PullRecipe());
        await WaitingAsync(rig, "claim-1");
        const string EntryId = "9999999999999-0";
        await rig.Context.Database.StringSetAsync(rig.Context.Keys.SfcEventClaim(Scope, EntryId), "claim-0");

        await rig.Context.Database.StreamAddAsync(rig.Context.Keys.SfcWork(Scope), [new("job_id", string.Empty), new("action", SfcWork.Event), new("scope", Scope), new(SfcWork.TypeIdField, HelloType), new(SfcWork.OutcomeField, "done")], EntryId);

        await EventuallyAsync(async () => (await rig.Context.Database.StreamPendingAsync(rig.Context.Keys.SfcWork(Scope), SfcWork.Group)).PendingMessageCount == 0, "the entry to be acknowledged");
        (await rig.StateAsync("claim-1")).ShouldNotBeNull().IsWaitingFor(HelloType).ShouldBeTrue();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AnEventDeliveredTwiceCompletesOneJob()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("twice-1", PullRecipe());
        await WaitingAsync(rig, "twice-1");
        await rig.StartJobAsync("twice-2", null);
        await WaitingAsync(rig, "twice-2");
        var stream = rig.Context.Keys.SfcWork(Scope);

        foreach (var _ in new[] { 1, 2 })
        {
            await rig.Context.Database.StreamAddAsync(stream, [new("job_id", string.Empty), new("action", SfcWork.Event), new("scope", Scope), new(SfcWork.EventKeyField, "urn:it|same-id"), new(SfcWork.TypeIdField, HelloType), new(SfcWork.OutcomeField, "done")]);
        }

        await CompletedAsync(rig, "twice-1");
        await EventuallyAsync(async () => (await rig.Context.Database.StreamPendingAsync(stream, SfcWork.Group)).PendingMessageCount == 0, "the entries to be acknowledged");
        (await rig.StateAsync("twice-2")).ShouldNotBeNull().IsWaitingFor(HelloType).ShouldBeTrue();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AJobThatWaitsAgainForTheSameTypeGoesToTheEndOfTheQueue()
    {
        const string TwoWaits = $$$"""
            {"Steps":[{"Name":"s1","Initial":true},{"Name":"s2"}],
             "Transitions":[{"Source":"s1","Target":"s2","Condition":"true","Priority":0}],
             "Actions":[{"Name":"p1","Step":"s1","Qualifier":"N","Interaction":"pull_event","TypeId":"{{{HelloType}}}","TimeoutSeconds":30},
                        {"Name":"p2","Step":"s2","Qualifier":"N","Interaction":"pull_event","TypeId":"{{{HelloType}}}","TimeoutSeconds":30}]}
            """;
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("queue-1", TwoWaits);
        await WaitingAsync(rig, "queue-1");
        await Task.Delay(5, CancellationToken.None);
        await rig.StartJobAsync("queue-2", PullRecipe(), "wm-2");
        await WaitingAsync(rig, "queue-2");

        await PublishHelloAsync(rig, "ev-1");
        await EventuallyAsync(async () => (await rig.StateAsync("queue-1"))?.ActiveSteps.Contains("s2") == true, "the first job to reach its second wait");
        await PublishHelloAsync(rig, "ev-2");

        await CompletedAsync(rig, "queue-2");
        (await rig.StateAsync("queue-1")).ShouldNotBeNull().IsWaitingFor(HelloType).ShouldBeTrue();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AnEventNobodyWaitsForIsNotQueued()
    {
        await using var rig = await Rig.StartAsync();

        await PublishHelloAsync(rig, "ev-idle");

        await Task.Delay(300, CancellationToken.None);
        (await rig.Context.Database.StreamLengthAsync(rig.Context.Keys.SfcWork(Scope))).ShouldBe(0);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task TheRecoveryScanDoesNotResendACommandThatIsStillOpen()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("scan-1", Recipe());
        await rig.Equipment.ReceiveAsync(Ct);

        await rig.ScanAsync();

        await EventuallyAsync(async () => (await rig.Context.Database.StreamPendingAsync(rig.Context.Keys.SfcWork(Scope), SfcWork.Group)).PendingMessageCount == 0, "the scan to be handled");
        await Task.Delay(200, CancellationToken.None);
        var action = (await rig.StateAsync("scan-1")).ShouldNotBeNull().Actions["a1"];
        action.Attempt.ShouldBe(1);
        action.Status.ShouldBe(SfcActionStatus.Dispatched);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task TheRecoveryScanPutsBackLostIndexEntries()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("scan-2", PullRecipe());
        await WaitingAsync(rig, "scan-2");
        await rig.Context.Database.KeyDeleteAsync(rig.Context.Keys.SfcDue(Scope));
        await rig.Context.Database.KeyDeleteAsync(rig.Context.Keys.SfcWaits(Scope, HelloType));

        await rig.ScanAsync();

        await EventuallyAsync(async () => (await rig.States.ListWaitingAsync(Scope, HelloType, 10, Ct)).Count == 1, "the wait to come back");
        await EventuallyAsync(async () => (await rig.States.ListDueAsync(Scope, 10, Ct)).Count == 1, "the deadline to come back");
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

    /// <summary>Waits briefly and fails when the equipment receives a command in that time.</summary>
    private static async Task NoCommandAsync(Rig rig, TimeSpan quiet, string why)
    {
        using var window = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        window.CancelAfter(quiet);
        MqttApplicationMessage? unexpected = null;
        try
        {
            unexpected = await rig.Equipment.ReceiveAsync(window.Token);
        }
        catch (OperationCanceledException) when (!Ct.IsCancellationRequested)
        {
            // Quiet for the whole window, as expected.
        }

        unexpected.ShouldBeNull($"no command expected: {why}, got {(unexpected is null ? string.Empty : MqttTestClient.Property(unexpected, "mdcsactionkey"))}");
    }

    private static Task SendMesAsync<T>(Rig rig, string id, string type, T call)
        where T : class, IEncodeable =>
        rig.Equipment.PublishEventAsync($"{rig.Prefix}-mes/site/area/commands", id, type, Scope, Encoding.UTF8.GetString(OpcUaJsonCodec.Encode(call)), rig.Prefix + "-client/replies", Ct);

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task APausedRecipeSendsNothingUntilResumeAndThenCompletesTheJob()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("hold-1", Recipe(timeoutSeconds: 2, maxAttempts: 1));
        var first = await rig.Equipment.ReceiveAsync(Ct);
        MqttTestClient.Property(first, "mdcsactionkey").ShouldBe("hold-1:a1");
        await EventuallyAsync(async () => await rig.OrderStateAsync("hold-1") == JobOrderState.Running, "the job to run");

        await SendMesAsync(rig, "mes-pause", JobControlTypes.Pause, new PauseCall { JobOrderID = "hold-1" });
        await EventuallyAsync(async () => (await rig.StateAsync("hold-1"))?.IsHeld == true, "the recipe to be held");
        (await rig.OrderStateAsync("hold-1")).ShouldBe(JobOrderState.Suspended);
        (await rig.States.ListDueAsync(Scope, 10, Ct)).ShouldBeEmpty();

        // Longer than the only attempt of a1: without the hold the job would fault and abort here.
        await NoCommandAsync(rig, TimeSpan.FromSeconds(3), "the deadline of a held job does not expire");
        (await rig.StateAsync("hold-1")).ShouldNotBeNull().Status.ShouldBe(SfcJobStatus.Running);

        // Equipment finishes what it started: the answer is recorded, the recipe stays at its step boundary.
        await rig.AnswerAsync(first);
        await EventuallyAsync(async () => (await rig.StateAsync("hold-1"))?.Actions["a1"].Status == SfcActionStatus.Completed, "the answer to be recorded");
        await NoCommandAsync(rig, TimeSpan.FromSeconds(1), "a held recipe does not enter its next step");
        (await rig.StateAsync("hold-1")).ShouldNotBeNull().ActiveSteps.ShouldBe(["s1"]);

        await SendMesAsync(rig, "mes-resume", JobControlTypes.Resume, new ResumeCall { JobOrderID = "hold-1" });
        var second = await rig.Equipment.ReceiveAsync(Ct);
        MqttTestClient.Property(second, "mdcsactionkey").ShouldBe("hold-1:a2");
        await rig.AnswerAsync(second);

        await EventuallyAsync(async () => await rig.OrderStateAsync("hold-1") == JobOrderState.Completed, "the job to complete");
        (await rig.StateAsync("hold-1")).ShouldNotBeNull().Status.ShouldBe(SfcJobStatus.Completed);
    }

    private static async Task SetOrderAsync(Rig rig, string job, JobOrderState state, string operation, JobRecipeWork work)
    {
        var order = (await rig.Jobs.GetOrderAsync(job, Ct)).ShouldNotBeNull();
        (await rig.Jobs.SaveOrderAsync(new JobOrderWrite(order.Order, state, Scope, operation, order.Version, "op-" + operation + Guid.NewGuid().ToString("N"), work), Ct)).ShouldBeTrue();
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task ADispatchThatReachesAHeldJobIsSentOnReleaseAndOnlyOnce()
    {
        await using var rig = await Rig.StartAsync();
        var stream = rig.Context.Keys.SfcWork(Scope);
        var state = rig.Context.Keys.SfcState("hold-2");
        await rig.StartJobAsync("hold-2", Recipe(timeoutSeconds: 4));
        var first = await rig.Equipment.ReceiveAsync(Ct);
        await EventuallyAsync(async () => await rig.OrderStateAsync("hold-2") == JobOrderState.Running, "the job to run");
        var dispatch = (await rig.Context.Database.StreamRangeAsync(stream)).Single(static e => e["action"] == SfcWork.DispatchPrefix + "a1");

        await SetOrderAsync(rig, "hold-2", JobOrderState.Suspended, "Pause", JobRecipeWork.Hold);
        await EventuallyAsync(async () => (await rig.StateAsync("hold-2"))?.IsHeld == true, "the recipe to be held");

        // As if the first dispatch had waited in the stream until the hold: no marker, and the entry reaches a consumer while held. The hold outlasts
        // the first deadline.
        await rig.Context.Database.HashDeleteAsync(state, "sent:a1");
        await rig.Context.Database.StreamAddAsync(stream, dispatch.Values);
        await NoCommandAsync(rig, TimeSpan.FromSeconds(4.5), "a dispatch of a held job is not sent");
        (await rig.StateAsync("hold-2")).ShouldNotBeNull().Actions["a1"].Attempt.ShouldBe(1);

        await SetOrderAsync(rig, "hold-2", JobOrderState.Running, "Resume", JobRecipeWork.Release);
        var again = await rig.Equipment.ReceiveAsync(Ct);
        MqttTestClient.Property(again, "id").ShouldBe(MqttTestClient.Property(first, "id"));
        var released = (await rig.StateAsync("hold-2")).ShouldNotBeNull();
        released.Actions["a1"].Attempt.ShouldBe(1);
        released.Status.ShouldBe(SfcJobStatus.Running);

        // A redelivered dispatch of a command that went out does not reach the equipment again.
        await rig.Context.Database.StreamAddAsync(stream, dispatch.Values);
        await NoCommandAsync(rig, TimeSpan.FromSeconds(1), "each command id is sent once");

        // An entry from before the hold carries the old deadline, which has passed; the moved deadline in the state still lets it out.
        await rig.Context.Database.HashDeleteAsync(state, "sent:a1");
        await rig.Context.Database.StreamAddAsync(stream, dispatch.Values);
        MqttTestClient.Property(await rig.Equipment.ReceiveAsync(Ct), "id").ShouldBe(MqttTestClient.Property(first, "id"));
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AHoldEntryThatArrivesAfterTheResumeLeavesTheRecipeReleased()
    {
        await using var rig = await Rig.StartAsync();
        var stream = rig.Context.Keys.SfcWork(Scope);
        await rig.StartJobAsync("hold-4", Recipe());
        await rig.Equipment.ReceiveAsync(Ct);
        await EventuallyAsync(async () => await rig.OrderStateAsync("hold-4") == JobOrderState.Running, "the job to run");
        await SetOrderAsync(rig, "hold-4", JobOrderState.Suspended, "Pause", JobRecipeWork.Hold);
        await EventuallyAsync(async () => (await rig.StateAsync("hold-4"))?.IsHeld == true, "the recipe to be held");

        // The order runs again, but its release entry went to a consumer that read the order before the Resume; then a stale hold entry arrives.
        await SetOrderAsync(rig, "hold-4", JobOrderState.Running, "Resume", JobRecipeWork.None);
        await rig.Context.Database.StreamAddAsync(stream, [new("job_id", "hold-4"), new("action", SfcWork.Hold), new("scope", Scope)]);

        await EventuallyAsync(async () => (await rig.StateAsync("hold-4"))?.IsHeld == false, "the recipe to follow the running order");
        (await rig.States.ListDueAsync(Scope, 10, Ct)).ShouldHaveSingleItem().JobId.ShouldBe("hold-4");
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcHostTests), Skip = Skip)]
    public async Task AStopOfAPausedJobEndsItsRecipe()
    {
        await using var rig = await Rig.StartAsync();
        await rig.StartJobAsync("hold-3", Recipe());
        await rig.Equipment.ReceiveAsync(Ct);
        await EventuallyAsync(async () => await rig.OrderStateAsync("hold-3") == JobOrderState.Running, "the job to run");
        await SendMesAsync(rig, "mes-pause-3", JobControlTypes.Pause, new PauseCall { JobOrderID = "hold-3" });
        await EventuallyAsync(async () => (await rig.StateAsync("hold-3"))?.IsHeld == true, "the recipe to be held");

        await SendMesAsync(rig, "mes-stop-3", JobControlTypes.Stop, new StopCall { JobOrderID = "hold-3" });

        await EventuallyAsync(async () => (await rig.StateAsync("hold-3"))?.Status == SfcJobStatus.Cancelled, "the recipe to end");
        (await rig.StateAsync("hold-3")).ShouldNotBeNull().HeldAt.ShouldBeNull();
        (await rig.States.ListActiveAsync(Scope, Ct)).ShouldBeEmpty();
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
