using System.Globalization;
using MicroDCS.Core.Hosting;
using MicroDCS.IntegrationTests.Mqtt;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.MachineryJobs;
using MicroDCS.Sfc;
using MicroDCS.Sfc.Hosting;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Sfc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using MQTTnet;

namespace MicroDCS.IntegrationTests.Sfc;

/// <summary>
/// Two real replicas of the application image, killed from outside (S20, D-53). Runs through <c>scripts/replica-tests</c>, not in the main compose run:
/// a container that exits would end <c>--abort-on-container-exit</c>. The settings are in the <c>replicas</c> profile of <c>compose.test.yaml</c>.
/// </summary>
[Trait("Category", "Replica")]
public sealed class SfcReplicaTests : IAsyncLifetime
{
    private const string Skip = $"{DockerSocketVariable} is not set";
    private const string DockerSocketVariable = "MICRODCS_TEST_DOCKER";
    private const string Prefix = "sfcr";
    private const string Scope = "site.area";
    private const string ReplicaA = "microdcs-test-sfc-a";
    private const string ReplicaB = "microdcs-test-sfc-b";
    private const string ByeType = "com.github.aschamberger.microdcs.greetings.bye.v1";
    private const string HelloType = "com.github.aschamberger.microdcs.greetings.hello.v1";

    private static readonly string? _socket = Environment.GetEnvironmentVariable(DockerSocketVariable);

    private DockerEngine _docker = null!;
    private MqttTestClient _equipment = null!;
    private IHost _stores = null!;

    public static bool Configured => !string.IsNullOrEmpty(_socket);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private IJobStore Jobs => _stores.Services.GetRequiredService<IJobStore>();

    private SfcStateDao States => _stores.Services.GetRequiredService<SfcStateDao>();

    public async ValueTask InitializeAsync()
    {
        if (!Configured)
        {
            return;
        }

        _docker = new DockerEngine(_socket!);
        var (valkey, port) = TestEndpoints.Valkey!.Value;
        var builder = Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["MicroDcs:InstanceId"] = "replica-tests",
            ["MicroDcs:CloudEventSource"] = "urn:it:replicas",
            [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:Transport"] = "memory",
            [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:Prefix"] = Prefix + "-mes",
            [$"MicroDcs:Bindings:{MachineryJobsProcessor.BindingId}:ScopeLevels"] = "2",
            ["MicroDcs:Valkey:ConnectionString"] = $"{valkey}:{port}",
            ["MicroDcs:Valkey:KeyPrefix"] = Prefix,
        });
        builder.Services.AddMicroDcs(builder.Configuration).AddInMemoryTransport();
        builder.Services.AddMicroDcsValkey(builder.Configuration);
        builder.Services.AddMachineryJobs(builder.Configuration);
        _stores = builder.Build();

        _equipment = await MqttTestClient.ConnectAsync(null, Ct);
        await _equipment.SubscribeAsync($"{Prefix}/site/area/commands", Ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (!Configured)
        {
            return;
        }

        await _equipment.DisposeAsync();
        _stores.Dispose();
        _docker.Dispose();
    }

    private static string NewJob(string name) => $"{name}-{Guid.NewGuid():N}"[..24];

    private static string Recipe(int timeoutSeconds, int maxAttempts) => $$$"""
        {"Steps":[{"Name":"s1","Initial":true},{"Name":"s2"}],
         "Transitions":[{"Source":"s1","Target":"s2","Condition":"true","Priority":0}],
         "Actions":[{"Name":"a1","Step":"s1","Qualifier":"N","Interaction":"push_command","TypeId":"{{{ByeType}}}","TimeoutSeconds":{{{timeoutSeconds}}},"MaxAttempts":{{{maxAttempts}}},"Parameters":{}},
                    {"Name":"a2","Step":"s2","Qualifier":"N","Interaction":"push_command","TypeId":"{{{ByeType}}}","TimeoutSeconds":{{{timeoutSeconds}}},"MaxAttempts":{{{maxAttempts}}},"Parameters":{}}]}
        """;

    private static string OwnerOf(MqttApplicationMessage command) =>
        command.ResponseTopic.ShouldNotBeNull().EndsWith("/sfc-a", StringComparison.Ordinal) ? ReplicaA : ReplicaB;

    private static string Other(string replica) => replica == ReplicaA ? ReplicaB : ReplicaA;

    private static async Task EventuallyAsync(Func<Task<bool>> condition, string what)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        while (!await condition())
        {
            timeout.IsCancellationRequested.ShouldBeFalse($"Timed out waiting: {what}");
            await Task.Delay(50, CancellationToken.None);
        }
    }

    private async Task BothRunningAsync()
    {
        await _docker.StartAsync(ReplicaA, Ct);
        await _docker.StartAsync(ReplicaB, Ct);
    }

    private async Task StartJobAsync(string job, string recipe)
    {
        var workMaster = "wm-" + job;
        (await Jobs.SaveWorkMasterAsync(new JobWorkMaster { Id = workMaster, DataSchema = SfcInterpreter.RecipeDataSchema, Data = recipe }, Scope, Ct)).ShouldBeTrue();
        var order = new JobOrder { Id = job, WorkMasters = [new JobWorkMaster { Id = workMaster }] };
        (await Jobs.SaveOrderAsync(new JobOrderWrite(order, JobOrderState.AllowedToStartReady, Scope, "StoreAndStart", 0, "op-start-" + job, JobRecipeWork.Start), Ct)).ShouldBeTrue();
    }

    private async Task<SfcState> StateAsync(string job)
    {
        var document = (await States.GetAsync(job, Ct)).ShouldNotBeNull();
        return SfcJobRunner.Parse(document);
    }

    /// <summary>Next command of the job; commands of earlier tests that re-dispatch are skipped. A restart can take longer than one receive window.</summary>
    private async Task<MqttApplicationMessage> CommandOfAsync(string job, string action)
    {
        var key = $"{job}:{action}";
        for (var tries = 0; tries < 200; tries++)
        {
            MqttApplicationMessage command;
            try
            {
                command = await _equipment.ReceiveAsync(Ct);
            }
            catch (OperationCanceledException) when (!Ct.IsCancellationRequested)
            {
                continue;
            }

            if (MqttTestClient.Property(command, "mdcsactionkey") == key)
            {
                return command;
            }
        }

        throw new TimeoutException($"No command for {key}.");
    }

    private Task AnswerAsync(MqttApplicationMessage command) =>
        _equipment.PublishEventAsync(command.ResponseTopic, Guid.NewGuid().ToString(), HelloType, Scope, "{\"Name\":\"Plant\"}", null, Ct, causationId: MqttTestClient.Property(command, "id"));

    private Task CompletedAsync(string job) =>
        EventuallyAsync(async () => await Jobs.GetOrderAsync(job, Ct) is { State: JobOrderState.Completed }, $"{job} to complete");

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcReplicaTests), Skip = Skip)]
    public async Task KillingTheReplicaThatDispatchedLetsTheOtherFinishTheJob()
    {
        await BothRunningAsync();
        var job = NewJob("kill");
        await StartJobAsync(job, Recipe(timeoutSeconds: 3, maxAttempts: 3));

        await AnswerAsync(await CommandOfAsync(job, "a1"));
        var second = await CommandOfAsync(job, "a2");
        var owner = OwnerOf(second);
        await _docker.KillAsync(owner, Ct);

        // The dead replica will never see the answer (D-9); the survivor sends the command again once the deadline passes.
        var again = await CommandOfAsync(job, "a2");
        OwnerOf(again).ShouldBe(Other(owner));
        await AnswerAsync(again);

        await CompletedAsync(job);
        (await StateAsync(job)).Actions["a2"].Attempt.ShouldBe(2);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcReplicaTests), Skip = Skip)]
    public async Task AReplicaStartedLaterTakesOverWorkAndDeadlinesOfAKilledOne()
    {
        await _docker.KillAsync(ReplicaB, Ct);
        await _docker.StartAsync(ReplicaA, Ct);
        var job = NewJob("takeover");
        await StartJobAsync(job, Recipe(timeoutSeconds: 3, maxAttempts: 3));
        var first = await CommandOfAsync(job, "a1");
        OwnerOf(first).ShouldBe(ReplicaA);

        await _docker.KillAsync(ReplicaA, Ct);
        await _docker.StartAsync(ReplicaB, Ct);

        // B knows the scope only from the registry and finds the deadline in the due-index.
        var again = await CommandOfAsync(job, "a1");
        OwnerOf(again).ShouldBe(ReplicaB);
        await AnswerAsync(again);
        await AnswerAsync(await CommandOfAsync(job, "a2"));
        await CompletedAsync(job);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcReplicaTests), Skip = Skip)]
    public async Task AnOutageLongerThanADeadlineCostsNoAttempt()
    {
        await BothRunningAsync();
        var job = NewJob("outage");
        await StartJobAsync(job, Recipe(timeoutSeconds: 2, maxAttempts: 2));
        await CommandOfAsync(job, "a1");
        var before = (await StateAsync(job)).Actions["a1"];

        await _docker.KillAsync(ReplicaA, Ct);
        await _docker.KillAsync(ReplicaB, Ct);
        await Task.Delay(TimeSpan.FromSeconds(8), CancellationToken.None);
        await BothRunningAsync();

        // With MaxAttempts 2, a tick that treated the outage as an expired attempt would leave one try; a second would fault the job.
        await EventuallyAsync(async () => (await StateAsync(job)).Actions["a1"].Deadline > before.Deadline, "the deadline to move");
        (await StateAsync(job)).Actions["a1"].Attempt.ShouldBe(1);

        await AnswerAsync(await CommandOfAsync(job, "a1"));
        await AnswerAsync(await CommandOfAsync(job, "a2"));
        await CompletedAsync(job);
        (await StateAsync(job)).Status.ShouldBe(SfcJobStatus.Completed);
    }

    [Fact(SkipUnless = nameof(Configured), SkipType = typeof(SfcReplicaTests), Skip = Skip)]
    public async Task SweepLatencyIsMeasured()
    {
        await BothRunningAsync();
        const int Jobs_ = 20;
        var jobs = Enumerable.Range(0, Jobs_).Select(i => NewJob("sweep" + i.ToString(CultureInfo.InvariantCulture))).ToList();
        foreach (var job in jobs)
        {
            await StartJobAsync(job, Recipe(timeoutSeconds: 3, maxAttempts: 3));

            // Spread the deadlines over a sweep interval; started together they would be one sample of the sweep's phase.
            await Task.Delay(70, CancellationToken.None);
        }

        var deadlines = new Dictionary<string, long>(StringComparer.Ordinal);
        var latencies = new List<long>();
        while (latencies.Count < Jobs_)
        {
            var command = await _equipment.ReceiveAsync(Ct);
            var received = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var key = MqttTestClient.Property(command, "mdcsactionkey");
            var job = key[..key.IndexOf(':', StringComparison.Ordinal)];
            if (!jobs.Contains(job) || !key.EndsWith(":a1", StringComparison.Ordinal))
            {
                continue;
            }

            if (deadlines.TryGetValue(job, out var deadline))
            {
                latencies.Add(received - deadline);
                await AnswerAsync(command);
                continue;
            }

            deadlines[job] = (await StateAsync(job)).Actions["a1"].Deadline ?? 0;
        }

        latencies.Sort();
        var report = string.Create(CultureInfo.InvariantCulture, $"sweep latency over {latencies.Count} deadlines (default SweepInterval 1 s, two replicas): min {latencies[0]} ms, p50 {latencies[latencies.Count / 2]} ms, p95 {latencies[(int)(latencies.Count * 0.95)]} ms, max {latencies[^1]} ms");
        await File.WriteAllTextAsync("/results/sweep-latency.txt", report + "\n", CancellationToken.None);
        TestContext.Current.TestOutputHelper?.WriteLine(report);

        // A sweep each second plus dispatch: anything far beyond it means a replica did not sweep.
        latencies[^1].ShouldBeLessThan(5000);
    }
}
