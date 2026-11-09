using System.Collections.Immutable;
using System.Text.Json;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.Valkey.Sfc;

namespace MicroDCS.Sfc.Hosting.Tests;

public sealed class SfcWorkTests
{
    private const string Recipe = """
        {"Steps":[{"Name":"s1","Initial":true},{"Name":"s2"}],
         "Transitions":[{"Source":"s1","Target":"s2","Condition":"true","Priority":0}],
         "Actions":[{"Name":"a1","Step":"s1","Qualifier":"N","Interaction":"push_command","TypeId":"t.v1","TimeoutSeconds":5,"Parameters":{}},
                    {"Name":"a2","Step":"s2","Qualifier":"N","Interaction":"push_command","TypeId":"t.v1","TimeoutSeconds":5,"Parameters":{}}]}
        """;

    private static readonly IReadOnlyDictionary<string, IReadOnlySet<string>> _outcomes = new Dictionary<string, IReadOnlySet<string>>
    {
        ["t.v1"] = new HashSet<string> { "done" },
    };

    private static SfcGraph Graph(SfcFeature features = SfcFeature.PushCommand) =>
        SfcGraph.Build(System.Text.Encoding.UTF8.GetBytes(Recipe), new SfcCapabilities(features, _outcomes)).Graph.ShouldNotBeNull();

    private static SfcStepResult Start(SfcGraph graph) =>
        new SfcInterpreter().Evolve(graph, SfcState.Create(graph, "job", "site.area"), SfcSnapshot.Default, new SfcStart { At = 1000 });

    private static string Field(SfcWorkEntry entry, string name) => entry.Fields.First(f => f.Key == name).Value;

    [Theory]
    [InlineData("1234-0", 1234)]
    [InlineData("1234-7", 1234)]
    [InlineData("99", 99)]
    public void TimeOfTakesTheMillisecondsOfTheEntryId(string id, long expected) => SfcWork.TimeOf(id).ShouldBe(expected);

    [Fact]
    public void TheFirstStepQueuesBeginExecutionBeforeItsDispatch()
    {
        var entries = SfcWork.EntriesOf(Start(Graph()), started: true);

        entries.Count.ShouldBe(2);
        Field(entries[0], SfcWork.ActionField).ShouldBe(SfcWork.Job);
        Field(entries[0], SfcWork.OperationsField).ShouldBe(nameof(JobOrderOperation.BeginExecution));
        Field(entries[1], SfcWork.ActionField).ShouldBe(SfcWork.DispatchPrefix + "a1");
        var command = JsonSerializer.Deserialize(Field(entries[1], SfcWork.CommandField), SfcJsonContext.Default.SfcCommand).ShouldBeOfType<StartWork>();
        command.ActionKey.ShouldBe("job:a1");
        command.Deadline.ShouldBe(6000);
    }

    [Fact]
    public void AnEndedRecipeAsksForTheJobTransition()
    {
        var state = Start(Graph()).State;

        SfcWork.JobOperations(new SfcComplete { Outcome = "completed" }, started: false).ShouldBe([JobOrderOperation.Complete]);
        SfcWork.JobOperations(new SfcFault { Code = "timeout", Message = "m" }, started: false).ShouldBe([JobOrderOperation.Abort]);
        SfcWork.JobOperations(new SfcCancelled(), started: false).ShouldBe([JobOrderOperation.Abort]);
        SfcWork.JobOperations(new SfcDefer(), started: false).ShouldBeEmpty();
        SfcWork.JobOperations(new SfcComplete { Outcome = "completed" }, started: true).ShouldBe([JobOrderOperation.BeginExecution, JobOrderOperation.Complete]);

        var fault = SfcWork.EntriesOf(new SfcStepResult { State = state, Continuation = new SfcFault { Code = "timeout", Message = "m" } }, started: false);
        Field(fault.ShouldHaveSingleItem(), SfcWork.ReasonField).ShouldBe("timeout");
    }

    [Fact]
    public void WaitsAndWithdrawnAttemptsBecomeNoEntries()
    {
        var state = Start(Graph()).State;
        var result = new SfcStepResult
        {
            State = state,
            Continuation = new SfcDefer(),
            Commands =
            [
                new StartWork { ActionKey = "job:w", CommandId = "c1", Work = SfcWorkKind.AwaitEvent, Action = "w", TypeId = "e.v1", Round = 1, Attempt = 1, Deadline = 9 },
                new CancelWork { ActionKey = "job:a1", CommandId = "c2", Reason = "redispatch" },
            ],
        };

        SfcWork.EntriesOf(result, started: false).ShouldBeEmpty();
    }

    [Fact]
    public void PendingEntriesOutliveTheirDeadlineByAMarginThenGo()
    {
        var pending = new SfcPendingCommands();
        pending.Add("c1", new PendingCommand("job", "job:a", 1, 1000));

        pending.Purge(1000 + 60_000);
        pending.Count.ShouldBe(1);
        pending.Purge(1000 + 60_001);
        pending.Count.ShouldBe(0);
    }

    [Fact]
    public void CapabilitiesComeFromTheOutgoingFactoriesAndHoldTheDefaultOutcome()
    {
        var capabilities = SfcGraphs.CapabilitiesOf(Registry(ImmutableArray.Create("good", "bad")));

        capabilities.Outcomes["t.v1"].ShouldBe(["good", "bad", ActionOutcome.DefaultName], ignoreOrder: true);
        capabilities.IsOutcomeValid("t.v1", "good").ShouldBeTrue();
        capabilities.IsOutcomeValid("t.v1", "other").ShouldBeFalse();
    }

    [Fact]
    public void PullEventsAreOffered() =>
        SfcGraphs.CapabilitiesOf(Registry(default)).Features.HasFlag(SfcFeature.PullEvent).ShouldBeTrue();

    [Theory]
    [InlineData(nameof(SfcHostOptions.MaxAttempts))]
    [InlineData(nameof(SfcHostOptions.MaxDeliveries))]
    [InlineData(nameof(SfcHostOptions.ReadCount))]
    [InlineData(nameof(SfcHostOptions.SweepLimit))]
    public void CountsMustBePositive(string property)
    {
        var options = new SfcHostOptions();
        new SfcHostOptions().IsValid().ShouldBeTrue();
        switch (property)
        {
            case nameof(SfcHostOptions.MaxAttempts): options.MaxAttempts = 0; break;
            case nameof(SfcHostOptions.MaxDeliveries): options.MaxDeliveries = 0; break;
            case nameof(SfcHostOptions.ReadCount): options.ReadCount = 0; break;
            default: options.SweepLimit = 0; break;
        }

        options.IsValid().ShouldBeFalse();
    }

    [Fact]
    public void AnOutageThresholdMustExceedTheSweepInterval() =>
        new SfcHostOptions { SweepInterval = TimeSpan.FromSeconds(30), OutageThreshold = TimeSpan.FromSeconds(10) }.IsValid().ShouldBeFalse();

    private static ProcessorRegistry Registry(ImmutableArray<string> outcomes)
    {
        var factory = new HandlerDescriptor(HandlerKind.Outgoing, "t.v1", null, static (_, _, _, _) => new ValueTask<IReadOnlyList<Reply>>([]), outcomes);
        var descriptor = new ProcessorDescriptor(typeof(SfcWorkTests), "station", ProcessorBinding.Southbound, [], [], [factory], new LifecycleDescriptor(null, null, false, null));
        return new ProcessorRegistry([descriptor], []);
    }
}
