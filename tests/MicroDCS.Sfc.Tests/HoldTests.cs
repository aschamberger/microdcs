using System.Text.Json;
using static MicroDCS.Sfc.Tests.Rec;

namespace MicroDCS.Sfc.Tests;

/// <summary>
/// Hold and release (D-52, S20a): MES Pause holds the recipe at the next step boundary, Resume releases it. Python never held a recipe, so these rules
/// are new and have no Python test to port.
/// </summary>
public class HoldTests
{
    private static SfcGraph TwoSteps(int timeout = 10, int? max = null) => Graph(Recipe(
        [Step("A", true), Step("B")],
        [To("A", "B")],
        [Push("a", "A", timeout, max), Push("b", "B")]));

    private static SfcStepResult Hold(Job job, long at) => job.Send(new SfcHold { At = at });

    private static SfcStepResult Release(Job job, long at) => job.Send(new SfcRelease { At = at });

    [Fact]
    public void Hold_MarksTheJobHeldAndTakesItOffTheDueIndex()
    {
        var job = new Job(TwoSteps());
        job.Start();

        SfcStepResult result = Hold(job, 2_000);

        result.Disposition.ShouldBe(SfcDisposition.Applied);
        result.Commands.ShouldBeEmpty();
        result.Continuation.ShouldBeOfType<SfcDefer>();
        job.State.HeldAt.ShouldBe(2_000L);
        job.State.IsHeld.ShouldBeTrue();
        job.State.NextDeadline.ShouldBeNull();
        job.State.Actions["a"].Deadline.ShouldBe(10_000L);
    }

    [Fact]
    public void ARepeatedHoldOrAReleaseOfAJobThatIsNotHeldIsAbsorbed()
    {
        var job = new Job(TwoSteps());
        job.Start();

        Release(job, 1_000).Disposition.ShouldBe(SfcDisposition.Absorbed);
        Hold(job, 2_000);
        SfcState held = job.State;
        Hold(job, 3_000).Disposition.ShouldBe(SfcDisposition.Absorbed);
        job.State.ShouldBeSameAs(held);
        job.State.HeldAt.ShouldBe(2_000L);
        Release(job, 4_000);
        Release(job, 5_000).Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void HoldAndReleaseOfAJobThatIsNotRunningAreAbsorbed()
    {
        var notStarted = new Job(TwoSteps());
        var ended = new Job(TwoSteps());
        ended.Start();
        ended.Send(new SfcCancel { At = 1 });

        Hold(notStarted, 1).Disposition.ShouldBe(SfcDisposition.Absorbed);
        Hold(ended, 2).Disposition.ShouldBe(SfcDisposition.Absorbed);
        Release(ended, 3).Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void WhileHeld_AResponseIsRecordedButNoTransitionFires()
    {
        var job = new Job(TwoSteps());
        job.Start();
        Hold(job, 1_000);

        SfcStepResult result = job.Done("a", at: 2_000);

        result.Disposition.ShouldBe(SfcDisposition.Applied);
        result.Commands.ShouldBeEmpty();
        job.Status("a").ShouldBe(SfcActionStatus.Completed);
        job.State.ActiveSteps.ShouldBe(["A"]);
        job.Status("b").ShouldBe(SfcActionStatus.Pending);
        job.Done("a", at: 2_500).Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void WhileHeld_AnEventForAWaitThatBeganBeforeIsRecorded()
    {
        var job = new Job(Graph(Recipe([Step("A", true), Step("B")], [To("A", "B")], [Pull("p", "A"), Push("b", "B")])));
        job.Start();
        Hold(job, 1_000);

        SfcStepResult result = job.Send(new SfcEventReceived { At = 2_000, TypeId = "t.p", Outcome = "ok" });

        result.Disposition.ShouldBe(SfcDisposition.Applied);
        job.Status("p").ShouldBe(SfcActionStatus.Completed);
        job.State.ActiveSteps.ShouldBe(["A"]);
        result.Commands.ShouldBeEmpty();

        SfcStepResult released = Release(job, 3_000);

        released.Commands.OfType<StartWork>().Select(static c => c.Action).ShouldBe(["b"]);
        job.State.ActiveSteps.ShouldBe(["B"]);
    }

    [Fact]
    public void WhileHeld_DeadlinesDoNotExpireAndNothingIsResent()
    {
        var job = new Job(TwoSteps(timeout: 10));
        job.Start();
        Hold(job, 1_000);

        SfcStepResult tick = job.Tick(60_000);
        SfcStepResult outage = job.Tick(61_000, outageMs: 50_000);
        SfcStepResult resume = job.Send(new SfcResume { At = 62_000 });

        tick.Disposition.ShouldBe(SfcDisposition.Absorbed);
        outage.Disposition.ShouldBe(SfcDisposition.Absorbed);
        resume.Disposition.ShouldBe(SfcDisposition.Absorbed);
        job.State.Actions["a"].Attempt.ShouldBe(1);
        job.State.Actions["a"].Deadline.ShouldBe(10_000L);
    }

    [Fact]
    public void Release_MovesOpenDeadlinesByTheLengthOfTheHold()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Push("a", "A", 10), Pull("p", "A", 20)])));
        job.Start();
        Hold(job, 4_000);

        Release(job, 9_000).Disposition.ShouldBe(SfcDisposition.Applied);

        job.State.HeldAt.ShouldBeNull();
        job.State.Actions["a"].Deadline.ShouldBe(15_000L);
        job.State.Actions["p"].Deadline.ShouldBe(25_000L);
        job.State.NextDeadline.ShouldBe(15_000L);
        job.Tick(14_999).Disposition.ShouldBe(SfcDisposition.Absorbed);
        job.Tick(15_000).Commands.OfType<StartWork>().Single().Attempt.ShouldBe(2);
    }

    [Fact]
    public void Release_IssuesOpenPushAttemptsAgainWithTheirCommandIdAndNoNewAttempt()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Push("a", "A", 10), Pull("p", "A")])));
        StartWork first = job.Start().Commands.OfType<StartWork>().First(static c => c.Action == "a");
        Hold(job, 1_000);

        SfcStepResult released = Release(job, 3_000);

        StartWork again = released.Commands.OfType<StartWork>().Single();
        again.Action.ShouldBe("a");
        again.CommandId.ShouldBe(first.CommandId);
        again.Attempt.ShouldBe(1);
        again.Round.ShouldBe(first.Round);
        again.Deadline.ShouldBe(12_000L);
        again.Work.ShouldBe(SfcWorkKind.PushCommand);
        released.Commands.OfType<CancelWork>().ShouldBeEmpty();
        job.State.Actions["a"].Attempt.ShouldBe(1);
        job.State.Sequence.ShouldBe(2);
    }

    [Fact]
    public void Release_FiresTheTransitionsThatBecamePossible()
    {
        var job = new Job(TwoSteps());
        job.Start();
        Hold(job, 1_000);
        job.Done("a", at: 2_000);

        SfcStepResult released = Release(job, 5_000);

        job.State.ActiveSteps.ShouldBe(["B"]);
        StartWork b = released.Commands.OfType<StartWork>().Single();
        b.Action.ShouldBe("b");
        b.Deadline.ShouldBe(35_000L);
        job.Done("b", at: 6_000).Continuation.ShouldBeOfType<SfcComplete>();
    }

    [Fact]
    public void Release_RoutesAFailureRecordedDuringTheHold()
    {
        var job = new Job(TwoSteps());
        job.Start();
        Hold(job, 1_000);
        job.Send(new SfcActionFailed { At = 2_000, ActionKey = Key("a"), Error = "jammed" });
        job.State.Status.ShouldBe(SfcJobStatus.Running);

        SfcStepResult released = Release(job, 3_000);

        released.Continuation.ShouldBeOfType<SfcFault>().Code.ShouldBe(SfcOutcome.Error);
        job.State.Status.ShouldBe(SfcJobStatus.Faulted);
        job.State.HeldAt.ShouldBeNull();
    }

    [Fact]
    public void CancelOfAHeldJobEndsTheRecipe()
    {
        var job = new Job(TwoSteps());
        job.Start();
        Hold(job, 1_000);

        SfcStepResult result = job.Send(new SfcCancel { At = 2_000, Reason = "job ended" });

        result.Continuation.ShouldBeOfType<SfcCancelled>();
        result.Commands.OfType<CancelWork>().Single().ActionKey.ShouldBe(Key("a"));
        job.State.Status.ShouldBe(SfcJobStatus.Cancelled);
        job.State.HeldAt.ShouldBeNull();
        job.State.IsHeld.ShouldBeFalse();
    }

    [Fact]
    public void HeldStateRoundTripsAndOlderStatesReadAsNotHeld()
    {
        var job = new Job(TwoSteps());
        job.Start();
        Hold(job, 1_000);

        string json = JsonSerializer.Serialize(job.State, SfcJsonContext.Default.SfcState);
        SfcState back = JsonSerializer.Deserialize(json, SfcJsonContext.Default.SfcState)!;
        var node = System.Text.Json.Nodes.JsonNode.Parse(json)!.AsObject();
        node.Remove("heldAt").ShouldBeTrue();
        string older = node.ToJsonString();

        json.ShouldContain("\"heldAt\":1000");
        json.ShouldContain("\"formatMinor\":3");
        back.IsHeld.ShouldBeTrue();
        JsonSerializer.Deserialize(older, SfcJsonContext.Default.SfcState)!.IsHeld.ShouldBeFalse();
    }

    [Fact]
    public void HoldAndReleaseReplayToByteIdenticalStates()
    {
        SfcGraph graph = TwoSteps();
        string Run()
        {
            var job = new Job(graph);
            job.Start();
            Hold(job, 1_000);
            job.Done("a", at: 1_500);
            Release(job, 2_000);
            return JsonSerializer.Serialize(job.State, SfcJsonContext.Default.SfcState);
        }

        Run().ShouldBe(Run());
    }

    [Fact]
    public void TheExplainerSaysThatAJobIsHeld()
    {
        SfcGraph graph = TwoSteps();
        var job = new Job(graph);
        job.Start();
        Hold(job, 1_000);

        string text = MicroDCS.Sfc.Simulation.SfcExplainer.Describe(graph, job.State);

        text.ShouldContain("held since 1000");
        text.ShouldNotContain("next deadline");
    }

    [Fact]
    public void AStateOfAnOlderMinorFormatIsWrittenWithTheCurrentOne()
    {
        SfcGraph graph = TwoSteps();
        var job = new Job(graph);
        job.Start();
        SfcState older = job.State with { FormatMinor = 1 };

        SfcStepResult held = Interpreter.Evolve(graph, older, SfcSnapshot.Default, new SfcHold { At = 1_000 });

        held.State.FormatMinor.ShouldBe(SfcState.CurrentFormatMinor);
    }
}
