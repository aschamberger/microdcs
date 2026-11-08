using System.Text.Json;
using static MicroDCS.Sfc.Tests.Rec;

namespace MicroDCS.Sfc.Tests;

/// <summary>
/// Step semantics. Scenarios ported from <c>test_sfc_engine.py</c> on python-archive (start, dispatch, completion, pull events,
/// resume) plus the rules of D-17, D-18 and D-44 that Python lacked (outcomes, deadlines, attempts, late responses).
/// </summary>
public class InterpreterTests
{
    private static SfcGraph TwoSteps(int timeout = 10, int? max = null) => Graph(Recipe(
        [Step("A", true), Step("B")],
        [To("A", "B")],
        [Push("a", "A", timeout, max), Push("b", "B")]));

    // ── Start (TestSfcEngineStartRecipe) ────────────────────────────────

    [Fact]
    public void Start_ActivatesTheInitialStepAndDispatchesItsPushActions()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Push("a1", "A", 5), Push("a2", "A", 7)])));

        SfcStepResult result = job.Start(at: 1000);

        result.Disposition.ShouldBe(SfcDisposition.Applied);
        job.State.Status.ShouldBe(SfcJobStatus.Running);
        job.State.ActiveSteps.ShouldBe(["A"]);
        job.State.Current.ShouldBe("A");
        StartWork[] commands = [.. result.Commands.Cast<StartWork>()];
        commands.Select(static c => c.ActionKey).ShouldBe(["job-1:a1", "job-1:a2"]);
        commands.Select(static c => c.Deadline).ShouldBe([6000L, 8000L]);
        commands.ShouldAllBe(static c => c.Attempt == 1 && c.Work == SfcWorkKind.PushCommand);
        result.Continuation.ShouldBeOfType<SfcDefer>();
        job.State.NextDeadline.ShouldBe(6000L);
    }

    [Fact]
    public void Start_IsIdempotent()
    {
        var job = new Job(TwoSteps());
        job.Start();
        SfcState before = job.State;

        SfcStepResult second = job.Start(at: 5);

        second.Disposition.ShouldBe(SfcDisposition.Absorbed);
        second.Commands.ShouldBeEmpty();
        job.State.ShouldBeSameAs(before);
    }

    [Fact]
    public void Evolve_WithAStatePinnedToAnotherRecipeIsRejected()
    {
        SfcGraph one = TwoSteps();
        SfcGraph other = Graph(Recipe([Step("X", true)], [], []));
        SfcState state = SfcState.Create(one, "job-1", "s");

        SfcStepResult result = Interpreter.Evolve(other, state, SfcSnapshot.Default, new SfcStart { At = 0 });

        result.Disposition.ShouldBe(SfcDisposition.Rejected);
    }

    [Fact]
    public void Start_OfARecipeWithoutActionsCompletesAtOnce()
    {
        var job = new Job(Graph(Recipe([Step("A", true), Step("B")], [To("A", "B")], [])));

        SfcStepResult result = job.Start();

        job.State.Status.ShouldBe(SfcJobStatus.Completed);
        result.Continuation.ShouldBeEquivalentTo(new SfcComplete { Outcome = "completed" });
    }

    // ── Dispatch and attempts (TestSfcEngineDispatchAction) ─────────────

    [Fact]
    public void Redispatch_KeepsTheActionKeyAndGivesANewCommandId()
    {
        var job = new Job(TwoSteps(timeout: 10));
        job.Start();
        string first = job.State.Actions["a"].CommandId!;

        SfcStepResult result = job.Tick(at: 10_000);

        CancelWork cancel = result.Commands.OfType<CancelWork>().Single();
        StartWork start = result.Commands.OfType<StartWork>().Single();
        cancel.CommandId.ShouldBe(first);
        cancel.Reason.ShouldBe("redispatch");
        start.ActionKey.ShouldBe("job-1:a");
        start.CommandId.ShouldNotBe(first);
        start.Attempt.ShouldBe(2);
        start.Deadline.ShouldBe(20_000L);
        job.State.Actions["a"].Attempt.ShouldBe(2);
    }

    [Fact]
    public void Tick_BeforeTheDeadlineChangesNothing()
    {
        var job = new Job(TwoSteps(timeout: 10));
        job.Start();

        job.Tick(at: 9_999).Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void Exhausted_Attempts_EndInTheReservedTimeoutOutcome()
    {
        var job = new Job(TwoSteps(timeout: 10, max: 2));
        job.Start();
        job.Tick(10_000);

        SfcStepResult result = job.Tick(20_000);

        job.State.Actions["a"].Status.ShouldBe(SfcActionStatus.Failed);
        job.State.Actions["a"].Outcome.ShouldBe("timeout");
        result.Continuation.ShouldBeEquivalentTo(new SfcFault { Code = "timeout", Message = result.Continuation is SfcFault f ? f.Message : string.Empty });
        job.State.Status.ShouldBe(SfcJobStatus.Faulted);
        job.State.NextDeadline.ShouldBeNull();
    }

    [Fact]
    public void TheSnapshotSuppliesTheDefaultAttemptLimit()
    {
        var job = new Job(TwoSteps(timeout: 10), new SfcSnapshot { DefaultMaxAttempts = 1 });
        job.Start();

        job.Tick(10_000).Continuation.ShouldBeOfType<SfcFault>();
    }

    [Fact]
    public void ATimeoutIsRoutableAndTheDefaultDoesNotTakeFailures()
    {
        SfcGraph graph = Graph(Recipe(
            [Step("A", true), Step("Recover"), Step("Next")],
            [To("A", "Recover", "timeout", 0), To("A", "Next", "true", 1)],
            [Push("a", "A", 10, 1), Push("r", "Recover"), Push("n", "Next")]));
        var job = new Job(graph);
        job.Start();

        job.Tick(10_000);

        job.State.ActiveSteps.ShouldBe(["Recover"]);
        job.State.Status.ShouldBe(SfcJobStatus.Running);
    }

    [Fact]
    public void AFailureWithOnlyADefaultTransitionIsAFaultNotAnAdvance()
    {
        var job = new Job(TwoSteps());
        job.Start();

        SfcStepResult result = job.Send(new SfcActionFailed { At = 1, ActionKey = Key("a"), Error = "gripper jam" });

        result.Continuation.ShouldBeOfType<SfcFault>().Code.ShouldBe("error");
        job.State.Actions["a"].Error.ShouldBe("gripper jam");
        job.State.FaultCode.ShouldBe("error");
    }

    [Fact]
    public void AnErrorOutcomeCanBeRouted()
    {
        SfcGraph graph = Graph(Recipe(
            [Step("A", true), Step("Repair")],
            [To("A", "Repair", "error")],
            [Push("a", "A"), Push("r", "Repair")]));
        var job = new Job(graph);
        job.Start();

        job.Send(new SfcActionFailed { At = 1, ActionKey = Key("a") });

        job.State.ActiveSteps.ShouldBe(["Repair"]);
    }

    [Fact]
    public void AnOutageExtendsOpenDeadlinesInsteadOfFailingThem()
    {
        var job = new Job(TwoSteps(timeout: 10));
        job.Start();

        SfcStepResult result = job.Tick(at: 500_000, outageMs: 495_000);

        result.Disposition.ShouldBe(SfcDisposition.Applied);
        job.State.Actions["a"].Deadline.ShouldBe(505_000L);
        job.State.Actions["a"].Attempt.ShouldBe(1);
        result.Commands.ShouldBeEmpty();
        job.Tick(505_000).Commands.OfType<StartWork>().Single().Attempt.ShouldBe(2);
    }

    // ── Completion (TestSfcEngineActionCompletion) ──────────────────────

    [Fact]
    public void Response_CompletesTheActionAndAdvances()
    {
        var job = new Job(TwoSteps());
        job.Start();

        SfcStepResult result = job.Done("a", at: 3);

        job.State.ActiveSteps.ShouldBe(["B"]);
        job.Started.ShouldBe(["b"]);
        job.State.Actions["a"].Status.ShouldBe(SfcActionStatus.Completed);
        result.Disposition.ShouldBe(SfcDisposition.Applied);
    }

    [Fact]
    public void TheLastStepCompletesTheJob()
    {
        var job = new Job(TwoSteps());
        job.Start();
        job.Done("a");

        SfcStepResult result = job.Done("b");

        result.Continuation.ShouldBeEquivalentTo(new SfcComplete { Outcome = "completed" });
        job.State.Status.ShouldBe(SfcJobStatus.Completed);
        job.State.ActiveSteps.ShouldBeEmpty();
        job.State.IsEnded.ShouldBeTrue();
    }

    [Fact]
    public void ADuplicateResponseIsAbsorbed()
    {
        var job = new Job(TwoSteps());
        job.Start();
        job.Done("a");
        SfcState before = job.State;

        SfcStepResult duplicate = job.Done("a");

        duplicate.Disposition.ShouldBe(SfcDisposition.Absorbed);
        job.State.ShouldBeSameAs(before);
    }

    [Theory]
    [InlineData("job-1:unknown")]
    [InlineData("job-2:a")]
    [InlineData("garbage")]
    public void AResponseForAnotherActionOrJobIsRejected(string actionKey)
    {
        var job = new Job(TwoSteps());
        job.Start();

        SfcStepResult result = job.Send(new SfcActionCompleted { At = 1, ActionKey = actionKey, Outcome = "ok" });

        result.Disposition.ShouldBe(SfcDisposition.Rejected);
        job.State.Actions["a"].Status.ShouldBe(SfcActionStatus.Dispatched);
    }

    [Fact]
    public void AResponseWhoseTypeContradictsTheOpenActionIsRejectedNotMatchedToASibling()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Push("a1", "A"), Push("a2", "A")])));
        job.Start();

        SfcStepResult result = job.Send(new SfcActionCompleted { At = 1, ActionKey = Key("a1"), Outcome = "ok", TypeId = "t.a2" });

        result.Disposition.ShouldBe(SfcDisposition.Rejected);
        job.Status("a1").ShouldBe(SfcActionStatus.Dispatched);
        job.Status("a2").ShouldBe(SfcActionStatus.Dispatched);
    }

    [Fact]
    public void AResponseForAnActionThatWasNeverDispatchedIsRejected()
    {
        var job = new Job(TwoSteps());
        job.Start();

        job.Done("b").Disposition.ShouldBe(SfcDisposition.Rejected);
    }

    [Fact]
    public void AStepWaitsForAllItsActions()
    {
        var job = new Job(Graph(Recipe([Step("A", true), Step("B")], [To("A", "B")], [Push("a1", "A"), Push("a2", "A"), Push("b", "B")])));
        job.Start();

        job.Done("a1");
        job.State.ActiveSteps.ShouldBe(["A"]);
        job.Done("a2");

        job.State.ActiveSteps.ShouldBe(["B"]);
    }

    [Fact]
    public void AStepWithoutActionsIsPassedThrough()
    {
        var job = new Job(Graph(Recipe(
            [Step("A", true), Step("Gate"), Step("B")],
            [To("A", "Gate"), To("Gate", "B")],
            [Push("a", "A"), Push("b", "B")])));
        job.Start();

        job.Done("a");

        job.State.ActiveSteps.ShouldBe(["B"]);
        job.Started.ShouldBe(["b"]);
    }

    [Fact]
    public void TheOutcomeSelectsTheTransitionByPriority()
    {
        SfcGraph graph = Graph(
            Recipe(
                [Step("A", true), Step("Good"), Step("Other")],
                [To("A", "Other", "true", 5), To("A", "Good", "good", 1)],
                [Push("a", "A"), Push("g", "Good"), Push("o", "Other")]),
            "good");

        var matching = new Job(graph);
        matching.Start();
        matching.Done("a", "good");
        var fallback = new Job(graph);
        fallback.Start();
        fallback.Done("a", "ok");

        matching.State.ActiveSteps.ShouldBe(["Good"]);
        fallback.State.ActiveSteps.ShouldBe(["Other"]);
    }

    [Fact]
    public void AnOutcomeNoTransitionMatchesIsAFault()
    {
        var job = new Job(Graph(
            Recipe([Step("A", true), Step("B")], [To("A", "B", "good")], [Push("a", "A")]),
            "good"));
        job.Start();

        SfcStepResult result = job.Done("a", "ok");

        result.Continuation.ShouldBeOfType<SfcFault>().Code.ShouldBe("no_transition");
    }

    [Fact]
    public void AnEarlierAttemptsResponseStillCompletesAnOpenAction()
    {
        var job = new Job(TwoSteps(timeout: 10));
        job.Start();
        job.Tick(10_000);

        SfcStepResult late = job.Done("a", at: 10_500);

        late.Disposition.ShouldBe(SfcDisposition.Applied);
        job.State.Actions["a"].Status.ShouldBe(SfcActionStatus.Completed);
        job.Done("a").Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void LateResponsesForEndedJobsAreAbsorbed()
    {
        var faulted = new Job(TwoSteps(max: 1));
        faulted.Start();
        faulted.Tick(10_000);
        var cancelled = new Job(TwoSteps());
        cancelled.Start();
        cancelled.Send(new SfcCancel { At = 1 });

        faulted.Done("a").Disposition.ShouldBe(SfcDisposition.Absorbed);
        cancelled.Done("a").Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void Cancel_TearsDownOpenWorkAndEndsTheJob()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Push("a1", "A"), Pull("p", "A")])));
        job.Start();

        SfcStepResult result = job.Send(new SfcCancel { At = 5, Reason = "operator" });

        result.Continuation.ShouldBeOfType<SfcCancelled>();
        result.Commands.OfType<CancelWork>().Select(static c => c.ActionKey).ShouldBe(["job-1:a1", "job-1:p"], ignoreOrder: true);
        result.Commands.OfType<CancelWork>().ShouldAllBe(static c => c.Reason == "operator");
        job.State.Status.ShouldBe(SfcJobStatus.Cancelled);
        job.Send(new SfcCancel { At = 6 }).Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    // ── Resume (TestSfcEngineRecovery, TestBranchResume) ────────────────

    [Fact]
    public void Resume_RedispatchesDispatchedWorkThatIsNotLive()
    {
        var job = new Job(TwoSteps());
        job.Start();

        SfcStepResult result = job.Send(new SfcResume { At = 100 });

        result.Commands.OfType<StartWork>().Single().Attempt.ShouldBe(2);
        result.Commands.OfType<CancelWork>().Single().Reason.ShouldBe("redispatch");
    }

    [Fact]
    public void Resume_LeavesLiveWorkAlone()
    {
        var job = new Job(TwoSteps(), new SfcSnapshot { LiveWork = ["job-1:a"] });
        job.Start();

        job.Send(new SfcResume { At = 100 }).Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void Resume_UsesUpAttempts()
    {
        var job = new Job(TwoSteps(max: 1));
        job.Start();

        job.Send(new SfcResume { At = 100 }).Continuation.ShouldBeOfType<SfcFault>().Code.ShouldBe("timeout");
    }

    [Fact]
    public void Resume_AnnouncesAWaitAgainWithoutChangingIt()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Pull("p", "A", 20)])));
        job.Start(at: 1000);
        StartWork original = job.Last.Commands.OfType<StartWork>().Single();

        StartWork again = job.Send(new SfcResume { At = 5000 }).Commands.OfType<StartWork>().Single();

        again.CommandId.ShouldBe(original.CommandId);
        again.Deadline.ShouldBe(original.Deadline);
        again.Work.ShouldBe(SfcWorkKind.AwaitEvent);
    }

    [Fact]
    public void Resume_RedispatchesEveryActiveBranchStep()
    {
        var job = new Job(Graph(Recipe(
            [Step("A", true), Step("P"), Step("Q")],
            [To("A", "S")],
            [Push("a", "A"), Push("p", "P"), Push("q", "Q")],
            Sim("S", ["P"], ["Q"]))));
        job.Start();
        job.Done("a");

        SfcStepResult result = job.Send(new SfcResume { At = 100 });

        result.Commands.OfType<StartWork>().Select(static c => c.Action).ShouldBe(["p", "q"], ignoreOrder: true);
    }

    // ── Pull events (TestSfcEnginePullCompletion) ───────────────────────

    [Fact]
    public void PullEvent_WaitsInStateAndAnEventCompletesIt()
    {
        var job = new Job(Graph(Recipe([Step("A", true), Step("B")], [To("A", "B")], [Pull("qa", "A", 45, "t.qa"), Push("b", "B")])));

        StartWork wait = job.Start(at: 100).Commands.OfType<StartWork>().Single();
        wait.Work.ShouldBe(SfcWorkKind.AwaitEvent);
        wait.Deadline.ShouldBe(45_100L);
        job.State.IsWaitingFor("t.qa").ShouldBeTrue();
        job.State.IsWaitingFor("t.other").ShouldBeFalse();

        SfcStepResult result = job.Send(new SfcEventReceived { At = 200, TypeId = "t.qa", Outcome = "ok" });

        result.Disposition.ShouldBe(SfcDisposition.Applied);
        job.State.ActiveSteps.ShouldBe(["B"]);
        job.State.IsWaitingFor("t.qa").ShouldBeFalse();
    }

    [Fact]
    public void PullEvent_OfAnUnknownTypeIsAbsorbed()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Pull("qa", "A", type: "t.qa")])));
        job.Start();

        job.Send(new SfcEventReceived { At = 1, TypeId = "t.unknown", Outcome = "ok" }).Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void PullEvent_TimesOutWithoutRedispatch()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Pull("qa", "A", 5)])));
        job.Start();

        SfcStepResult result = job.Tick(5_000);

        result.Commands.OfType<StartWork>().ShouldBeEmpty();
        result.Commands.OfType<CancelWork>().Single().Reason.ShouldBe("timeout");
        result.Continuation.ShouldBeOfType<SfcFault>().Code.ShouldBe("timeout");
    }

    // ── Simultaneous divergences (TestSimultaneousBranch) ──────────────────

    private static SfcGraph Parallel() => Graph(Recipe(
        [Step("A", true), Step("P1"), Step("P2"), Step("Q"), Step("Z")],
        [To("A", "S"), To("S", "Z")],
        [Push("a", "A"), Push("p1", "P1"), Push("p2", "P2"), Push("q", "Q"), Push("z", "Z")],
        Sim("S", ["P1", "P2"], ["Q"])));

    [Fact]
    public void Simultaneous_EntersEveryPathAtOnce()
    {
        var job = new Job(Parallel());
        job.Start();

        job.Done("a");

        job.State.Current.ShouldBe("S");
        job.State.ActiveSteps.ShouldBe(["P1", "Q"], ignoreOrder: true);
        job.Started.ShouldBe(["p1", "q"], ignoreOrder: true);
    }

    [Fact]
    public void Simultaneous_AdvancesInsideAPathAndConvergesWhenAllPathsAreDone()
    {
        var job = new Job(Parallel());
        job.Start();
        job.Done("a");

        job.Done("p1");
        job.State.ActiveSteps.ShouldBe(["Q", "P2"], ignoreOrder: true);
        job.Done("q");
        job.State.ActiveSteps.ShouldBe(["P2"]);
        job.Done("p2");

        job.State.ActiveSteps.ShouldBe(["Z"]);
        job.State.Current.ShouldBe("Z");
        job.Started.ShouldBe(["z"]);
    }

    [Fact]
    public void Simultaneous_WithoutAnExitTransitionCompletesTheJob()
    {
        var job = new Job(Graph(Recipe(
            [Step("A", true), Step("P"), Step("Q")],
            [To("A", "S")],
            [Push("a", "A"), Push("p", "P"), Push("q", "Q")],
            Sim("S", ["P"], ["Q"]))));
        job.Start();
        job.Done("a");
        job.Done("p");

        job.Done("q").Continuation.ShouldBeOfType<SfcComplete>();
    }

    [Fact]
    public void Simultaneous_AFailedPathFaultsTheJobAndCancelsTheOtherPaths()
    {
        var job = new Job(Parallel());
        job.Start();
        job.Done("a");

        SfcStepResult result = job.Send(new SfcActionFailed { At = 1, ActionKey = Key("p1") });

        result.Continuation.ShouldBeOfType<SfcFault>().Code.ShouldBe("error");
        result.Commands.OfType<CancelWork>().Single().ActionKey.ShouldBe("job-1:q");
        result.Commands.OfType<CancelWork>().Single().Reason.ShouldBe("teardown");
    }

    // ── Selection divergences (TestSelectionBranch) ────────────────────────

    private static SfcGraph Selection() => Graph(
        Recipe(
            [Step("A", true), Step("Rework"), Step("Pack"), Step("Z")],
            [To("A", "Q"), To("Q", "Rework", "bad", 0), To("Q", "Pack", "true", 1), To("Q", "Z")],
            [Push("a", "A"), Push("rw", "Rework"), Push("pk", "Pack"), Push("z", "Z")],
            Sel("Q", ["Rework"], ["Pack"])),
        "bad");

    [Fact]
    public void Selection_TakesTheFirstMatchingEntryByPriority()
    {
        var bad = new Job(Selection());
        bad.Start();
        bad.Done("a", "bad");
        var good = new Job(Selection());
        good.Start();
        good.Done("a", "ok");

        bad.State.ActiveSteps.ShouldBe(["Rework"]);
        good.State.ActiveSteps.ShouldBe(["Pack"]);
        bad.State.Current.ShouldBe("Q");
    }

    [Fact]
    public void Selection_ConvergesThroughItsExitAfterTheChosenPath()
    {
        var job = new Job(Selection());
        job.Start();
        job.Done("a", "ok");

        job.Done("pk");

        job.State.ActiveSteps.ShouldBe(["Z"]);
    }

    // ── Limits, determinism, persistence ────────────────────────────────

    [Fact]
    public void MoreThanTenThousandStepEntriesInOneEvolutionAreAFault()
    {
        const int Count = SfcInterpreter.MaxStepEntriesPerEvolution + 50;
        SfcStep[] steps = [.. Enumerable.Range(0, Count).Select(i => Step($"S{i}", i == 0))];
        SfcTransition[] transitions = [.. Enumerable.Range(0, Count - 1).Select(i => To($"S{i}", $"S{i + 1}"))];
        var job = new Job(Graph(Recipe(steps, transitions, [])));

        SfcStepResult result = job.Start();

        result.Continuation.ShouldBeOfType<SfcFault>().Code.ShouldBe("propagation_limit");
        job.State.Status.ShouldBe(SfcJobStatus.Faulted);
    }

    [Fact]
    public void ReplayingTheSameEventsGivesByteIdenticalState()
    {
        static string Run(SfcGraph graph)
        {
            var job = new Job(graph);
            job.Start(10);
            job.Tick(10_010);
            job.Done("a", at: 10_020);
            return JsonSerializer.Serialize(job.State, SfcJsonContext.Default.SfcState);
        }

        SfcGraph graph = TwoSteps();

        Run(graph).ShouldBe(Run(graph));
    }

    [Fact]
    public void AStateSurvivesAJsonRoundTripAndGoesOnIdentically()
    {
        SfcGraph graph = Parallel();
        var job = new Job(graph);
        job.Start();
        job.Done("a");
        string json = JsonSerializer.Serialize(job.State, SfcJsonContext.Default.SfcState);
        SfcState restored = JsonSerializer.Deserialize(json, SfcJsonContext.Default.SfcState)!;

        SfcStepResult direct = Interpreter.Evolve(graph, job.State, SfcSnapshot.Default, new SfcActionCompleted { At = 9, ActionKey = Key("q"), Outcome = "ok" });
        SfcStepResult viaJson = Interpreter.Evolve(graph, restored, SfcSnapshot.Default, new SfcActionCompleted { At = 9, ActionKey = Key("q"), Outcome = "ok" });

        JsonSerializer.Serialize(viaJson, SfcJsonContext.Default.SfcStepResult).ShouldBe(JsonSerializer.Serialize(direct, SfcJsonContext.Default.SfcStepResult));
        restored.Format.ShouldBe(SfcState.CurrentFormat);
    }

    [Fact]
    public void CommandIdsAreAPureFunctionOfTheState()
    {
        var job = new Job(TwoSteps());
        job.Start();

        job.State.Actions["a"].CommandId.ShouldBe("job-1:a#1");
        job.Done("a");
        job.State.Actions["b"].CommandId.ShouldBe("job-1:b#2");
        job.State.Sequence.ShouldBe(2L);
    }
}
