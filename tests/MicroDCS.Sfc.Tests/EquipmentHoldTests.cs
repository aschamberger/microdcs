using System.Text.Json;
using static MicroDCS.Sfc.Tests.Rec;

namespace MicroDCS.Sfc.Tests;

/// <summary>
/// Equipment hold (D-57, S27): on a pause the running work of a push command with hold logic is told to hold, and on release it is restarted. Python never
/// held equipment, so these rules are new and have no Python test to port.
/// </summary>
public class EquipmentHoldTests
{
    private const string HoldType = "t.hold";
    private const string RestartType = "t.restart";

    private static SfcEquipmentCommand HoldCmd(int timeout = 5) => new() { TypeId = HoldType, TimeoutSeconds = timeout };

    private static SfcEquipmentCommand RestartCmd(int timeout = 20) => new() { TypeId = RestartType, TimeoutSeconds = timeout };

    private static SfcActionBlock Holdable(string name, string step, int timeout = 10) =>
        Push(name, step, timeout) with { Hold = HoldCmd(), Restart = RestartCmd() };

    private static SfcCapabilities HoldCaps(SfcRecipe recipe, SfcFeature extra = SfcFeature.EquipmentHold, string[]? holdOutcomes = null)
    {
        var outcomes = recipe.Actions.Select(static a => a.TypeId).Distinct()
            .ToDictionary(static t => t, _ => (IReadOnlySet<string>)new HashSet<string>(["ok"]), StringComparer.Ordinal);
        outcomes[HoldType] = new HashSet<string>(holdOutcomes ?? [SfcOutcome.Held, "not_held"]);
        outcomes[RestartType] = new HashSet<string>(["ok", "restarted_ok"]);
        return new SfcCapabilities(SfcFeature.PushCommand | SfcFeature.PullEvent | extra, outcomes);
    }

    private static SfcBuildResult BuildWithHold(SfcRecipe recipe, SfcFeature extra = SfcFeature.EquipmentHold, string[]? holdOutcomes = null) =>
        SfcGraph.Build(recipe, "hash-" + Guid.NewGuid().ToString("N"), HoldCaps(recipe, extra, holdOutcomes));

    private static SfcGraph TwoSteps(int timeout = 10)
    {
        SfcBuildResult result = BuildWithHold(Recipe([Step("A", true), Step("B")], [To("A", "B")], [Holdable("a", "A", timeout), Push("b", "B")]));
        result.Succeeded.ShouldBeTrue(string.Join("; ", result.Errors.Select(static e => $"{e.Code} {e.Message}")));
        return result.Graph!;
    }

    private static SfcStepResult Answer(Job job, string commandId, string outcome, long at) =>
        job.Send(new SfcActionCompleted { At = at, ActionKey = Key("a"), Outcome = outcome, CommandId = commandId });

    private static StartWork Only(SfcStepResult result, SfcWorkKind kind) => result.Commands.OfType<StartWork>().Single(c => c.Work == kind);

    [Fact]
    public void Hold_SendsTheHoldCommandOfRunningWorkAndKeepsItsDeadlineOnTheIndex()
    {
        var job = new Job(TwoSteps());
        job.Start();

        SfcStepResult result = job.Send(new SfcHold { At = 2_000 });

        StartWork hold = Only(result, SfcWorkKind.HoldCommand);
        hold.TypeId.ShouldBe(HoldType);
        hold.ActionKey.ShouldBe(Key("a"));
        hold.CommandId.ShouldEndWith(":hold");
        hold.Deadline.ShouldBe(7_000L);
        hold.Attempt.ShouldBe(1);
        job.State.Actions["a"].Hold.ShouldBe(SfcHoldStatus.Holding);
        job.State.NextDeadline.ShouldBe(7_000L);
    }

    [Fact]
    public void HeldWork_IsRestartedOnReleaseAndTheRestartResponseCompletesTheAction()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string original = job.State.Actions["a"].CommandId!;
        string holdId = Only(job.Send(new SfcHold { At = 2_000 }), SfcWorkKind.HoldCommand).CommandId;

        SfcStepResult held = Answer(job, holdId, SfcOutcome.Held, 3_000);
        held.Commands.OfType<CancelWork>().Single().ShouldSatisfyAllConditions(
            c => c.CommandId.ShouldBe(original),
            c => c.Reason.ShouldBe("held"));
        job.State.Actions["a"].Hold.ShouldBe(SfcHoldStatus.Held);
        job.State.NextDeadline.ShouldBeNull();

        StartWork restart = Only(job.Send(new SfcRelease { At = 50_000 }), SfcWorkKind.RestartCommand);
        restart.TypeId.ShouldBe(RestartType);
        restart.Deadline.ShouldBe(70_000L);
        restart.Attempt.ShouldBe(1);
        job.Last.Commands.OfType<StartWork>().ShouldNotContain(c => c.Work == SfcWorkKind.PushCommand);
        job.State.Actions["a"].Restarted.ShouldBeTrue();

        // A late answer to the held attempt and a repeated hold answer are absorbed; the restart decides.
        job.Send(new SfcActionCompleted { At = 51_000, ActionKey = Key("a"), Outcome = "ok", CommandId = original }).Disposition.ShouldBe(SfcDisposition.Absorbed);
        Answer(job, holdId, SfcOutcome.Held, 51_000).Disposition.ShouldBe(SfcDisposition.Absorbed);
        Answer(job, restart.CommandId, "restarted_ok", 52_000).Disposition.ShouldBe(SfcDisposition.Applied);
        job.State.Actions["a"].Outcome.ShouldBe("restarted_ok");
        job.Started.ShouldBe(["b"]);
    }

    [Fact]
    public void WhileHeld_AnAnswerToTheHeldAttemptIsAbsorbed()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;
        Answer(job, holdId, SfcOutcome.Held, 2_000);

        job.Done("a", at: 3_000).Disposition.ShouldBe(SfcDisposition.Absorbed);
        job.Status("a").ShouldBe(SfcActionStatus.Dispatched);
    }

    [Theory]
    [InlineData("not_held")]
    [InlineData("error")]
    public void WorkThatCouldNotBeHeld_RunsOnAndIsIssuedAgainOnRelease(string answer)
    {
        var job = new Job(TwoSteps());
        job.Start();
        string original = job.State.Actions["a"].CommandId!;
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;

        if (answer == SfcOutcome.Error)
        {
            job.Send(new SfcActionFailed { At = 2_000, ActionKey = Key("a"), Error = "cannot", CommandId = holdId }).Disposition.ShouldBe(SfcDisposition.Applied);
        }
        else
        {
            Answer(job, holdId, answer, 2_000).Disposition.ShouldBe(SfcDisposition.Applied);
        }

        job.State.Actions["a"].Hold.ShouldBe(SfcHoldStatus.NotHeld);
        job.Status("a").ShouldBe(SfcActionStatus.Dispatched);

        StartWork again = Only(job.Send(new SfcRelease { At = 5_000 }), SfcWorkKind.PushCommand);
        again.CommandId.ShouldBe(original);
        again.Deadline.ShouldBe(14_000L);
        job.State.Actions["a"].Hold.ShouldBeNull();
        job.Done("a", at: 6_000);
        job.Started.ShouldBe(["b"]);
    }

    [Fact]
    public void AHoldThatIsNotAnswered_ExpiresWhileHeldAndTheWorkRunsOn()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;

        job.Tick(5_999).Disposition.ShouldBe(SfcDisposition.Absorbed);
        SfcStepResult expired = job.Tick(6_000);

        expired.Disposition.ShouldBe(SfcDisposition.Applied);
        expired.Commands.OfType<CancelWork>().Single().ShouldSatisfyAllConditions(
            c => c.CommandId.ShouldBe(holdId),
            c => c.Reason.ShouldBe("timeout"));
        job.State.Actions["a"].Hold.ShouldBe(SfcHoldStatus.NotHeld);
        job.State.NextDeadline.ShouldBeNull();
        Answer(job, holdId, "not_held", 7_000).Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void WorkThatEndsBeforeTheHoldIsAnswered_IsRecordedAndTheHoldIsWithdrawn()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;

        SfcStepResult done = job.Done("a", at: 2_000);

        done.Commands.OfType<CancelWork>().Single().ShouldSatisfyAllConditions(
            c => c.CommandId.ShouldBe(holdId),
            c => c.Reason.ShouldBe("superseded"));
        job.Status("a").ShouldBe(SfcActionStatus.Completed);
        job.State.Actions["a"].Hold.ShouldBeNull();
        Answer(job, holdId, "not_held", 3_000).Disposition.ShouldBe(SfcDisposition.Absorbed);
        job.Send(new SfcRelease { At = 4_000 });
        job.Started.ShouldBe(["b"]);
    }

    [Fact]
    public void AReleaseBeforeTheHoldAnswer_LeavesTheWorkToTheAnswer()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;

        job.Send(new SfcRelease { At = 2_000 }).Commands.ShouldBeEmpty();
        job.State.Actions["a"].Hold.ShouldBe(SfcHoldStatus.Holding);
        job.State.Actions["a"].Deadline.ShouldBe(11_000L);
        job.Send(new SfcResume { At = 2_500 }).Commands.ShouldBeEmpty();

        StartWork restart = Only(Answer(job, holdId, SfcOutcome.Held, 3_000), SfcWorkKind.RestartCommand);
        restart.Deadline.ShouldBe(23_000L);
    }

    [Fact]
    public void AReleaseBeforeANotHeldAnswer_IssuesTheWorkAgainWithTheAnswer()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string original = job.State.Actions["a"].CommandId!;
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;
        job.Send(new SfcRelease { At = 2_000 });

        StartWork again = Only(Answer(job, holdId, "not_held", 3_000), SfcWorkKind.PushCommand);

        again.CommandId.ShouldBe(original);
        job.State.Actions["a"].Hold.ShouldBeNull();
    }

    [Fact]
    public void ARestartIsSentOnce_ItsExpiryEndsTheActionWithTimeout()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;
        Answer(job, holdId, SfcOutcome.Held, 2_000);
        StartWork restart = Only(job.Send(new SfcRelease { At = 3_000 }), SfcWorkKind.RestartCommand);

        StartWork reissued = Only(job.Send(new SfcResume { At = 4_000 }), SfcWorkKind.RestartCommand);
        reissued.CommandId.ShouldBe(restart.CommandId);

        SfcStepResult expired = job.Tick(23_000);
        expired.Commands.OfType<StartWork>().ShouldBeEmpty();
        job.State.Actions["a"].Outcome.ShouldBe(SfcOutcome.Timeout);
        expired.Continuation.ShouldBeOfType<SfcFault>().Code.ShouldBe(SfcOutcome.Timeout);
    }

    [Fact]
    public void ALateHeldAnswerAfterTheHoldExpired_StillRestartsTheWork()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;
        job.Tick(6_000);
        job.State.Actions["a"].Hold.ShouldBe(SfcHoldStatus.NotHeld);

        // The equipment did hold the work; its command was sent already, so only a restart gets it going again.
        Answer(job, holdId, SfcOutcome.Held, 7_000).Disposition.ShouldBe(SfcDisposition.Applied);
        job.State.Actions["a"].Hold.ShouldBe(SfcHoldStatus.Held);
        Only(job.Send(new SfcRelease { At = 8_000 }), SfcWorkKind.RestartCommand).TypeId.ShouldBe(RestartType);
    }

    [Fact]
    public void ALateHeldAnswerAfterTheRelease_RestartsTheWorkAtOnce()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;
        job.Tick(6_000);
        job.Send(new SfcRelease { At = 7_000 });

        Only(Answer(job, holdId, SfcOutcome.Held, 8_000), SfcWorkKind.RestartCommand).Deadline.ShouldBe(28_000L);
        Answer(job, holdId, SfcOutcome.Held, 9_000).Disposition.ShouldBe(SfcDisposition.Absorbed);
    }

    [Fact]
    public void AnAnswerWithTheHoldOrRestartTypeOutsideItsCommand_IsRejected()
    {
        var job = new Job(TwoSteps());
        job.Start();

        job.Send(new SfcActionCompleted { At = 1_000, ActionKey = Key("a"), Outcome = "ok", TypeId = HoldType }).Disposition.ShouldBe(SfcDisposition.Rejected);
        job.Send(new SfcActionCompleted { At = 1_000, ActionKey = Key("a"), Outcome = "ok", TypeId = RestartType }).Disposition.ShouldBe(SfcDisposition.Rejected);
        string holdId = Only(job.Send(new SfcHold { At = 2_000 }), SfcWorkKind.HoldCommand).CommandId;
        job.Send(new SfcActionCompleted { At = 3_000, ActionKey = Key("a"), Outcome = SfcOutcome.Held, TypeId = HoldType, CommandId = holdId }).Disposition.ShouldBe(SfcDisposition.Applied);
    }

    [Fact]
    public void ThePermissiveBuildOfTheMcpTools_AcceptsHoldLogic()
    {
        const string Recipe = """
            {"Steps":[{"Name":"A","Initial":true}],"Transitions":[],
             "Actions":[{"Name":"a","Step":"A","Qualifier":"N","Interaction":"push_command","TypeId":"t.a","TimeoutSeconds":10,
                         "Hold":{"TypeId":"t.hold","TimeoutSeconds":5},"Restart":{"TypeId":"t.restart","TimeoutSeconds":5}}]}
            """;

        SfcBuildResult result = MicroDCS.Sfc.Simulation.SfcScriptParser.BuildPermissive(System.Text.Encoding.UTF8.GetBytes(Recipe));

        result.Succeeded.ShouldBeTrue(string.Join("; ", result.Errors.Select(static e => $"{e.Code} {e.Message}")));
        MicroDCS.Sfc.Simulation.SfcExplainer.Describe(result.Graph!).ShouldContain("on pause: hold t.hold");
    }

    [Fact]
    public void ACancelWhileHolding_WithdrawsTheHoldCommandToo()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string original = job.State.Actions["a"].CommandId!;
        string holdId = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;

        SfcStepResult cancelled = job.Send(new SfcCancel { At = 2_000 });

        cancelled.Commands.OfType<CancelWork>().Select(static c => c.CommandId).ShouldBe([holdId, original]);
        job.State.NextDeadline.ShouldBeNull();
    }

    [Fact]
    public void ASecondPause_HoldsTheRestartedWork()
    {
        var job = new Job(TwoSteps());
        job.Start();
        string firstHold = Only(job.Send(new SfcHold { At = 1_000 }), SfcWorkKind.HoldCommand).CommandId;
        Answer(job, firstHold, SfcOutcome.Held, 2_000);
        StartWork restart = Only(job.Send(new SfcRelease { At = 3_000 }), SfcWorkKind.RestartCommand);

        string secondHold = Only(job.Send(new SfcHold { At = 4_000 }), SfcWorkKind.HoldCommand).CommandId;
        secondHold.ShouldNotBe(firstHold);
        Answer(job, secondHold, SfcOutcome.Held, 5_000).Commands.OfType<CancelWork>().Single().CommandId.ShouldBe(restart.CommandId);

        Only(job.Send(new SfcRelease { At = 6_000 }), SfcWorkKind.RestartCommand).CommandId.ShouldNotBe(restart.CommandId);
    }

    [Fact]
    public void WorkWithoutHoldLogic_KeepsTheS20aBehaviour()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Push("a", "A"), Pull("p", "A")])));
        job.Start();

        job.Send(new SfcHold { At = 1_000 }).Commands.ShouldBeEmpty();
        job.State.NextDeadline.ShouldBeNull();
    }

    [Fact]
    public void TheStateWithHoldFieldsRoundTripsAndCarriesMinor3()
    {
        var job = new Job(TwoSteps());
        job.Start();
        job.Send(new SfcHold { At = 1_000 });

        string json = JsonSerializer.Serialize(job.State, SfcJsonContext.Default.SfcState);
        SfcState back = JsonSerializer.Deserialize(json, SfcJsonContext.Default.SfcState)!;

        json.ShouldContain("\"hold\":\"Holding\"");
        json.ShouldNotContain("restarted");
        JsonSerializer.Serialize(back, SfcJsonContext.Default.SfcState).ShouldBe(json);
        back.FormatMinor.ShouldBe(3);
    }

    [Fact]
    public void Build_RefusesHoldLogicThatIsIncompleteOrOnAPullEvent()
    {
        SfcBuildResult onPull = BuildWithHold(Recipe([Step("A", true)], [], [Pull("p", "A") with { Hold = HoldCmd(), Restart = RestartCmd() }]));
        SfcBuildResult holdOnly = BuildWithHold(Recipe([Step("A", true)], [], [Push("a", "A") with { Hold = HoldCmd() }]));
        SfcBuildResult badTimeout = BuildWithHold(Recipe([Step("A", true)], [], [Push("a", "A") with { Hold = HoldCmd(0), Restart = RestartCmd() }]));

        onPull.Errors.ShouldContain(e => e.Code == "SFC019");
        holdOnly.Errors.ShouldContain(e => e.Code == "SFC019");
        badTimeout.Errors.ShouldContain(e => e.Code == "SFC019");
    }

    [Fact]
    public void Build_RefusesHoldLogicTheStationCannotRun()
    {
        SfcRecipe recipe = Recipe([Step("A", true)], [], [Holdable("a", "A")]);
        SfcRecipe unknownType = Recipe([Step("A", true)], [], [Push("a", "A") with { Hold = HoldCmd() with { TypeId = "t.nobody" }, Restart = RestartCmd() }]);

        BuildWithHold(recipe, SfcFeature.None).Errors.ShouldContain(e => e.Code == "SFC015");
        BuildWithHold(recipe, holdOutcomes: ["stopped"]).Errors.ShouldContain(e => e.Code == "SFC020");
        BuildWithHold(unknownType).Errors.ShouldContain(e => e.Code == "SFC014");
    }

    [Fact]
    public void Build_AcceptsAConditionOnAnOutcomeOfTheRestartType()
    {
        SfcRecipe recipe = Recipe(
            [Step("A", true), Step("B"), Step("C")],
            [To("A", "B", "restarted_ok", 0), To("A", "C", "true", 1)],
            [Holdable("a", "A"), Push("b", "B"), Push("c", "C")]);

        BuildWithHold(recipe).Succeeded.ShouldBeTrue();
    }

    [Fact]
    public void TheInterpreterAcceptsBothRecipeVersions()
    {
        Interpreter.Accepts(SfcInterpreter.RecipeDataSchema).ShouldBeTrue();
        Interpreter.Accepts(SfcInterpreter.RecipeDataSchemaV10).ShouldBeTrue();
        Interpreter.Accepts("https://aschamberger.github.io/microdcs/schemas/sfc-recipe/v2.0.0.json").ShouldBeFalse();
        Interpreter.Accepts(null).ShouldBeFalse();
    }
}
