using static MicroDCS.Sfc.Tests.Rec;

namespace MicroDCS.Sfc.Tests;

/// <summary>Regression tests for the findings of the S15 review: each one failed before its fix.</summary>
public class ReviewFixTests
{
    private static string[] Codes(SfcRecipe recipe, params string[] outcomes) => [.. TryBuild(recipe, outcomes).Errors.Select(static e => e.Code)];

    private static (SfcStep[] Steps, SfcTransition[] Transitions) Chain(int count, bool closeLoop)
    {
        SfcStep[] steps = [.. Enumerable.Range(0, count).Select(i => Step($"S{i}", i == 0))];
        List<SfcTransition> transitions = [.. Enumerable.Range(0, count - 1).Select(i => To($"S{i}", $"S{i + 1}"))];
        if (closeLoop)
        {
            transitions.Add(To($"S{count - 1}", "S0"));
        }

        return (steps, [.. transitions]);
    }

    // ── Build: stack safety, size, names, nulls, cycles ─────────────────

    [Fact]
    public void ALongChainOfActionFreeStepsBuildsWithoutExhaustingTheStack()
    {
        (SfcStep[] steps, SfcTransition[] transitions) = Chain(19_000, closeLoop: false);

        TryBuild(Recipe(steps, transitions, [])).Succeeded.ShouldBeTrue();
    }

    [Fact]
    public void ALongActionFreeLoopIsRefusedAsSfc013NotACrash()
    {
        (SfcStep[] steps, SfcTransition[] transitions) = Chain(19_000, closeLoop: true);

        Codes(Recipe(steps, transitions, [])).ShouldContain("SFC013");
    }

    [Fact]
    public void ARecipeAboveTheSizeCapIsSfc018()
    {
        (SfcStep[] steps, SfcTransition[] transitions) = Chain(SfcGraphValidator.MaxElements + 1, closeLoop: false);

        Codes(Recipe(steps, transitions, [])).ShouldBe(["SFC018"]);
    }

    [Theory]
    [InlineData("load:tray")]
    [InlineData("")]
    public void ActionNamesThatBreakTheActionKeyAreSfc017(string name)
    {
        Codes(Recipe([Step("A", true)], [], [Push(name, "A")])).ShouldContain("SFC017");
    }

    [Theory]
    [InlineData("""{"Steps":null,"Transitions":[],"Actions":[]}""")]
    [InlineData("""{"Steps":[null],"Transitions":[],"Actions":[]}""")]
    [InlineData("""{"Steps":[{"Name":null,"Initial":true}],"Transitions":[],"Actions":[]}""")]
    [InlineData("""{"Steps":[{"Name":"A","Initial":true}],"Transitions":null,"Actions":[]}""")]
    public void NullsInRequiredPartsAreSfc000NotAnException(string json)
    {
        SfcBuildResult result = SfcGraph.Build(System.Text.Encoding.UTF8.GetBytes(json), Caps(Recipe([Step("A", true)], [], [])));

        result.Errors.Single().Code.ShouldBe("SFC000");
    }

    [Fact]
    public void ASelectionWithAnActionFreeSequenceCanSpinInALoop()
    {
        // D can take the action-free sequence E, and D -> M -> D has nothing to wait for.
        SfcRecipe recipe = Recipe(
            [Step("Z", true), Step("E"), Step("F"), Step("M")],
            [To("Z", "D"), To("D", "F", "ok", 0), To("D", "E", "true", 1), To("D", "M"), To("M", "D")],
            [Push("z", "Z"), Push("f", "F")],
            Sel("D", ["E"], ["F"]));

        Codes(recipe).ShouldContain("SFC013");
    }

    // ── Semantics ───────────────────────────────────────────────────────

    [Fact]
    public void ASelectionExitIgnoresOutcomesOfSequencesNotTakenInThisPass()
    {
        SfcGraph graph = Graph(
            Recipe(
                [Step("A", true), Step("S1"), Step("S2"), Step("End")],
                [To("A", "D"), To("D", "S1", "left", 0), To("D", "S2", "true", 1), To("D", "A", "retry", 0), To("D", "End", "true", 1)],
                [Push("a", "A"), Push("s1", "S1"), Push("s2", "S2"), Push("e", "End")],
                Sel("D", ["S1"], ["S2"])),
            "left",
            "retry");
        var job = new Job(graph);
        job.Start();

        job.Done("a", "ok");
        job.Done("s2", "retry");
        job.State.ActiveSteps.ShouldBe(["A"]);
        job.Done("a", "left");
        job.Done("s1", "ok");

        job.State.ActiveSteps.ShouldBe(["End"]);
    }

    [Fact]
    public void TheLastExpiryWithdrawsTheCommand()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Push("a", "A", 10, 1)])));
        job.Start();
        string commandId = job.State.Actions["a"].CommandId!;

        CancelWork cancel = job.Tick(10_000).Commands.OfType<CancelWork>().Single();

        cancel.CommandId.ShouldBe(commandId);
        cancel.Reason.ShouldBe("timeout");
    }

    [Fact]
    public void AResponseOfAnEarlierPassThroughALoopIsAbsorbed()
    {
        SfcGraph graph = Graph(
            Recipe(
                [Step("A", true), Step("B")],
                [To("A", "A", "retry", 0), To("A", "B", "true", 1)],
                [Push("a", "A"), Push("b", "B")]),
            "retry");
        var job = new Job(graph);
        int firstRound = job.Start().Commands.OfType<StartWork>().Single().Round;
        job.Send(new SfcActionCompleted { At = 1, ActionKey = Key("a"), Outcome = "retry", Round = firstRound });
        StartWork second = job.Last.Commands.OfType<StartWork>().Single();

        SfcStepResult stale = job.Send(new SfcActionCompleted { At = 2, ActionKey = Key("a"), Outcome = "ok", Round = firstRound });
        SfcStepResult current = job.Send(new SfcActionCompleted { At = 3, ActionKey = Key("a"), Outcome = "ok", Round = second.Round });

        second.Round.ShouldBe(firstRound + 1);
        stale.Disposition.ShouldBe(SfcDisposition.Absorbed);
        stale.Reason.ShouldNotBeNull().ShouldContain("earlier pass");
        current.Disposition.ShouldBe(SfcDisposition.Applied);
        job.State.ActiveSteps.ShouldBe(["B"]);
    }

    [Fact]
    public void AResponseWithoutARoundStillMatchesByActionKey()
    {
        var job = new Job(Graph(Recipe([Step("A", true)], [], [Push("a", "A")])));
        job.Start();

        job.Done("a").Disposition.ShouldBe(SfcDisposition.Applied);
    }
}
