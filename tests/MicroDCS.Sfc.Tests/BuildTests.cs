using static MicroDCS.Sfc.Tests.Rec;

namespace MicroDCS.Sfc.Tests;

/// <summary>Build-time validation (D-44): every refusal has a code and a test.</summary>
public class BuildTests
{
    private static SfcRecipe Linear() => Recipe(
        [Step("A", true), Step("B")],
        [To("A", "B")],
        [Push("a", "A"), Push("b", "B")]);

    private static string[] Codes(SfcRecipe recipe, params string[] outcomes) => [.. TryBuild(recipe, outcomes).Errors.Select(static e => e.Code)];

    [Fact]
    public void ValidRecipeBuildsWithAStableHash()
    {
        SfcGraph graph = Graph(Linear());

        graph.InitialStep.ShouldBe("A");
        graph.ActionsOf("A").Select(static a => a.Name).ShouldBe(["a"]);
        graph.Outgoing("A").Single().Target.ShouldBe("B");
        graph.RecipeHash.ShouldNotBeNullOrEmpty();
    }

    [Fact]
    public void JsonBuildHashesTheDocumentBytes()
    {
        byte[] json = System.Text.Encoding.UTF8.GetBytes("""{"Steps":[{"Name":"A","Initial":true}],"Transitions":[],"Actions":[{"Name":"a","Step":"A","Qualifier":"N","Interaction":"push_command","TypeId":"t.a","TimeoutSeconds":5}]}""");
        SfcCapabilities caps = new(SfcFeature.PushCommand, new Dictionary<string, IReadOnlySet<string>> { ["t.a"] = new HashSet<string>() });

        SfcBuildResult one = new SfcInterpreter().Build(json, caps);
        SfcBuildResult two = new SfcInterpreter().Build(json, caps);

        one.Succeeded.ShouldBeTrue();
        one.Graph!.RecipeHash.ShouldBe(two.Graph!.RecipeHash);
        one.Graph.RecipeHash.Length.ShouldBe(64);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("{}")]
    [InlineData("null")]
    public void MalformedDocumentsAreSfc000(string json)
    {
        SfcBuildResult result = SfcGraph.Build(System.Text.Encoding.UTF8.GetBytes(json), Caps(Linear()));

        result.Succeeded.ShouldBeFalse();
        result.Errors.Single().Code.ShouldBe("SFC000");
    }

    [Fact]
    public void NoInitialStepAndTwoInitialStepsAreSfc001()
    {
        Codes(Recipe([Step("A")], [], [])).ShouldContain("SFC001");
        Codes(Recipe([Step("A", true), Step("B", true)], [], [])).ShouldContain("SFC001");
    }

    [Fact]
    public void DuplicateNamesAreSfc002()
    {
        Codes(Recipe([Step("A", true), Step("A")], [], [])).ShouldContain("SFC002");
        Codes(Recipe([Step("A", true)], [], [Push("x", "A"), Push("x", "A")])).ShouldContain("SFC002");
        Codes(Recipe([Step("A", true), Step("B")], [To("A", "B")], [], Sim("B", ["A"]))).ShouldContain("SFC002");
    }

    [Fact]
    public void UnknownReferencesAreSfc003()
    {
        Codes(Recipe([Step("A", true)], [To("A", "Nowhere")], [])).ShouldContain("SFC003");
        Codes(Recipe([Step("A", true)], [], [Push("x", "Nowhere")])).ShouldContain("SFC003");
        Codes(Recipe([Step("A", true), Step("B")], [To("A", "S")], [], Sim("S", ["B", "Ghost"], ["B"]))).ShouldContain("SFC003");
    }

    [Fact]
    public void BrokenDivergencesAreSfc004AndSfc005()
    {
        Codes(Recipe([Step("A", true)], [], [], Sim("S"))).ShouldContain("SFC004");
        Codes(Recipe([Step("A", true), Step("B")], [To("A", "S")], [], Sim("S", ["B"], []))).ShouldContain("SFC004");
        Codes(Recipe([Step("A", true), Step("B")], [To("A", "S")], [], new SfcDivergence { Name = "S", Kind = "xor", Sequences = [["B"]] })).ShouldContain("SFC004");
        Codes(Recipe([Step("A", true), Step("B")], [To("A", "S")], [], Sim("S", ["B"], ["B"]))).ShouldContain("SFC005");
    }

    [Fact]
    public void TransitionsMustNotTouchTheInsideOfAPath()
    {
        SfcStep[] steps = [Step("A", true), Step("B"), Step("C"), Step("D")];

        Codes(Recipe(steps, [To("A", "S"), To("B", "D")], [], Sim("S", ["B", "C"]))).ShouldContain("SFC006");
        Codes(Recipe(steps, [To("A", "S"), To("A", "C")], [], Sim("S", ["B", "C"]))).ShouldContain("SFC006");
        Codes(Recipe(steps, [To("A", "B")], [], Sim("S", ["B", "C"]))).ShouldContain("SFC006");
        Codes(Recipe([Step("A", true), Step("B")], [], [], Sim("S", ["A"], ["B"]))).ShouldContain("SFC006");
    }

    [Fact]
    public void ADecisionNeedsADefaultAndUniquePriorities()
    {
        SfcStep[] steps = [Step("A", true), Step("B"), Step("C")];
        SfcActionBlock[] actions = [Push("a", "A")];

        Codes(Recipe(steps, [To("A", "B", "ok", 0), To("A", "C", "bad", 1)], actions), "bad").ShouldContain("SFC007");
        Codes(Recipe(steps, [To("A", "B", "true", 0), To("A", "C", "ok", 0)], actions)).ShouldContain("SFC008");
        Codes(Recipe(steps, [To("A", "B", "ok", 0), To("A", "C", "true", 1)], actions)).ShouldBeEmpty();
    }

    [Fact]
    public void SelectionNeedsADefaultEntryAndEveryPathNeedsAnEntry()
    {
        SfcStep[] steps = [Step("A", true), Step("P1"), Step("P2"), Step("Z")];
        SfcActionBlock[] actions = [Push("a", "A")];

        Codes(Recipe(steps, [To("A", "S"), To("S", "P1", "ok"), To("S", "P2", "bad", 1), To("S", "Z")], actions, Sel("S", ["P1"], ["P2"])), "bad")
            .ShouldContain("SFC007");
        Codes(Recipe(steps, [To("A", "S"), To("S", "P1", "true"), To("S", "Z")], actions, Sel("S", ["P1"], ["P2"])))
            .ShouldContain("SFC007");
        Codes(Recipe(steps, [To("A", "S"), To("S", "P1", "ok", 0), To("S", "P2", "true", 1), To("S", "Z")], actions, Sel("S", ["P1"], ["P2"])))
            .ShouldBeEmpty();
    }

    [Fact]
    public void ActionRulesAreSfc009ToSfc011()
    {
        Codes(Recipe([Step("A", true)], [], [Push("a", "A", qualifier: "L")])).ShouldContain("SFC010");
        Codes(Recipe([Step("A", true)], [], [Push("a", "A", timeout: 0)])).ShouldContain("SFC009");
        Codes(Recipe([Step("A", true)], [], [Push("a", "A", max: 0)])).ShouldContain("SFC009");
        Codes(Recipe([Step("A", true)], [], [Push("a", "A") with { Interaction = "rpc" }])).ShouldContain("SFC009");
    }

    [Fact]
    public void AllStandardQualifiersParseAndRoundTrip()
    {
        SfcActionQualifiers.Codes.ShouldBe(["N", "R", "S", "L", "D", "P", "SD", "DS", "SL", "P1", "P0"]);
        foreach (string code in SfcActionQualifiers.Codes)
        {
            SfcActionQualifiers.TryParse(code, out SfcActionQualifier qualifier).ShouldBeTrue(code);
            SfcActionQualifiers.ToCode(qualifier).ShouldBe(code);
        }

        SfcActionQualifiers.TryParse("n", out _).ShouldBeFalse();
        SfcActionQualifiers.TryParse(null, out _).ShouldBeFalse();
    }

    [Fact]
    public void QualifierErrorsNameTheStandardQualifier()
    {
        string stored = TryBuild(Recipe([Step("A", true)], [], [Push("a", "A", qualifier: "SD")])).Errors.Single().Message;
        string unknown = TryBuild(Recipe([Step("A", true)], [], [Push("a", "A", qualifier: "X")])).Errors.Single().Message;

        stored.ShouldContain("StoredAndTimeDelayed");
        unknown.ShouldContain("unknown qualifier 'X'");
        unknown.ShouldContain("N, R, S, L, D, P, SD, DS, SL, P1, P0");
    }

    [Fact]
    public void UnreachableStepsAreSfc012()
    {
        Codes(Recipe([Step("A", true), Step("Island")], [], [])).ShouldContain("SFC012");
    }

    [Fact]
    public void ACycleThatNeverWaitsIsSfc013ButOneWithAnActionIsFine()
    {
        Codes(Recipe([Step("A", true), Step("B")], [To("A", "B"), To("B", "A")], [])).ShouldContain("SFC013");
        Codes(Recipe([Step("A", true), Step("B")], [To("A", "B"), To("B", "A")], [Push("b", "B")])).ShouldBeEmpty();
        Codes(Recipe([Step("A", true), Step("P")], [To("A", "S"), To("S", "A")], [], Sim("S", ["P"]))).ShouldContain("SFC013");
    }

    [Fact]
    public void AmbiguousPullEventsAreSfc011()
    {
        SfcStep[] steps = [Step("A", true), Step("P1"), Step("P2")];

        Codes(Recipe([Step("A", true)], [], [Pull("x", "A", type: "t.e"), Pull("y", "A", type: "t.e")])).ShouldContain("SFC011");
        Codes(Recipe(steps, [To("A", "S")], [Pull("x", "P1", type: "t.e"), Pull("y", "P2", type: "t.e")], Sim("S", ["P1"], ["P2"]))).ShouldContain("SFC011");
        // Parameters are not a selector: only the Selector field tells two waits apart.
        Codes(Recipe([Step("A", true)], [], [Pull("x", "A", type: "t.e", parameter: "left"), Pull("y", "A", type: "t.e", parameter: "right")])).ShouldContain("SFC011");
        Codes(Recipe([Step("A", true), Step("B")], [To("A", "B")], [Pull("x", "A", type: "t.e"), Pull("y", "B", type: "t.e")])).ShouldBeEmpty();
    }

    [Fact]
    public void SelectorsThatDifferInALiteralSeparateWaits()
    {
        SfcSelector Subject(string value) => new() { Subject = value };

        Codes(Recipe([Step("A", true)], [], [Pull("x", "A", type: "t.e", selector: Subject("left")), Pull("y", "A", type: "t.e", selector: Subject("right"))])).ShouldBeEmpty();
        Codes(Recipe([Step("A", true)], [], [Pull("x", "A", type: "t.e", selector: Subject("same")), Pull("y", "A", type: "t.e", selector: Subject("same"))])).ShouldContain("SFC011");
        Codes(Recipe([Step("A", true)], [], [Pull("x", "A", type: "t.e", selector: Subject("left")), Pull("y", "A", type: "t.e")])).ShouldContain("SFC011");

        // The job id can equal any literal, and one field each says nothing about the other field.
        Codes(Recipe([Step("A", true)], [], [Pull("x", "A", type: "t.e", selector: Subject("{jobId}")), Pull("y", "A", type: "t.e", selector: Subject("left"))])).ShouldContain("SFC011");
        Codes(Recipe([Step("A", true)], [], [Pull("x", "A", type: "t.e", selector: Subject("left")), Pull("y", "A", type: "t.e", selector: new() { CorrelationId = "c" })])).ShouldContain("SFC011");
    }

    [Theory]
    [InlineData("")]
    [InlineData("{job}")]
    [InlineData("{jobId}-{other}")]
    public void ABadSelectorValueIsSfc009(string value) =>
        Codes(Recipe([Step("A", true)], [], [Pull("x", "A", selector: new() { Subject = value })])).ShouldContain("SFC009");

    [Fact]
    public void ASelectorOnAPushActionIsSfc009() =>
        Codes(Recipe([Step("A", true)], [], [Push("x", "A") with { Selector = new() { Subject = "s" } }])).ShouldContain("SFC009");

    [Fact]
    public void CapabilitiesRefuseWhatTheStationCannotRun()
    {
        SfcRecipe recipe = Recipe([Step("A", true), Step("B"), Step("C")], [To("A", "S")], [Push("a", "A"), Pull("b", "B")], Sim("S", ["B"], ["C"]));
        SfcCapabilities none = new(SfcFeature.None, new Dictionary<string, IReadOnlyDictionary<string, bool>>().ToDictionary(static p => p.Key, static _ => (IReadOnlySet<string>)new HashSet<string>()));

        string[] codes = [.. SfcGraph.Build(recipe, "h", none).Errors.Select(static e => e.Code)];

        codes.ShouldContain("SFC015");
        codes.ShouldContain("SFC014");
    }

    [Fact]
    public void OutcomeNamesMustBeDeclaredByAnActionTypeInvolved()
    {
        SfcStep[] steps = [Step("A", true), Step("B"), Step("C")];
        SfcActionBlock[] actions = [Push("a", "A")];

        Codes(Recipe(steps, [To("A", "B", "typo", 0), To("A", "C", "true", 1)], actions), "good").ShouldContain("SFC016");
        Codes(Recipe(steps, [To("A", "B", "good", 0), To("A", "C", "timeout", 1), To("C", "B", "true")], actions), "good").ShouldNotContain("SFC016");
        Codes(Recipe([Step("A", true), Step("B")], [To("A", "B", "ok")], [])).ShouldContain("SFC016");
    }

    [Fact]
    public void InterpreterReportsItsDataSchema()
    {
        new SfcInterpreter().DataSchema.ShouldBe("https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/");
    }
}
