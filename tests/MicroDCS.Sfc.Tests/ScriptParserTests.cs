using System.Text;
using MicroDCS.Sfc.Simulation;
using static MicroDCS.Sfc.Tests.Rec;

namespace MicroDCS.Sfc.Tests;

public class ScriptParserTests
{
    [Fact]
    public void AnEmptyScriptIsTheDefaultScript()
    {
        SfcScriptParser.Parse(null).Default.ShouldBe(SfcReply.Ok());
        SfcScriptParser.Parse("  ").Injected.ShouldBeEmpty();
    }

    [Fact]
    public void RepliesAreParsedInAllForms()
    {
        SfcScript script = SfcScriptParser.Parse("""{"default":"good@5","replies":{"a":["ok","error","error:jam@300","silent","torque_low@2000"]},"events":[{"atMs":1000,"typeId":"t.qa"},{"atMs":2000,"typeId":"t.x","outcome":"bad"}]}""");

        script.Default.ShouldBe(SfcReply.Ok("good", 5));
        script.Injected.Count.ShouldBe(2);
        script.Injected[0].ShouldBe(new SfcEventReceived { At = 1000, TypeId = "t.qa", Outcome = "ok" });
        script.Injected[1].ShouldBe(new SfcEventReceived { At = 2000, TypeId = "t.x", Outcome = "bad" });
    }

    [Theory]
    [InlineData("[]", "must be a JSON object")]
    [InlineData("{\"x\":1}", "Unknown script field 'x'")]
    [InlineData("{\"replies\":[]}", "'replies' must be an object")]
    [InlineData("{\"replies\":{\"a\":\"ok\"}}", "must be a list")]
    [InlineData("{\"replies\":{\"a\":[1]}}", "A reply is a string")]
    [InlineData("{\"replies\":{\"a\":[\"ok@soon\"]}}", "delay")]
    [InlineData("{\"replies\":{\"a\":[\"@5\"]}}", "needs an outcome name")]
    [InlineData("{\"events\":{}}", "'events' must be a list")]
    [InlineData("{\"events\":[{\"typeId\":\"t\"}]}", "atMs")]
    [InlineData("{broken", "not valid JSON")]
    public void InvalidScriptsNameTheProblem(string json, string message)
    {
        Should.Throw<FormatException>(() => SfcScriptParser.Parse(json)).Message.ShouldContain(message);
    }

    [Fact]
    public void ScriptedRepliesPickByHowOftenTheEquipmentIsAsked()
    {
        var script = new SfcScript().For("a", SfcReply.Ok("first"), SfcReply.Ok("second"));

        script.ReplyFor("a", 1).Outcome.ShouldBe("first");
        script.ReplyFor("a", 2).Outcome.ShouldBe("second");
        script.ReplyFor("a", 9).Outcome.ShouldBe("second");
        script.ReplyFor("other", 1).ShouldBe(SfcReply.Ok());
    }

    [Fact]
    public void BuildPermissiveAcceptsAnyOutcomeTheRecipeNames()
    {
        byte[] json = Encoding.UTF8.GetBytes("""{"Steps":[{"Name":"A","Initial":true},{"Name":"B"}],"Transitions":[{"Source":"A","Target":"B","Condition":"torque_low"}],"Actions":[{"Name":"a","Step":"A","Qualifier":"N","Interaction":"push_command","TypeId":"t.a","TimeoutSeconds":5},{"Name":"b","Step":"B","Qualifier":"N","Interaction":"push_command","TypeId":"t.b","TimeoutSeconds":5}]}""");

        SfcScriptParser.BuildPermissive(json).Succeeded.ShouldBeTrue();
        SfcScriptParser.BuildPermissive("nope"u8.ToArray()).Errors.Single().Code.ShouldBe("SFC000");
        SfcScriptParser.BuildPermissive("null"u8.ToArray()).Errors.Single().Code.ShouldBe("SFC000");
    }

    [Fact]
    public void ALoopThatAnswersAtOnceStopsAtTheEvolutionLimit()
    {
        var sim = new SfcSimulation(Graph(Recipe([Step("A", true), Step("B")], [To("A", "B"), To("B", "A")], [Push("a", "A"), Push("b", "B")])));

        sim.Run(maxEvolutions: 50).ShouldBe(SfcRunEnd.EvolutionLimit);
    }
}
