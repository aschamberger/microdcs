using MicroDCS.Sfc.Simulation;
using static MicroDCS.Sfc.Tests.Rec;

namespace MicroDCS.Sfc.Tests;

/// <summary>The in-memory host: virtual clock, scripted equipment, transcript. Python scenarios as whole-job simulations.</summary>
public class SimulationTests
{
    private static SfcGraph Assembly() => Graph(
        Recipe(
            [Step("Init", true), Step("Positioning"), Step("Tighten"), Step("Qa"), Step("Verify"), Step("Rework"), Step("Complete")],
            [To("Init", "FitAndQa"), To("FitAndQa", "Verify"), To("Verify", "Rework", "torque_low", 0), To("Verify", "Complete", "true", 1), To("Rework", "Verify")],
            [
                Push("position", "Positioning", 30),
                Push("tighten", "Tighten", 60, 2),
                Pull("qa", "Qa", 45),
                Push("verify", "Verify", 15),
                Push("rework", "Rework", 20),
                Push("finish", "Complete", 10),
            ],
            Sim("FitAndQa", ["Positioning", "Tighten"], ["Qa"])),
        "torque_low");

    [Fact]
    public void TheHappyPathCompletesWithoutWaiting()
    {
        var sim = new SfcSimulation(Assembly());

        sim.Run().ShouldBe(SfcRunEnd.Completed);

        sim.Continuation.ShouldBeEquivalentTo(new SfcComplete { Outcome = "completed" });
        sim.State.Actions.Values.ShouldAllBe(static a => a.Status == SfcActionStatus.Completed || a.Status == SfcActionStatus.Pending);
        sim.Transcript.Entries[0].Input.ShouldBe("start");
        sim.Transcript.ToText().ShouldContain("start_work push job-1:position");
    }

    [Fact]
    public void ScriptedOutcomesRouteTheJobAndTheClockFollowsTheDelays()
    {
        var script = new SfcScript().For("verify", SfcReply.Ok("torque_low", delayMs: 2000), SfcReply.Ok("ok", delayMs: 1000));
        var sim = new SfcSimulation(Assembly(), script);

        SfcRunEnd end = sim.Run();
        end.ShouldBe(SfcRunEnd.Completed, sim.Transcript.ToText());

        sim.Commands.OfType<StartWork>().Select(static c => c.Action).ShouldContain("rework");
        sim.Commands.OfType<StartWork>().Count(static c => c.Action == "verify").ShouldBe(2);
        sim.Now.ShouldBe(3000L);
    }

    [Fact]
    public void SilentEquipmentIsRedispatchedAndThenTimesOutOnTheVirtualClock()
    {
        var script = new SfcScript().For("tighten", SfcReply.Silence());
        var sim = new SfcSimulation(Assembly(), script);

        sim.Run().ShouldBe(SfcRunEnd.Faulted);

        sim.Commands.OfType<StartWork>().Count(static c => c.Action == "tighten").ShouldBe(2);
        sim.State.FaultCode.ShouldBe("timeout");
        sim.Now.ShouldBe(120_000L);
        sim.Transcript.ToText().ShouldNotContain("cancel_work job-1:position");
    }

    [Fact]
    public void ASlowAnswerBeforeTheDeadlineIsNotRedispatched()
    {
        var script = new SfcScript().For("tighten", SfcReply.Ok(delayMs: 59_000));
        var sim = new SfcSimulation(Assembly(), script);

        sim.Run().ShouldBe(SfcRunEnd.Completed);

        sim.Commands.OfType<StartWork>().Count(static c => c.Action == "tighten").ShouldBe(1);
    }

    [Fact]
    public void AnAnswerAtTheDeadlineWinsOverTheTick()
    {
        var script = new SfcScript().For("tighten", SfcReply.Ok(delayMs: 60_000));
        var sim = new SfcSimulation(Assembly(), script);

        sim.Run().ShouldBe(SfcRunEnd.Completed);

        sim.Commands.OfType<StartWork>().Count(static c => c.Action == "tighten").ShouldBe(1);
    }

    [Fact]
    public void AReportedFailureFaultsTheJob()
    {
        var sim = new SfcSimulation(Assembly(), new SfcScript().For("position", SfcReply.Fail("jam")));

        sim.Run().ShouldBe(SfcRunEnd.Faulted);

        sim.State.Actions["position"].Error.ShouldBe("jam");
    }

    [Fact]
    public void APullEventWithoutAnAnswerWaitsAndThenTimesOut()
    {
        var sim = new SfcSimulation(Assembly(), new SfcScript().For("qa", SfcReply.Silence()));

        sim.Run().ShouldBe(SfcRunEnd.Faulted);

        sim.State.FaultCode.ShouldBe("timeout");
        sim.Now.ShouldBe(45_000L);
    }

    [Fact]
    public void AnInjectedEventFromOutsideCompletesAPullAction()
    {
        var script = new SfcScript().For("qa", SfcReply.Silence()).Inject(new SfcEventReceived { At = 10_000, TypeId = "t.qa", Outcome = "ok" });
        var sim = new SfcSimulation(Assembly(), script);

        sim.Run().ShouldBe(SfcRunEnd.Completed);

        sim.Now.ShouldBe(10_000L);
    }

    [Fact]
    public void AScriptedReplyMatchesTheSelectorOfThePullAction()
    {
        var recipe = Graph(Recipe([Step("A", true)], [], [Pull("p", "A", 30, selector: new() { Subject = "st-1", CorrelationId = "{jobId}" })]));
        var sim = new SfcSimulation(recipe, new SfcScript());

        sim.Run().ShouldBe(SfcRunEnd.Completed);
    }

    [Fact]
    public void TheTimeLimitStopsARunBeforeAPendingDeadline()
    {
        var recipe = Graph(Recipe([Step("A", true)], [], [Pull("p", "A", 30)]));
        var sim = new SfcSimulation(recipe, new SfcScript().For("p", SfcReply.Silence()));

        sim.Run(maxVirtualMilliseconds: 10_000).ShouldBe(SfcRunEnd.TimeLimit);
        sim.State.Status.ShouldBe(SfcJobStatus.Running);
    }

    [Fact]
    public void TheTimeLimitStopsALongRun()
    {
        var sim = new SfcSimulation(Assembly(), new SfcScript().For("tighten", SfcReply.Silence()));

        sim.Run(maxVirtualMilliseconds: 61_000).ShouldBe(SfcRunEnd.TimeLimit);
    }

    [Fact]
    public void ManualSendsAreRecordedInTheTranscript()
    {
        var sim = new SfcSimulation(Assembly());
        sim.Send(new SfcStart { At = 0 });
        sim.Send(new SfcActionCompleted { At = 5, ActionKey = "job-1:nothing", Outcome = "ok" });

        sim.Transcript.Entries.Count.ShouldBe(2);
        sim.Transcript.Entries[1].Disposition.ShouldBe(SfcDisposition.Rejected);
        sim.Transcript.ToText().ShouldContain("Rejected");
    }

    [Fact]
    public void ARunIsDeterministic()
    {
        static string Text() => new Func<string>(() =>
        {
            var sim = new SfcSimulation(Assembly(), new SfcScript().For("tighten", SfcReply.Silence(), SfcReply.Ok(delayMs: 5)));
            sim.Run();
            return sim.Transcript.ToText();
        })();

        Text().ShouldBe(Text());
    }

    [Fact]
    public void TheExplainerDescribesTheStructureAndAJobOnIt()
    {
        SfcGraph graph = Assembly();
        var sim = new SfcSimulation(graph, new SfcScript().For("qa", SfcReply.Silence()));
        sim.Send(new SfcStart { At = 0 });
        sim.Send(new SfcActionCompleted { At = 1, ActionKey = "job-1:position", Outcome = "ok" });

        string structure = SfcExplainer.Describe(graph);
        string job = SfcExplainer.Describe(graph, sim.State);

        structure.ShouldContain("divergence 'FitAndQa' (simultaneous)");
        structure.ShouldContain("sequence 1: Positioning -> Tighten");
        structure.ShouldContain("when outcome 'torque_low'");
        structure.ShouldContain("max attempts host default");
        job.ShouldContain("Running");
        job.ShouldContain("action 'qa' (t.qa): Waiting");
        job.ShouldContain("next deadline");
        job.ShouldContain("step 'Tighten' waits for 'tighten'");
    }
}
