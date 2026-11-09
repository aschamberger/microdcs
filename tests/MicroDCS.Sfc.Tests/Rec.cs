using System.Text.Json;

namespace MicroDCS.Sfc.Tests;

/// <summary>A small DSL to write recipes and drive the interpreter in tests.</summary>
internal static class Rec
{
    internal static readonly SfcInterpreter Interpreter = new();

    internal static SfcStep Step(string name, bool initial = false) => new() { Name = name, Initial = initial };

    internal static SfcTransition To(string source, string target, string condition = "true", int priority = 0) =>
        new() { Source = source, Target = target, Condition = condition, Priority = priority };

    internal static SfcActionBlock Push(string name, string step, int timeout = 30, int? max = null, string? type = null, string qualifier = "N") =>
        new() { Name = name, Step = step, Qualifier = qualifier, Interaction = SfcNames.PushCommand, TypeId = type ?? $"t.{name}", TimeoutSeconds = timeout, MaxAttempts = max };

    internal static SfcActionBlock Pull(string name, string step, int timeout = 30, string? type = null, string? parameter = null, SfcSelector? selector = null) =>
        new()
        {
            Name = name,
            Step = step,
            Qualifier = "N",
            Interaction = SfcNames.PullEvent,
            TypeId = type ?? $"t.{name}",
            TimeoutSeconds = timeout,
            Parameters = parameter is null ? null : new Dictionary<string, JsonElement> { ["selector"] = JsonSerializer.SerializeToElement(parameter) },
            Selector = selector,
        };

    internal static SfcDivergence Sim(string name, params string[][] paths) => new() { Name = name, Kind = SfcNames.Simultaneous, Sequences = paths };

    internal static SfcDivergence Sel(string name, params string[][] paths) => new() { Name = name, Kind = SfcNames.Selection, Sequences = paths };

    internal static SfcRecipe Recipe(SfcStep[] steps, SfcTransition[] transitions, SfcActionBlock[] actions, params SfcDivergence[] divergences) =>
        new() { Steps = steps, Transitions = transitions, Actions = actions, Divergences = divergences.Length == 0 ? null : divergences };

    internal static SfcCapabilities Caps(SfcRecipe recipe, params string[] outcomes) => new(
        SfcFeature.PushCommand | SfcFeature.PullEvent | SfcFeature.SimultaneousDivergence | SfcFeature.SelectionDivergence,
        recipe.Actions.Select(static a => a.TypeId).Distinct().ToDictionary(static t => t, _ => (IReadOnlySet<string>)new HashSet<string>(["ok", .. outcomes]), StringComparer.Ordinal));

    internal static SfcBuildResult TryBuild(SfcRecipe recipe, params string[] outcomes) =>
        SfcGraph.Build(recipe, "hash-" + Guid.NewGuid().ToString("N"), Caps(recipe, outcomes));

    internal static SfcGraph Graph(SfcRecipe recipe, params string[] outcomes)
    {
        SfcBuildResult result = TryBuild(recipe, outcomes);
        result.Succeeded.ShouldBeTrue(string.Join("; ", result.Errors.Select(static e => $"{e.Code} {e.Message}")));
        return result.Graph!;
    }

    internal static string Key(string action, string job = "job-1") => ActionKey.Create(job, action);

    /// <summary>Drives one job by hand: each call is one evolution.</summary>
    internal sealed class Job
    {
        private readonly SfcGraph _graph;

        internal Job(SfcGraph graph, SfcSnapshot? snapshot = null)
        {
            _graph = graph;
            Snapshot = snapshot ?? SfcSnapshot.Default;
            State = SfcState.Create(graph, "job-1", "scope");
        }

        internal SfcSnapshot Snapshot { get; set; }

        internal SfcState State { get; private set; }

        internal SfcStepResult Last { get; private set; } = null!;

        internal SfcStepResult Send(SfcEvent input)
        {
            Last = Interpreter.Evolve(_graph, State, Snapshot, input);
            State = Last.State;
            return Last;
        }

        internal SfcStepResult Start(long at = 0) => Send(new SfcStart { At = at });

        internal SfcStepResult Done(string action, string outcome = "ok", long at = 0) =>
            Send(new SfcActionCompleted { At = at, ActionKey = Key(action), Outcome = outcome });

        internal SfcStepResult Tick(long at, long outageMs = 0) => Send(new SfcTick { At = at, OutageMs = outageMs });

        internal IEnumerable<string> Started => Last.Commands.OfType<StartWork>().Select(static c => c.Action);

        internal SfcActionStatus Status(string action) => State.Actions[action].Status;
    }
}
