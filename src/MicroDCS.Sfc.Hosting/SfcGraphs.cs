using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Text;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Valkey.Sfc;

namespace MicroDCS.Sfc.Hosting;

/// <summary>Builds recipes against what this station can run and keeps the graphs of pinned recipes (D-44).</summary>
internal sealed class SfcGraphs
{
    private const int MaxCached = 512;

    private readonly IRecipeInterpreter _interpreter;
    private readonly SfcStateDao _states;
    private readonly ConcurrentDictionary<string, SfcGraph> _graphs = new(StringComparer.Ordinal);
    private readonly SfcCapabilities _capabilities;

    public SfcGraphs(IRecipeInterpreter interpreter, SfcStateDao states, ProcessorRegistry registry)
    {
        _interpreter = interpreter;
        _states = states;
        _capabilities = CapabilitiesOf(registry);
    }

    public IRecipeInterpreter Interpreter => _interpreter;

    /// <summary>The features of this version. Outcomes of commands come from the <c>[Outgoing]</c> factories, outcomes of pull events from the <c>[Incoming]</c> handlers.</summary>
    public static SfcCapabilities CapabilitiesOf(ProcessorRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);
        return new SfcCapabilities(
            SfcFeature.PushCommand | SfcFeature.PullEvent | SfcFeature.SimultaneousDivergence | SfcFeature.SelectionDivergence,
            OutcomesOf(registry, HandlerKind.Outgoing),
            OutcomesOf(registry, HandlerKind.Incoming));
    }

    private static Dictionary<string, IReadOnlySet<string>> OutcomesOf(ProcessorRegistry registry, HandlerKind kind)
    {
        var outcomes = new Dictionary<string, IReadOnlySet<string>>(StringComparer.Ordinal);
        foreach (var handler in registry.Processors.SelectMany(static p => p.Handlers).Where(h => h.Kind == kind && h.TypeId is not null))
        {
            IEnumerable<string> declared = handler.Outcomes.IsDefault ? [] : handler.Outcomes;
            outcomes[handler.TypeId!] = declared.Append(ActionOutcome.DefaultName).ToFrozenSet(StringComparer.Ordinal);
        }

        return outcomes;
    }

    public SfcBuildResult Build(string recipe)
    {
        var result = _interpreter.Build(Encoding.UTF8.GetBytes(recipe), _capabilities);
        if (result.Graph is { } graph)
        {
            Remember(graph);
        }

        return result;
    }

    public async ValueTask<SfcGraph> GetAsync(string recipeHash, CancellationToken cancellationToken)
    {
        if (_graphs.TryGetValue(recipeHash, out var cached))
        {
            return cached;
        }

        var recipe = await _states.GetRecipeAsync(recipeHash, cancellationToken).ConfigureAwait(false)
            ?? throw new SfcStepFailedException($"The pinned recipe '{recipeHash}' is not stored.");
        var result = Build(recipe);
        return result.Graph ?? throw new SfcStepFailedException($"The pinned recipe '{recipeHash}' does not build on this station: {string.Join("; ", result.Errors.Select(static e => e.Code))}.");
    }

    private void Remember(SfcGraph graph)
    {
        // Recipes are few; the bound only guards against an unbounded number of generated ones.
        if (_graphs.Count >= MaxCached)
        {
            _graphs.Clear();
        }

        _graphs[graph.RecipeHash] = graph;
    }
}
