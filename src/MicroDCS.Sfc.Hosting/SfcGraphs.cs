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

    /// <summary>The features of this version: pull events follow in S20. Outcomes come from the <c>[Outgoing]</c> factories.</summary>
    public static SfcCapabilities CapabilitiesOf(ProcessorRegistry registry)
    {
        ArgumentNullException.ThrowIfNull(registry);
        var outcomes = new Dictionary<string, IReadOnlySet<string>>(StringComparer.Ordinal);
        foreach (var factory in registry.Processors.SelectMany(static p => p.Handlers).Where(static h => h.Kind == HandlerKind.Outgoing && h.TypeId is not null))
        {
            IEnumerable<string> declared = factory.Outcomes.IsDefault ? [] : factory.Outcomes;
            outcomes[factory.TypeId!] = declared.Append(ActionOutcome.DefaultName).ToFrozenSet(StringComparer.Ordinal);
        }

        return new SfcCapabilities(SfcFeature.PushCommand | SfcFeature.SimultaneousDivergence | SfcFeature.SelectionDivergence, outcomes);
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
