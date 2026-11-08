using System.Security.Cryptography;
using System.Text.Json;

namespace MicroDCS.Sfc;

/// <summary>The position of a step inside a divergence sequence.</summary>
/// <param name="Divergence">The divergence that holds the step.</param>
/// <param name="Sequence">The index of the sequence.</param>
/// <param name="Index">The index of the step in the sequence.</param>
public readonly record struct SfcSequencePosition(SfcDivergence Divergence, int Sequence, int Index);

/// <summary>
/// A validated, indexed recipe. It is built once (<see cref="Build(ReadOnlyMemory{byte}, SfcCapabilities)"/>) and reused; the
/// interpreter only runs graphs that built (D-44). Immutable and safe to share between threads.
/// </summary>
public sealed class SfcGraph
{
    private readonly Dictionary<string, SfcDivergence> _divergences;
    private readonly Dictionary<string, SfcActionBlock> _actions;
    private readonly Dictionary<string, List<SfcActionBlock>> _actionsByStep;
    private readonly Dictionary<string, SfcSequencePosition> _sequences;
    private readonly Dictionary<string, List<SfcTransition>> _outgoing;

    internal SfcGraph(SfcRecipe recipe, string recipeHash, string initialStep)
    {
        Recipe = recipe;
        RecipeHash = recipeHash;
        InitialStep = initialStep;
        _divergences = (recipe.Divergences ?? []).ToDictionary(static b => b.Name, StringComparer.Ordinal);
        _actions = recipe.Actions.ToDictionary(static a => a.Name, StringComparer.Ordinal);
        _actionsByStep = recipe.Steps.ToDictionary(static s => s.Name, static _ => new List<SfcActionBlock>(), StringComparer.Ordinal);
        foreach (SfcActionBlock action in recipe.Actions)
        {
            _actionsByStep[action.Step].Add(action);
        }

        _sequences = new Dictionary<string, SfcSequencePosition>(StringComparer.Ordinal);
        foreach (SfcDivergence divergence in _divergences.Values)
        {
            for (int p = 0; p < divergence.Sequences.Count; p++)
            {
                for (int i = 0; i < divergence.Sequences[p].Count; i++)
                {
                    _sequences[divergence.Sequences[p][i]] = new SfcSequencePosition(divergence, p, i);
                }
            }
        }

        _outgoing = new Dictionary<string, List<SfcTransition>>(StringComparer.Ordinal);
        foreach (SfcTransition transition in recipe.Transitions.OrderBy(static t => t.Priority))
        {
            if (!_outgoing.TryGetValue(transition.Source, out List<SfcTransition>? list))
            {
                _outgoing[transition.Source] = list = [];
            }

            list.Add(transition);
        }
    }

    /// <summary>Gets the recipe.</summary>
    public SfcRecipe Recipe { get; }

    /// <summary>Gets the SHA-256 of the recipe document in lower-case hex; jobs pin the recipe by it (D-44).</summary>
    public string RecipeHash { get; }

    /// <summary>Gets the name of the initial step.</summary>
    public string InitialStep { get; }

    /// <summary>Gets the actions of a step in recipe order.</summary>
    /// <param name="step">The step name.</param>
    /// <returns>The actions; empty for a step without actions.</returns>
    public IReadOnlyList<SfcActionBlock> ActionsOf(string step) =>
        _actionsByStep.TryGetValue(step, out List<SfcActionBlock>? list) ? list : [];

    /// <summary>Finds an action by name.</summary>
    /// <param name="name">The action name.</param>
    /// <returns>The action, or <see langword="null"/>.</returns>
    public SfcActionBlock? FindAction(string name) => _actions.GetValueOrDefault(name);

    /// <summary>Finds a divergence construct by name.</summary>
    /// <param name="name">The name; a step name gives <see langword="null"/>.</param>
    /// <returns>The divergence, or <see langword="null"/>.</returns>
    public SfcDivergence? FindDivergence(string name) => _divergences.GetValueOrDefault(name);

    /// <summary>Tells where a step sits in a divergence sequence.</summary>
    /// <param name="step">The step name.</param>
    /// <returns>The position, or <see langword="null"/> for a top-level step.</returns>
    public SfcSequencePosition? SequenceOf(string step) => _sequences.TryGetValue(step, out SfcSequencePosition position) ? position : null;

    /// <summary>Gets all transitions that leave a step or divergence, ordered by priority (lowest first), then recipe order.</summary>
    /// <param name="source">The step or divergence name.</param>
    /// <returns>The transitions.</returns>
    public IReadOnlyList<SfcTransition> Outgoing(string source) => _outgoing.TryGetValue(source, out List<SfcTransition>? list) ? list : [];

    /// <summary>Gets the transitions from a divergence to the first steps of its sequences (the selection of a sequence).</summary>
    /// <param name="divergence">The divergence.</param>
    /// <returns>The entry transitions, by priority.</returns>
    public IReadOnlyList<SfcTransition> SelectionTransitions(SfcDivergence divergence) =>
        [.. Outgoing(divergence.Name).Where(t => IsFirstStep(divergence, t.Target))];

    /// <summary>Gets the transitions that leave a divergence after its sequences are done, by priority.</summary>
    /// <param name="divergence">The divergence.</param>
    /// <returns>The exit transitions.</returns>
    public IReadOnlyList<SfcTransition> ConvergenceTransitions(SfcDivergence divergence) =>
        [.. Outgoing(divergence.Name).Where(t => !IsFirstStep(divergence, t.Target))];

    internal static bool IsFirstStep(SfcDivergence divergence, string step) =>
        divergence.Sequences.Any(sequence => sequence.Count > 0 && string.Equals(sequence[0], step, StringComparison.Ordinal));

    /// <summary>Validates a recipe document and compiles it; a station refuses what its capabilities do not cover.</summary>
    /// <param name="recipe">The recipe JSON (UTF-8).</param>
    /// <param name="capabilities">The station's capabilities.</param>
    /// <returns>The graph, or the build errors.</returns>
    public static SfcBuildResult Build(ReadOnlyMemory<byte> recipe, SfcCapabilities capabilities)
    {
        ArgumentNullException.ThrowIfNull(capabilities);
        SfcRecipe? parsed;
        try
        {
            parsed = JsonSerializer.Deserialize(recipe.Span, SfcJsonContext.Default.SfcRecipe);
        }
        catch (JsonException ex)
        {
            return Failed(new SfcBuildError("SFC000", $"The recipe is not a valid SFC recipe document (at '{ex.Path}')."));
        }

        if (parsed is null)
        {
            return Failed(new SfcBuildError("SFC000", "The recipe is empty."));
        }

        return Build(parsed, Convert.ToHexStringLower(SHA256.HashData(recipe.Span)), capabilities);
    }

    /// <summary>Validates an already parsed recipe.</summary>
    /// <param name="recipe">The recipe.</param>
    /// <param name="recipeHash">The content hash the jobs pin.</param>
    /// <param name="capabilities">The station's capabilities.</param>
    /// <returns>The graph, or the build errors.</returns>
    public static SfcBuildResult Build(SfcRecipe recipe, string recipeHash, SfcCapabilities capabilities)
    {
        ArgumentNullException.ThrowIfNull(recipe);
        ArgumentException.ThrowIfNullOrEmpty(recipeHash);
        ArgumentNullException.ThrowIfNull(capabilities);
        List<SfcBuildError> errors = SfcGraphValidator.Validate(recipe, capabilities, out string? initial);
        return errors.Count > 0 || initial is null
            ? new SfcBuildResult(null, errors)
            : new SfcBuildResult(new SfcGraph(recipe, recipeHash, initial), []);
    }

    private static SfcBuildResult Failed(SfcBuildError error) => new(null, [error]);
}
