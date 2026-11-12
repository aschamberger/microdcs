namespace MicroDCS.Sfc;

/// <summary>
/// The SFC interpreter (D-17, D-44): a pure function <c>(graph, state, snapshot, event) -> (state, continuation, commands)</c>.
/// It reads no clock, does no I/O and holds no state, so one instance is shared by all jobs and threads.
/// </summary>
public sealed class SfcInterpreter : IRecipeInterpreter
{
    /// <summary>The <c>dataschema</c> URI of the current recipe format (v1.1.0 adds hold and restart commands, D-57).</summary>
    public const string RecipeDataSchema = "https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.1.0/";

    /// <summary>The <c>dataschema</c> URI of the first recipe format. Still accepted: v1.1.0 only adds optional fields.</summary>
    public const string RecipeDataSchemaV10 = "https://aschamberger.github.io/schemas/microdcs/sfc-recipe/v1.0.0/";

    /// <summary>The hard limit of step entries per evolution; reaching it returns a fault (D-44).</summary>
    public const int MaxStepEntriesPerEvolution = 10_000;

    /// <inheritdoc/>
    public string DataSchema => RecipeDataSchema;

    /// <inheritdoc/>
    public bool Accepts(string? dataSchema) =>
        string.Equals(dataSchema, RecipeDataSchema, StringComparison.Ordinal) || string.Equals(dataSchema, RecipeDataSchemaV10, StringComparison.Ordinal);

    /// <inheritdoc/>
    public SfcBuildResult Build(ReadOnlyMemory<byte> recipe, SfcCapabilities capabilities) => SfcGraph.Build(recipe, capabilities);

    /// <inheritdoc/>
    public SfcStepResult Evolve(SfcGraph graph, SfcState state, SfcSnapshot snapshot, SfcEvent input)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentNullException.ThrowIfNull(input);
        if (!string.Equals(state.RecipeHash, graph.RecipeHash, StringComparison.Ordinal))
        {
            return new SfcStepResult
            {
                State = state,
                Continuation = new SfcDefer(),
                Disposition = SfcDisposition.Rejected,
                Reason = "The state is pinned to a different recipe than the graph.",
            };
        }

        return new SfcEvolution(graph, state, snapshot, input.At).Run(input);
    }
}
