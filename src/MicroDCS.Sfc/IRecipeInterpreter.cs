namespace MicroDCS.Sfc;

/// <summary>An error found while building a recipe graph; the message names fields, never payload values.</summary>
/// <param name="Code">A stable code such as <c>SFC001</c>.</param>
/// <param name="Message">The human-readable text.</param>
public sealed record SfcBuildError(string Code, string Message);

/// <summary>The result of building a recipe: a graph or the errors that refuse it.</summary>
/// <param name="Graph">The compiled graph, or <see langword="null"/> when there are errors.</param>
/// <param name="Errors">The build errors; empty when the graph was built.</param>
public sealed record SfcBuildResult(SfcGraph? Graph, IReadOnlyList<SfcBuildError> Errors)
{
    /// <summary>Tells whether the recipe was accepted.</summary>
    public bool Succeeded => Graph is not null && Errors.Count == 0;
}

/// <summary>
/// Interprets one recipe format, selected by the work master's <c>dataschema</c> URI (D-17). The step function is
/// <c>(graph, state, snapshot, event) -> (state, continuation, commands)</c>: pure, no clock, no I/O.
/// </summary>
public interface IRecipeInterpreter
{
    /// <summary>Gets the <c>dataschema</c> URI of the recipe format this interpreter handles.</summary>
    string DataSchema { get; }

    /// <summary>Tells whether this interpreter runs recipes of a <c>dataschema</c> URI: the current one and earlier versions it still reads.</summary>
    /// <param name="dataSchema">The <c>dataschema</c> of the work master.</param>
    /// <returns><see langword="true"/> when the recipe is for this interpreter.</returns>
    bool Accepts(string? dataSchema);

    /// <summary>Validates a recipe once and compiles it; capabilities refuse what the station cannot run.</summary>
    /// <param name="recipe">The recipe document.</param>
    /// <param name="capabilities">The station's capabilities.</param>
    /// <returns>The graph or the build errors.</returns>
    SfcBuildResult Build(ReadOnlyMemory<byte> recipe, SfcCapabilities capabilities);

    /// <summary>Evolves the chart of a job by one event: transitions are cleared, steps become active or inactive, work is started or cancelled.</summary>
    /// <param name="graph">The graph the job's recipe hash names.</param>
    /// <param name="state">The state the host loaded.</param>
    /// <param name="snapshot">The host's view (attempt default, live work).</param>
    /// <param name="input">The event.</param>
    /// <returns>The next state, the continuation and the commands to apply in order.</returns>
    SfcStepResult Evolve(SfcGraph graph, SfcState state, SfcSnapshot snapshot, SfcEvent input);
}
