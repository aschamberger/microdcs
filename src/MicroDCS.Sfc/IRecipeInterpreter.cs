namespace MicroDCS.Sfc;

/// <summary>An error found while building a recipe graph; the message names fields, never payload values.</summary>
/// <param name="Code">A stable code such as <c>SFC001</c>.</param>
/// <param name="Message">The human-readable text.</param>
public sealed record SfcBuildError(string Code, string Message);

/// <summary>The result of building a recipe: a graph or the errors that refuse it.</summary>
/// <param name="Graph">The compiled graph, or <see langword="null"/> when there are errors.</param>
/// <param name="Errors">The build errors; empty when the graph was built.</param>
public sealed record SfcBuildResult(object? Graph, IReadOnlyList<SfcBuildError> Errors)
{
    /// <summary>Tells whether the recipe was accepted.</summary>
    public bool Succeeded => Graph is not null && Errors.Count == 0;
}

/// <summary>
/// Interprets one recipe format, selected by the work master's <c>dataschema</c> URI (D-17). The step function
/// <c>(graph, state, snapshot, event) -> (state, continuation, commands)</c> is added with the core in S15.
/// </summary>
public interface IRecipeInterpreter
{
    /// <summary>Gets the <c>dataschema</c> URI of the recipe format this interpreter handles.</summary>
    string DataSchema { get; }

    /// <summary>Validates a recipe once and compiles it; capabilities refuse what the station cannot run.</summary>
    /// <param name="recipe">The recipe document.</param>
    /// <param name="capabilities">The station's capabilities.</param>
    /// <returns>The graph or the build errors.</returns>
    SfcBuildResult Build(ReadOnlyMemory<byte> recipe, SfcCapabilities capabilities);
}
