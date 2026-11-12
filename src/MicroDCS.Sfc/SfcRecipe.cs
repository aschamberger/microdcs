using System.Text.Json;
using System.Text.Json.Serialization;

namespace MicroDCS.Sfc;

/// <summary>
/// The recipe document (<c>dataschema</c> <see cref="SfcInterpreter.RecipeDataSchema"/>). Wire names are the PascalCase names of
/// the Python schema, so existing recipes stay valid (D-44). Enumerated values are strings so that an unknown value is a build
/// error with a message, not a parse failure.
/// </summary>
public sealed record SfcRecipe
{
    /// <summary>Gets all steps in the recipe.</summary>
    [JsonPropertyName("Steps")]
    public required IReadOnlyList<SfcStep> Steps { get; init; }

    /// <summary>Gets all transitions between steps and divergence constructs.</summary>
    [JsonPropertyName("Transitions")]
    public required IReadOnlyList<SfcTransition> Transitions { get; init; }

    /// <summary>Gets all action blocks (the actions of the steps).</summary>
    [JsonPropertyName("Actions")]
    public required IReadOnlyList<SfcActionBlock> Actions { get; init; }

    /// <summary>Gets the divergences (selection or simultaneous) with their sequences, if any. Wire name <c>Branches</c>.</summary>
    [JsonPropertyName("Branches")]
    public IReadOnlyList<SfcDivergence>? Divergences { get; init; }
}

/// <summary>A named step; the states of the chart.</summary>
public sealed record SfcStep
{
    /// <summary>Gets the unique step identifier.</summary>
    [JsonPropertyName("Name")]
    public required string Name { get; init; }

    /// <summary>Gets a value indicating whether this is the initial step (exactly one per recipe).</summary>
    [JsonPropertyName("Initial")]
    public bool Initial { get; init; }
}

/// <summary>A directed edge between steps or divergence constructs.</summary>
public sealed record SfcTransition
{
    /// <summary>Gets the source step or divergence name.</summary>
    [JsonPropertyName("Source")]
    public required string Source { get; init; }

    /// <summary>Gets the target step or divergence name.</summary>
    [JsonPropertyName("Target")]
    public required string Target { get; init; }

    /// <summary>Gets the outcome name the transition reacts to, or <see cref="SfcOutcome.Default"/> (D-17).</summary>
    [JsonPropertyName("Condition")]
    public required string Condition { get; init; }

    /// <summary>Gets the priority among the transitions of one source; lower is tried first.</summary>
    [JsonPropertyName("Priority")]
    public int Priority { get; init; }
}

/// <summary>An action block: one action of a step with its qualifier (IEC 61131-3). Wire name <c>Actions</c>.</summary>
public sealed record SfcActionBlock
{
    /// <summary>Gets the action identifier, unique in the recipe.</summary>
    [JsonPropertyName("Name")]
    public required string Name { get; init; }

    /// <summary>Gets the step this action belongs to.</summary>
    [JsonPropertyName("Step")]
    public required string Step { get; init; }

    /// <summary>Gets the IEC 61131-3 qualifier; only <c>N</c> is accepted (D-44).</summary>
    [JsonPropertyName("Qualifier")]
    public required string Qualifier { get; init; }

    /// <summary>Gets the interaction pattern: <c>push_command</c> or <c>pull_event</c>.</summary>
    [JsonPropertyName("Interaction")]
    public required string Interaction { get; init; }

    /// <summary>Gets the type id of the outgoing command or the expected incoming event.</summary>
    [JsonPropertyName("TypeId")]
    public required string TypeId { get; init; }

    /// <summary>Gets the time in seconds after which an attempt expires (D-18).</summary>
    [JsonPropertyName("TimeoutSeconds")]
    public required int TimeoutSeconds { get; init; }

    /// <summary>Gets the maximum number of dispatch attempts of a push command; the host default applies when absent (D-44).</summary>
    [JsonPropertyName("MaxAttempts")]
    public int? MaxAttempts { get; init; }

    /// <summary>Gets the parameters handed to the action.</summary>
    [JsonPropertyName("Parameters")]
    public IReadOnlyDictionary<string, JsonElement>? Parameters { get; init; }

    /// <summary>Gets which incoming events a pull event accepts besides its type (R-1); absent means every event of the type.</summary>
    [JsonPropertyName("Selector")]
    public SfcSelector? Selector { get; init; }

    /// <summary>
    /// Gets the command that holds the running work of a push command when the job is paused (ISA-88 hold logic, D-57). Set together with
    /// <see cref="Restart"/>; absent means the work runs to its end during a pause.
    /// </summary>
    [JsonPropertyName("Hold")]
    public SfcEquipmentCommand? Hold { get; init; }

    /// <summary>Gets the command that restarts held work when the job is resumed; its response completes the action (D-57).</summary>
    [JsonPropertyName("Restart")]
    public SfcEquipmentCommand? Restart { get; init; }
}

/// <summary>A hold or restart command of a push-command action (D-57): sent with the action key of the action, so the equipment knows which work it means.</summary>
public sealed record SfcEquipmentCommand
{
    /// <summary>Gets the type id of the command.</summary>
    [JsonPropertyName("TypeId")]
    public required string TypeId { get; init; }

    /// <summary>Gets the time in seconds the equipment has to answer. The command is sent once.</summary>
    [JsonPropertyName("TimeoutSeconds")]
    public required int TimeoutSeconds { get; init; }

    /// <summary>Gets the parameters handed to the command.</summary>
    [JsonPropertyName("Parameters")]
    public IReadOnlyDictionary<string, JsonElement>? Parameters { get; init; }
}

/// <summary>
/// Narrows the events a pull-event action accepts (R-1). Set fields must match exactly; <see cref="JobIdPlaceholder"/> in a value stands for the job id,
/// because a recipe is shared by many jobs and a literal alone cannot tell them apart.
/// </summary>
public sealed record SfcSelector
{
    /// <summary>The placeholder for the job id.</summary>
    public const string JobIdPlaceholder = "{jobId}";

    /// <summary>Gets the value the event subject must have.</summary>
    [JsonPropertyName("Subject")]
    public string? Subject { get; init; }

    /// <summary>Gets the value the event correlation id must have.</summary>
    [JsonPropertyName("CorrelationId")]
    public string? CorrelationId { get; init; }

    /// <summary>Tells whether an event matches for a job.</summary>
    /// <param name="jobId">The job id.</param>
    /// <param name="subject">The event subject.</param>
    /// <param name="correlationId">The event correlation id.</param>
    /// <returns><see langword="true"/> when every set field matches.</returns>
    public bool Matches(string jobId, string? subject, string? correlationId) =>
        FieldMatches(Subject, jobId, subject) && FieldMatches(CorrelationId, jobId, correlationId);

    private static bool FieldMatches(string? expected, string jobId, string? actual) =>
        expected is null || string.Equals(expected.Replace(JobIdPlaceholder, jobId, StringComparison.Ordinal), actual, StringComparison.Ordinal);
}

/// <summary>
/// A divergence with its sequences and the convergence that closes them (IEC 61131-3 "selection divergence" and "simultaneous
/// divergence"): selection takes one sequence, simultaneous runs all of them. The wire name of the sequences is <c>Branches</c>.
/// </summary>
public sealed record SfcDivergence
{
    /// <summary>Gets the divergence identifier, used as source and target in transitions.</summary>
    [JsonPropertyName("Name")]
    public required string Name { get; init; }

    /// <summary>Gets the kind: <c>selection</c> or <c>simultaneous</c>. Wire name <c>Type</c>.</summary>
    [JsonPropertyName("Type")]
    public required string Kind { get; init; }

    /// <summary>Gets the sequences; each inner list is a sequence of step names.</summary>
    [JsonPropertyName("Branches")]
    public required IReadOnlyList<IReadOnlyList<string>> Sequences { get; init; }
}

/// <summary>Constants for the enumerated recipe values.</summary>
public static class SfcNames
{
    /// <summary>Interaction: the engine sends a command.</summary>
    public const string PushCommand = "push_command";

    /// <summary>Interaction: the engine waits for an incoming event.</summary>
    public const string PullEvent = "pull_event";

    /// <summary>Divergence type: one sequence is taken.</summary>
    public const string Selection = "selection";

    /// <summary>Divergence type: all sequences run in parallel.</summary>
    public const string Simultaneous = "simultaneous";
}
