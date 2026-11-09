using System.Text.Json.Serialization;

namespace MicroDCS.Sfc;

/// <summary>Where a job is in its life.</summary>
public enum SfcJobStatus
{
    /// <summary>Created, not started.</summary>
    NotStarted,

    /// <summary>Running; steps are active.</summary>
    Running,

    /// <summary>All steps are done.</summary>
    Completed,

    /// <summary>Ended by a fault verdict.</summary>
    Faulted,

    /// <summary>Ended by a cancel request.</summary>
    Cancelled,
}

/// <summary>State of one action within a running job.</summary>
public enum SfcActionStatus
{
    /// <summary>Its step is not active yet or the dispatch is still to be done.</summary>
    Pending,

    /// <summary>A push command is out and unanswered.</summary>
    Dispatched,

    /// <summary>A pull event is awaited.</summary>
    Waiting,

    /// <summary>Done with an outcome.</summary>
    Completed,

    /// <summary>Done with a reserved outcome (<c>timeout</c> or <c>error</c>).</summary>
    Failed,
}

/// <summary>The execution record of one action.</summary>
public sealed record SfcActionState
{
    /// <summary>Gets the action name from the recipe.</summary>
    [JsonPropertyName("name")]
    public required string Name { get; init; }

    /// <summary>Gets the type id copied from the recipe.</summary>
    [JsonPropertyName("typeId")]
    public required string TypeId { get; init; }

    /// <summary>Gets the status.</summary>
    [JsonPropertyName("status")]
    public SfcActionStatus Status { get; init; }

    /// <summary>Gets the pass of the chart: counts how often the step of this action was entered (loops enter a step again).</summary>
    [JsonPropertyName("round")]
    public int Round { get; init; }

    /// <summary>Gets the number of dispatch attempts so far.</summary>
    [JsonPropertyName("attempt")]
    public int Attempt { get; init; }

    /// <summary>Gets the event id of the latest command attempt.</summary>
    [JsonPropertyName("commandId")]
    public string? CommandId { get; init; }

    /// <summary>Gets the absolute deadline in epoch milliseconds while the action is open.</summary>
    [JsonPropertyName("deadline")]
    public long? Deadline { get; init; }

    /// <summary>Gets the outcome once the action is done.</summary>
    [JsonPropertyName("outcome")]
    public string? Outcome { get; init; }

    /// <summary>Gets the error text of an <c>error</c> outcome; data, never part of a decision.</summary>
    [JsonPropertyName("error")]
    public string? Error { get; init; }

    /// <summary>Gets a value indicating whether the action waits for a response or an event.</summary>
    [JsonIgnore]
    public bool IsOpen => Status is SfcActionStatus.Dispatched or SfcActionStatus.Waiting;

    /// <summary>Gets a value indicating whether the action is done.</summary>
    [JsonIgnore]
    public bool IsDone => Status is SfcActionStatus.Completed or SfcActionStatus.Failed;
}

/// <summary>
/// The execution state of one job: an immutable, versioned document the host stores verbatim (one value per job). The core keeps
/// no other memory between calls, so replaying the same events against the same state gives byte-identical results.
/// </summary>
public sealed record SfcState
{
    /// <summary>The current format number; additive changes bump <see cref="FormatMinor"/>.</summary>
    public const int CurrentFormat = 1;

    /// <summary>The current minor format number.</summary>
    public const int CurrentFormatMinor = 1;

    /// <summary>Gets the major format number.</summary>
    [JsonPropertyName("format")]
    public int Format { get; init; } = CurrentFormat;

    /// <summary>Gets the minor format number.</summary>
    [JsonPropertyName("formatMinor")]
    public int FormatMinor { get; init; } = CurrentFormatMinor;

    /// <summary>Gets the job id.</summary>
    [JsonPropertyName("jobId")]
    public required string JobId { get; init; }

    /// <summary>Gets the scope of the job.</summary>
    [JsonPropertyName("scope")]
    public required string Scope { get; init; }

    /// <summary>Gets the content hash of the pinned recipe (D-44).</summary>
    [JsonPropertyName("recipeHash")]
    public required string RecipeHash { get; init; }

    /// <summary>Gets the job status.</summary>
    [JsonPropertyName("status")]
    public SfcJobStatus Status { get; init; }

    /// <summary>Gets the current top-level step, or the divergence name while a divergence runs.</summary>
    [JsonPropertyName("current")]
    public string? Current { get; init; }

    /// <summary>Gets the steps that are active now (several inside a simultaneous divergence).</summary>
    [JsonPropertyName("activeSteps")]
    public IReadOnlyList<string> ActiveSteps { get; init; } = [];

    /// <summary>Gets the execution records of all actions, by action name.</summary>
    [JsonPropertyName("actions")]
    public IReadOnlyDictionary<string, SfcActionState> Actions { get; init; } = new SortedDictionary<string, SfcActionState>(StringComparer.Ordinal);

    /// <summary>Gets the counter that makes command ids a pure function of the state.</summary>
    [JsonPropertyName("sequence")]
    public long Sequence { get; init; }

    /// <summary>Gets the time the job started in epoch milliseconds (additive, format 1.1).</summary>
    [JsonPropertyName("startedAt")]
    public long? StartedAt { get; init; }

    /// <summary>Gets the fault code when the job ended with a fault.</summary>
    [JsonPropertyName("faultCode")]
    public string? FaultCode { get; init; }

    /// <summary>Gets the fault message when the job ended with a fault; names and identifiers only.</summary>
    [JsonPropertyName("faultMessage")]
    public string? FaultMessage { get; init; }

    /// <summary>Gets the earliest open deadline in epoch milliseconds, or <see langword="null"/> when none is open.</summary>
    [JsonIgnore]
    public long? NextDeadline
    {
        get
        {
            if (IsEnded)
            {
                return null;
            }

            long? next = null;
            foreach (SfcActionState action in Actions.Values)
            {
                if (action.IsOpen && action.Deadline is long deadline && (next is null || deadline < next))
                {
                    next = deadline;
                }
            }

            return next;
        }
    }

    /// <summary>Gets a value indicating whether the job has ended.</summary>
    [JsonIgnore]
    public bool IsEnded => Status is SfcJobStatus.Completed or SfcJobStatus.Faulted or SfcJobStatus.Cancelled;

    /// <summary>Creates the state of a job that has not started.</summary>
    /// <param name="graph">The graph of the pinned recipe.</param>
    /// <param name="jobId">The job id.</param>
    /// <param name="scope">The scope.</param>
    /// <returns>The initial state; send <see cref="SfcStart"/> to begin.</returns>
    public static SfcState Create(SfcGraph graph, string jobId, string scope)
    {
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        ArgumentException.ThrowIfNullOrEmpty(scope);
        var actions = new SortedDictionary<string, SfcActionState>(StringComparer.Ordinal);
        foreach (SfcActionBlock action in graph.Recipe.Actions)
        {
            actions[action.Name] = new SfcActionState { Name = action.Name, TypeId = action.TypeId };
        }

        return new SfcState { JobId = jobId, Scope = scope, RecipeHash = graph.RecipeHash, Actions = actions };
    }

    /// <summary>Tells whether an active pull-event action awaits an event of the type (host helper to pick the job).</summary>
    /// <param name="typeId">The event type id.</param>
    /// <returns><see langword="true"/> when an action waits for it.</returns>
    public bool IsWaitingFor(string typeId) =>
        Status == SfcJobStatus.Running
        && Actions.Values.Any(a => a.Status == SfcActionStatus.Waiting && string.Equals(a.TypeId, typeId, StringComparison.Ordinal));
}
