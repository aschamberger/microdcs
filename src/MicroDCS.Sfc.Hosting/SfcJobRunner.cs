using System.Text.Json;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Sfc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Sfc.Hosting;

/// <summary>A step of a job cannot be done with what is stored, so repeating it at once would not help.</summary>
internal class SfcStepFailedException : InvalidOperationException
{
    public SfcStepFailedException()
    {
    }

    public SfcStepFailedException(string message)
        : base(message)
    {
    }

    public SfcStepFailedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>Other instances kept changing the job. Unlike the other failures this one is worth repeating.</summary>
internal sealed class SfcContentionException : SfcStepFailedException
{
    public SfcContentionException()
    {
    }

    public SfcContentionException(string message)
        : base(message)
    {
    }

    public SfcContentionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>What happened to an event.</summary>
internal enum SfcRunStatus
{
    /// <summary>The state and its work were stored.</summary>
    Applied,

    /// <summary>The core absorbed or rejected the event; nothing was written.</summary>
    Unchanged,

    /// <summary>The job has no state.</summary>
    NoState,

    /// <summary>Another instance applied the same event, to this job or another one.</summary>
    AlreadyClaimed,
}

/// <summary>The result of running an event.</summary>
/// <param name="Status">What happened.</param>
/// <param name="State">The state after the event, when there is one.</param>
internal sealed record SfcRunResult(SfcRunStatus Status, SfcState? State);

/// <summary>
/// One evolution of a job: load the state, call the pure core, store the new state with its work in one compare-and-set (D-17, D-44). A lost race
/// re-reads and evaluates again, so the core always decides on the latest state.
/// </summary>
internal sealed class SfcJobRunner
{
    private const int MaxAttempts = 8;

    private readonly SfcStateDao _states;
    private readonly SfcGraphs _graphs;
    private readonly SfcHostOptions _options;
    private readonly ILogger<SfcJobRunner> _logger;

    public SfcJobRunner(SfcStateDao states, SfcGraphs graphs, IOptions<SfcHostOptions> options, ILogger<SfcJobRunner> logger)
    {
        _states = states;
        _graphs = graphs;
        _options = options.Value;
        _logger = logger;
    }

    public static SfcState Parse(StoredDocument document) =>
        JsonSerializer.Deserialize(document.Document, SfcJsonContext.Default.SfcState)
        ?? throw new SfcStepFailedException($"The SFC state of job '{document.Id}' is empty.");

    public static string Serialize(SfcState state) => JsonSerializer.Serialize(state, SfcJsonContext.Default.SfcState);

    public ValueTask<SfcRunResult> RunAsync(string jobId, SfcEvent input, string operation, Func<SfcState, IReadOnlyList<string>>? liveWork, CancellationToken cancellationToken) =>
        RunAsync(jobId, input, operation, liveWork, claimedEvent: null, cancellationToken);

    /// <summary>Runs an event; <paramref name="claimedEvent"/> names the event to mark as applied in the same write (R-1).</summary>
    public async ValueTask<SfcRunResult> RunAsync(string jobId, SfcEvent input, string operation, Func<SfcState, IReadOnlyList<string>>? liveWork, string? claimedEvent, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        for (var attempt = 0; attempt < MaxAttempts; attempt++)
        {
            var document = await _states.GetAsync(jobId, cancellationToken).ConfigureAwait(false);
            if (document is null)
            {
                Log.NoState(_logger, jobId, input.GetType().Name);
                return new SfcRunResult(SfcRunStatus.NoState, null);
            }

            // The same event again after its write happened (a crash before the acknowledgement): applying it twice would, for one, move deadlines twice.
            if (string.Equals(document.Operation, operation, StringComparison.Ordinal))
            {
                return new SfcRunResult(SfcRunStatus.Unchanged, Parse(document));
            }

            var state = Parse(document);
            var graph = await _graphs.GetAsync(state.RecipeHash, cancellationToken).ConfigureAwait(false);
            var snapshot = new SfcSnapshot { DefaultMaxAttempts = _options.MaxAttempts, LiveWork = liveWork?.Invoke(state) ?? [] };
            var result = _graphs.Interpreter.Evolve(graph, state, snapshot, input);
            if (result.Disposition != SfcDisposition.Applied)
            {
                Report(result, jobId, input);
                return new SfcRunResult(SfcRunStatus.Unchanged, state);
            }

            if (await StoreAsync(graph, result, document.Version, operation, started: false, input.At, claimedEvent, cancellationToken).ConfigureAwait(false))
            {
                return new SfcRunResult(SfcRunStatus.Applied, result.State);
            }

            if (claimedEvent is not null && await _states.IsEventClaimedAsync(state.Scope, claimedEvent, cancellationToken).ConfigureAwait(false))
            {
                return new SfcRunResult(SfcRunStatus.AlreadyClaimed, state);
            }
        }

        throw new SfcContentionException($"The SFC state of job '{jobId}' kept changing.");
    }

    /// <summary>Stores the first state of a job. <paramref name="expectedVersion"/> is 0, or the version of the ended state a re-stored job id replaces.</summary>
    public async ValueTask<bool> StartAsync(SfcGraph graph, SfcState initial, long expectedVersion, long at, string operation, CancellationToken cancellationToken)
    {
        var snapshot = new SfcSnapshot { DefaultMaxAttempts = _options.MaxAttempts };
        var result = _graphs.Interpreter.Evolve(graph, initial, snapshot, new SfcStart { At = at });
        return await StoreAsync(graph, result, expectedVersion, operation, started: true, at, claimedEvent: null, cancellationToken).ConfigureAwait(false);
    }

    private static SfcWaitChanges? WaitChanges(SfcGraph graph, SfcStepResult result, long at, string? claimedEvent)
    {
        if (graph.PullTypeIds.Count == 0)
        {
            return null;
        }

        // A wait that begins in this step goes to the end of the queue, even when the job already waited for the type in the step before.
        var waiting = result.State.WaitingTypeIds;
        var begun = result.Commands.OfType<StartWork>().Where(static c => c.Work == SfcWorkKind.AwaitEvent).Select(static c => c.TypeId);
        var remove = graph.PullTypeIds.Except(waiting, StringComparer.Ordinal).Union(begun, StringComparer.Ordinal);
        return new SfcWaitChanges([.. waiting.Select(t => (t, at))], [.. remove], claimedEvent);
    }

    private async ValueTask<bool> StoreAsync(SfcGraph graph, SfcStepResult result, long version, string operation, bool started, long at, string? claimedEvent, CancellationToken cancellationToken)
    {
        var state = result.State;
        var written = await _states.CompareAndSetAsync(
            state.JobId,
            state.Scope,
            Serialize(state),
            version,
            state.NextDeadline,
            !state.IsEnded,
            SfcWork.EntriesOf(result, started),
            operation,
            WaitChanges(graph, result, at, claimedEvent),
            cancellationToken).ConfigureAwait(false);
        return written.Written;
    }

    private void Report(SfcStepResult result, string jobId, SfcEvent input)
    {
        var reason = result.Reason ?? string.Empty;
        if (result.Disposition == SfcDisposition.Rejected)
        {
            Log.EventRejected(_logger, input.GetType().Name, jobId, reason);
        }
        else
        {
            Log.EventNotApplied(_logger, input.GetType().Name, jobId, result.Disposition, reason);
        }
    }
}
