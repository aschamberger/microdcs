using System.Text.Json;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Sfc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Sfc.Hosting;

/// <summary>A step of a job cannot be done with what is stored, so repeating it at once would not help.</summary>
internal sealed class SfcStepFailedException : InvalidOperationException
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

/// <summary>What happened to an event.</summary>
internal enum SfcRunStatus
{
    /// <summary>The state and its work were stored.</summary>
    Applied,

    /// <summary>The core absorbed or rejected the event; nothing was written.</summary>
    Unchanged,

    /// <summary>The job has no state.</summary>
    NoState,
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

    public async ValueTask<SfcRunResult> RunAsync(string jobId, SfcEvent input, string operation, Func<SfcState, IReadOnlyList<string>>? liveWork, CancellationToken cancellationToken)
    {
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

            if (await StoreAsync(result, document.Version, operation, started: false, cancellationToken).ConfigureAwait(false))
            {
                return new SfcRunResult(SfcRunStatus.Applied, result.State);
            }
        }

        throw new SfcStepFailedException($"The SFC state of job '{jobId}' kept changing.");
    }

    /// <summary>Stores the first state of a job. <paramref name="expectedVersion"/> is 0, or the version of the ended state a re-stored job id replaces.</summary>
    public async ValueTask<bool> StartAsync(SfcGraph graph, SfcState initial, long expectedVersion, long at, string operation, CancellationToken cancellationToken)
    {
        var snapshot = new SfcSnapshot { DefaultMaxAttempts = _options.MaxAttempts };
        var result = _graphs.Interpreter.Evolve(graph, initial, snapshot, new SfcStart { At = at });
        return await StoreAsync(result, expectedVersion, operation, started: true, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<bool> StoreAsync(SfcStepResult result, long version, string operation, bool started, CancellationToken cancellationToken)
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
