using MicroDCS.Valkey;
using MicroDCS.Valkey.Sfc;
using MicroDCS.Valkey.Streams;

namespace MicroDCS.Sfc.Hosting;

/// <summary>Operator actions on running jobs, for the write group of the MCP tools (D-19). Each appends a work entry; the normal path carries it out.</summary>
public interface ISfcOperations
{
    /// <summary>Queues a tick, as if the sweep had found the job due.</summary>
    /// <param name="jobId">The job id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="false"/> when the job has no SFC state.</returns>
    ValueTask<bool> TickAsync(string jobId, CancellationToken cancellationToken);

    /// <summary>Queues a new dispatch of one open action.</summary>
    /// <param name="jobId">The job id.</param>
    /// <param name="action">The action name.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="false"/> when the job has no SFC state.</returns>
    ValueTask<bool> RedispatchAsync(string jobId, string action, CancellationToken cancellationToken);

    /// <summary>Queues the end of the recipe; the job order is aborted when the recipe has ended.</summary>
    /// <param name="jobId">The job id.</param>
    /// <param name="reason">The reason.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="false"/> when the job has no SFC state.</returns>
    ValueTask<bool> AbortAsync(string jobId, string? reason, CancellationToken cancellationToken);
}

/// <summary>The default <see cref="ISfcOperations"/>.</summary>
internal sealed class SfcOperations : ISfcOperations
{
    private readonly SfcStateDao _states;
    private readonly ValkeyStreams _streams;
    private readonly KeySchema _keys;

    public SfcOperations(SfcStateDao states, ValkeyStreams streams, ValkeyContext context)
    {
        _states = states;
        _streams = streams;
        _keys = context.Keys;
    }

    public ValueTask<bool> TickAsync(string jobId, CancellationToken cancellationToken) =>
        AppendAsync(jobId, SfcWork.Tick, [(SfcWork.OutageField, "0")], cancellationToken);

    public ValueTask<bool> RedispatchAsync(string jobId, string action, CancellationToken cancellationToken) =>
        AppendAsync(jobId, SfcWork.Resume, [(SfcWork.OnlyField, action)], cancellationToken);

    public ValueTask<bool> AbortAsync(string jobId, string? reason, CancellationToken cancellationToken) =>
        AppendAsync(jobId, SfcWork.Cancel, [(SfcWork.ReasonField, string.IsNullOrWhiteSpace(reason) ? "operator abort" : reason)], cancellationToken);

    private async ValueTask<bool> AppendAsync(string jobId, string action, (string, string)[] extra, CancellationToken cancellationToken)
    {
        var state = await _states.GetAsync(jobId, cancellationToken).ConfigureAwait(false);
        if (state is null)
        {
            return false;
        }

        await _streams.AppendAsync(_keys.SfcWork(state.Scope), SfcWork.NudgeKind, state.Scope, SfcWork.Entry(jobId, state.Scope, action, extra), cancellationToken).ConfigureAwait(false);
        return true;
    }
}
