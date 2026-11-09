using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Globalization;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.Valkey.Streams;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Opc.Ua;

namespace MicroDCS.MachineryJobs.Publishing;

/// <summary>
/// Keeps the job state topics of every scope current from the change streams (D-51). The poller only marks a scope as dirty here and
/// returns, so a stalled broker cannot hold up the other readers of the poller; the publishing runs in its own task. Runs under the
/// <see cref="InstanceRoles.Publisher"/> role, and only the holder of the lease publishes. The cursor advances after the topics are
/// written, so a change is published at least once.
/// </summary>
public sealed class JobOrderPublisher : BackgroundService, IStreamConsumer
{
    private const string EmptyStreamCursor = "0-0";

    private static readonly Meter _meter = new(PipelineTelemetry.Name);
    private static readonly Counter<long> _published = _meter.CreateCounter<long>("microdcs.jobs.publisher.published", "{topic}", "State topics written or removed by the job order publisher.");
    private static readonly Counter<long> _errors = _meter.CreateCounter<long>("microdcs.jobs.publisher.errors", "{round}", "Scopes the job order publisher could not publish in a round; they are tried again.");
    private static readonly Counter<long> _skipped = _meter.CreateCounter<long>("microdcs.jobs.publisher.skipped", "{order}", "Job orders the publisher skipped because no topic can carry them or they cannot be decoded.");
    private static readonly Counter<long> _resyncs = _meter.CreateCounter<long>("microdcs.jobs.publisher.resyncs", "{scope}", "Scopes rebuilt from the stored documents.");

    private readonly IJobStore _store;
    private readonly IJobPublisherStore _state;
    private readonly JobStateTopics _topics;
    private readonly JobPublisherOptions _options;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly TimeProvider _time;
    private readonly ILogger<JobOrderPublisher> _logger;
    private readonly bool _enabled;
    private readonly string _holder;
    private readonly ConcurrentDictionary<string, bool> _dirty = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, DateTimeOffset> _lastResync = new(StringComparer.Ordinal);
    private readonly SemaphoreSlim _signal = new(0);
    private volatile bool _active;
    private DateTimeOffset _leaseRenewedAt;

    [Flags]
    private enum JobTouch
    {
        None = 0,
        Touched = 1,
        Order = 2,
        Removal = 4,
    }

    /// <summary>Initializes a new instance of the <see cref="JobOrderPublisher"/> class.</summary>
    /// <param name="store">The job store.</param>
    /// <param name="state">The publisher state.</param>
    /// <param name="topics">The state topics.</param>
    /// <param name="options">The publisher options.</param>
    /// <param name="core">The core options, for role and instance id.</param>
    /// <param name="lifetime">The host lifetime, to wait for the transports.</param>
    /// <param name="time">The clock.</param>
    /// <param name="logger">The logger.</param>
    public JobOrderPublisher(
        IJobStore store,
        IJobPublisherStore state,
        JobStateTopics topics,
        IOptions<JobPublisherOptions> options,
        IOptions<MicroDcsOptions> core,
        IHostApplicationLifetime lifetime,
        TimeProvider time,
        ILogger<JobOrderPublisher> logger)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(state);
        ArgumentNullException.ThrowIfNull(topics);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(core);
        ArgumentNullException.ThrowIfNull(lifetime);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(logger);
        _store = store;
        _state = state;
        _topics = topics;
        _options = options.Value;
        _lifetime = lifetime;
        _time = time;
        _logger = logger;
        _enabled = core.Value.Roles.HasFlag(InstanceRoles.Publisher);
        _holder = core.Value.InstanceId ?? Guid.NewGuid().ToString("N");
    }

    /// <summary>Gets a value indicating whether this instance holds the lease and publishes.</summary>
    public bool IsActive => _active;

    /// <inheritdoc />
    public string Kind => "changes";

    /// <inheritdoc />
    public ValueTask ScopeDiscoveredAsync(string scope, CancellationToken cancellationToken)
    {
        MarkDirty(scope);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<bool> PollAsync(string scope, CancellationToken cancellationToken)
    {
        MarkDirty(scope);
        return ValueTask.FromResult(false);
    }

    /// <inheritdoc />
    public override Task StartAsync(CancellationToken cancellationToken)
    {
        if (_enabled)
        {
            // Fails the start when the transport of the binding has no state topics (D-27).
            _topics.ResolvePort();
        }

        return base.StartAsync(cancellationToken);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _signal.Dispose();
        base.Dispose();
    }

    /// <summary>Compares two stream ids (<c>milliseconds-sequence</c>).</summary>
    /// <param name="left">The first id.</param>
    /// <param name="right">The second id.</param>
    /// <returns>Negative, zero or positive, as <see cref="string.CompareOrdinal(string, string)"/>.</returns>
    internal static int CompareIds(string left, string right)
    {
        var (leftTime, leftSequence) = Parse(left);
        var (rightTime, rightSequence) = Parse(right);
        var byTime = leftTime.CompareTo(rightTime);
        return byTime != 0 ? byTime : leftSequence.CompareTo(rightSequence);

        static (long Time, long Sequence) Parse(string id)
        {
            var dash = id.IndexOf('-', StringComparison.Ordinal);
            return dash < 0
                ? (long.Parse(id, CultureInfo.InvariantCulture), 0)
                : (long.Parse(id.AsSpan(0, dash), CultureInfo.InvariantCulture), long.Parse(id.AsSpan(dash + 1), CultureInfo.InvariantCulture));
        }
    }

    /// <summary>Takes or renews the lease.</summary>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    internal async ValueTask RenewLeaseAsync(CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow();
        bool held;
        try
        {
            held = await _state.AcquireLeaseAsync(_holder, _options.LeaseTtl, cancellationToken).ConfigureAwait(false);
            if (held)
            {
                _leaseRenewedAt = now;
            }
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            // The lease is still ours until it runs out: a short Valkey failure must not make every scope rebuild.
            Log.PublisherLeaseFailed(_logger, ex);
            held = _active && now - _leaseRenewedAt < _options.LeaseTtl;
        }

        if (held != _active)
        {
            if (held)
            {
                // A new holder cannot know what the last one wrote, so every scope is rebuilt on its first visit.
                _lastResync.Clear();
            }

            Log.PublisherLeaseChanged(_logger, _holder, held);
            _active = held;
        }
    }

    /// <summary>Handles every scope marked as dirty.</summary>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    internal async ValueTask RunRoundAsync(CancellationToken cancellationToken)
    {
        var scopes = TakeDirty();
        if (!_active)
        {
            return;
        }

        foreach (var scope in scopes.TakeWhile(_ => _active))
        {
            try
            {
                if (await PublishScopeAsync(scope, cancellationToken).ConfigureAwait(false))
                {
                    MarkDirty(scope);
                }
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                // The cursor did not move; the next poll marks the scope again.
                _errors.Add(1);
                Log.PublisherScopeFailed(_logger, ex, scope);
            }
        }
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_enabled)
        {
            return;
        }

        await WaitForHostStartAsync(stoppingToken).ConfigureAwait(false);
        var lease = KeepLeaseAsync(stoppingToken);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await _signal.WaitAsync(stoppingToken).ConfigureAwait(false);
                await RunRoundAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // The host is stopping.
        }

        await lease.ConfigureAwait(false);
        await ReleaseLeaseAsync().ConfigureAwait(false);
    }

    private void MarkDirty(string scope)
    {
        if (_enabled && _dirty.TryAdd(scope, true))
        {
            _signal.Release();
        }
    }

    private List<string> TakeDirty()
    {
        var taken = new List<string>();
        foreach (var scope in _dirty.Keys)
        {
            if (_dirty.TryRemove(scope, out _))
            {
                taken.Add(scope);
            }
        }

        return taken;
    }

    private async Task WaitForHostStartAsync(CancellationToken cancellationToken)
    {
        // The transports connect during the host start; a state topic cannot be written before.
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = _lifetime.ApplicationStarted.Register(static s => ((TaskCompletionSource)s!).TrySetResult(), started);
        await started.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    // The lease has its own loop: a publish that waits for a broker must not let the lease run out.
    private async Task KeepLeaseAsync(CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(_options.LeaseTtl / 3, _time);
        try
        {
            do
            {
                await RenewLeaseAsync(cancellationToken).ConfigureAwait(false);
            }
            while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The host is stopping.
        }
    }

    private async Task ReleaseLeaseAsync()
    {
        _active = false;
        try
        {
            await _state.ReleaseLeaseAsync(_holder, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // The lease runs out by itself.
            Log.PublisherLeaseFailed(_logger, ex);
        }
    }

    /// <returns><see langword="true"/> when more entries may wait.</returns>
    private async ValueTask<bool> PublishScopeAsync(string scope, CancellationToken cancellationToken)
    {
        var scopeIsValid = scope.Split('.').All(LogicalAddress.IsValidSegment);
        if (!scopeIsValid)
        {
            _skipped.Add(1);
            Log.PublisherScopeInvalid(_logger, scope);
            return false;
        }

        var cursor = await _state.GetCursorAsync(scope, cancellationToken).ConfigureAwait(false);
        var resync = await NeedsResyncAsync(scope, cursor, cancellationToken).ConfigureAwait(false);
        var changes = await _state.ReadChangesAsync(scope, cursor, _options.ReadCount, cancellationToken).ConfigureAwait(false);
        if (changes.Count == 0 && !resync)
        {
            return false;
        }

        var touched = Reduce(changes);
        if (resync)
        {
            _resyncs.Add(1);
            foreach (var id in await _store.ListOrderIdsAsync(scope, 0, int.MaxValue, cancellationToken).ConfigureAwait(false))
            {
                touched[id] = touched.GetValueOrDefault(id) | JobTouch.Touched | JobTouch.Order;
            }
        }

        var port = _topics.ResolvePort();
        var address = _topics.AddressOf(scope);
        foreach (var (id, touch) in touched)
        {
            // A successor may already be writing: stop here and leave the cursor where it is.
            if (!_active)
            {
                return false;
            }

            await PublishJobAsync(port, address, scope, id, touch, cancellationToken).ConfigureAwait(false);
        }

        if (!_active)
        {
            return false;
        }

        await PublishStateIndexAsync(port, address, scope, cancellationToken).ConfigureAwait(false);

        // An empty stream still needs a cursor, or an idle scope would be rebuilt on every poll.
        var newCursor = changes.Count > 0 ? changes[^1].Id : cursor ?? EmptyStreamCursor;
        if (!string.Equals(newCursor, cursor, StringComparison.Ordinal))
        {
            await _state.SaveCursorAsync(scope, newCursor, cancellationToken).ConfigureAwait(false);
        }

        if (resync)
        {
            _lastResync[scope] = _time.GetUtcNow();
        }

        return changes.Count >= _options.ReadCount;
    }

    private async ValueTask<bool> NeedsResyncAsync(string scope, string? cursor, CancellationToken cancellationToken)
    {
        // Without a cursor (first start, publisher reset) the stream alone is not enough: it may have been trimmed.
        if (cursor is null)
        {
            return true;
        }

        // A first entry newer than the cursor means entries may be gone (MAXLEN, D-42). When only the entry of the cursor was trimmed
        // this rebuilds once for nothing, which is harmless.
        if (cursor != EmptyStreamCursor
            && await _state.FirstChangeIdAsync(scope, cancellationToken).ConfigureAwait(false) is { } first
            && CompareIds(first, cursor) > 0)
        {
            Log.PublisherGap(_logger, scope, cursor, first);
            return true;
        }

        // First visit under this lease, or the topics are due before they run out (half the time to live).
        return !_lastResync.TryGetValue(scope, out var last) || _time.GetUtcNow() - last >= _options.RetainedTtl / 2;
    }

    private static Dictionary<string, JobTouch> Reduce(IReadOnlyList<JobChange> changes)
    {
        var touched = new Dictionary<string, JobTouch>(StringComparer.Ordinal);
        foreach (var change in changes.Where(static c => c.JobOrderId.Length > 0))
        {
            var touch = JobTouch.Touched;
            if (change.ChangeType is "Store" or "StoreAndStart" or "Update")
            {
                touch |= JobTouch.Order;
            }
            else if (change.ChangeType is "Cancel" or "Clear")
            {
                // Python removed the topics on Clear only, so a cancelled order kept them (D-50).
                touch |= JobTouch.Removal;
            }

            touched[change.JobOrderId] = touched.GetValueOrDefault(change.JobOrderId) | touch;
        }

        return touched;
    }

    private static bool HasResult(JobOrderState state) => state is JobOrderState.Completed or JobOrderState.Closed or JobOrderState.Aborted;

    private async ValueTask PublishJobAsync(IStateTopicPort port, LogicalAddress address, string scope, string id, JobTouch touch, CancellationToken cancellationToken)
    {
        // The processor refuses such ids; one can still arrive through a restored document or an older store.
        if (!LogicalAddress.IsValidSegment(id))
        {
            _skipped.Add(1);
            Log.PublisherIdSkipped(_logger, id, scope);
            return;
        }

        // Also when the order exists again: a removal and a new Store of the id can fall into one batch, and the old result must go.
        if (touch.HasFlag(JobTouch.Removal))
        {
            await port.DeleteAsync(address, JobStateTopics.OrderName(id), cancellationToken).ConfigureAwait(false);
            await port.DeleteAsync(address, JobStateTopics.ResultName(id), cancellationToken).ConfigureAwait(false);
            _published.Add(2);
        }

        // Without a removal entry a missing order only went away after this change; its own entry follows.
        var order = await ReadOrderAsync(id, scope, cancellationToken).ConfigureAwait(false);
        if (order is null || !string.Equals(order.Scope, scope, StringComparison.Ordinal))
        {
            return;
        }

        if (touch.HasFlag(JobTouch.Order))
        {
            // The order alone: its state goes stale with every transition, and the state index carries the current state (D-51).
            await WriteAsync(port, address, JobStateTopics.OrderName(id), JobControlConversions.ToV2Order(order.Order), cancellationToken).ConfigureAwait(false);
        }

        // Every touch, not only ResultUpdate: a result that was stored before the job ended would otherwise never be published.
        if (HasResult(order.State)
            && await ReadResponseAsync(id, scope, cancellationToken).ConfigureAwait(false) is { } response)
        {
            await WriteAsync(port, address, JobStateTopics.ResultName(id), JobControlConversions.ToV2Response(response.Response), cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask<StoredJobResponse?> ReadResponseAsync(string id, string scope, CancellationToken cancellationToken)
    {
        try
        {
            return await _store.GetResponseByOrderAsync(id, cancellationToken).ConfigureAwait(false);
        }
        catch (PayloadException)
        {
            _skipped.Add(1);
            Log.PublisherOrderUndecodable(_logger, id, scope);
            return null;
        }
    }

    private async ValueTask<StoredJobOrder?> ReadOrderAsync(string id, string scope, CancellationToken cancellationToken)
    {
        try
        {
            return await _store.GetOrderAsync(id, cancellationToken).ConfigureAwait(false);
        }
        catch (PayloadException)
        {
            // A document that cannot be decoded must not hold back the rest of the scope for ever.
            _skipped.Add(1);
            Log.PublisherOrderUndecodable(_logger, id, scope);
            return null;
        }
    }

    private async ValueTask WriteAsync<T>(IStateTopicPort port, LogicalAddress address, string name, T value, CancellationToken cancellationToken)
        where T : class, IEncodeable
    {
        await port.PublishAsync(address, name, OpcUaJsonCodec.Encode(value), _options.RetainedTtl, cancellationToken).ConfigureAwait(false);
        _published.Add(1);
    }

    private async ValueTask PublishStateIndexAsync(IStateTopicPort port, LogicalAddress address, string scope, CancellationToken cancellationToken)
    {
        var ids = await _store.ListOrderIdsAsync(scope, 0, int.MaxValue, cancellationToken).ConfigureAwait(false);

        // The reads share one multiplexed connection, so they travel as a pipeline and do not wait for each other.
        var orders = await Task.WhenAll(ids.Select(id => ReadOrderAsync(id, scope, cancellationToken).AsTask())).ConfigureAwait(false);
        var results = await Task.WhenAll(ids.Select(id => _store.HasResponseAsync(id, cancellationToken).AsTask())).ConfigureAwait(false);

        var jobs = new List<JobOrderStateIndexEntryDataType>(ids.Count);
        for (var i = 0; i < ids.Count; i++)
        {
            if (orders[i] is { State: not JobOrderState.Removed } order)
            {
                jobs.Add(new JobOrderStateIndexEntryDataType
                {
                    JobOrderID = ids[i],
                    State = JobStateArray.ToV2(order.State),
                    // Only what the MES can then read: a response of a running job has no topic yet.
                    HasResult = results[i] && HasResult(order.State),
                });
            }
        }

        var sequence = await _state.NextSequenceAsync(scope, cancellationToken).ConfigureAwait(false);
        var index = new JobOrderStateIndexDataType
        {
            Sequence = sequence,
            Scope = scope,
            PublishedAt = _time.GetUtcNow().UtcDateTime,
            Jobs = jobs.ToArrayOf(),
        };
        await WriteAsync(port, address, JobStateTopics.StateIndex, index, cancellationToken).ConfigureAwait(false);
        Log.PublisherStateIndex(_logger, scope, sequence, jobs.Count);
    }
}
