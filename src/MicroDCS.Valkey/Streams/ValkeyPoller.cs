using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;

namespace MicroDCS.Valkey.Streams;

/// <summary>Reads one kind of stream for a scope; implemented by the publisher and the SFC host.</summary>
public interface IStreamConsumer
{
    /// <summary>The nudge kind this consumer listens to, see <see cref="KeySchema.Nudge"/>.</summary>
    string Kind { get; }

    /// <summary>Called once per scope when the poller learns about it, for example to create the consumer group.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask ScopeDiscoveredAsync(string scope, CancellationToken cancellationToken);

    /// <summary>Reads and handles available entries of the scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when entries were handled, so the poller reads again at once.</returns>
    ValueTask<bool> PollAsync(string scope, CancellationToken cancellationToken);
}

/// <summary>Periodic work that runs in the poller, for example the SFC due-index sweep (D-43).</summary>
public interface IPollerTask
{
    /// <summary>Runs one round.</summary>
    /// <param name="scopes">The known scopes.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask RunAsync(IReadOnlyCollection<string> scopes, CancellationToken cancellationToken);
}

/// <summary>
/// The one poller per instance (D-43). It subscribes to the nudge channels and reads the named stream at once; it also reads every
/// stream of every scope each <see cref="ValkeyOptions.PollInterval"/>, because pub/sub can lose messages. It reads the scope registry at
/// start (a failure ends the process, D-27) and every <see cref="ValkeyOptions.ScopeRefreshInterval"/>. No blocking commands (D-7).
/// </summary>
public sealed class ValkeyPoller : BackgroundService
{
    private const int MaxRoundsPerStream = 1000;

    private readonly ValkeyContext _context;
    private readonly IReadOnlyList<IStreamConsumer> _consumers;
    private readonly IReadOnlyList<IPollerTask> _tasks;
    private readonly ConcurrentDictionary<string, bool> _scopes = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<(string Kind, string Scope), bool> _nudged = new();
    private readonly SemaphoreSlim _signal = new(0);

    /// <summary>Initializes a new instance of the <see cref="ValkeyPoller"/> class.</summary>
    /// <param name="context">The shared context.</param>
    /// <param name="consumers">The stream consumers.</param>
    /// <param name="tasks">The periodic tasks.</param>
    public ValkeyPoller(ValkeyContext context, IEnumerable<IStreamConsumer> consumers, IEnumerable<IPollerTask> tasks)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(consumers);
        ArgumentNullException.ThrowIfNull(tasks);
        _context = context;
        _consumers = [.. consumers];
        _tasks = [.. tasks];
    }

    /// <summary>The scopes the poller knows.</summary>
    public IReadOnlyCollection<string> Scopes => [.. _scopes.Keys];

    /// <inheritdoc />
    public override void Dispose()
    {
        _signal.Dispose();
        base.Dispose();
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_consumers.Count == 0 && _tasks.Count == 0)
        {
            return;
        }

        await Task.Yield();
        var options = _context.Options;
        var subscriber = _context.Multiplexer.GetSubscriber();
        foreach (var kind in _consumers.Select(static c => c.Kind).Distinct(StringComparer.Ordinal))
        {
            var captured = kind;
            await subscriber.SubscribeAsync(RedisChannel.Literal(_context.Keys.Nudge(kind)), (_, message) =>
            {
                if (message.HasValue)
                {
                    _nudged[(captured, message.ToString())] = true;
                    _signal.Release();
                }
            }).ConfigureAwait(false);
        }

        var nextRefresh = DateTimeOffset.MinValue;
        var firstRound = true;
        var lastFullRound = DateTimeOffset.MinValue;
        var time = _context.Time;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (time.GetUtcNow() >= nextRefresh)
                {
                    await RefreshScopesAsync(stoppingToken).ConfigureAwait(false);
                    nextRefresh = time.GetUtcNow() + options.ScopeRefreshInterval;
                }

                var nudged = TakeNudges();
                foreach (var (_, scope) in nudged)
                {
                    await DiscoverAsync(scope, stoppingToken).ConfigureAwait(false);
                }

                var timedOut = firstRound || !await WaitAsync(options.PollInterval, nudged.Count > 0, stoppingToken).ConfigureAwait(false);
                firstRound = false;
                nudged.AddRange(TakeNudges());

                // Constant nudges must not starve the fallback read and the periodic tasks.
                timedOut |= time.GetUtcNow() - lastFullRound >= options.PollInterval;
                if (timedOut)
                {
                    lastFullRound = time.GetUtcNow();
                }

                await ReadAsync(timedOut ? null : nudged, stoppingToken).ConfigureAwait(false);
                if (timedOut)
                {
                    await RunTasksAsync(stoppingToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex) when (!firstRound)
            {
                // A failed round is logged and repeated on the next one.
                ValkeyMetrics.PollErrors.Add(1);
                Log.PollFailed(_context.Logger, ex, "round", string.Empty);
                await Task.Delay(options.PollInterval, time, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private List<(string Kind, string Scope)> TakeNudges()
    {
        var taken = new List<(string, string)>();
        foreach (var key in _nudged.Keys)
        {
            if (_nudged.TryRemove(key, out _))
            {
                taken.Add(key);
            }
        }

        return taken;
    }

    /// <summary>Waits for a nudge or the poll interval. Returns <see langword="true"/> when a nudge ended the wait.</summary>
    private async Task<bool> WaitAsync(TimeSpan interval, bool alreadyNudged, CancellationToken cancellationToken)
    {
        if (alreadyNudged)
        {
            return true;
        }

        using var timeout = new CancellationTokenSource(interval, _context.Time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            await _signal.WaitAsync(linked.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return false;
        }
    }

    private async Task RefreshScopesAsync(CancellationToken cancellationToken)
    {
        var members = await _context.Database.SetMembersAsync(_context.Keys.Scopes()).WaitAsync(cancellationToken).ConfigureAwait(false);
        foreach (var member in members)
        {
            await DiscoverAsync(member.ToString(), cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task DiscoverAsync(string scope, CancellationToken cancellationToken)
    {
        if (!_scopes.TryAdd(scope, true))
        {
            return;
        }

        Log.ScopeDiscovered(_context.Logger, scope);
        foreach (var consumer in _consumers)
        {
            await consumer.ScopeDiscoveredAsync(scope, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ReadAsync(List<(string Kind, string Scope)>? only, CancellationToken cancellationToken)
    {
        IEnumerable<(IStreamConsumer Consumer, string Scope)> targets = only is null
            ? _consumers.SelectMany(c => _scopes.Keys.Select(s => (c, s)))
            : only.SelectMany(n => _consumers.Where(c => c.Kind == n.Kind).Select(c => (c, n.Scope)));
        foreach (var (consumer, scope) in targets.Distinct())
        {
            for (var round = 0; round < MaxRoundsPerStream; round++)
            {
                try
                {
                    if (!await consumer.PollAsync(scope, cancellationToken).ConfigureAwait(false))
                    {
                        break;
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    ValkeyMetrics.PollErrors.Add(1);
                    Log.PollFailed(_context.Logger, ex, consumer.Kind, scope);
                    break;
                }
            }
        }
    }

    private async Task RunTasksAsync(CancellationToken cancellationToken)
    {
        foreach (var task in _tasks)
        {
            try
            {
                await task.RunAsync(Scopes, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                ValkeyMetrics.PollErrors.Add(1);
                Log.PollFailed(_context.Logger, ex, task.GetType().Name, string.Empty);
            }
        }
    }
}
