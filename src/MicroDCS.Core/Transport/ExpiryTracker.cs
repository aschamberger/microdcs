namespace MicroDCS.Core.Transport;

/// <summary>
/// Local expiry timers for commands that expect a response (P-12, D-9, D-37). The timer lives in this instance only; the
/// response cancels it. At shutdown the pending timers are awaited until the grace period ends, then cancelled.
/// </summary>
internal sealed class ExpiryTracker : IDisposable
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, Timer> _timers = new(StringComparer.Ordinal);
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _stop = new();

    public ExpiryTracker(TimeProvider time)
    {
        _time = time;
    }

    public void Dispose() => _stop.Dispose();

    public int Pending
    {
        get
        {
            lock (_gate)
            {
                return _timers.Count;
            }
        }
    }

    /// <summary>Starts a timer. A second start with the same key replaces the first.</summary>
    public void Start(string key, TimeSpan interval, Func<CancellationToken, ValueTask> onExpired)
    {
        var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);

        // The clock timer is created here, synchronously: when Start returns, advancing the clock fires it.
        var delay = Task.Delay(interval, _time, cancellation.Token);
        lock (_gate)
        {
            if (_timers.Remove(key, out var previous))
            {
                previous.Cancellation.Cancel();
            }

            // RunAsync yields first and takes the lock in its finally block, so the entry exists before the timer can finish.
            _timers[key] = new Timer(cancellation, RunAsync(key, delay, onExpired, cancellation));
        }
    }

    /// <summary>Cancels the timer for a command, if one is pending. Returns whether there was one.</summary>
    public bool Cancel(string key)
    {
        Timer timer;
        lock (_gate)
        {
            if (!_timers.Remove(key, out timer))
            {
                return false;
            }
        }

        timer.Cancellation.Cancel();
        return true;
    }

    /// <summary>Waits until every pending timer fired or was cancelled. When <paramref name="cancellationToken"/> is cancelled the rest is cancelled.</summary>
    public async ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        Task[] pending;
        lock (_gate)
        {
            pending = [.. _timers.Values.Select(t => t.Task)];
        }

        try
        {
            await Task.WhenAll(pending).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await _stop.CancelAsync().ConfigureAwait(false);
            await Task.WhenAll(pending).ConfigureAwait(false);
        }
    }

    private async Task RunAsync(string key, Task delay, Func<CancellationToken, ValueTask> onExpired, CancellationTokenSource cancellation)
    {
        try
        {
            await Task.Yield();
            await delay.ConfigureAwait(false);
            await onExpired(_stop.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cancelled by a response or by the end of the grace period.
        }
        finally
        {
            lock (_gate)
            {
                if (_timers.TryGetValue(key, out var current) && ReferenceEquals(current.Cancellation, cancellation))
                {
                    _timers.Remove(key);
                }
            }

            cancellation.Dispose();
        }
    }

    private readonly record struct Timer(CancellationTokenSource Cancellation, Task Task);
}
