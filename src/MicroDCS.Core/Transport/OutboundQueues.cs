using System.IO.Enumeration;
using System.Threading.Channels;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;

namespace MicroDCS.Core.Transport;

/// <summary>
/// One bounded queue per binding for application-initiated events (T-6, D-28, D-37). A full queue fails the enqueue with
/// <see cref="OutboundQueueFullException"/>. One pump per binding hands the messages to the transport in order; the transport
/// waits while it reconnects (T-7). The type filter (T-5) is applied at enqueue.
/// </summary>
internal sealed class OutboundQueues : IDisposable
{
    private readonly Dictionary<string, Queue> _queues = new(StringComparer.Ordinal);
    private readonly CancellationTokenSource _abort = new();
    private readonly Lock _enqueue = new();
    private Task[] _pumps = [];
    private volatile bool _closed;

    public OutboundQueues(MicroDcsOptions options, IEnumerable<string> bindingIds)
    {
        foreach (var id in bindingIds)
        {
            var binding = options.Bindings[id];
            var size = binding.OutgoingQueueSize > 0 ? binding.OutgoingQueueSize : options.OutgoingQueueMaxSize;
            // The options validator rejects a binding size above the maximum at start (D-27), so no capping is needed here.
            var channel = Channel.CreateBounded<OutboundMessage>(new BoundedChannelOptions(size)
            {
                SingleReader = true,
                FullMode = BoundedChannelFullMode.Wait,
            });
            _queues[id] = new Queue(channel, size, ParseFilter(binding.OutgoingTypeFilter));
        }
    }

    /// <summary>Whether the binding's type filter lets the type through. An empty filter lets everything through.</summary>
    public bool Accepts(string bindingId, string type)
    {
        var patterns = _queues[bindingId].Filter;
        return patterns.Length == 0 || patterns.Any(p => p == type || FileSystemName.MatchesSimpleExpression(p, type, ignoreCase: false));
    }

    /// <summary>Whether shutdown closed the queues; nothing is accepted any more.</summary>
    public bool IsClosed => _closed;

    /// <summary>Adds a message. Throws when the queue is full instead of blocking (D-28).</summary>
    public void Enqueue(string bindingId, OutboundMessage message) => EnqueueAll([message]);

    /// <summary>
    /// Adds one message per binding, all or nothing (D-28 revised): when any target queue is full, nothing is added and
    /// <see cref="OutboundQueueFullException"/> names that binding. Only the pumps take messages out, so the capacity checked
    /// under the lock cannot shrink before the writes.
    /// </summary>
    public void EnqueueAll(IReadOnlyList<OutboundMessage> messages)
    {
        lock (_enqueue)
        {
            foreach (var group in messages.GroupBy(static m => m.BindingId, StringComparer.Ordinal))
            {
                var queue = _queues[group.Key];
                if (queue.Channel.Reader.Count + group.Count() > queue.Capacity)
                {
                    PipelineTelemetry.QueueRejected.Add(1);
                    throw new OutboundQueueFullException(group.Key);
                }
            }

            // TryWrite fails only after DrainAsync completed the writer: the shutdown refuses everything from here on.
            var refused = messages.FirstOrDefault(m => !_queues[m.BindingId].Channel.Writer.TryWrite(m));
            if (refused is not null)
            {
                PipelineTelemetry.QueueRejected.Add(1);
                throw new OutboundQueueFullException(refused.BindingId);
            }
        }
    }

    /// <summary>Starts one pump per binding. A pump ends the host through <paramref name="onFault"/> when publishing throws (D-27).</summary>
    public void Start(Func<OutboundMessage, CancellationToken, ValueTask> publish, Action<string, Exception> onFault)
    {
        _pumps = [.. _queues.Select(q => PumpAsync(q.Key, q.Value.Channel.Reader, publish, onFault, _abort.Token))];
    }

    /// <summary>Shutdown phase 3: refuses new messages and waits until the queues are empty. When the token is cancelled the pumps are aborted and what is left is lost.</summary>
    public async ValueTask DrainAsync(CancellationToken cancellationToken)
    {
        _closed = true;
        foreach (var queue in _queues.Values)
        {
            queue.Channel.Writer.TryComplete();
        }

        try
        {
            await Task.WhenAll(_pumps).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            await _abort.CancelAsync().ConfigureAwait(false);
            await Task.WhenAll(_pumps).ConfigureAwait(false);
        }
    }

    public void Dispose() => _abort.Dispose();

    private static async Task PumpAsync(string bindingId, ChannelReader<OutboundMessage> reader, Func<OutboundMessage, CancellationToken, ValueTask> publish, Action<string, Exception> onFault, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await publish(message, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The grace period ended: whatever is left is lost, the same as in Python after a forced cancel.
        }
        catch (Exception ex)
        {
            // Also a cancellation the pump did not ask for (a timeout inside the transport): never end the pump silently (D-27).
            onFault(bindingId, ex);
        }
    }

    private static string[] ParseFilter(string? filter) =>
        string.IsNullOrWhiteSpace(filter) ? [] : [.. filter.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];

    private sealed record Queue(Channel<OutboundMessage> Channel, int Capacity, string[] Filter);
}
