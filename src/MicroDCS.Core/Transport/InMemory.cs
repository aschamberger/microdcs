using System.Collections.Concurrent;
using System.Collections.Immutable;
using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Transport;

/// <summary>Renders addresses as <c>prefix/scope/scope/discriminator/intent</c>. Used by the in-memory transport; MQTT and Zenoh render the same way (D-38).</summary>
public sealed class SlashAddressRenderer : IAddressRenderer
{
    /// <inheritdoc />
    public string RenderPublish(LogicalAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        var parts = new List<string> { address.Prefix };
        parts.AddRange(address.ScopePath);
        if (address.Discriminator is not null)
        {
            parts.Add(address.Discriminator);
        }

        parts.Add(IntentNames.ToSegment(address.Intent));
        return string.Join('/', parts);
    }

    /// <inheritdoc />
    public IReadOnlyList<string> RenderSubscriptions(AddressPattern pattern)
    {
        ArgumentNullException.ThrowIfNull(pattern);
        var result = new List<string>();
        for (var depth = 0; depth <= pattern.WildcardLevels; depth++)
        {
            var parts = new List<string> { pattern.Prefix };
            parts.AddRange(Enumerable.Repeat("+", depth));
            if (pattern.Discriminator is not null)
            {
                parts.Add(pattern.Discriminator);
            }

            parts.Add(IntentNames.ToSegment(pattern.Intent));
            result.Add(string.Join('/', parts));
        }

        return result;
    }

    /// <inheritdoc />
    public bool TryParse(string prefix, string rendered, out LogicalAddress address)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(rendered);
        address = null!;
        if (!rendered.StartsWith(prefix + "/", StringComparison.Ordinal))
        {
            return false;
        }

        var rest = rendered[(prefix.Length + 1)..].Split('/');
        if (!IntentNames.TryParse(rest[^1], out var intent) || !rest.SkipLast(1).All(LogicalAddress.IsValidSegment))
        {
            return false;
        }

        address = new LogicalAddress(prefix, [.. rest.SkipLast(1)], null, intent);
        return true;
    }
}

/// <summary>
/// A transport that lives in the process: tests and the harness inject messages and read what was published. It is also
/// the reference for the transport contract (D-37).
/// </summary>
public sealed class InMemoryTransport : ITransport
{
    private readonly ConcurrentQueue<OutboundMessage> _published = new();
    private IInboundHandler? _handler;
    private volatile bool _receiving;

    /// <summary>Creates a transport with the given delivery guarantee.</summary>
    /// <param name="guarantee">The guarantee the transport declares.</param>
    public InMemoryTransport(DeliveryGuarantee guarantee = DeliveryGuarantee.AtLeastOnce)
    {
        Profile = new DeliveryProfile(guarantee, NativeReply: true, NativeExpiry: false, StateTopics: true, SharedConsumption: false);
    }

    /// <inheritdoc />
    public string Name => "memory";

    /// <inheritdoc />
    public DeliveryProfile Profile { get; }

    /// <inheritdoc />
    public IAddressRenderer Addresses { get; } = new SlashAddressRenderer();

    /// <inheritdoc />
    public bool IsConnected { get; private set; }

    /// <summary>Everything published so far, oldest first.</summary>
    public ImmutableArray<OutboundMessage> Published => [.. _published];

    /// <inheritdoc />
    public ValueTask StartAsync(IInboundHandler handler, CancellationToken cancellationToken)
    {
        _handler = handler;
        _receiving = true;
        IsConnected = true;
        return ValueTask.CompletedTask;
    }

    /// <summary>Delivers a message to the pipeline as a transport would.</summary>
    /// <param name="message">The message.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The outcome the pipeline asks the transport to act on.</returns>
    /// <exception cref="InvalidOperationException">The transport is not receiving.</exception>
    public ValueTask<DeliveryOutcome> InjectAsync(InboundMessage message, CancellationToken cancellationToken)
    {
        if (!_receiving || _handler is null)
        {
            throw new InvalidOperationException("The transport is not receiving.");
        }

        return _handler.HandleAsync(message, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask StopReceivingAsync(CancellationToken cancellationToken)
    {
        _receiving = false;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask PublishAsync(OutboundMessage message, CancellationToken cancellationToken)
    {
        if (!IsConnected)
        {
            throw new InvalidOperationException("The transport is stopped.");
        }

        _published.Enqueue(message);
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask StopAsync(CancellationToken cancellationToken)
    {
        IsConnected = false;
        return ValueTask.CompletedTask;
    }
}

/// <summary>Dedupe store in memory with leases and retention on a <see cref="TimeProvider"/>.</summary>
public sealed class InMemoryDedupeStore : IDedupeStore
{
    private readonly Lock _gate = new();
    private readonly Dictionary<(string, string), Entry> _entries = [];
    private readonly TimeProvider _time;
    private readonly TimeSpan _lease;
    private readonly TimeSpan _retention;

    /// <summary>Creates the store.</summary>
    /// <param name="time">The clock.</param>
    /// <param name="lease">Lease time of a claim.</param>
    /// <param name="retention">How long a processed event stays a duplicate.</param>
    public InMemoryDedupeStore(TimeProvider time, TimeSpan lease, TimeSpan retention)
    {
        _time = time;
        _lease = lease;
        _retention = retention;
    }

    /// <inheritdoc />
    public ValueTask<ClaimState> ClaimAsync(string source, string id, string token, CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow();
        lock (_gate)
        {
            if (_entries.TryGetValue((source, id), out var entry) && entry.Until > now)
            {
                if (entry.Done)
                {
                    return ValueTask.FromResult(ClaimState.Done);
                }

                if (entry.Token != token)
                {
                    return ValueTask.FromResult(ClaimState.Busy);
                }
            }

            _entries[(source, id)] = new Entry(token, Done: false, now + _lease);
            return ValueTask.FromResult(ClaimState.Claimed);
        }
    }

    /// <inheritdoc />
    public ValueTask MarkDoneAsync(string source, string id, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _entries[(source, id)] = new Entry(string.Empty, Done: true, _time.GetUtcNow() + _retention);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask<TimeSpan> LeaseRemainingAsync(string source, string id, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            var remaining = _entries.TryGetValue((source, id), out var entry) && !entry.Done
                ? entry.Until - _time.GetUtcNow()
                : TimeSpan.Zero;
            return ValueTask.FromResult(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
        }
    }

    private readonly record struct Entry(string Token, bool Done, DateTimeOffset Until);
}

/// <summary>State topics in memory: the last value per address and name, with expiry on a <see cref="TimeProvider"/>.</summary>
public sealed class InMemoryStateTopicPort : IStateTopicPort
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, (ReadOnlyMemory<byte> Payload, DateTimeOffset Until)> _topics = [];
    private readonly TimeProvider _time;
    private readonly IAddressRenderer _renderer;

    /// <summary>Creates the port.</summary>
    /// <param name="time">The clock.</param>
    /// <param name="renderer">Renders topic keys.</param>
    public InMemoryStateTopicPort(TimeProvider time, IAddressRenderer renderer)
    {
        _time = time;
        _renderer = renderer;
    }

    /// <summary>Reads the current value of a topic.</summary>
    /// <param name="address">The address.</param>
    /// <param name="name">The topic name.</param>
    /// <returns>The value, or <see langword="null"/> when absent or expired.</returns>
    public ReadOnlyMemory<byte>? Get(LogicalAddress address, string name)
    {
        lock (_gate)
        {
            if (_topics.TryGetValue(Key(address, name), out var topic) && topic.Until > _time.GetUtcNow())
            {
                return topic.Payload;
            }

            // Not `? payload : null`: the null literal would convert to an empty ReadOnlyMemory through byte[].
            return default(ReadOnlyMemory<byte>?);
        }
    }

    /// <inheritdoc />
    public ValueTask PublishAsync(LogicalAddress address, string name, ReadOnlyMemory<byte> payload, TimeSpan timeToLive, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _topics[Key(address, name)] = (payload, _time.GetUtcNow() + timeToLive);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public ValueTask DeleteAsync(LogicalAddress address, string name, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            _topics.Remove(Key(address, name));
        }

        return ValueTask.CompletedTask;
    }

    private string Key(LogicalAddress address, string name) => _renderer.RenderPublish(address) + "/" + name;
}
