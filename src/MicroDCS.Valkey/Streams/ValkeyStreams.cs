using StackExchange.Redis;

namespace MicroDCS.Valkey.Streams;

/// <summary>
/// Stream helpers for work, outbox and change streams (R-22, R-29, R-30, D-42, D-43). Reads never block (D-7). Work and outbox streams
/// are never trimmed by length: <see cref="TrimAcknowledgedAsync"/> removes only entries below the lowest pending id. One consumer
/// group per stream is assumed.
/// </summary>
public sealed class ValkeyStreams
{
    private readonly ValkeyContext _context;

    /// <summary>Initializes a new instance of the <see cref="ValkeyStreams"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public ValkeyStreams(ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
    }

    /// <summary>Creates the consumer group (and the stream) if it does not exist; a repeated call is harmless.</summary>
    /// <param name="stream">The stream key.</param>
    /// <param name="group">The group.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task EnsureGroupAsync(string stream, string group, CancellationToken cancellationToken = default)
    {
        try
        {
            await _context.Database.StreamCreateConsumerGroupAsync(stream, group, StreamPosition.Beginning, createStream: true).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.StartsWith("BUSYGROUP", StringComparison.Ordinal))
        {
            // The group exists.
        }
    }

    /// <summary>Appends an entry without a length limit and nudges the poller.</summary>
    /// <param name="stream">The stream key.</param>
    /// <param name="nudgeKind">The nudge kind, see <see cref="KeySchema.Nudge"/>.</param>
    /// <param name="scope">The scope (the nudge message).</param>
    /// <param name="fields">The entry fields.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The entry id.</returns>
    public async Task<string> AppendAsync(string stream, string nudgeKind, string scope, IReadOnlyList<KeyValuePair<string, string>> fields, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(fields);
        var db = _context.Database;
        var id = await db.StreamAddAsync(stream, [.. fields.Select(static f => new NameValueEntry(f.Key, f.Value))]).WaitAsync(cancellationToken).ConfigureAwait(false);
        await db.PublishAsync(RedisChannel.Literal(_context.Keys.Nudge(nudgeKind)), scope).WaitAsync(cancellationToken).ConfigureAwait(false);
        return id.ToString();
    }

    /// <summary>Reads new entries for a consumer, without blocking.</summary>
    /// <param name="stream">The stream key.</param>
    /// <param name="group">The group.</param>
    /// <param name="consumer">The consumer name.</param>
    /// <param name="count">The maximum number of entries.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The entries; empty when there are none.</returns>
    public async Task<IReadOnlyList<StreamEntry>> ReadAsync(string stream, string group, string consumer, int count, CancellationToken cancellationToken = default) =>
        await _context.Database.StreamReadGroupAsync(stream, group, consumer, StreamPosition.NewMessages, count).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Takes over entries that another consumer left pending longer than <paramref name="minIdle"/>. Entries that were deleted
    /// while still pending are logged and counted (R-30).</summary>
    /// <param name="stream">The stream key.</param>
    /// <param name="group">The group.</param>
    /// <param name="consumer">The new owner.</param>
    /// <param name="minIdle">The minimum idle time.</param>
    /// <param name="count">The maximum number of entries.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The claimed entries.</returns>
    public async Task<IReadOnlyList<StreamEntry>> AutoClaimAsync(string stream, string group, string consumer, TimeSpan minIdle, int count, CancellationToken cancellationToken = default)
    {
        var result = await _context.Database.StreamAutoClaimAsync(stream, group, consumer, (long)minIdle.TotalMilliseconds, StreamPosition.Beginning, count).WaitAsync(cancellationToken).ConfigureAwait(false);
        if (result.DeletedIds is { Length: > 0 } deleted)
        {
            ValkeyMetrics.StreamEntriesLost.Add(deleted.Length);
            Log.StreamEntriesLost(_context.Logger, stream, deleted.Length, string.Join(',', deleted.Select(static d => d.ToString())));
        }

        return result.ClaimedEntries;
    }

    /// <summary>Reads the delivery counts of pending entries.</summary>
    /// <param name="stream">The stream key.</param>
    /// <param name="group">The group.</param>
    /// <param name="fromId">The first id.</param>
    /// <param name="toId">The last id.</param>
    /// <param name="count">The maximum number of entries.</param>
    /// <param name="consumer">Only the entries of this consumer; <see cref="RedisValue.Null"/> for all.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The pending entries with consumer, idle time and delivery count.</returns>
    public async Task<IReadOnlyList<StreamPendingMessageInfo>> PendingAsync(string stream, string group, RedisValue fromId, RedisValue toId, int count, RedisValue consumer, CancellationToken cancellationToken = default) =>
        await _context.Database.StreamPendingMessagesAsync(stream, group, count, consumer, fromId, toId).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Acknowledges entries and trims what is acknowledged.</summary>
    /// <param name="stream">The stream key.</param>
    /// <param name="group">The group.</param>
    /// <param name="ids">The entry ids.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task AcknowledgeAsync(string stream, string group, IReadOnlyList<RedisValue> ids, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(ids);
        if (ids.Count == 0)
        {
            return;
        }

        await _context.Database.StreamAcknowledgeAsync(stream, group, [.. ids]).WaitAsync(cancellationToken).ConfigureAwait(false);
        await TrimAcknowledgedAsync(stream, group, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Removes entries below the lowest pending id of the group; when nothing is pending, below the last delivered id. Entries that are
    /// not yet delivered or still pending are never removed (D-42).
    /// </summary>
    /// <param name="stream">The stream key.</param>
    /// <param name="group">The group.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The number of entries removed.</returns>
    public async Task<long> TrimAcknowledgedAsync(string stream, string group, CancellationToken cancellationToken = default)
    {
        var db = _context.Database;
        var pending = await db.StreamPendingAsync(stream, group).WaitAsync(cancellationToken).ConfigureAwait(false);
        string? limit;
        if (pending.PendingMessageCount > 0)
        {
            limit = pending.LowestPendingMessageId.ToString();
        }
        else
        {
            var groups = await db.StreamGroupInfoAsync(stream).WaitAsync(cancellationToken).ConfigureAwait(false);
            limit = groups.FirstOrDefault(g => g.Name == group).LastDeliveredId;
        }

        if (string.IsNullOrEmpty(limit))
        {
            return 0;
        }

        var removed = (long)await db.ExecuteAsync("XTRIM", stream, "MINID", limit).WaitAsync(cancellationToken).ConfigureAwait(false);
        if (removed > 0)
        {
            ValkeyMetrics.StreamEntriesTrimmed.Add(removed);
            Log.StreamTrimmed(_context.Logger, stream, removed);
        }

        return removed;
    }

    /// <summary>Publishes a nudge for a scope, for code that appends to a stream outside a script.</summary>
    /// <param name="kind">The nudge kind.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task NudgeAsync(string kind, string scope, CancellationToken cancellationToken = default) =>
        await _context.Database.PublishAsync(RedisChannel.Literal(_context.Keys.Nudge(kind)), scope).WaitAsync(cancellationToken).ConfigureAwait(false);
}
