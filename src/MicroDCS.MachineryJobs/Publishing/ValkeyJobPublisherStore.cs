using System.Globalization;
using MicroDCS.Valkey;
using StackExchange.Redis;

namespace MicroDCS.MachineryJobs.Publishing;

/// <summary>The publisher's state over Valkey (D-42, D-51). Cursors are read on every round, so a reset takes effect without a restart.</summary>
public sealed class ValkeyJobPublisherStore : IJobPublisherStore
{
    private const string AcquireScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
          redis.call('PEXPIRE', KEYS[1], ARGV[2])
          return 1
        end
        if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
          return 1
        end
        return 0
        """;

    private const string ReleaseScript = """
        if redis.call('GET', KEYS[1]) == ARGV[1] then
          return redis.call('DEL', KEYS[1])
        end
        return 0
        """;

    private readonly ValkeyContext _context;

    /// <summary>Initializes a new instance of the <see cref="ValkeyJobPublisherStore"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public ValkeyJobPublisherStore(ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
    }

    private IDatabase Database => _context.Multiplexer.GetDatabase();

    /// <inheritdoc />
    public async ValueTask<string?> GetCursorAsync(string scope, CancellationToken cancellationToken) =>
        await Database.HashGetAsync(_context.Keys.PublisherCursors(), scope).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask SaveCursorAsync(string scope, string id, CancellationToken cancellationToken) =>
        await Database.HashSetAsync(_context.Keys.PublisherCursors(), scope, id).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<string?> FirstChangeIdAsync(string scope, CancellationToken cancellationToken)
    {
        var entries = await Database.StreamRangeAsync(_context.Keys.Changes(scope), "-", "+", 1, Order.Ascending).WaitAsync(cancellationToken).ConfigureAwait(false);
        return entries.Length == 0 ? null : entries[0].Id.ToString();
    }

    /// <inheritdoc />
    public async ValueTask<string?> LastChangeIdAsync(string scope, CancellationToken cancellationToken)
    {
        var entries = await Database.StreamRangeAsync(_context.Keys.Changes(scope), "-", "+", 1, Order.Descending).WaitAsync(cancellationToken).ConfigureAwait(false);
        return entries.Length == 0 ? null : entries[0].Id.ToString();
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<JobChange>> ReadChangesAsync(string scope, string? after, int count, CancellationToken cancellationToken)
    {
        var entries = await Database.StreamRangeAsync(_context.Keys.Changes(scope), after is null ? "-" : "(" + after, "+", count, Order.Ascending).WaitAsync(cancellationToken).ConfigureAwait(false);
        return [.. entries.Select(static e => new JobChange(e.Id.ToString(), e["change_type"].ToString(), e["job_order_id"].ToString()))];
    }

    /// <inheritdoc />
    public async ValueTask<long> NextSequenceAsync(string scope, CancellationToken cancellationToken) =>
        await Database.StringIncrementAsync(_context.Keys.PublishSequence(scope)).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<long> GetSequenceAsync(string scope, CancellationToken cancellationToken)
    {
        var value = await Database.StringGetAsync(_context.Keys.PublishSequence(scope)).WaitAsync(cancellationToken).ConfigureAwait(false);
        return value.HasValue ? long.Parse(value.ToString(), CultureInfo.InvariantCulture) : 0;
    }

    /// <inheritdoc />
    public async ValueTask<bool> AcquireLeaseAsync(string holder, TimeSpan timeToLive, CancellationToken cancellationToken)
    {
        var result = await Database.ScriptEvaluateAsync(AcquireScript, [_context.Keys.PublisherLease()], [holder, (long)timeToLive.TotalMilliseconds]).WaitAsync(cancellationToken).ConfigureAwait(false);
        return (long)result == 1;
    }

    /// <inheritdoc />
    public async ValueTask ReleaseLeaseAsync(string holder, CancellationToken cancellationToken) =>
        await Database.ScriptEvaluateAsync(ReleaseScript, [_context.Keys.PublisherLease()], [holder]).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<string?> GetLeaseHolderAsync(CancellationToken cancellationToken) =>
        await Database.StringGetAsync(_context.Keys.PublisherLease()).WaitAsync(cancellationToken).ConfigureAwait(false);
}
