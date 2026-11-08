using MicroDCS.Core.Hosting;
using MicroDCS.Core.Transport;
using StackExchange.Redis;

namespace MicroDCS.Valkey;

/// <summary>
/// Dedupe lease in Valkey with the Python semantics (R-3, R-4): <c>SET NX</c> with the lease time, the same token claims again,
/// <c>done</c> stays for the retention time. The key holds <c>processing:{token}</c> or <c>done</c>.
/// </summary>
public sealed class ValkeyDedupeStore : IDedupeStore
{
    private const int ClaimTries = 3;
    private const string Done = "done";

    // Deletes the key only when it still holds this caller's lease.
    private const string ReleaseScript = "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end return 0";

    // The remaining lease in milliseconds; 0 for a done marker or a missing key (the TTL of done is the retention, not a lease).
    private const string LeaseScript = """
        local v = redis.call('GET', KEYS[1])
        if not v or v == 'done' then return 0 end
        local ttl = redis.call('PTTL', KEYS[1])
        if ttl < 0 then return 0 end
        return ttl
        """;

    private readonly ValkeyContext _context;
    private readonly TimeSpan _lease;
    private readonly TimeSpan _retention;

    /// <summary>Initializes a new instance of the <see cref="ValkeyDedupeStore"/> class.</summary>
    /// <param name="context">The shared context.</param>
    /// <param name="lease">The lease time (<c>MicroDcs:DedupeLease</c>).</param>
    /// <param name="retention">How long a processed event stays a duplicate (<c>MicroDcs:DedupeRetention</c>).</param>
    public ValkeyDedupeStore(ValkeyContext context, TimeSpan lease, TimeSpan retention)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
        _lease = lease;
        _retention = retention;
    }

    /// <inheritdoc />
    public async ValueTask<ClaimState> ClaimAsync(string source, string id, string token, CancellationToken cancellationToken)
    {
        var db = _context.Database;
        var key = _context.Keys.CloudEventDedupe(source, id);
        var mine = "processing:" + token;
        // A holder's lease can expire between the SET and the GET, so try again briefly.
        for (var i = 0; i < ClaimTries; i++)
        {
            if (await db.StringSetAsync(key, mine, _lease, When.NotExists).WaitAsync(cancellationToken).ConfigureAwait(false))
            {
                return ClaimState.Claimed;
            }

            var value = (string?)await db.StringGetAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (value == Done)
            {
                return ClaimState.Done;
            }

            if (value == mine)
            {
                return ClaimState.Claimed;
            }

            if (value is not null)
            {
                return ClaimState.Busy;
            }
        }

        return ClaimState.Busy;
    }

    /// <inheritdoc />
    public async ValueTask MarkDoneAsync(string source, string id, CancellationToken cancellationToken) =>
        await _context.Database.StringSetAsync(_context.Keys.CloudEventDedupe(source, id), Done, _retention).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask ReleaseAsync(string source, string id, string token, CancellationToken cancellationToken) =>
        await _context.Database.ScriptEvaluateAsync(ReleaseScript, [_context.Keys.CloudEventDedupe(source, id)], ["processing:" + token]).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<TimeSpan> LeaseRemainingAsync(string source, string id, CancellationToken cancellationToken)
    {
        var result = await _context.Database.ScriptEvaluateAsync(LeaseScript, [_context.Keys.CloudEventDedupe(source, id)]).WaitAsync(cancellationToken).ConfigureAwait(false);
        var milliseconds = (long)result;
        return milliseconds > 0 ? TimeSpan.FromMilliseconds(milliseconds) : TimeSpan.Zero;
    }
}

/// <summary>The singleton start lease in Valkey (R-6, P-14): <c>SET NX</c> with a lifetime.</summary>
public sealed class ValkeyStartLease : IStartLease
{
    private readonly ValkeyContext _context;

    /// <summary>Initializes a new instance of the <see cref="ValkeyStartLease"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public ValkeyStartLease(ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
    }

    /// <inheritdoc />
    public async ValueTask<bool> TryAcquireAsync(string name, TimeSpan timeToLive, CancellationToken cancellationToken) =>
        await _context.Database.StringSetAsync(_context.Keys.PostStartLock(name), "1", timeToLive, When.NotExists).WaitAsync(cancellationToken).ConfigureAwait(false);
}
