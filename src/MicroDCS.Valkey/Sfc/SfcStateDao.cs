using System.Globalization;
using MicroDCS.Valkey.Documents;
using StackExchange.Redis;

namespace MicroDCS.Valkey.Sfc;

/// <summary>One work or outbox entry that a state write appends to the scope's work stream.</summary>
/// <param name="Fields">The stream fields.</param>
public sealed record SfcWorkEntry(IReadOnlyList<KeyValuePair<string, string>> Fields);

/// <summary>
/// SFC state documents and the generic compare-and-set of D-44: one script compares the state version, stores the new state, appends the
/// work entries, updates the due-index and the scope's active-job set, registers the scope and nudges the poller. R-26 to R-29.
/// </summary>
public sealed class SfcStateDao : DocumentDao
{
    /// <summary>
    /// KEYS: state hash, work stream, due-index, active set, scopes, nudge channel. ARGV: 1 expected version, 2 operation, 3 job id,
    /// 4 scope, 5 document, 6 next deadline (empty for none), 7 running (1 or 0), 8 entry count, then per entry the field count
    /// followed by the field names and values. No length limit on the work stream (D-42).
    /// </summary>
    internal const string CompareAndSetScript = """
        local current = redis.call('HMGET', KEYS[1], 'v', 'scope', 'op')
        local currentVersion = tonumber(current[1]) or 0
        if current[2] and current[2] ~= ARGV[4] then return {3, currentVersion, current[3] or ''} end
        local expected = tonumber(ARGV[1])
        if expected >= 0 and currentVersion ~= expected then return {0, currentVersion, current[3] or ''} end
        local nextVersion = currentVersion + 1
        redis.call('HSET', KEYS[1], 'v', nextVersion, 'scope', ARGV[4], 'op', ARGV[2], 'doc', ARGV[5])
        if ARGV[6] ~= '' then redis.call('ZADD', KEYS[3], ARGV[6], ARGV[3]) else redis.call('ZREM', KEYS[3], ARGV[3]) end
        if ARGV[7] == '1' then redis.call('SADD', KEYS[4], ARGV[3]) else redis.call('SREM', KEYS[4], ARGV[3]) end
        redis.call('SADD', KEYS[5], ARGV[4])
        local entries = tonumber(ARGV[8])
        local position = 9
        for _ = 1, entries do
          local count = tonumber(ARGV[position])
          local fields = {}
          for i = 1, count do fields[i] = ARGV[position + i] end
          redis.call('XADD', KEYS[2], '*', unpack(fields))
          position = position + count + 1
        end
        if entries > 0 then redis.call('PUBLISH', KEYS[6], ARGV[4]) end
        return {1, nextVersion, ARGV[2]}
        """;

    private const string DeleteScript = """
        if redis.call('EXISTS', KEYS[1]) == 1 and redis.call('HGET', KEYS[1], 'scope') ~= ARGV[2] then return 2 end
        redis.call('DEL', KEYS[1])
        redis.call('ZREM', KEYS[2], ARGV[1])
        redis.call('SREM', KEYS[3], ARGV[1])
        return 1
        """;

    /// <summary>Initializes a new instance of the <see cref="SfcStateDao"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public SfcStateDao(ValkeyContext context)
        : base(context)
    {
    }

    /// <summary>Stores a new state atomically with the commands it produced.</summary>
    /// <param name="jobId">The job id.</param>
    /// <param name="scope">The scope; fixed for the life of the job.</param>
    /// <param name="document">The serialized state.</param>
    /// <param name="expectedVersion">The version this step builds on, 0 for the first state; <see langword="null"/> overwrites unconditionally.</param>
    /// <param name="nextDeadlineMs">The next deadline as Unix milliseconds for the due-index; <see langword="null"/> removes the job from it.</param>
    /// <param name="running">Whether the job stays in the active set.</param>
    /// <param name="workEntries">Entries appended to the scope's work stream.</param>
    /// <param name="operation">An id that makes a repeated logical write recognisable (R-4); generated when absent.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result; a conflict means another instance moved the job first.</returns>
    public Task<WriteResult> CompareAndSetAsync(string jobId, string scope, string document, long? expectedVersion, long? nextDeadlineMs, bool running, IReadOnlyList<SfcWorkEntry> workEntries, string? operation = null, CancellationToken cancellationToken = default)
    {
        RequireText(jobId, nameof(jobId));
        RequireText(scope, nameof(scope));
        RequireText(document, nameof(document));
        ArgumentNullException.ThrowIfNull(workEntries);
        var op = NewOperation(operation);
        var key = Keys.SfcState(jobId);
        var arguments = new List<RedisValue>
        {
            expectedVersion ?? -1,
            op,
            jobId,
            scope,
            document,
            nextDeadlineMs?.ToString(CultureInfo.InvariantCulture) ?? string.Empty,
            running ? "1" : "0",
            workEntries.Count,
        };
        foreach (var fields in workEntries.Select(static e => e.Fields))
        {
            arguments.Add(fields.Count * 2);
            foreach (var (name, value) in fields)
            {
                arguments.Add(name);
                arguments.Add(value);
            }
        }

        RedisKey[] keys = [key, Keys.SfcWork(scope), Keys.SfcDue(scope), Keys.SfcActive(scope), Keys.Scopes(), Keys.Nudge("sfc")];
        var values = arguments.ToArray();
        return Context.WriteAsync(key, op, async _ => await Database.ScriptEvaluateAsync(CompareAndSetScript, keys, values).ConfigureAwait(false), cancellationToken);
    }

    /// <summary>Reads a job's state.</summary>
    /// <param name="jobId">The job id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The document, or <see langword="null"/>.</returns>
    public Task<StoredDocument?> GetAsync(string jobId, CancellationToken cancellationToken = default)
    {
        RequireText(jobId, nameof(jobId));
        return ReadAsync(Database, Keys.SfcState(jobId), jobId, cancellationToken);
    }

    /// <summary>Deletes a job's state with its due-index and active-set entries.</summary>
    /// <param name="jobId">The job id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task DeleteAsync(string jobId, CancellationToken cancellationToken = default)
    {
        RequireText(jobId, nameof(jobId));
        var key = Keys.SfcState(jobId);
        for (var attempt = 0; attempt < Context.Options.WriteAttempts; attempt++)
        {
            var scope = (string?)await Database.HashGetAsync(key, "scope").WaitAsync(cancellationToken).ConfigureAwait(false);
            if (scope is null)
            {
                return;
            }

            var status = (int)await Database.ScriptEvaluateAsync(DeleteScript, [key, Keys.SfcDue(scope), Keys.SfcActive(scope)], [jobId, scope]).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (status == ValkeyContext.StatusWritten)
            {
                return;
            }
        }

        throw new InvalidOperationException("The SFC state kept changing during the delete.");
    }

    /// <summary>Lists the running jobs of a scope (R-27).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The job ids, sorted.</returns>
    public async Task<IReadOnlyList<string>> ListActiveAsync(string scope, CancellationToken cancellationToken = default)
    {
        var values = await Database.SetMembersAsync(Keys.SfcActive(scope)).WaitAsync(cancellationToken).ConfigureAwait(false);
        return [.. values.Select(static v => v.ToString()).Order(StringComparer.Ordinal)];
    }

    /// <summary>Lists the jobs whose deadline has passed (D-18).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="nowMs">The time as Unix milliseconds.</param>
    /// <param name="limit">The maximum number of jobs.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The job ids, earliest deadline first.</returns>
    public async Task<IReadOnlyList<string>> DueAsync(string scope, long nowMs, int limit = 100, CancellationToken cancellationToken = default)
    {
        var values = await Database.SortedSetRangeByScoreAsync(Keys.SfcDue(scope), double.NegativeInfinity, nowMs, take: limit).WaitAsync(cancellationToken).ConfigureAwait(false);
        return [.. values.Select(static v => v.ToString())];
    }

    /// <summary>Stores the time of the last sweep (outage rule, D-18).</summary>
    /// <param name="unixMs">The time as Unix milliseconds.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task SetHeartbeatAsync(long unixMs, CancellationToken cancellationToken = default) =>
        await Database.StringSetAsync(Keys.SfcHeartbeat(), unixMs).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Reads the time of the last sweep.</summary>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>Unix milliseconds, or <see langword="null"/> before the first sweep.</returns>
    public async Task<long?> GetHeartbeatAsync(CancellationToken cancellationToken = default)
    {
        var value = await Database.StringGetAsync(Keys.SfcHeartbeat()).WaitAsync(cancellationToken).ConfigureAwait(false);
        return value.HasValue && value.TryParse(out long ms) ? ms : null;
    }

    /// <summary>Stores a recipe text by content hash; written once, an existing hash is kept (D-44).</summary>
    /// <param name="hash">The content hash.</param>
    /// <param name="recipe">The recipe text.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when this call stored it.</returns>
    public async Task<bool> PutRecipeAsync(string hash, string recipe, CancellationToken cancellationToken = default) =>
        await Database.StringSetAsync(Keys.SfcRecipe(hash), recipe, when: When.NotExists).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Reads a recipe text by content hash.</summary>
    /// <param name="hash">The content hash.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The text, or <see langword="null"/>.</returns>
    public async Task<string?> GetRecipeAsync(string hash, CancellationToken cancellationToken = default) =>
        (string?)await Database.StringGetAsync(Keys.SfcRecipe(hash)).WaitAsync(cancellationToken).ConfigureAwait(false);
}
