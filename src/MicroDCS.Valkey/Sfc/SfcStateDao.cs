using System.Globalization;
using MicroDCS.Valkey.Documents;
using StackExchange.Redis;

namespace MicroDCS.Valkey.Sfc;

/// <summary>One work or outbox entry that a state write appends to the scope's work stream.</summary>
/// <param name="Fields">The stream fields.</param>
public sealed record SfcWorkEntry(IReadOnlyList<KeyValuePair<string, string>> Fields);

/// <summary>The waits index and event claim a state write updates in the same step (R-1).</summary>
/// <param name="Waiting">Event types the job waits for now, with the time the wait starts as Unix milliseconds. An existing entry keeps its time.</param>
/// <param name="NotWaiting">Event types the job no longer waits for.</param>
/// <param name="ClaimedEvent">The identity of the event this step applied, if any. A write whose event is already claimed is refused like a version conflict.</param>
public sealed record SfcWaitChanges(IReadOnlyList<(string TypeId, long Since)> Waiting, IReadOnlyList<string> NotWaiting, string? ClaimedEvent = null);

/// <summary>
/// SFC state documents and the generic compare-and-set of D-44: one script compares the state version, stores the new state, appends the
/// work entries, updates the due-index and the scope's active-job set, registers the scope and nudges the poller. R-26 to R-29.
/// </summary>
public sealed class SfcStateDao : DocumentDao
{
    /// <summary>The consumer group of the work streams (S-38).</summary>
    public const string ConsumerGroup = "sfc-engine";

    /// <summary>How long an applied event entry stays claimed: far beyond the time a pending entry can be redelivered (idle time times deliveries).</summary>
    private static readonly TimeSpan _eventClaimLifetime = TimeSpan.FromHours(1);

    /// <summary>
    /// KEYS: state hash, work stream, due-index, active set, scopes, nudge channel, then the waits to add, the waits to remove and the event claim.
    /// ARGV: 1 expected version, 2 operation, 3 job id, 4 scope, 5 document, 6 next deadline (empty for none), 7 running (1 or 0), 8 entry count, then
    /// per entry the field count followed by the field names and values, then the count of waits to add with their start times, the count of waits to
    /// remove, and the claim lifetime in ms (empty for none). Waits are removed before they are added, so a type in both lists gets a new start time.
    /// No length limit on the work stream (D-42).
    /// </summary>
    internal const string CompareAndSetScript = """
        local current = redis.call('HMGET', KEYS[1], 'v', 'scope', 'op')
        local currentVersion = tonumber(current[1]) or 0
        if current[2] and current[2] ~= ARGV[4] then return {3, currentVersion, current[3] or ''} end
        local expected = tonumber(ARGV[1])
        if expected >= 0 and currentVersion ~= expected then return {0, currentVersion, current[3] or ''} end
        if #KEYS > 6 and ARGV[#ARGV] ~= '' and redis.call('EXISTS', KEYS[#KEYS]) == 1 then return {0, currentVersion, current[3] or ''} end
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
        local adds = tonumber(ARGV[position] or '0')
        local firstAdd = position + 1
        local removes = tonumber(ARGV[firstAdd + adds] or '0')
        for i = 1, removes do redis.call('ZREM', KEYS[6 + adds + i], ARGV[3]) end
        for i = 1, adds do redis.call('ZADD', KEYS[6 + i], 'NX', ARGV[firstAdd + i - 1], ARGV[3]) end
        local claim = ARGV[firstAdd + adds + 1]
        if claim and claim ~= '' then redis.call('SET', KEYS[6 + adds + removes + 1], ARGV[3], 'PX', claim) end
        return {1, nextVersion, ARGV[2]}
        """;

    private const string DeleteScript = """
        if redis.call('EXISTS', KEYS[1]) == 1 and redis.call('HGET', KEYS[1], 'scope') ~= ARGV[2] then return 2 end
        redis.call('DEL', KEYS[1])
        redis.call('ZREM', KEYS[2], ARGV[1])
        redis.call('SREM', KEYS[3], ARGV[1])
        return 1
        """;

    /// <summary>
    /// The sweep of one scope (D-18, D-44): takes due jobs off the index and appends their <c>tick</c> entries in one step, so a job is popped by one
    /// instance and cannot be lost in between. Reads the server clock itself. The heartbeat per scope is part of the step: exactly one sweep sees a gap
    /// above the threshold, and that sweep takes every entry of the index (all open deadlines move by the gap, D-18).
    /// KEYS: due-index, work stream, heartbeat hash, nudge channel. ARGV: scope, limit, outage threshold in ms. Returns <c>{popped, outageMs}</c>.
    /// </summary>
    internal const string SweepScript = """
        local time = redis.call('TIME')
        local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
        local last = tonumber(redis.call('HGET', KEYS[3], ARGV[1]) or '0')
        redis.call('HSET', KEYS[3], ARGV[1], string.format('%.0f', now))
        local outage = 0
        if last > 0 and now - last > tonumber(ARGV[3]) then outage = now - last end
        local due
        if outage > 0 then
          due = redis.call('ZRANGE', KEYS[1], 0, -1)
        else
          due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', string.format('%.0f', now), 'LIMIT', 0, ARGV[2])
        end
        for _, job in ipairs(due) do
          redis.call('ZREM', KEYS[1], job)
          redis.call('XADD', KEYS[2], '*', 'job_id', job, 'action', 'tick', 'scope', ARGV[1], 'outage_ms', string.format('%.0f', outage))
        end
        if #due > 0 then redis.call('PUBLISH', KEYS[4], ARGV[1]) end
        return {#due, outage}
        """;

    /// <summary>The recovery scan of a scope: one <c>resume</c> entry per running job, in one step. KEYS: active set, work stream, nudge channel. ARGV: scope. Returns the number of jobs.</summary>
    internal const string QueueRecoveryScript = """
        local jobs = redis.call('SMEMBERS', KEYS[1])
        for _, job in ipairs(jobs) do
          redis.call('XADD', KEYS[2], '*', 'job_id', job, 'action', 'resume', 'scope', ARGV[1], 'scan', '1')
        end
        if #jobs > 0 then redis.call('PUBLISH', KEYS[3], ARGV[1]) end
        return #jobs
        """;

    /// <summary>Moves a pending work entry to the dead stream and acknowledges it in one step. KEYS: work stream, dead stream. ARGV: group, entry id, reason, then field names and values.</summary>
    internal const string MoveToDeadScript = """
        local fields = {}
        for i = 4, #ARGV do fields[#fields + 1] = ARGV[i] end
        redis.call('XADD', KEYS[2], 'MAXLEN', '~', 5000, '*', 'dead_reason', ARGV[3], 'source_id', ARGV[2], unpack(fields))
        redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
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
    /// <param name="waits">The waits index and event claim to update; absent for a job without pull events.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The result; a conflict means another instance moved the job first.</returns>
    public Task<WriteResult> CompareAndSetAsync(string jobId, string scope, string document, long? expectedVersion, long? nextDeadlineMs, bool running, IReadOnlyList<SfcWorkEntry> workEntries, string? operation = null, SfcWaitChanges? waits = null, CancellationToken cancellationToken = default)
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

        var keyList = new List<RedisKey> { key, Keys.SfcWork(scope), Keys.SfcDue(scope), Keys.SfcActive(scope), Keys.Scopes(), Keys.Nudge("sfc") };
        IReadOnlyList<(string TypeId, long Since)> waiting = waits?.Waiting ?? [];
        IReadOnlyList<string> notWaiting = waits?.NotWaiting ?? [];
        arguments.Add(waiting.Count);
        foreach (var (typeId, since) in waiting)
        {
            keyList.Add(Keys.SfcWaits(scope, typeId));
            arguments.Add(since);
        }

        arguments.Add(notWaiting.Count);
        keyList.AddRange(notWaiting.Select(typeId => (RedisKey)Keys.SfcWaits(scope, typeId)));
        if (waits?.ClaimedEvent is { } claimed)
        {
            keyList.Add(Keys.SfcEventClaim(scope, claimed));
            arguments.Add((long)_eventClaimLifetime.TotalMilliseconds);
        }
        else
        {
            arguments.Add(string.Empty);
        }

        RedisKey[] keys = [.. keyList];
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

    /// <summary>Sweeps the due-index of a scope and appends the <c>tick</c> entries (D-18).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="limit">The most jobs to take in an ordinary sweep.</param>
    /// <param name="outageThresholdMs">The heartbeat gap in milliseconds that counts as an outage.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The number of jobs taken and the outage length, zero without one.</returns>
    public async Task<(int Popped, long OutageMs)> SweepAsync(string scope, int limit, long outageThresholdMs, CancellationToken cancellationToken = default)
    {
        RequireText(scope, nameof(scope));
        RedisKey[] keys = [Keys.SfcDue(scope), Keys.SfcWork(scope), Keys.SfcHeartbeat(), Keys.Nudge("sfc")];
        var result = (RedisResult[]?)await Database.ScriptEvaluateAsync(SweepScript, keys, [scope, limit, outageThresholdMs]).WaitAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("The sweep script returned no result.");
        return ((int)result[0], (long)result[1]);
    }

    /// <summary>Reads the time of the last sweep of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>Unix milliseconds, or <see langword="null"/> before the first sweep.</returns>
    public async Task<long?> GetHeartbeatAsync(string scope, CancellationToken cancellationToken = default)
    {
        var value = await Database.HashGetAsync(Keys.SfcHeartbeat(), scope).WaitAsync(cancellationToken).ConfigureAwait(false);
        return value.HasValue && value.TryParse(out long ms) ? ms : null;
    }

    /// <summary>Puts a job on the due-index, for a tick that found nothing to do after the sweep took the job off (D-18).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="jobId">The job id.</param>
    /// <param name="deadlineMs">The next deadline as Unix milliseconds.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task SetDueAsync(string scope, string jobId, long deadlineMs, CancellationToken cancellationToken = default) =>
        await Database.SortedSetAddAsync(Keys.SfcDue(scope), jobId, deadlineMs).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Queues the recovery check of every running job of a scope (S-29).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The number of jobs.</returns>
    public async Task<int> QueueRecoveryAsync(string scope, CancellationToken cancellationToken = default)
    {
        RequireText(scope, nameof(scope));
        return (int)await Database.ScriptEvaluateAsync(QueueRecoveryScript, [Keys.SfcActive(scope), Keys.SfcWork(scope), Keys.Nudge("sfc")], [scope]).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Puts a job on the due-index unless it is there, for the recovery scan.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="jobId">The job id.</param>
    /// <param name="dueMs">When the job is due, as Unix milliseconds.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task AddDueIfMissingAsync(string scope, string jobId, long dueMs, CancellationToken cancellationToken = default) =>
        await Database.SortedSetAddAsync(Keys.SfcDue(scope), jobId, dueMs, When.NotExists).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Tells whether any job of a scope waits for an event type: the cheap check for every incoming event.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="typeId">The event type id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when the index has an entry; it can be stale, the state decides.</returns>
    public async Task<bool> HasWaitingAsync(string scope, string typeId, CancellationToken cancellationToken = default) =>
        await Database.SortedSetLengthAsync(Keys.SfcWaits(scope, typeId)).WaitAsync(cancellationToken).ConfigureAwait(false) > 0;

    /// <summary>Lists the jobs waiting for an event type, oldest wait first.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="typeId">The event type id.</param>
    /// <param name="limit">The most jobs to return.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>Job id and wait start as Unix milliseconds.</returns>
    public async Task<IReadOnlyList<(string JobId, long Since)>> ListWaitingAsync(string scope, string typeId, int limit, CancellationToken cancellationToken = default)
    {
        var values = await Database.SortedSetRangeByRankWithScoresAsync(Keys.SfcWaits(scope, typeId), 0, limit - 1).WaitAsync(cancellationToken).ConfigureAwait(false);
        return [.. values.Select(static v => (v.Element.ToString(), (long)v.Score))];
    }

    /// <summary>Repairs the waits index for a job; an existing entry keeps its time.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="typeId">The event type id.</param>
    /// <param name="jobId">The job id.</param>
    /// <param name="since">The wait start as Unix milliseconds.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task AddWaitingAsync(string scope, string typeId, string jobId, long since, CancellationToken cancellationToken = default) =>
        await Database.SortedSetAddAsync(Keys.SfcWaits(scope, typeId), jobId, since, When.NotExists).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Removes a stale entry of the waits index.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="typeId">The event type id.</param>
    /// <param name="jobId">The job id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task RemoveWaitingAsync(string scope, string typeId, string jobId, CancellationToken cancellationToken = default) =>
        await Database.SortedSetRemoveAsync(Keys.SfcWaits(scope, typeId), jobId).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Tells whether an event was already applied to a job.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="eventKey">The identity of the event.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when it was.</returns>
    public async Task<bool> IsEventClaimedAsync(string scope, string eventKey, CancellationToken cancellationToken = default) =>
        await Database.KeyExistsAsync(Keys.SfcEventClaim(scope, eventKey)).WaitAsync(cancellationToken).ConfigureAwait(false);

    /// <summary>Lists the due-index of a scope with the deadlines.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="limit">The most jobs to return.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>Job id and deadline as Unix milliseconds, earliest first.</returns>
    public async Task<IReadOnlyList<(string JobId, long DeadlineMs)>> ListDueAsync(string scope, int limit, CancellationToken cancellationToken = default)
    {
        var values = await Database.SortedSetRangeByRankWithScoresAsync(Keys.SfcDue(scope), 0, limit - 1).WaitAsync(cancellationToken).ConfigureAwait(false);
        return [.. values.Select(static v => (v.Element.ToString(), (long)v.Score))];
    }

    /// <summary>Reads the server clock (D-44): the time of an event that does not pass through a stream.</summary>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>Unix milliseconds.</returns>
    public async Task<long> ServerTimeAsync(CancellationToken cancellationToken = default)
    {
        var result = (RedisResult[]?)await Database.ExecuteAsync("TIME").WaitAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("TIME returned no result.");
        return ((long)result[0] * 1000) + ((long)result[1] / 1000);
    }

    /// <summary>Moves a work entry that keeps failing to the dead stream of its scope (D-44).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="group">The consumer group.</param>
    /// <param name="entry">The entry.</param>
    /// <param name="reason">The reason.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    public async Task MoveToDeadAsync(string scope, string group, StreamEntry entry, string reason, CancellationToken cancellationToken = default)
    {
        var arguments = new List<RedisValue> { group, entry.Id, reason };
        foreach (var value in entry.Values)
        {
            arguments.Add(value.Name);
            arguments.Add(value.Value);
        }

        await Database.ScriptEvaluateAsync(MoveToDeadScript, [Keys.SfcWork(scope), Keys.SfcDead(scope)], [.. arguments]).WaitAsync(cancellationToken).ConfigureAwait(false);
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
