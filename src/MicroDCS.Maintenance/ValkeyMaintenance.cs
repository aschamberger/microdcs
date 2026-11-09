using System.Globalization;
using System.Text.Json;
using MicroDCS.Core.Transport;
using MicroDCS.Sfc;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Documents;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace MicroDCS.Maintenance;

/// <summary>
/// The maintenance operations over the Valkey state (D-19, D-45). Targets come from the key schema, never from raw patterns.
/// Every apply checks the digest of the plan, writes a snapshot, and then removes or writes in one script or transaction.
/// Hash documents are deleted only when their version still matches the plan (compare-and-delete).
/// </summary>
public sealed class ValkeyMaintenance : IMaintenanceOperations
{
    private const string DedupeTool = "maint_dedupe_release";
    private const string PurgeTool = "maint_purge_scope";
    private const string RestoreTool = "maint_restore";
    private const string JobDeleteTool = "maint_job_delete";
    private const string RetainedClearTool = "maint_retained_clear";
    private const string PublisherResetTool = "maint_publisher_reset";
    private const string SfcJobDeleteTool = "maint_sfc_job_delete";
    private const string SfcRebuildDueTool = "maint_sfc_rebuild_due_index";
    private const string StreamRequeueTool = "maint_stream_requeue";
    private const string StreamAckTool = "maint_stream_ack";

    // Replaces a due-index, guarded by the cardinality the plan saw. ARGV: expected cardinality, then member and score pairs. KEYS: due-index.
    private const string RebuildDueScript = """
        if redis.call('ZCARD', KEYS[1]) ~= tonumber(ARGV[1]) then return 0 end
        redis.call('DEL', KEYS[1])
        for i = 2, #ARGV, 2 do redis.call('ZADD', KEYS[1], ARGV[i + 1], ARGV[i]) end
        return 1
        """;

    // Moves a dead entry back to work. KEYS: dead stream, work stream, nudge channel. ARGV: entry id, scope, then field names and values.
    private const string RequeueScript = """
        if #redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1]) == 0 then return 0 end
        local fields = {}
        for i = 3, #ARGV do fields[#fields + 1] = ARGV[i] end
        redis.call('XADD', KEYS[2], '*', unpack(fields))
        redis.call('XDEL', KEYS[1], ARGV[1])
        redis.call('PUBLISH', KEYS[3], ARGV[2])
        return 1
        """;

    // Removes the targets in one step. ARGV holds three values per key: operation, argument (set member or hash field) and the
    // fingerprint of the plan image (KeyImage.Fingerprint). Nothing is removed when any target differs from its fingerprint (RV-21).
    // redis.pcall: a key whose type changed returns an error table, which compares unequal.
    private const string DeleteScript = """
        local function matches(key, arg, e)
          local c = string.sub(e, 1, 1)
          local p = string.sub(e, 2)
          if c == 'v' then return redis.pcall('HGET', key, 'v') == p
          elseif c == 'S' then return redis.pcall('GET', key) == p
          elseif c == 'h' then return tostring(redis.pcall('HLEN', key)) == p
          elseif c == 's' then return tostring(redis.pcall('SCARD', key)) == p
          elseif c == 'z' then return tostring(redis.pcall('ZCARD', key)) == p
          elseif c == 'x' then
            local last = redis.pcall('XREVRANGE', key, '+', '-', 'COUNT', 1)
            local id = ''
            if type(last) == 'table' and last[1] then id = last[1][1] end
            return (tostring(redis.pcall('XLEN', key)) .. '|' .. id) == p
          elseif c == 'm' then return redis.pcall('SISMEMBER', key, arg) == 1
          elseif c == 'M' then return type(redis.pcall('ZSCORE', key, arg)) == 'string'
          elseif c == 'f' then return redis.pcall('HGET', key, arg) == p
          elseif c == 'e' then return #redis.pcall('XRANGE', key, arg, arg) == 1
          end
          return false
        end
        for i = 1, #KEYS do
          if not matches(KEYS[i], ARGV[i * 3 - 1], ARGV[i * 3]) then return {0, i} end
        end
        local removed = 0
        for i = 1, #KEYS do
          local op = ARGV[i * 3 - 2]
          local arg = ARGV[i * 3 - 1]
          if op == 'del' then removed = removed + redis.call('DEL', KEYS[i])
          elseif op == 'srem' then removed = removed + redis.call('SREM', KEYS[i], arg)
          elseif op == 'zrem' then removed = removed + redis.call('ZREM', KEYS[i], arg)
          elseif op == 'xdel' then
            redis.call('XACK', KEYS[i], string.sub(ARGV[i * 3], 2), arg)
            removed = removed + redis.call('XDEL', KEYS[i], arg)
          else removed = removed + redis.call('HDEL', KEYS[i], arg) end
        end
        return {1, removed}
        """;

    private readonly ValkeyContext _context;
    private readonly MaintenanceOptions _options;
    private readonly IHostEnvironment _environment;
    private readonly TimeProvider _time;
    private readonly ILogger _audit;
    private readonly SnapshotStore _snapshots;
    private readonly PlanStore _plans;
    private readonly IReadOnlyList<IStateTopicCatalog> _catalogs;

    /// <summary>Initializes a new instance of the <see cref="ValkeyMaintenance"/> class.</summary>
    /// <param name="context">The Valkey context.</param>
    /// <param name="options">The options.</param>
    /// <param name="environment">The host environment, for the guard.</param>
    /// <param name="time">The clock.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="catalogs">The components that keep state topics; none when absent.</param>
    public ValkeyMaintenance(ValkeyContext context, IOptions<MaintenanceOptions> options, IHostEnvironment environment, TimeProvider time, ILoggerFactory loggerFactory, IEnumerable<IStateTopicCatalog>? catalogs = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(environment);
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _context = context;
        _options = options.Value;
        _environment = environment;
        _time = time;
        _audit = loggerFactory.CreateLogger(PipelineTelemetry.SecurityCategory);
        _snapshots = new SnapshotStore(_options, time, _audit);
        _plans = new PlanStore(time);
        _catalogs = catalogs is null ? [] : [.. catalogs];
    }

    private IDatabase Database => _context.Multiplexer.GetDatabase();

    /// <summary>Test seam: runs after the snapshot is written and before the change, where a concurrent write is the race of RV-21/RV-22.</summary>
    internal Func<Task>? BeforeChange { get; set; }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanDedupeReleaseAsync(string caller, string source, string id, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(source);
        ArgumentException.ThrowIfNullOrEmpty(id);
        await GuardAsync(caller, DedupeTool, cancellationToken).ConfigureAwait(false);
        var key = _context.Keys.CloudEventDedupe(source, id);
        var current = await KeyImage.CaptureAsync(Database, new Target(key), cancellationToken).ConfigureAwait(false);
        if (current is null)
        {
            throw Refuse(caller, DedupeTool, $"There is no dedupe entry for source '{source}' and id '{id}'.");
        }

        var state = current.Value is "done" ? "done (processed)" : "processing (lease)";
        return await CreateDeletePlanAsync(
            caller,
            DedupeTool,
            $"source={source},id={id}",
            $"Releases the dedupe entry, state {state}. The event can be claimed and processed again.",
            [new Target(key)],
            ["The entry is removed without checking whether a handler is still running; a handler that still holds the lease finishes anyway."],
            allowLarge: false,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanPurgeScopeAsync(string caller, string scope, bool allowLarge, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(scope);
        await GuardAsync(caller, PurgeTool, cancellationToken).ConfigureAwait(false);
        var targets = await ScopeTargetsAsync(scope, cancellationToken).ConfigureAwait(false);
        return await CreateDeletePlanAsync(
            caller,
            PurgeTool,
            $"scope={scope},allow_large={allowLarge.ToString().ToLowerInvariant()}",
            $"Removes everything stored for scope '{scope}': documents, indexes, change and work streams, due-index and the registry entry.",
            targets,
            [
                "This bypasses the OPC UA state machine and the pipeline. Nobody downstream is told: the MES is not notified and retained topics on the broker stay (use the publisher reset to republish).",
                "Recipe texts (sfc:recipe:*) are shared between scopes and stay. Consumer groups and pending entries of streams are not part of a snapshot.",
            ],
            allowLarge,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanJobDeleteAsync(string caller, string id, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        await GuardAsync(caller, JobDeleteTool, cancellationToken).ConfigureAwait(false);
        var scope = (string?)await Database.HashGetAsync(_context.Keys.JobOrder(id), "scope").WaitAsync(cancellationToken).ConfigureAwait(false)
            ?? throw Refuse(caller, JobDeleteTool, $"There is no job order '{id}'.");
        var targets = await JobTargetsAsync(id, scope, cancellationToken).ConfigureAwait(false);
        return await CreateDeletePlanAsync(
            caller,
            JobDeleteTool,
            $"id={id}",
            $"Removes job order '{id}' of scope '{scope}', its job response and its SFC state.",
            targets,
            [
                "This bypasses the OPC UA state machine and the pipeline. No status event is sent, no change entry is written, and retained topics on the broker stay (use the publisher reset to republish).",
                "Entries of this job in the change and work streams stay.",
            ],
            allowLarge: false,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanRetainedClearAsync(string caller, string scope, string? id, bool allowLarge, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(scope);
        await GuardAsync(caller, RetainedClearTool, cancellationToken).ConfigureAwait(false);
        var catalog = _catalogs.Count > 0 ? _catalogs[0] : throw Refuse(caller, RetainedClearTool, "No component of this instance keeps state topics.");
        var names = await catalog.ListAsync(scope, string.IsNullOrEmpty(id) ? null : id, cancellationToken).ConfigureAwait(false);
        if (names.Count == 0)
        {
            throw Refuse(caller, RetainedClearTool, "There are no state topics for these arguments.");
        }

        CheckCap(caller, RetainedClearTool, names.Count, allowLarge);
        var items = names.Select(static n => new PlanItem("retained", n, "delete", "state topic on the broker")).ToList();
        var arguments = $"scope={scope},id={id},allow_large={allowLarge.ToString().ToLowerInvariant()}";
        var plan = NewPlan(
            RetainedClearTool,
            arguments,
            $"Removes {names.Count} state topics of scope '{scope}' on the broker.",
            items,
            [
                "Broker state is not part of a snapshot and cannot be restored. A running publisher writes the topics of live job orders again on their next change or refresh; use the publisher reset to do so at once.",
                "The list comes from the stored job orders. Topics of orders that are no longer stored are only removed when the id is named.",
            ],
            KeyImage.Digest([]));
        _plans.Add(new PendingPlan { Plan = plan, Targets = [], Retained = new RetainedTopics(catalog, scope, names) });
        Log.Planned(_audit, caller, plan.Tool, plan.Arguments, items.Count, plan.Digest);
        return plan;
    }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanPublisherResetAsync(string caller, string scope, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(scope);
        await GuardAsync(caller, PublisherResetTool, cancellationToken).ConfigureAwait(false);
        return await CreateDeletePlanAsync(
            caller,
            PublisherResetTool,
            $"scope={scope}",
            $"Forgets the publisher cursor of scope '{scope}'. The publisher then writes all its state topics again from the stored documents and replays the change stream.",
            [new Target(_context.Keys.PublisherCursors(), scope, "hashfield")],
            ["Subscribers receive every state topic of the scope again. The sequence of the state index keeps growing."],
            allowLarge: false,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanSfcJobDeleteAsync(string caller, string id, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        await GuardAsync(caller, SfcJobDeleteTool, cancellationToken).ConfigureAwait(false);
        var keys = _context.Keys;
        var scope = (string?)await Database.HashGetAsync(keys.SfcState(id), "scope").WaitAsync(cancellationToken).ConfigureAwait(false)
            ?? throw Refuse(caller, SfcJobDeleteTool, $"There is no SFC state for job '{id}'.");
        return await CreateDeletePlanAsync(
            caller,
            SfcJobDeleteTool,
            $"id={id}",
            $"Removes the SFC state of job '{id}' of scope '{scope}'. The job order and its response stay.",
            [new Target(keys.SfcState(id)), new Target(keys.SfcActive(scope), id, "setmember"), new Target(keys.SfcDue(scope), id, "zsetmember")],
            ["The job order keeps its state, so a running job stays Running with no recipe behind it. Work entries of this job stay and find no state."],
            allowLarge: false,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanSfcRebuildDueIndexAsync(string caller, string scope, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(scope);
        await GuardAsync(caller, SfcRebuildDueTool, cancellationToken).ConfigureAwait(false);
        var keys = _context.Keys;
        var active = await Database.SetMembersAsync(keys.SfcActive(scope)).WaitAsync(cancellationToken).ConfigureAwait(false);
        var wanted = new SortedDictionary<string, long>(StringComparer.Ordinal);
        foreach (var job in active.Select(static v => v.ToString()))
        {
            var document = (string?)await Database.HashGetAsync(keys.SfcState(job), "doc").WaitAsync(cancellationToken).ConfigureAwait(false);
            if (document is not null && JsonSerializer.Deserialize(document, SfcJsonContext.Default.SfcState)?.NextDeadline is { } deadline)
            {
                wanted[job] = deadline;
            }
        }

        var target = new Target(keys.SfcDue(scope));
        var image = await KeyImage.CaptureAsync(Database, target, cancellationToken).ConfigureAwait(false);
        var current = (image?.Scored ?? []).ToDictionary(static s => s.Member, static s => (long)s.Score, StringComparer.Ordinal);
        var items = new List<PlanItem>();
        foreach (var (job, deadline) in wanted.Where(w => !current.TryGetValue(w.Key, out var score) || score != w.Value))
        {
            items.Add(new PlanItem("sfcdue", job, current.ContainsKey(job) ? "update" : "add", deadline.ToString(CultureInfo.InvariantCulture)));
        }

        items.AddRange(current.Keys.Where(c => !wanted.ContainsKey(c)).Order(StringComparer.Ordinal).Select(static job => new PlanItem("sfcdue", job, "remove", "no open deadline")));
        if (items.Count == 0)
        {
            throw Refuse(caller, SfcRebuildDueTool, "The due-index already matches the job states.");
        }

        CheckCap(caller, SfcRebuildDueTool, items.Count, allowLarge: false);
        var plan = NewPlan(SfcRebuildDueTool, $"scope={scope}", $"Writes the due-index of scope '{scope}' again: {items.Count} of {wanted.Count} entries differ.", items, ["A sweep or a step of a running instance can change the index between the plan and the apply; the apply then refuses."], KeyImage.Digest([image]));
        _plans.Add(new PendingPlan { Plan = plan, Targets = [target], DueIndex = new DueIndexRebuild(keys.SfcDue(scope), [.. wanted.Select(static w => (w.Key, w.Value))]) });
        Log.Planned(_audit, caller, plan.Tool, plan.Arguments, items.Count, plan.Digest);
        return plan;
    }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanStreamRequeueAsync(string caller, string scope, string entryId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(scope);
        ArgumentException.ThrowIfNullOrEmpty(entryId);
        await GuardAsync(caller, StreamRequeueTool, cancellationToken).ConfigureAwait(false);
        var keys = _context.Keys;
        var target = new Target(keys.SfcDead(scope), entryId, "streamentry");
        var image = await KeyImage.CaptureAsync(Database, target, cancellationToken).ConfigureAwait(false)
            ?? throw Refuse(caller, StreamRequeueTool, $"The dead stream of scope '{scope}' has no entry '{entryId}'.");
        var plan = NewPlan(
            StreamRequeueTool,
            $"scope={scope},id={entryId}",
            $"Moves dead entry '{entryId}' of scope '{scope}' back to the work stream.",
            [new PlanItem("sfcdead", entryId, "requeue", image.Describe)],
            ["The entry runs again with a new id and a new delivery count. A step that already ran is absorbed by the core."],
            KeyImage.Digest([image]));
        _plans.Add(new PendingPlan { Plan = plan, Targets = [target], Requeue = new StreamRequeue(keys.SfcWork(scope), keys.Nudge("sfc"), scope, entryId) });
        Log.Planned(_audit, caller, plan.Tool, plan.Arguments, 1, plan.Digest);
        return plan;
    }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanStreamAckAsync(string caller, string scope, string entryId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(scope);
        ArgumentException.ThrowIfNullOrEmpty(entryId);
        await GuardAsync(caller, StreamAckTool, cancellationToken).ConfigureAwait(false);
        return await CreateDeletePlanAsync(
            caller,
            StreamAckTool,
            $"scope={scope},id={entryId}",
            $"Acknowledges and removes work entry '{entryId}' of scope '{scope}' without running it.",
            [new Target(_context.Keys.SfcWork(scope), entryId, "streamentry")],
            ["What the entry would have done does not happen: a job can stay waiting until its next deadline or for ever. Restoring the snapshot adds the entry again with a new id."],
            allowLarge: false,
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<MaintenancePlan> PlanRestoreAsync(string caller, string snapshotId, bool overwrite, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(snapshotId);
        await GuardAsync(caller, RestoreTool, cancellationToken).ConfigureAwait(false);
        var snapshot = await LoadSnapshotAsync(caller, snapshotId, cancellationToken).ConfigureAwait(false);
        var targets = snapshot.Items.Select(static i => new Target(i.Key, i.Part, i.Type switch { "setmember" => "setmember", "zsetmember" => "zsetmember", "hashfield" => "hashfield", "streamentry" => "streamentry", _ => null })).ToList();
        var current = await CaptureAllAsync(targets, cancellationToken).ConfigureAwait(false);

        var items = new List<PlanItem>();
        var conflicts = new List<string>();
        var warnings = new List<string>();
        var write = 0;
        for (var i = 0; i < snapshot.Items.Count; i++)
        {
            var wanted = snapshot.Items[i];
            var action = "restore";
            if (current[i] is { } existing)
            {
                action = existing.Canonical() == wanted.Canonical() ? "present" : "overwrite";
            }

            if (action == "overwrite" && !overwrite)
            {
                conflicts.Add(Describe(wanted));
            }

            write += action == "present" ? 0 : 1;
            items.Add(new PlanItem(KindOf(wanted.Key), Describe(wanted), action, wanted.Describe));
        }

        if (conflicts.Count > 0)
        {
            throw Refuse(caller, RestoreTool, $"{conflicts.Count} targets exist with different content and would be replaced: {string.Join(", ", conflicts.Take(10))}. Plan again with overwrite to replace them (they are snapshotted first).");
        }

        if (write == 0)
        {
            throw Refuse(caller, RestoreTool, "Everything in the snapshot is already present and unchanged.");
        }

        CheckCap(caller, RestoreTool, write, allowLarge: false);
        if (snapshot.Items.Any(static i => i.Type is "stream" or "streamentry"))
        {
            warnings.Add("Stream entries are added again with new ids; consumers see them as new work, and a publisher cursor from before the purge makes the publisher read the change entries again. Consumer groups and pending entries are not restored.");
        }

        warnings.Add("Restore writes the stored images back as they were, including their versions and index structures. Retained topics on the broker are not touched.");
        var plan = NewPlan(RestoreTool, $"snapshot={snapshotId},overwrite={overwrite.ToString().ToLowerInvariant()}", $"Puts {write} of {snapshot.Items.Count} structures of snapshot '{snapshotId}' back.", items, warnings, KeyImage.Digest(current));
        _plans.Add(new PendingPlan { Plan = plan, Targets = targets, RestoreItems = snapshot.Items, Overwrite = overwrite });
        Log.Planned(_audit, caller, plan.Tool, plan.Arguments, items.Count, plan.Digest);
        return plan;
    }

    /// <inheritdoc />
    public async ValueTask<MaintenanceResult> ApplyAsync(string caller, string token, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);
        await GuardAsync(caller, "maint_apply", cancellationToken).ConfigureAwait(false);
        PendingPlan pending;
        try
        {
            pending = _plans.Take(token);
        }
        catch (MaintenanceRefusedException ex)
        {
            throw Refuse(caller, "maint_apply", ex.Message);
        }

        var plan = pending.Plan;
        var current = await CaptureAllAsync(pending.Targets, cancellationToken).ConfigureAwait(false);
        var digest = KeyImage.Digest(current);
        if (digest != plan.Digest)
        {
            throw Refuse(caller, plan.Tool, $"The targets changed since the plan (digest {plan.Digest}, now {digest}). Nothing was changed. Plan again.");
        }

        MaintenanceResult result;
        if (pending.Retained is { } retained)
        {
            result = await ApplyRetainedClearAsync(plan, retained, cancellationToken).ConfigureAwait(false);
        }
        else if (pending.DueIndex is { } dueIndex)
        {
            result = await ApplyDueRebuildAsync(caller, pending, dueIndex, current, cancellationToken).ConfigureAwait(false);
        }
        else if (pending.Requeue is { } requeue)
        {
            result = await ApplyRequeueAsync(caller, pending, requeue, current, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            result = pending.RestoreItems is null
                ? await ApplyDeleteAsync(caller, pending, current, cancellationToken).ConfigureAwait(false)
                : await ApplyRestoreAsync(caller, pending, current, cancellationToken).ConfigureAwait(false);
        }

        Log.Applied(_audit, caller, plan.Tool, plan.Arguments, result.Changed, result.SnapshotId ?? "-", plan.Digest);
        return result;
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<SnapshotInfo>> ListSnapshotsAsync(string caller, CancellationToken cancellationToken)
    {
        await GuardAsync(caller, "maint_snapshots_list", cancellationToken).ConfigureAwait(false);
        return await _snapshots.ListAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<MaintenancePlan> CreateDeletePlanAsync(string caller, string tool, string arguments, string summary, List<Target> targets, List<string> warnings, bool allowLarge, CancellationToken cancellationToken)
    {
        var images = await CaptureAllAsync(targets, cancellationToken).ConfigureAwait(false);
        var existing = images.Where(static i => i is not null).Select(static i => i!).ToList();
        if (existing.Count == 0)
        {
            throw Refuse(caller, tool, "Nothing is stored for these arguments.");
        }

        CheckCap(caller, tool, existing.Count, allowLarge);
        var items = existing.Select(i => new PlanItem(KindOf(i.Key), Describe(i), "delete", i.Describe)).ToList();
        var plan = NewPlan(tool, arguments, summary, items, warnings, KeyImage.Digest(images));
        _plans.Add(new PendingPlan { Plan = plan, Targets = targets });
        Log.Planned(_audit, caller, tool, arguments, items.Count, plan.Digest);
        return plan;
    }

    private static async Task<MaintenanceResult> ApplyRetainedClearAsync(MaintenancePlan plan, RetainedTopics retained, CancellationToken cancellationToken)
    {
        foreach (var name in retained.Names)
        {
            await retained.Catalog.DeleteAsync(retained.Scope, name, cancellationToken).ConfigureAwait(false);
        }

        return new MaintenanceResult(plan.Tool, retained.Names.Count, null, ["No snapshot: broker state is not covered."]);
    }

    private async Task<string> WriteSnapshotAsync(string caller, MaintenancePlan plan, List<KeyImage> images, CancellationToken cancellationToken)
    {
        try
        {
            return await _snapshots.WriteAsync(plan.Tool, plan.Arguments, plan.Digest, _context.Options.KeyPrefix, images, cancellationToken).ConfigureAwait(false);
        }
        catch (MaintenanceRefusedException ex)
        {
            throw Refuse(caller, plan.Tool, ex.Message);
        }
    }

    private async Task<MaintenanceResult> ApplyDueRebuildAsync(string caller, PendingPlan pending, DueIndexRebuild rebuild, KeyImage?[] current, CancellationToken cancellationToken)
    {
        var plan = pending.Plan;
        var snapshotId = current[0] is { } image ? await WriteSnapshotAsync(caller, plan, [image], cancellationToken).ConfigureAwait(false) : null;
        if (BeforeChange is { } beforeChange)
        {
            await beforeChange().ConfigureAwait(false);
        }

        var values = new List<RedisValue>(1 + (rebuild.Entries.Count * 2)) { current[0]?.Scored?.Count ?? 0 };
        foreach (var (job, deadline) in rebuild.Entries)
        {
            values.Add(job);
            values.Add(deadline);
        }

        var written = (int)await Database.ScriptEvaluateAsync(RebuildDueScript, [rebuild.Key], [.. values]).WaitAsync(cancellationToken).ConfigureAwait(false);
        return written == 1
            ? new MaintenanceResult(plan.Tool, rebuild.Entries.Count, snapshotId, [])
            : throw Refuse(caller, plan.Tool, "The due-index changed between the check and the write. Nothing was written. Plan again.");
    }

    private async Task<MaintenanceResult> ApplyRequeueAsync(string caller, PendingPlan pending, StreamRequeue requeue, KeyImage?[] current, CancellationToken cancellationToken)
    {
        var plan = pending.Plan;
        var image = current[0] ?? throw Refuse(caller, plan.Tool, "The entry is gone.");
        var snapshotId = await WriteSnapshotAsync(caller, plan, [image], cancellationToken).ConfigureAwait(false);
        if (BeforeChange is { } beforeChange)
        {
            await beforeChange().ConfigureAwait(false);
        }

        var values = new List<RedisValue> { requeue.EntryId, requeue.Scope };
        foreach (var pair in (image.Pairs ?? []).Where(static p => p.Name is not ("dead_reason" or "source_id")))
        {
            values.Add(pair.Name);
            values.Add(pair.Value);
        }

        var moved = (int)await Database.ScriptEvaluateAsync(RequeueScript, [image.Key, requeue.WorkKey, requeue.Nudge], [.. values]).WaitAsync(cancellationToken).ConfigureAwait(false);
        return moved == 1
            ? new MaintenanceResult(plan.Tool, 1, snapshotId, [])
            : throw Refuse(caller, plan.Tool, $"The entry was removed between the check and the move. The snapshot {snapshotId} was written anyway. Plan again.");
    }

    private async Task<MaintenanceResult> ApplyDeleteAsync(string caller, PendingPlan pending, KeyImage?[] current, CancellationToken cancellationToken)
    {
        var plan = pending.Plan;
        var removals = new List<(Target Target, KeyImage Image)>();
        for (var i = 0; i < current.Length; i++)
        {
            if (current[i] is { } image)
            {
                removals.Add((pending.Targets[i], image));
            }
        }

        string snapshotId;
        try
        {
            snapshotId = await _snapshots.WriteAsync(plan.Tool, plan.Arguments, plan.Digest, _context.Options.KeyPrefix, [.. removals.Select(static r => r.Image)], cancellationToken).ConfigureAwait(false);
        }
        catch (MaintenanceRefusedException ex)
        {
            throw Refuse(caller, plan.Tool, ex.Message);
        }

        if (BeforeChange is { } beforeDelete)
        {
            await beforeDelete().ConfigureAwait(false);
        }

        var keys = new RedisKey[removals.Count];
        var values = new RedisValue[removals.Count * 3];
        for (var i = 0; i < removals.Count; i++)
        {
            var (target, image) = removals[i];
            keys[i] = target.Key;
            values[(i * 3)] = target.PartKind switch { "setmember" => "srem", "zsetmember" => "zrem", "hashfield" => "hdel", "streamentry" => "xdel", _ => "del" };
            values[(i * 3) + 1] = target.Part ?? string.Empty;
            values[(i * 3) + 2] = image.Fingerprint();
        }

        var reply = (RedisResult[]?)await Database.ScriptEvaluateAsync(DeleteScript, keys, values).WaitAsync(cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException("The delete script returned no result.");
        if ((int)reply[0] != 1)
        {
            var changed = Describe(removals[(int)reply[1] - 1].Image);
            throw Refuse(caller, plan.Tool, $"{changed} changed between the check and the delete (a running instance wrote it). Nothing was removed. The snapshot {snapshotId} was written anyway. Plan again.");
        }

        return new MaintenanceResult(plan.Tool, (int)reply[1], snapshotId, []);
    }

    private async Task<MaintenanceResult> ApplyRestoreAsync(string caller, PendingPlan pending, KeyImage?[] current, CancellationToken cancellationToken)
    {
        var plan = pending.Plan;
        var items = pending.RestoreItems!;
        var write = new List<KeyImage>();
        var overwritten = new List<KeyImage>();
        for (var i = 0; i < items.Count; i++)
        {
            if (current[i] is { } existing)
            {
                if (existing.Canonical() == items[i].Canonical())
                {
                    continue;
                }

                overwritten.Add(existing);
            }

            write.Add(items[i]);
        }

        string? snapshotId = null;
        if (overwritten.Count > 0)
        {
            try
            {
                snapshotId = await _snapshots.WriteAsync(plan.Tool, plan.Arguments, plan.Digest, _context.Options.KeyPrefix, overwritten, cancellationToken).ConfigureAwait(false);
            }
            catch (MaintenanceRefusedException ex)
            {
                throw Refuse(caller, plan.Tool, ex.Message);
            }
        }

        if (BeforeChange is { } beforeRestore)
        {
            await beforeRestore().ConfigureAwait(false);
        }

        var transaction = Database.CreateTransaction();
        for (var i = 0; i < items.Count; i++)
        {
            // RV-22: the live state must still be what the recheck saw, or nothing is written (a running instance wrote in between).
            // A stream entry is appended with a new id, so there is nothing it could overwrite.
            if (pending.Targets[i].PartKind != "streamentry" && (current[i] is null || current[i]!.Canonical() != items[i].Canonical()))
            {
                transaction.AddCondition(KeyImage.ConditionFor(pending.Targets[i], current[i]));
            }
        }

        var tasks = new List<Task>();
        var readded = 0;
        foreach (var image in write)
        {
            readded += image.QueueRestore(transaction, tasks);
        }

        if (!await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false))
        {
            var kept = snapshotId is null ? string.Empty : $" The snapshot {snapshotId} was written anyway.";
            throw Refuse(caller, plan.Tool, "A target changed between the check and the restore (a running instance wrote it). Nothing was written." + kept + " Plan again.");
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
        var notes = new List<string>();
        if (readded > 0)
        {
            notes.Add($"{readded} stream entries were added again with new ids.");
            Log.StreamEntriesReadded(_audit, caller, readded);
        }

        return new MaintenanceResult(plan.Tool, write.Count, snapshotId, notes);
    }

    private async Task<SnapshotDocument> LoadSnapshotAsync(string caller, string snapshotId, CancellationToken cancellationToken)
    {
        SnapshotDocument snapshot;
        try
        {
            snapshot = await _snapshots.LoadAsync(snapshotId, cancellationToken).ConfigureAwait(false);
        }
        catch (MaintenanceRefusedException ex)
        {
            throw Refuse(caller, RestoreTool, ex.Message);
        }

        return snapshot.KeyPrefix == _context.Options.KeyPrefix
            ? snapshot
            : throw Refuse(caller, RestoreTool, $"The snapshot was taken with key prefix '{snapshot.KeyPrefix}', this instance uses '{_context.Options.KeyPrefix}'.");
    }

    /// <summary>The structures of one job order, found through the key schema.</summary>
    private async Task<List<Target>> JobTargetsAsync(string id, string scope, CancellationToken cancellationToken)
    {
        var keys = _context.Keys;
        var db = Database;
        var targets = new List<Target>
        {
            new(keys.JobOrder(id)),
            new(keys.JobOrderList(scope), id, "zsetmember"),
            new(keys.JobResponseByOrder(id)),
            new(keys.SfcState(id)),
            new(keys.SfcActive(scope), id, "setmember"),
            new(keys.SfcDue(scope), id, "zsetmember"),
        };

        var responseId = (string?)await db.StringGetAsync(keys.JobResponseByOrder(id)).WaitAsync(cancellationToken).ConfigureAwait(false);
        if (responseId is not null)
        {
            var state = (string?)await db.HashGetAsync(keys.JobResponse(responseId), "state").WaitAsync(cancellationToken).ConfigureAwait(false);
            targets.Add(new Target(keys.JobResponse(responseId)));
            targets.Add(new Target(keys.JobResponseList(scope), responseId, "zsetmember"));
            if (!string.IsNullOrEmpty(state))
            {
                targets.Add(new Target(keys.JobResponseByState(scope, state), responseId, "setmember"));
            }
        }

        return targets;
    }

    /// <summary>The structures of one scope, found through the key schema and the lists it holds.</summary>
    private async Task<List<Target>> ScopeTargetsAsync(string scope, CancellationToken cancellationToken)
    {
        var keys = _context.Keys;
        var db = Database;
        var targets = new List<Target>();

        async Task<RedisValue[]> Sorted(string key) => await db.SortedSetRangeByRankAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);
        async Task<RedisValue[]> Members(string key) => await db.SetMembersAsync(key).WaitAsync(cancellationToken).ConfigureAwait(false);

        targets.Add(new Target(keys.JobOrderList(scope)));
        foreach (var id in await Sorted(keys.JobOrderList(scope)).ConfigureAwait(false))
        {
            targets.Add(new Target(keys.JobOrder(id.ToString())));
        }

        targets.Add(new Target(keys.JobResponseList(scope)));
        var states = new HashSet<string>(StringComparer.Ordinal);
        foreach (var id in await Sorted(keys.JobResponseList(scope)).ConfigureAwait(false))
        {
            targets.Add(new Target(keys.JobResponse(id.ToString())));
            var fields = await db.HashGetAsync(keys.JobResponse(id.ToString()), ["order", "state"]).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (fields[0].HasValue && fields[0] != string.Empty)
            {
                targets.Add(new Target(keys.JobResponseByOrder(fields[0].ToString())));
            }

            if (fields[1].HasValue && fields[1] != string.Empty)
            {
                states.Add(fields[1].ToString());
            }
        }

        targets.AddRange(states.Order(StringComparer.Ordinal).Select(state => new Target(keys.JobResponseByState(scope, state))));

        targets.Add(new Target(keys.WorkMasterList(scope)));
        foreach (var id in await Members(keys.WorkMasterList(scope)).ConfigureAwait(false))
        {
            targets.Add(new Target(keys.WorkMaster(id.ToString())));
        }

        targets.AddRange(ReferenceListDao.Kinds.Select(kind => new Target(keys.Reference(kind, scope))));
        targets.Add(new Target(keys.JobAcceptance(scope)));
        targets.Add(new Target(keys.Changes(scope)));
        targets.Add(new Target(keys.PublishSequence(scope)));
        targets.Add(new Target(keys.SfcWork(scope)));
        targets.Add(new Target(keys.SfcDead(scope)));
        targets.Add(new Target(keys.SfcActive(scope)));
        targets.Add(new Target(keys.SfcDue(scope)));
        var sfcJobs = new HashSet<string>(StringComparer.Ordinal);
        sfcJobs.UnionWith((await Members(keys.SfcActive(scope)).ConfigureAwait(false)).Select(static v => v.ToString()));
        sfcJobs.UnionWith((await Sorted(keys.SfcDue(scope)).ConfigureAwait(false)).Select(static v => v.ToString()));
        targets.AddRange(sfcJobs.Order(StringComparer.Ordinal).Select(id => new Target(keys.SfcState(id))));

        targets.Add(new Target(keys.PublisherCursors(), scope, "hashfield"));
        targets.Add(new Target(keys.Scopes(), scope, "setmember"));
        return targets;
    }

    private async Task<KeyImage?[]> CaptureAllAsync(List<Target> targets, CancellationToken cancellationToken)
    {
        var images = new KeyImage?[targets.Count];
        for (var i = 0; i < targets.Count; i++)
        {
            images[i] = await KeyImage.CaptureAsync(Database, targets[i], cancellationToken).ConfigureAwait(false);
        }

        return images;
    }

    private MaintenancePlan NewPlan(string tool, string arguments, string summary, List<PlanItem> items, List<string> warnings, string digest) =>
        new(PlanStore.NewToken(), tool, arguments, summary, items, warnings, digest, _time.GetUtcNow() + _options.PlanLifetime);

    private void CheckCap(string caller, string tool, int count, bool allowLarge)
    {
        if (count > _options.ItemCap && !allowLarge)
        {
            throw Refuse(caller, tool, $"The plan touches {count} targets; the cap is {_options.ItemCap}. Plan again with allow_large if that is intended.");
        }
    }

    /// <summary>The guards: enabled, environment, and the optional marker key. Every public call passes through here.</summary>
    private async Task GuardAsync(string caller, string tool, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(caller);
        if (!_options.Enabled)
        {
            throw Refuse(caller, tool, "Maintenance is not enabled (MicroDcs:Maintenance:Enabled).");
        }

        if (!MaintenanceOptionsValidator.IsAllowedEnvironment(_environment.EnvironmentName))
        {
            throw Refuse(caller, tool, $"Maintenance only runs in the environments Development and Test, not '{_environment.EnvironmentName}'.");
        }

        if (_options.RequireEnvironmentMarker)
        {
            var marker = await Database.StringGetAsync(_context.Keys.EnvironmentMarker()).WaitAsync(cancellationToken).ConfigureAwait(false);
            if (!marker.HasValue || !string.Equals(marker.ToString(), _environment.EnvironmentName, StringComparison.OrdinalIgnoreCase))
            {
                throw Refuse(caller, tool, $"The marker key {RelativeKey(_context.Keys.EnvironmentMarker())} is missing or does not say '{_environment.EnvironmentName}'. This Valkey is not marked as a dev or test instance.");
            }
        }
    }

    private MaintenanceRefusedException Refuse(string caller, string tool, string reason)
    {
        Log.Refused(_audit, caller, tool, reason);
        return new MaintenanceRefusedException(reason);
    }

    private string RelativeKey(string key) =>
        key.StartsWith(_context.Options.KeyPrefix + ":", StringComparison.Ordinal) ? key[(_context.Options.KeyPrefix.Length + 1)..] : key;

    private string KindOf(string key)
    {
        var relative = RelativeKey(key);
        var colon = relative.IndexOf(':', StringComparison.Ordinal);
        return colon < 0 ? relative : relative[..colon];
    }

    private string Describe(KeyImage image) => image.Part is null ? RelativeKey(image.Key) : $"{RelativeKey(image.Key)}[{image.Part}]";
}
