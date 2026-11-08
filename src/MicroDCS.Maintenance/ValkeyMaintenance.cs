using System.Globalization;
using MicroDCS.Core.Transport;
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

    // Removes the targets in one step. ARGV holds three values per key: operation, argument, expected version (or empty).
    // Nothing is removed when a version differs.
    private const string DeleteScript = """
        for i = 1, #KEYS do
          local expected = ARGV[i * 3]
          if expected ~= '' then
            if redis.call('HGET', KEYS[i], 'v') ~= expected then return {0, i} end
          end
        end
        local removed = 0
        for i = 1, #KEYS do
          local op = ARGV[i * 3 - 2]
          local arg = ARGV[i * 3 - 1]
          if op == 'del' then removed = removed + redis.call('DEL', KEYS[i])
          elseif op == 'srem' then removed = removed + redis.call('SREM', KEYS[i], arg)
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

    /// <summary>Initializes a new instance of the <see cref="ValkeyMaintenance"/> class.</summary>
    /// <param name="context">The Valkey context.</param>
    /// <param name="options">The options.</param>
    /// <param name="environment">The host environment, for the guard.</param>
    /// <param name="time">The clock.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public ValkeyMaintenance(ValkeyContext context, IOptions<MaintenanceOptions> options, IHostEnvironment environment, TimeProvider time, ILoggerFactory loggerFactory)
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
    }

    private IDatabase Database => _context.Multiplexer.GetDatabase();

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
    public async ValueTask<MaintenancePlan> PlanRestoreAsync(string caller, string snapshotId, bool overwrite, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(snapshotId);
        await GuardAsync(caller, RestoreTool, cancellationToken).ConfigureAwait(false);
        var snapshot = await LoadSnapshotAsync(caller, snapshotId, cancellationToken).ConfigureAwait(false);
        var targets = snapshot.Items.Select(static i => new Target(i.Key, i.Part, i.Type switch { "setmember" => "setmember", "hashfield" => "hashfield", _ => null })).ToList();
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
        if (snapshot.Items.Any(static i => i.Type == "stream"))
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

        var result = pending.RestoreItems is null
            ? await ApplyDeleteAsync(caller, pending, current, cancellationToken).ConfigureAwait(false)
            : await ApplyRestoreAsync(caller, pending, current, cancellationToken).ConfigureAwait(false);
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

        var keys = new RedisKey[removals.Count];
        var values = new RedisValue[removals.Count * 3];
        for (var i = 0; i < removals.Count; i++)
        {
            var (target, image) = removals[i];
            keys[i] = target.Key;
            values[(i * 3)] = target.PartKind switch { "setmember" => "srem", "hashfield" => "hdel", _ => "del" };
            values[(i * 3) + 1] = target.Part ?? string.Empty;
            values[(i * 3) + 2] = target.PartKind is null && image.Version is { } version ? version.ToString(CultureInfo.InvariantCulture) : string.Empty;
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

        var transaction = Database.CreateTransaction();
        var tasks = new List<Task>();
        var readded = 0;
        foreach (var image in write)
        {
            readded += image.QueueRestore(transaction, tasks);
        }

        if (!await transaction.ExecuteAsync().WaitAsync(cancellationToken).ConfigureAwait(false))
        {
            throw Refuse(caller, plan.Tool, "The restore transaction was not executed. Nothing was written.");
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
