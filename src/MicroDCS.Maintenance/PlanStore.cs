using System.Security.Cryptography;
using MicroDCS.Core.Transport;

namespace MicroDCS.Maintenance;

/// <summary>The wanted content of a due-index.</summary>
/// <param name="Key">The due-index key.</param>
/// <param name="Entries">Job id and deadline in Unix milliseconds.</param>
internal sealed record DueIndexRebuild(string Key, IReadOnlyList<(string Job, long Deadline)> Entries);

/// <summary>A dead work entry on its way back.</summary>
/// <param name="WorkKey">The work stream.</param>
/// <param name="Nudge">The nudge channel.</param>
/// <param name="Scope">The scope.</param>
/// <param name="EntryId">The entry id in the dead stream.</param>
internal sealed record StreamRequeue(string WorkKey, string Nudge, string Scope, string EntryId);

/// <summary>A plan waiting for its apply.</summary>
internal sealed class PendingPlan
{
    public required MaintenancePlan Plan { get; init; }

    /// <summary>The targets in the order of the digest.</summary>
    public required List<Target> Targets { get; init; }

    /// <summary>Images from a snapshot to write back; <see langword="null"/> for a delete plan.</summary>
    public List<KeyImage>? RestoreItems { get; init; }

    public bool Overwrite { get; init; }

    /// <summary>State topics to remove on the broker; <see langword="null"/> for a plan over Valkey state.</summary>
    public RetainedTopics? Retained { get; init; }

    /// <summary>The wanted due-index; <see langword="null"/> for other plans.</summary>
    public DueIndexRebuild? DueIndex { get; init; }

    /// <summary>The dead entry to move back; <see langword="null"/> for other plans.</summary>
    public StreamRequeue? Requeue { get; init; }
}

/// <summary>State topics a plan removes.</summary>
/// <param name="Catalog">The component that keeps them.</param>
/// <param name="Scope">The scope.</param>
/// <param name="Names">The topic names.</param>
internal sealed record RetainedTopics(IStateTopicCatalog Catalog, string Scope, IReadOnlyList<string> Names);

/// <summary>Plans in memory, single use, short lived. A restart of the process drops them on purpose.</summary>
internal sealed class PlanStore
{
    private const int MaxPending = 50;

    private readonly Dictionary<string, PendingPlan> _plans = [];
    private readonly Lock _gate = new();
    private readonly TimeProvider _time;

    public PlanStore(TimeProvider time) => _time = time;

    public static string NewToken() => Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16));

    public void Add(PendingPlan plan)
    {
        lock (_gate)
        {
            var now = _time.GetUtcNow();
            foreach (var expired in _plans.Where(p => p.Value.Plan.ExpiresAt <= now).Select(static p => p.Key).ToList())
            {
                _plans.Remove(expired);
            }

            while (_plans.Count >= MaxPending)
            {
                _plans.Remove(_plans.MinBy(static p => p.Value.Plan.ExpiresAt).Key);
            }

            _plans[plan.Plan.Token] = plan;
        }
    }

    /// <summary>Removes and returns the plan. A token works once, also when the apply then fails.</summary>
    public PendingPlan Take(string token)
    {
        lock (_gate)
        {
            if (!_plans.Remove(token, out var plan))
            {
                throw new MaintenanceRefusedException("Unknown or already used plan token. Plan again.");
            }

            return plan.Plan.ExpiresAt > _time.GetUtcNow()
                ? plan
                : throw new MaintenanceRefusedException("The plan token has expired. Plan again.");
        }
    }
}
