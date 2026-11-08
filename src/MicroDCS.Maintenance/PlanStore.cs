using System.Security.Cryptography;

namespace MicroDCS.Maintenance;

/// <summary>A plan waiting for its apply.</summary>
internal sealed class PendingPlan
{
    public required MaintenancePlan Plan { get; init; }

    /// <summary>The targets in the order of the digest.</summary>
    public required List<Target> Targets { get; init; }

    /// <summary>Images from a snapshot to write back; <see langword="null"/> for a delete plan.</summary>
    public List<KeyImage>? RestoreItems { get; init; }

    public bool Overwrite { get; init; }
}

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
