using MicroDCS.JobControl.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Opc.Ua;

namespace MicroDCS.MachineryJobs;

/// <summary>Decides whether a job order fits the station it was sent to (the checks of <c>is_job_acceptable</c>).</summary>
public sealed class JobAcceptanceCheck
{
    private readonly IJobStore _store;
    private readonly JobAcceptanceOptions _options;
    private readonly ILogger<JobAcceptanceCheck> _logger;

    /// <summary>Initializes a new instance of the <see cref="JobAcceptanceCheck"/> class.</summary>
    /// <param name="store">The store.</param>
    /// <param name="options">The options.</param>
    /// <param name="logger">The logger.</param>
    public JobAcceptanceCheck(IJobStore store, IOptions<JobAcceptanceOptions> options, ILogger<JobAcceptanceCheck> logger)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);
        _store = store;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>Checks a job order against the limit and the lists of the scope.</summary>
    /// <param name="order">The order.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="addsOrder">Whether the order is new; an update replaces an order and does not count against the limit.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when the order can be accepted.</returns>
    public async ValueTask<bool> IsAcceptableAsync(JobOrder order, string scope, bool addsOrder, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(order);
        if (addsOrder && !await IsBelowLimitAsync(order, scope, cancellationToken).ConfigureAwait(false))
        {
            return false;
        }

        return await AllAllowedAsync(order, scope, _options.CheckEquipmentId, JobReferenceKind.Equipment, Ids(order.EquipmentRequirements, static r => r.Id), cancellationToken).ConfigureAwait(false)
            && await AllAllowedAsync(order, scope, _options.CheckMaterialClassId, JobReferenceKind.MaterialClass, Ids(order.MaterialRequirements, static m => m.MaterialClassId), cancellationToken).ConfigureAwait(false)
            && await AllAllowedAsync(order, scope, _options.CheckMaterialDefinitionId, JobReferenceKind.MaterialDefinition, Ids(order.MaterialRequirements, static m => m.MaterialDefinitionId), cancellationToken).ConfigureAwait(false)
            && await AllAllowedAsync(order, scope, _options.CheckPersonnelId, JobReferenceKind.Personnel, Ids(order.PersonnelRequirements, static r => r.Id), cancellationToken).ConfigureAwait(false)
            && await AllAllowedAsync(order, scope, _options.CheckPhysicalAssetId, JobReferenceKind.PhysicalAsset, Ids(order.PhysicalAssetRequirements, static r => r.Id), cancellationToken).ConfigureAwait(false)
            && await WorkMastersKnownAsync(order, scope, cancellationToken).ConfigureAwait(false);
    }

    private static List<string?> Ids<T>(ArrayOf<T> items, Func<T, string?> id)
    {
        var ids = new List<string?>(items.Count);
        foreach (var item in items)
        {
            ids.Add(id(item));
        }

        return ids;
    }

    private async ValueTask<bool> IsBelowLimitAsync(JobOrder order, string scope, CancellationToken cancellationToken)
    {
        if (!_options.CheckMaxDownloadableJobOrders)
        {
            return true;
        }

        // A limit set for the scope always applies, also 0; the default limit 0 means there is none.
        var scoped = await _store.GetAcceptanceLimitAsync(scope, cancellationToken).ConfigureAwait(false);
        var limit = scoped ?? (_options.MaxDownloadableJobOrders > 0 ? _options.MaxDownloadableJobOrders : null);
        if (limit is null || await _store.CountOrdersAsync(scope, cancellationToken).ConfigureAwait(false) < limit)
        {
            return true;
        }

        Log.LimitReached(_logger, order.Id, scope, limit.Value);
        return false;
    }

    private async ValueTask<bool> AllAllowedAsync(JobOrder order, string scope, bool enabled, JobReferenceKind kind, IEnumerable<string?> ids, CancellationToken cancellationToken)
    {
        if (!enabled)
        {
            return true;
        }

        foreach (var id in ids)
        {
            if (!string.IsNullOrEmpty(id) && !await _store.IsReferenceAsync(kind, id, scope, cancellationToken).ConfigureAwait(false))
            {
                Log.NotAllowed(_logger, order.Id, scope, kind.ToString(), id);
                return false;
            }
        }

        return true;
    }

    private async ValueTask<bool> WorkMastersKnownAsync(JobOrder order, string scope, CancellationToken cancellationToken)
    {
        if (!_options.CheckWorkMaster)
        {
            return true;
        }

        foreach (var id in Ids(order.WorkMasters, static w => w.Id))
        {
            if (!string.IsNullOrEmpty(id) && !await _store.IsWorkMasterAsync(id, scope, cancellationToken).ConfigureAwait(false))
            {
                Log.NotAllowed(_logger, order.Id, scope, "WorkMaster", id);
                return false;
            }
        }

        return true;
    }
}
