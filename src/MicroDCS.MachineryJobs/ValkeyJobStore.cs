using System.Text;
using MicroDCS.JobControl.Codec;
using MicroDCS.JobControl.Conversions;
using MicroDCS.JobControl.Extension;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;
using MicroDCS.Valkey;
using MicroDCS.Valkey.Documents;
using V2 = Opc.Ua.ISA95.JobControl.V2;

namespace MicroDCS.MachineryJobs;

/// <summary>
/// The job store over the Valkey DAOs (D-41, D-42). Documents are V2 OPC UA JSON, because the processor serves Job Control V2 only (D-50).
/// </summary>
public sealed class ValkeyJobStore : IJobStore
{
    private readonly JobOrderDao _orders;
    private readonly JobResponseDao _responses;
    private readonly WorkMasterDao _workMasters;
    private readonly ReferenceListDao _references;
    private readonly JobAcceptanceDao _acceptance;

    /// <summary>Initializes a new instance of the <see cref="ValkeyJobStore"/> class.</summary>
    /// <param name="orders">The job orders.</param>
    /// <param name="responses">The job responses.</param>
    /// <param name="workMasters">The Work Masters.</param>
    /// <param name="references">The reference lists.</param>
    /// <param name="acceptance">The acceptance limits.</param>
    public ValkeyJobStore(JobOrderDao orders, JobResponseDao responses, WorkMasterDao workMasters, ReferenceListDao references, JobAcceptanceDao acceptance)
    {
        ArgumentNullException.ThrowIfNull(orders);
        ArgumentNullException.ThrowIfNull(responses);
        ArgumentNullException.ThrowIfNull(workMasters);
        ArgumentNullException.ThrowIfNull(references);
        ArgumentNullException.ThrowIfNull(acceptance);
        _orders = orders;
        _responses = responses;
        _workMasters = workMasters;
        _references = references;
        _acceptance = acceptance;
    }

    /// <summary>The name a state has in the by-state index (D-41).</summary>
    /// <param name="state">The state.</param>
    /// <returns>The index name, for example <c>AllowedToStart_Ready</c>.</returns>
    public static string StateIndexName(JobOrderState state) =>
        KeySchema.NormalizeState(StateTexts(state));

    /// <inheritdoc />
    public async ValueTask<StoredJobOrder?> GetOrderAsync(string id, CancellationToken cancellationToken)
    {
        var stored = await _orders.GetAsync(id, cancellationToken).ConfigureAwait(false);
        if (stored is null)
        {
            return null;
        }

        var document = OpcUaJsonCodec.Decode<V2.ISA95JobOrderAndStateDataType>(Encoding.UTF8.GetBytes(stored.Document));
        return new StoredJobOrder(JobControlConversions.FromV2Order(document.JobOrder), JobStateArray.FromV2(document.State), stored.Scope, stored.Version, stored.Operation);
    }

    /// <inheritdoc />
    public async ValueTask<bool> SaveOrderAsync(JobOrderWrite write, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(write);
        var document = new V2.ISA95JobOrderAndStateDataType
        {
            JobOrder = JobControlConversions.ToV2Order(write.Order),
            State = JobStateArray.ToV2(write.State),
        };
        var result = await _orders.SaveAsync(
            write.Order.Id,
            write.Scope,
            Encoding.UTF8.GetString(OpcUaJsonCodec.Encode(document)),
            write.Order.Priority,
            write.ChangeType,
            write.ExpectedVersion,
            write.Operation,
            write.StartRecipe,
            cancellationToken).ConfigureAwait(false);
        return result.Written;
    }

    /// <inheritdoc />
    public async ValueTask<bool> RemoveOrderAsync(string id, string scope, string changeType, long expectedVersion, string operation, CancellationToken cancellationToken)
    {
        var result = await _orders.RemoveAsync(id, scope, changeType, expectedVersion, operation, cancellationToken).ConfigureAwait(false);
        return result.Written;
    }

    /// <inheritdoc />
    public async ValueTask<bool> WasRemovedByAsync(string id, string scope, string operation, CancellationToken cancellationToken) =>
        await _orders.WasRemovedByAsync(id, scope, operation, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<long> CountOrdersAsync(string scope, CancellationToken cancellationToken) =>
        await _orders.CountAsync(scope, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<string>> ListOrderIdsAsync(string scope, int start, int limit, CancellationToken cancellationToken) =>
        await _orders.ListAsync(scope, start, limit, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<StoredJobResponse?> GetResponseByOrderAsync(string jobOrderId, CancellationToken cancellationToken)
    {
        var stored = await _responses.GetByJobOrderAsync(jobOrderId, cancellationToken).ConfigureAwait(false);
        return stored is null ? null : new StoredJobResponse(DecodeResponse(stored), stored.Scope);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyList<JobResponse>> GetResponsesByStateAsync(string scope, IReadOnlyCollection<JobOrderState> states, int limit, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(states);
        var ids = new List<string>();
        foreach (var name in states.Select(StateIndexName).Where(static n => n.Length > 0).Distinct(StringComparer.Ordinal))
        {
            await foreach (var id in _responses.IdsByStateAsync(scope, name, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                if (ids.Count >= limit)
                {
                    break;
                }

                ids.Add(id);
            }
        }

        // The reads share one multiplexed connection, so they need not wait for each other.
        var stored = await Task.WhenAll(ids.Select(id => _responses.GetAsync(id, cancellationToken))).ConfigureAwait(false);
        return [.. stored.OfType<StoredDocument>().Select(DecodeResponse)];
    }

    /// <inheritdoc />
    public async ValueTask SaveResponseAsync(JobResponse response, string scope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(response);
        await _responses.SaveAsync(
            response.Id,
            scope,
            Encoding.UTF8.GetString(OpcUaJsonCodec.Encode(JobControlConversions.ToV2Response(response))),
            ((DateTime)response.StartTime).Ticks,
            response.JobOrderId,
            StateIndexName(response.State),
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask AddReferenceAsync(JobReferenceKind kind, string id, string scope, CancellationToken cancellationToken) =>
        await _references.AddAsync(ListName(kind), id, scope, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask RemoveReferenceAsync(JobReferenceKind kind, string id, string scope, CancellationToken cancellationToken) =>
        await _references.RemoveAsync(ListName(kind), id, scope, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<bool> IsReferenceAsync(JobReferenceKind kind, string id, string scope, CancellationToken cancellationToken) =>
        await _references.IsMemberAsync(ListName(kind), id, scope, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<bool> SaveWorkMasterAsync(JobWorkMaster workMaster, string scope, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(workMaster);
        var id = workMaster.Id ?? throw new ArgumentException("A Work Master needs an id.", nameof(workMaster));
        ISA95WorkMasterExtDataType document = JobControlConversions.ToWorkMasterExt(workMaster);
        try
        {
            await _workMasters.SaveAsync(id, scope, Encoding.UTF8.GetString(OpcUaJsonCodec.Encode(document)), cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (DocumentScopeMismatchException)
        {
            return false;
        }
    }

    /// <inheritdoc />
    public async ValueTask DeleteWorkMasterAsync(string id, string scope, CancellationToken cancellationToken)
    {
        var existing = await _workMasters.GetAsync(id, cancellationToken).ConfigureAwait(false);
        if (existing is not null && string.Equals(existing.Scope, scope, StringComparison.Ordinal))
        {
            await _workMasters.DeleteAsync(id, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsWorkMasterAsync(string id, string scope, CancellationToken cancellationToken) =>
        await _workMasters.IsMemberAsync(id, scope, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask<int?> GetAcceptanceLimitAsync(string scope, CancellationToken cancellationToken) =>
        await _acceptance.GetAsync(scope, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask SetAcceptanceLimitAsync(string scope, int limit, CancellationToken cancellationToken) =>
        await _acceptance.SaveAsync(scope, limit, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async ValueTask DeleteAcceptanceLimitAsync(string scope, CancellationToken cancellationToken) =>
        await _acceptance.DeleteAsync(scope, cancellationToken).ConfigureAwait(false);

    private static List<string?> StateTexts(JobOrderState state)
    {
        var texts = new List<string?>(2);
        foreach (var entry in JobStateArray.ToV2(state))
        {
            texts.Add(entry.StateText.Text);
        }

        return texts;
    }

    private static string ListName(JobReferenceKind kind) => kind switch
    {
        JobReferenceKind.Equipment => "equipment",
        JobReferenceKind.MaterialClass => "materialclass",
        JobReferenceKind.MaterialDefinition => "materialdefinition",
        JobReferenceKind.Personnel => "personnel",
        JobReferenceKind.PhysicalAsset => "physicalasset",
        _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unknown reference list."),
    };

    private static JobResponse DecodeResponse(StoredDocument stored) =>
        JobControlConversions.FromV2Response(OpcUaJsonCodec.Decode<V2.ISA95JobResponseDataType>(Encoding.UTF8.GetBytes(stored.Document)));
}
