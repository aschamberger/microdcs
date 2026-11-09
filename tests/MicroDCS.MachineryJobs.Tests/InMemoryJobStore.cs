using System.Collections.Concurrent;
using MicroDCS.Core.Messaging;
using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;

namespace MicroDCS.MachineryJobs.Tests;

/// <summary>The store contract in memory, with the version guard of the Valkey scripts.</summary>
internal sealed class InMemoryJobStore : IJobStore
{
    private readonly ConcurrentDictionary<string, StoredJobOrder> _orders = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, StoredJobResponse> _responses = new(StringComparer.Ordinal);
    private readonly HashSet<(string Id, string Scope, string Operation)> _removedBy = [];
    private readonly HashSet<(JobReferenceKind Kind, string Id, string Scope)> _references = [];
    private readonly Dictionary<string, (JobWorkMaster Master, string Scope)> _workMasters = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _limits = new(StringComparer.Ordinal);

    public List<string> Changes { get; } = [];

    public List<string> RecipeStarts { get; } = [];

    public List<string> RecipeCancels { get; } = [];

    /// <summary>Runs after an order is read, to change something in the middle of a publisher round.</summary>
    public Action? AfterOrderRead { get; set; }

    /// <summary>Reading the response of this order fails as an undecodable document does.</summary>
    public string? FailDecodingResponseOf { get; set; }

    /// <summary>Runs before a write, to simulate another writer that got in first.</summary>
    public Action? BeforeWrite { get; set; }

    public StoredJobOrder? Order(string id) => _orders.GetValueOrDefault(id);

    public void SetState(string id, JobOrderState state)
    {
        var order = _orders[id];
        _orders[id] = order with { State = state, Version = order.Version + 1, Operation = "machine" };
    }

    public ValueTask<StoredJobOrder?> GetOrderAsync(string id, CancellationToken cancellationToken)
    {
        var order = _orders.GetValueOrDefault(id);
        AfterOrderRead?.Invoke();
        return new(order);
    }

    public ValueTask<bool> SaveOrderAsync(JobOrderWrite write, CancellationToken cancellationToken)
    {
        BeforeWrite?.Invoke();
        var current = _orders.GetValueOrDefault(write.Order.Id)?.Version ?? 0;
        if (current != write.ExpectedVersion)
        {
            return new(false);
        }

        _orders[write.Order.Id] = new StoredJobOrder(write.Order, write.State, write.Scope, current + 1, write.Operation);
        Changes.Add($"{write.ChangeType}:{write.Order.Id}");
        if (write.Recipe == JobRecipeWork.Start)
        {
            RecipeStarts.Add(write.Order.Id);
        }
        else if (write.Recipe == JobRecipeWork.Cancel)
        {
            RecipeCancels.Add(write.Order.Id);
        }

        return new(true);
    }

    public ValueTask<bool> RemoveOrderAsync(string id, string scope, string changeType, long expectedVersion, string operation, CancellationToken cancellationToken)
    {
        BeforeWrite?.Invoke();
        if ((_orders.GetValueOrDefault(id)?.Version ?? 0) != expectedVersion)
        {
            return new(false);
        }

        _orders.TryRemove(id, out _);
        _removedBy.Add((id, scope, operation));
        Changes.Add($"{changeType}:{id}");
        return new(true);
    }

    public ValueTask<bool> WasRemovedByAsync(string id, string scope, string operation, CancellationToken cancellationToken) =>
        new(_removedBy.Contains((id, scope, operation)));

    public ValueTask<long> CountOrdersAsync(string scope, CancellationToken cancellationToken) => new(_orders.Values.Count(o => o.Scope == scope));

    public ValueTask<IReadOnlyList<string>> ListOrderIdsAsync(string scope, int start, int limit, CancellationToken cancellationToken) =>
        new([.. _orders.Values.Where(o => o.Scope == scope).Select(static o => o.Order.Id).Skip(start).Take(limit)]);

    public ValueTask<StoredJobResponse?> GetResponseByOrderAsync(string jobOrderId, CancellationToken cancellationToken) =>
        jobOrderId == FailDecodingResponseOf ? throw new PayloadException("undecodable") : new(_responses.GetValueOrDefault(jobOrderId));

    public ValueTask<bool> HasResponseAsync(string jobOrderId, CancellationToken cancellationToken) => new(_responses.ContainsKey(jobOrderId));

    public ValueTask<IReadOnlyList<JobResponse>> GetResponsesByStateAsync(string scope, IReadOnlyCollection<JobOrderState> states, int limit, CancellationToken cancellationToken) =>
        new([.. _responses.Values.Where(r => r.Scope == scope && states.Contains(r.Response.State)).Select(static r => r.Response).Take(limit)]);

    public ValueTask SaveResponseAsync(JobResponse response, string scope, CancellationToken cancellationToken)
    {
        _responses[response.JobOrderId] = new StoredJobResponse(response, scope);
        return ValueTask.CompletedTask;
    }

    public ValueTask AddReferenceAsync(JobReferenceKind kind, string id, string scope, CancellationToken cancellationToken)
    {
        _references.Add((kind, id, scope));
        return ValueTask.CompletedTask;
    }

    public ValueTask RemoveReferenceAsync(JobReferenceKind kind, string id, string scope, CancellationToken cancellationToken)
    {
        _references.Remove((kind, id, scope));
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> IsReferenceAsync(JobReferenceKind kind, string id, string scope, CancellationToken cancellationToken) => new(_references.Contains((kind, id, scope)));

    public ValueTask<JobWorkMaster?> GetWorkMasterAsync(string id, string scope, CancellationToken cancellationToken) =>
        new(_workMasters.TryGetValue(id, out var existing) && existing.Scope == scope ? existing.Master : null);

    public ValueTask<bool> SaveWorkMasterAsync(JobWorkMaster workMaster, string scope, CancellationToken cancellationToken)
    {
        if (_workMasters.TryGetValue(workMaster.Id!, out var existing) && existing.Scope != scope)
        {
            return new(false);
        }

        _workMasters[workMaster.Id!] = (workMaster, scope);
        return new(true);
    }

    public ValueTask DeleteWorkMasterAsync(string id, string scope, CancellationToken cancellationToken)
    {
        if (_workMasters.TryGetValue(id, out var existing) && existing.Scope == scope)
        {
            _workMasters.Remove(id);
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> IsWorkMasterAsync(string id, string scope, CancellationToken cancellationToken) =>
        new(_workMasters.TryGetValue(id, out var existing) && existing.Scope == scope);

    public JobWorkMaster? WorkMaster(string id) => _workMasters.TryGetValue(id, out var existing) ? existing.Master : null;

    public ValueTask<int?> GetAcceptanceLimitAsync(string scope, CancellationToken cancellationToken) => new(_limits.TryGetValue(scope, out var limit) ? limit : null);

    public ValueTask SetAcceptanceLimitAsync(string scope, int limit, CancellationToken cancellationToken)
    {
        _limits[scope] = limit;
        return ValueTask.CompletedTask;
    }

    public ValueTask DeleteAcceptanceLimitAsync(string scope, CancellationToken cancellationToken)
    {
        _limits.Remove(scope);
        return ValueTask.CompletedTask;
    }
}
