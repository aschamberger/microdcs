using MicroDCS.JobControl.Model;
using MicroDCS.JobControl.StateMachine;

namespace MicroDCS.MachineryJobs;

/// <summary>A stored job order.</summary>
/// <param name="Order">The order.</param>
/// <param name="State">The state.</param>
/// <param name="Scope">The scope it belongs to.</param>
/// <param name="Version">The version a following write builds on.</param>
/// <param name="Operation">The id of the operation that wrote it.</param>
public sealed record StoredJobOrder(JobOrder Order, JobOrderState State, string Scope, long Version, string Operation);

/// <summary>A stored job response.</summary>
/// <param name="Response">The response.</param>
/// <param name="Scope">The scope it belongs to.</param>
public sealed record StoredJobResponse(JobResponse Response, string Scope);

/// <summary>Writes a job order, guarded by its version.</summary>
/// <param name="Order">The order.</param>
/// <param name="State">The state to store.</param>
/// <param name="Scope">The scope.</param>
/// <param name="ChangeType">The change type for the change stream.</param>
/// <param name="ExpectedVersion">The version this write builds on, 0 to create.</param>
/// <param name="Operation">The id of the operation.</param>
/// <param name="Recipe">The work the same step queues for the job's recipe.</param>
public sealed record JobOrderWrite(JobOrder Order, JobOrderState State, string Scope, string ChangeType, long ExpectedVersion, string Operation, JobRecipeWork Recipe);

/// <summary>What a job order write queues for the job's recipe in the same step (D-50, D-52).</summary>
public enum JobRecipeWork
{
    /// <summary>Nothing.</summary>
    None,

    /// <summary>The job reached AllowedToStart: start the recipe.</summary>
    Start,

    /// <summary>The job was aborted or stopped: end the recipe.</summary>
    Cancel,

    /// <summary>The job was interrupted (Pause): hold the recipe at the next step boundary.</summary>
    Hold,

    /// <summary>The job left Interrupted for Running (Resume): release the recipe.</summary>
    Release,
}

/// <summary>The lists of what a station may use.</summary>
public enum JobReferenceKind
{
    /// <summary>Equipment.</summary>
    Equipment,

    /// <summary>Material classes.</summary>
    MaterialClass,

    /// <summary>Material definitions.</summary>
    MaterialDefinition,

    /// <summary>Personnel.</summary>
    Personnel,

    /// <summary>Physical assets.</summary>
    PhysicalAsset,
}

/// <summary>Stored job orders, job responses and station configuration, so the processor runs without Valkey in tests.</summary>
public interface IJobStore
{
    /// <summary>Reads a job order.</summary>
    /// <param name="id">The id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The order, or <see langword="null"/>.</returns>
    ValueTask<StoredJobOrder?> GetOrderAsync(string id, CancellationToken cancellationToken);

    /// <summary>Writes a job order.</summary>
    /// <param name="write">The write.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="false"/> when the version differs.</returns>
    ValueTask<bool> SaveOrderAsync(JobOrderWrite write, CancellationToken cancellationToken);

    /// <summary>Removes a job order; a change entry tells the publisher.</summary>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="changeType">The change type for the change stream.</param>
    /// <param name="expectedVersion">The version this removal builds on.</param>
    /// <param name="operation">The id of the operation.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="false"/> when the version differs.</returns>
    ValueTask<bool> RemoveOrderAsync(string id, string scope, string changeType, long expectedVersion, string operation, CancellationToken cancellationToken);

    /// <summary>Checks whether an operation removed a job order; a redelivered removal has no document left to compare with.</summary>
    /// <param name="id">The job order id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="operation">The id of the operation.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when that operation removed it.</returns>
    ValueTask<bool> WasRemovedByAsync(string id, string scope, string operation, CancellationToken cancellationToken);

    /// <summary>Counts the job orders of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The count.</returns>
    ValueTask<long> CountOrdersAsync(string scope, CancellationToken cancellationToken);

    /// <summary>Lists job order ids of a scope by priority.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="start">The first position.</param>
    /// <param name="limit">The most ids to return.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The ids.</returns>
    ValueTask<IReadOnlyList<string>> ListOrderIdsAsync(string scope, int start, int limit, CancellationToken cancellationToken);

    /// <summary>Reads the job response of a job order.</summary>
    /// <param name="jobOrderId">The job order id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The response, or <see langword="null"/>.</returns>
    ValueTask<StoredJobResponse?> GetResponseByOrderAsync(string jobOrderId, CancellationToken cancellationToken);

    /// <summary>Checks whether a job order has a job response.</summary>
    /// <param name="jobOrderId">The job order id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when it has one.</returns>
    ValueTask<bool> HasResponseAsync(string jobOrderId, CancellationToken cancellationToken);

    /// <summary>Reads the job responses of a scope in any of the given states.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="states">The states.</param>
    /// <param name="limit">The most responses to return.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The responses.</returns>
    ValueTask<IReadOnlyList<JobResponse>> GetResponsesByStateAsync(string scope, IReadOnlyCollection<JobOrderState> states, int limit, CancellationToken cancellationToken);

    /// <summary>Stores a job response.</summary>
    /// <param name="response">The response.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask SaveResponseAsync(JobResponse response, string scope, CancellationToken cancellationToken);

    /// <summary>Adds an id to a list of the scope.</summary>
    /// <param name="kind">The list.</param>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask AddReferenceAsync(JobReferenceKind kind, string id, string scope, CancellationToken cancellationToken);

    /// <summary>Removes an id from a list of the scope.</summary>
    /// <param name="kind">The list.</param>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask RemoveReferenceAsync(JobReferenceKind kind, string id, string scope, CancellationToken cancellationToken);

    /// <summary>Checks whether a list of the scope holds an id.</summary>
    /// <param name="kind">The list.</param>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when it does.</returns>
    ValueTask<bool> IsReferenceAsync(JobReferenceKind kind, string id, string scope, CancellationToken cancellationToken);

    /// <summary>Reads a Work Master of a scope.</summary>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The Work Master, or <see langword="null"/> when the scope has none with this id.</returns>
    ValueTask<JobWorkMaster?> GetWorkMasterAsync(string id, string scope, CancellationToken cancellationToken);

    /// <summary>Stores a Work Master for a scope.</summary>
    /// <param name="workMaster">The Work Master.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="false"/> when the id belongs to another scope.</returns>
    ValueTask<bool> SaveWorkMasterAsync(JobWorkMaster workMaster, string scope, CancellationToken cancellationToken);

    /// <summary>Deletes a Work Master of a scope. An id of another scope is left alone.</summary>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask DeleteWorkMasterAsync(string id, string scope, CancellationToken cancellationToken);

    /// <summary>Checks whether a Work Master is known in a scope.</summary>
    /// <param name="id">The id.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when it is.</returns>
    ValueTask<bool> IsWorkMasterAsync(string id, string scope, CancellationToken cancellationToken);

    /// <summary>Reads the acceptance limit of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The limit, or <see langword="null"/> when the scope has none.</returns>
    ValueTask<int?> GetAcceptanceLimitAsync(string scope, CancellationToken cancellationToken);

    /// <summary>Sets the acceptance limit of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="limit">The limit.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask SetAcceptanceLimitAsync(string scope, int limit, CancellationToken cancellationToken);

    /// <summary>Removes the acceptance limit of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask DeleteAcceptanceLimitAsync(string scope, CancellationToken cancellationToken);
}
