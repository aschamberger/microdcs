namespace MicroDCS.Maintenance;

/// <summary>One target of a plan.</summary>
/// <param name="Kind">The kind of key, for example <c>joborder</c>.</param>
/// <param name="Key">The key without the prefix, with the part (set member or hash field) in brackets when only a part is touched.</param>
/// <param name="Action">What happens: <c>delete</c>, <c>restore</c>, <c>overwrite</c> or <c>present</c> (restore finds it unchanged).</param>
/// <param name="Detail">Type and size, never content.</param>
public sealed record PlanItem(string Kind, string Key, string Action, string Detail);

/// <summary>What an apply would change. Nothing is changed until <see cref="IMaintenanceOperations.ApplyAsync"/> runs with the token.</summary>
/// <param name="Token">The opaque, single-use token for the apply.</param>
/// <param name="Tool">The operation, for example <c>maint_purge_scope</c>.</param>
/// <param name="Arguments">The arguments as text.</param>
/// <param name="Summary">One sentence.</param>
/// <param name="Items">The targets.</param>
/// <param name="Warnings">What the operation does not cover or bypass.</param>
/// <param name="Digest">Hash over the state of the targets at plan time; the apply fails when it differs.</param>
/// <param name="ExpiresAt">When the token stops working.</param>
public sealed record MaintenancePlan(string Token, string Tool, string Arguments, string Summary, IReadOnlyList<PlanItem> Items, IReadOnlyList<string> Warnings, string Digest, DateTimeOffset ExpiresAt);

/// <summary>The outcome of an apply.</summary>
/// <param name="Tool">The operation.</param>
/// <param name="Changed">Number of structures removed or written.</param>
/// <param name="SnapshotId">The snapshot written before the change, or <see langword="null"/> when nothing was overwritten.</param>
/// <param name="Notes">What could not be restored or was recreated differently.</param>
public sealed record MaintenanceResult(string Tool, int Changed, string? SnapshotId, IReadOnlyList<string> Notes);

/// <summary>A snapshot file, without its content.</summary>
/// <param name="Id">The snapshot id.</param>
/// <param name="CreatedAt">When it was written.</param>
/// <param name="Tool">The operation that caused it.</param>
/// <param name="Arguments">The arguments of that operation.</param>
/// <param name="Items">Number of captured structures.</param>
/// <param name="Bytes">File size.</param>
public sealed record SnapshotInfo(string Id, DateTimeOffset CreatedAt, string Tool, string Arguments, int Items, long Bytes);

/// <summary>A maintenance operation was refused. The message says why and is safe to show.</summary>
public sealed class MaintenanceRefusedException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="MaintenanceRefusedException"/> class.</summary>
    public MaintenanceRefusedException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="MaintenanceRefusedException"/> class.</summary>
    /// <param name="message">The reason.</param>
    public MaintenanceRefusedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="MaintenanceRefusedException"/> class.</summary>
    /// <param name="message">The reason.</param>
    /// <param name="innerException">The cause.</param>
    public MaintenanceRefusedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// Cleanup and repair of the stored state (D-19, D-45). Every change has two steps: a <c>Plan…</c> call that returns what would
/// change and a token, and <see cref="ApplyAsync"/> with that token. These operations bypass the OPC UA state machine and the
/// pipeline, and nobody downstream is told. Tests call this API directly; MCP and no other adapter exposes it to people.
/// </summary>
public interface IMaintenanceOperations
{
    /// <summary>Plans the release of one dedupe entry (lease or done marker), so the event can be processed again.</summary>
    /// <param name="caller">Who asks, for the audit line.</param>
    /// <param name="source">The CloudEvent source.</param>
    /// <param name="id">The CloudEvent id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="MaintenanceRefusedException">The operation is refused.</exception>
    ValueTask<MaintenancePlan> PlanDedupeReleaseAsync(string caller, string source, string id, CancellationToken cancellationToken);

    /// <summary>Plans the removal of everything stored for one scope: documents, indexes, streams, due-index and registry entry.</summary>
    /// <param name="caller">Who asks, for the audit line.</param>
    /// <param name="scope">The scope.</param>
    /// <param name="allowLarge">Allows a plan above the item cap.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="MaintenanceRefusedException">The operation is refused.</exception>
    ValueTask<MaintenancePlan> PlanPurgeScopeAsync(string caller, string scope, bool allowLarge, CancellationToken cancellationToken);

    /// <summary>Plans putting the content of a snapshot back.</summary>
    /// <param name="caller">Who asks, for the audit line.</param>
    /// <param name="snapshotId">The snapshot id from <see cref="ListSnapshotsAsync"/>.</param>
    /// <param name="overwrite">Replaces targets that exist with different content; they are snapshotted first.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The plan.</returns>
    /// <exception cref="MaintenanceRefusedException">The operation is refused.</exception>
    ValueTask<MaintenancePlan> PlanRestoreAsync(string caller, string snapshotId, bool overwrite, CancellationToken cancellationToken);

    /// <summary>Runs a plan. The token works once.</summary>
    /// <param name="caller">Who asks, for the audit line.</param>
    /// <param name="token">The token of the plan.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>What happened.</returns>
    /// <exception cref="MaintenanceRefusedException">The token is unknown or expired, the targets changed since the plan, or the snapshot cannot be written.</exception>
    ValueTask<MaintenanceResult> ApplyAsync(string caller, string token, CancellationToken cancellationToken);

    /// <summary>Lists the snapshots, newest first, without their content.</summary>
    /// <param name="caller">Who asks, for the audit line.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The snapshots.</returns>
    ValueTask<IReadOnlyList<SnapshotInfo>> ListSnapshotsAsync(string caller, CancellationToken cancellationToken);
}
