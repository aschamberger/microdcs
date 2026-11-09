namespace MicroDCS.MachineryJobs.Publishing;

/// <summary>One entry of the change stream of a scope.</summary>
/// <param name="Id">The stream entry id.</param>
/// <param name="ChangeType">The change type, for example <c>Store</c> or <c>ResultUpdate</c>.</param>
/// <param name="JobOrderId">The job order the change belongs to; empty for a job response without an order.</param>
public sealed record JobChange(string Id, string ChangeType, string JobOrderId);

/// <summary>The state of the publisher itself: cursors, sequence and lease, so the publisher runs without Valkey in tests.</summary>
public interface IJobPublisherStore
{
    /// <summary>Reads the cursor of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The id of the last handled entry, or <see langword="null"/> when the scope has none.</returns>
    ValueTask<string?> GetCursorAsync(string scope, CancellationToken cancellationToken);

    /// <summary>Stores the cursor of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="id">The id of the last handled entry.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask SaveCursorAsync(string scope, string id, CancellationToken cancellationToken);

    /// <summary>Reads the id of the oldest entry still in the change stream.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The id, or <see langword="null"/> when the stream is empty.</returns>
    ValueTask<string?> FirstChangeIdAsync(string scope, CancellationToken cancellationToken);

    /// <summary>Reads the id of the newest entry in the change stream.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The id, or <see langword="null"/> when the stream is empty.</returns>
    ValueTask<string?> LastChangeIdAsync(string scope, CancellationToken cancellationToken);

    /// <summary>Reads change entries after a cursor, oldest first, without blocking (D-7).</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="after">The cursor; <see langword="null"/> reads from the beginning.</param>
    /// <param name="count">The most entries to return.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The entries.</returns>
    ValueTask<IReadOnlyList<JobChange>> ReadChangesAsync(string scope, string? after, int count, CancellationToken cancellationToken);

    /// <summary>Takes the next sequence number of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The number.</returns>
    ValueTask<long> NextSequenceAsync(string scope, CancellationToken cancellationToken);

    /// <summary>Reads the last sequence number of a scope.</summary>
    /// <param name="scope">The scope.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The number, 0 when none was taken.</returns>
    ValueTask<long> GetSequenceAsync(string scope, CancellationToken cancellationToken);

    /// <summary>Takes the lease, or renews it when the holder already has it.</summary>
    /// <param name="holder">The instance id.</param>
    /// <param name="timeToLive">The lifetime.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when the holder has the lease.</returns>
    ValueTask<bool> AcquireLeaseAsync(string holder, TimeSpan timeToLive, CancellationToken cancellationToken);

    /// <summary>Gives the lease up, if the holder still has it, so a successor need not wait for it to run out.</summary>
    /// <param name="holder">The instance id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask ReleaseLeaseAsync(string holder, CancellationToken cancellationToken);

    /// <summary>Reads who holds the lease.</summary>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The instance id, or <see langword="null"/> when nobody does.</returns>
    ValueTask<string?> GetLeaseHolderAsync(CancellationToken cancellationToken);
}
