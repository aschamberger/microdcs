namespace MicroDCS.Core.Transport;

/// <summary>Result of <see cref="IDedupeStore.ClaimAsync"/>.</summary>
public enum ClaimState
{
    /// <summary>The caller holds the lease and must process the event.</summary>
    Claimed,

    /// <summary>The event was already processed: a genuine duplicate.</summary>
    Done,

    /// <summary>Another worker holds the lease; retry when it ends.</summary>
    Busy,
}

/// <summary>
/// Lease based duplicate detection on <c>source</c> + <c>id</c> (D-13). Used only for transports with
/// <see cref="DeliveryGuarantee.AtLeastOnce"/>. A worker that dies while processing leaves a lease that lapses,
/// so the redelivered event is processed again: at-least-once, never lost.
/// </summary>
public interface IDedupeStore
{
    /// <summary>Tries to take the processing lease. Claiming again with the same token returns <see cref="ClaimState.Claimed"/>, so a retry after a lost reply is safe.</summary>
    /// <param name="source">The event source.</param>
    /// <param name="id">The event id.</param>
    /// <param name="token">Identifies the caller; the same token on every attempt.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The claim state.</returns>
    ValueTask<ClaimState> ClaimAsync(string source, string id, string token, CancellationToken cancellationToken);

    /// <summary>Records that the event was processed. It stays a duplicate for the retention time.</summary>
    /// <param name="source">The event source.</param>
    /// <param name="id">The event id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask MarkDoneAsync(string source, string id, CancellationToken cancellationToken);

    /// <summary>
    /// Gives up a lease the caller holds, so a redelivery can claim the event at once (after a handler fault). A lease of another
    /// token and a <c>done</c> entry stay untouched.
    /// </summary>
    /// <param name="source">The event source.</param>
    /// <param name="id">The event id.</param>
    /// <param name="token">The token the lease was claimed with.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>A task.</returns>
    ValueTask ReleaseAsync(string source, string id, string token, CancellationToken cancellationToken);

    /// <summary>Time until the current lease ends; zero when there is none or the event is done.</summary>
    /// <param name="source">The event source.</param>
    /// <param name="id">The event id.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The remaining time.</returns>
    ValueTask<TimeSpan> LeaseRemainingAsync(string source, string id, CancellationToken cancellationToken);
}
