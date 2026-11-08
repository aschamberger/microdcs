namespace MicroDCS.Core.Hosting;

/// <summary>
/// Lets one replica run the <c>[OnStarted(Singleton = true)]</c> hooks (P-14). The Valkey layer (S09) implements it with a
/// lease; the default grants it.
/// </summary>
public interface IStartLease
{
    /// <summary>Tries to take the lease.</summary>
    /// <param name="name">The lease name (the processor binding id).</param>
    /// <param name="timeToLive">How long the lease lives.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns><see langword="true"/> when this instance holds the lease and runs the hook.</returns>
    ValueTask<bool> TryAcquireAsync(string name, TimeSpan timeToLive, CancellationToken cancellationToken);
}

/// <summary>Grants every lease. Correct for one instance.</summary>
public sealed class AlwaysGrantedStartLease : IStartLease
{
    /// <inheritdoc />
    public ValueTask<bool> TryAcquireAsync(string name, TimeSpan timeToLive, CancellationToken cancellationToken) => ValueTask.FromResult(true);
}
