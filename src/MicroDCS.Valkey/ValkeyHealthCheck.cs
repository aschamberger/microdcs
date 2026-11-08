using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MicroDCS.Valkey;

/// <summary>Health check that pings Valkey. It reports the round trip time, never the connection string.</summary>
public sealed class ValkeyHealthCheck : IHealthCheck
{
    private readonly ValkeyContext _context;

    /// <summary>Initializes a new instance of the <see cref="ValkeyHealthCheck"/> class.</summary>
    /// <param name="context">The shared context.</param>
    public ValkeyHealthCheck(ValkeyContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        _context = context;
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var roundTrip = await _context.Database.PingAsync().WaitAsync(cancellationToken).ConfigureAwait(false);
            return HealthCheckResult.Healthy("Valkey answers", new Dictionary<string, object> { ["roundTripMs"] = roundTrip.TotalMilliseconds });
        }
        catch (Exception ex) when (ex is StackExchange.Redis.RedisException or TimeoutException)
        {
            return HealthCheckResult.Unhealthy("Valkey does not answer", ex);
        }
    }
}
