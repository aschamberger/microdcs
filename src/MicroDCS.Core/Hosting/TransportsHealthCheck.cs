using MicroDCS.Core.Transport;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Hosting;

/// <summary>
/// Readiness check <c>transports</c> (D-54): every registered transport is connected. An instance without a role starts no
/// transport and is ready without one. The description names the transports that are not connected, never their addresses.
/// </summary>
public sealed class TransportsHealthCheck : IHealthCheck
{
    /// <summary>The registration name.</summary>
    public const string Name = "transports";

    private readonly IReadOnlyList<ITransport> _transports;
    private readonly MicroDcsOptions _options;

    /// <summary>Initializes a new instance of the <see cref="TransportsHealthCheck"/> class.</summary>
    /// <param name="transports">The registered transports.</param>
    /// <param name="options">The core options.</param>
    public TransportsHealthCheck(IEnumerable<ITransport> transports, IOptions<MicroDcsOptions> options)
    {
        ArgumentNullException.ThrowIfNull(transports);
        ArgumentNullException.ThrowIfNull(options);
        _transports = [.. transports];
        _options = options.Value;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (_options.Roles == InstanceRoles.None)
        {
            return Task.FromResult(HealthCheckResult.Healthy("no role, no transport started"));
        }

        var down = _transports.Where(static t => !t.IsConnected).Select(static t => t.Name).ToList();
        return Task.FromResult(down.Count == 0
            ? HealthCheckResult.Healthy("all transports connected")
            : HealthCheckResult.Unhealthy("not connected: " + string.Join(", ", down)));
    }
}
