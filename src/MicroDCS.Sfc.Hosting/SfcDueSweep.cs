using MicroDCS.Core.Hosting;
using MicroDCS.Valkey.Sfc;
using MicroDCS.Valkey.Streams;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Sfc.Hosting;

/// <summary>Every instance sweeps the due-index of every scope, no leader (D-18, D-44). It runs in the poller (D-43).</summary>
internal sealed class SfcDueSweep : IPollerTask
{
    private const int MaxJitterMs = 250;

    private readonly SfcStateDao _states;
    private readonly SfcPendingCommands _pending;
    private readonly SfcHostOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<SfcDueSweep> _logger;
    private readonly bool _enabled;
    private DateTimeOffset _next;

    public SfcDueSweep(SfcStateDao states, SfcPendingCommands pending, IOptions<SfcHostOptions> options, IOptions<MicroDcsOptions> core, TimeProvider time, ILogger<SfcDueSweep> logger)
    {
        _states = states;
        _pending = pending;
        _options = options.Value;
        _time = time;
        _logger = logger;
        _enabled = _options.Enabled && core.Value.Roles.HasFlag(InstanceRoles.Processor);
#pragma warning disable CA5394 // Jitter of a timer, nothing security relevant.
        _next = time.GetUtcNow() + TimeSpan.FromMilliseconds(Random.Shared.Next(MaxJitterMs));
#pragma warning restore CA5394
    }

    public async ValueTask RunAsync(IReadOnlyCollection<string> scopes, CancellationToken cancellationToken)
    {
        var now = _time.GetUtcNow();
        if (!_enabled || now < _next)
        {
            return;
        }

        _next = now + _options.SweepInterval;
        foreach (var scope in scopes)
        {
            var (_, outage) = await _states.SweepAsync(scope, _options.SweepLimit, (long)_options.OutageThreshold.TotalMilliseconds, cancellationToken).ConfigureAwait(false);
            if (outage > 0)
            {
                SfcTelemetry.Outages.Add(1);
                Log.OutageDetected(_logger, scope, outage);
            }
        }

        _pending.Purge(now.ToUnixTimeMilliseconds());
    }
}
