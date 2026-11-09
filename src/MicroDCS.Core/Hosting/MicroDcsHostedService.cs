using MicroDCS.Core.Authoring;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Hosting;

/// <summary>
/// The lifecycle of the application (H-4, D-37): validate, initialise processors, start transports, run the started
/// hooks; on stop the shutdown phases in order. Any failure at start ends the process (D-27).
/// </summary>
public sealed class MicroDcsHostedService : IHostedService
{
    private readonly ProcessorRegistry _registry;
    private readonly IReadOnlyList<ITransport> _transports;
    private readonly MessagePipeline _pipeline;
    private readonly IStartLease _lease;
    private readonly IServiceProvider _services;
    private readonly IOptions<MicroDcsOptions> _options;
    private readonly TimeProvider _time;
    private readonly ILogger<MicroDcsHostedService> _logger;

    /// <summary>Creates the service.</summary>
    /// <param name="registry">The processors.</param>
    /// <param name="transports">The transports.</param>
    /// <param name="pipeline">The pipeline.</param>
    /// <param name="lease">The singleton start lease.</param>
    /// <param name="services">Provider for hooks.</param>
    /// <param name="options">The options.</param>
    /// <param name="time">The clock.</param>
    /// <param name="logger">The logger.</param>
    public MicroDcsHostedService(
        ProcessorRegistry registry,
        IEnumerable<ITransport> transports,
        MessagePipeline pipeline,
        IStartLease lease,
        IServiceProvider services,
        IOptions<MicroDcsOptions> options,
        TimeProvider time,
        ILogger<MicroDcsHostedService> logger)
    {
        _registry = registry;
        _transports = [.. transports];
        _pipeline = pipeline;
        _lease = lease;
        _services = services;
        _options = options;
        _time = time;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var options = _options.Value; // Throws OptionsValidationException: fail fast.
        _registry.EnsureBindingsConfigured(options);
        var unknown = _registry.Processors
            .Select(p => (p.BindingId, options.Bindings[p.BindingId].Transport))
            .FirstOrDefault(b => !_transports.Any(t => t.Name == b.Transport));
        if (unknown.BindingId is not null)
        {
            throw new InvalidOperationException($"Binding '{unknown.BindingId}' uses transport '{unknown.Transport}', which is not registered.");
        }

        var runsProcessors = options.Roles.HasFlag(InstanceRoles.Processor);
        if (runsProcessors)
        {
            foreach (var processor in _registry.Processors)
            {
                if (processor.Lifecycle.Initialize is { } initialize)
                {
                    await initialize(_services, cancellationToken).ConfigureAwait(false);
                }

                Log.ProcessorInitialised(_logger, processor.ProcessorType.Name, processor.BindingId);
            }

            // The queues accept events before the first transport delivers: a handler may publish from the first message (RV-1).
            _pipeline.OpenPublishing();
        }

        // A publisher-only instance also needs its connection; the transports subscribe only under the Processor role (D-51).
        if (options.Roles != InstanceRoles.None)
        {
            foreach (var transport in _transports)
            {
                await transport.StartAsync(_pipeline, cancellationToken).ConfigureAwait(false);
                Log.TransportStarted(_logger, transport.Name);
            }
        }

        if (runsProcessors)
        {
            _pipeline.StartPublishing();

            foreach (var processor in _registry.Processors)
            {
                await RunStartedAsync(processor, options, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // 1. stop receiving, 2. let in-flight messages finish, 3. drain the outgoing queues, 4. stop transports,
        // 5. wait for the expiry timers until the grace period ends, 6. processor shutdown hooks,
        // 7. shared infrastructure closes with the host.
        // When the grace period ends (the token is cancelled) every later phase still runs, with the cancelled token: each one
        // then takes its "grace period ended" path (abort the queues, cancel the timers) instead of being skipped.
        foreach (var transport in _transports)
        {
            await RunPhaseAsync("stop-receiving", () => transport.StopReceivingAsync(cancellationToken), cancellationToken).ConfigureAwait(false);
        }

        await RunPhaseAsync("drain", () => WaitForInFlightAsync(cancellationToken), cancellationToken).ConfigureAwait(false);
        await RunPhaseAsync("drain-outgoing", () => _pipeline.DrainOutgoingAsync(cancellationToken), cancellationToken).ConfigureAwait(false);
        foreach (var transport in _transports)
        {
            await RunPhaseAsync("stop-transports", () => transport.StopAsync(cancellationToken), cancellationToken).ConfigureAwait(false);
        }

        await RunPhaseAsync("expiries", () => _pipeline.WaitForExpiriesAsync(cancellationToken), cancellationToken).ConfigureAwait(false);
        foreach (var processor in _options.Value.Roles.HasFlag(InstanceRoles.Processor) ? _registry.Processors : [])
        {
            if (processor.Lifecycle.Shutdown is { } shutdown)
            {
                await RunPhaseAsync("processors", () => shutdown(_services, cancellationToken), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask RunPhaseAsync(string phase, Func<ValueTask> run, CancellationToken cancellationToken)
    {
        Log.ShutdownPhase(_logger, phase);
        try
        {
            await run().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Log.GracePeriodEnded(_logger, phase);
        }
    }

    private async ValueTask WaitForInFlightAsync(CancellationToken cancellationToken)
    {
        while (_pipeline.InFlight > 0)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10), _time, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask RunStartedAsync(ProcessorDescriptor processor, MicroDcsOptions options, CancellationToken cancellationToken)
    {
        if (processor.Lifecycle.Started is not { } started)
        {
            return;
        }

        if (processor.Lifecycle.StartedSingleton
            && !await _lease.TryAcquireAsync(processor.BindingId, options.StartLeaseTtl, cancellationToken).ConfigureAwait(false))
        {
            Log.StartLeaseHeldElsewhere(_logger);
            return;
        }

        await started(_services, cancellationToken).ConfigureAwait(false);
    }
}
