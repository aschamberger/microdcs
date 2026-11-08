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

            foreach (var transport in _transports)
            {
                await transport.StartAsync(_pipeline, cancellationToken).ConfigureAwait(false);
                Log.TransportStarted(_logger, transport.Name);
            }

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
        Log.ShutdownPhase(_logger, "stop-receiving");
        foreach (var transport in _transports)
        {
            await transport.StopReceivingAsync(cancellationToken).ConfigureAwait(false);
        }

        Log.ShutdownPhase(_logger, "drain");
        while (_pipeline.InFlight > 0)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(10), _time, cancellationToken).ConfigureAwait(false);
        }

        Log.ShutdownPhase(_logger, "drain-outgoing");
        await _pipeline.DrainOutgoingAsync(cancellationToken).ConfigureAwait(false);

        Log.ShutdownPhase(_logger, "stop-transports");
        foreach (var transport in _transports)
        {
            await transport.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        Log.ShutdownPhase(_logger, "expiries");
        await _pipeline.WaitForExpiriesAsync(cancellationToken).ConfigureAwait(false);

        Log.ShutdownPhase(_logger, "processors");
        foreach (var processor in _registry.Processors)
        {
            if (processor.Lifecycle.Shutdown is { } shutdown)
            {
                await shutdown(_services, cancellationToken).ConfigureAwait(false);
            }
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
