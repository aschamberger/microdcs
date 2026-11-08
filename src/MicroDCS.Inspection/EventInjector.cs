using System.Text;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Inspection;

/// <summary>
/// The write group's one tool (D-19): hands an event to the pipeline as if the binding's transport had received it. It goes through
/// <see cref="IInboundHandler"/>, so claim, scope check, dispatch and replies behave as for a real message; nothing writes to
/// Valkey directly. Only registered when <c>MicroDcs:Management:WriteEnabled</c> is on.
/// </summary>
public sealed class EventInjector
{
    private readonly IInboundHandler _handler;
    private readonly MicroDcsOptions _options;
    private readonly ILogger _audit;

    /// <summary>Initializes a new instance of the <see cref="EventInjector"/> class.</summary>
    /// <param name="handler">The pipeline.</param>
    /// <param name="options">The core options.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public EventInjector(IInboundHandler handler, IOptions<MicroDcsOptions> options, ILoggerFactory loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        _handler = handler;
        _options = options.Value;
        _audit = loggerFactory.CreateLogger(PipelineTelemetry.SecurityCategory);
    }

    /// <summary>Injects a request into a binding.</summary>
    /// <param name="caller">Who asks, for the audit line.</param>
    /// <param name="binding">The binding id.</param>
    /// <param name="type">The CloudEvent type id.</param>
    /// <param name="subject">The subject, <c>scope[/resource]</c>.</param>
    /// <param name="dataJson">The JSON data, or <see langword="null"/> for none.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <returns>The outcome the transport would act on.</returns>
    /// <exception cref="InspectionRefusedException">The binding is unknown or the subject is not valid.</exception>
    public async ValueTask<DeliveryOutcome> InjectAsync(string caller, string binding, string type, string subject, string? dataJson, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(caller);
        ArgumentException.ThrowIfNullOrEmpty(binding);
        ArgumentException.ThrowIfNullOrEmpty(type);
        ArgumentNullException.ThrowIfNull(subject);

        if (!_options.Bindings.TryGetValue(binding, out var options))
        {
            throw new InspectionRefusedException($"Unknown binding '{binding}'.");
        }

        if (!LogicalAddress.TryGetScopePath(subject, out var scope) || scope.Length != options.ScopeLevels)
        {
            throw new InspectionRefusedException($"The subject must start with {options.ScopeLevels} scope segment(s) separated by '.', optionally followed by '/resource'.");
        }

        var envelope = new EventEnvelope
        {
            Id = "inject-" + Guid.NewGuid().ToString("N"),
            Source = "urn:microdcs:inspection:" + _options.InstanceId,
            Type = type,
            Subject = subject,
            DataContentType = dataJson is null ? null : "application/json",
            Data = dataJson is null ? default : Encoding.UTF8.GetBytes(dataJson),
        };
        var message = new InboundMessage
        {
            BindingId = binding,
            Kind = InboundKind.Request,
            Envelope = envelope,
            Address = new LogicalAddress(options.Prefix ?? string.Empty, scope, options.Discriminator, Intent.Data),
        };

        var outcome = await _handler.HandleAsync(message, cancellationToken).ConfigureAwait(false);
        Log.EventInjected(_audit, caller, binding, envelope.Id, type, outcome.ToString());
        return outcome;
    }
}
