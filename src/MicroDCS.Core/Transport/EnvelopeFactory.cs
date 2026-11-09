using System.Collections.Immutable;
using System.Diagnostics;
using MicroDCS.Core.Authoring;
using MicroDCS.Core.Hosting;
using MicroDCS.Core.Messaging;
using Microsoft.Extensions.Options;

namespace MicroDCS.Core.Transport;

/// <summary>Builds outgoing envelopes from payloads: ids, source, correlation, causation, expiry, carried attributes and trace context.</summary>
internal sealed class EnvelopeFactory
{
    private readonly ProcessorRegistry _registry;
    private readonly IOptions<MicroDcsOptions> _options;
    private readonly TimeProvider _time;

    public EnvelopeFactory(ProcessorRegistry registry, IOptions<MicroDcsOptions> options, TimeProvider time)
    {
        _registry = registry;
        _options = options;
        _time = time;
    }

    /// <summary>Builds an envelope. <paramref name="request"/> is the event being answered, or <see langword="null"/> for an application-initiated event.</summary>
    public EventEnvelope Build(object payload, PublishOptions? publish, EventEnvelope? request)
    {
        var options = _options.Value;
        var now = _time.GetUtcNow();
        var (traceParent, traceState) = CurrentTrace();

        // Correlation and causation follow the request (P-6); a prebuilt envelope keeps what it already has.
        if (payload is EventEnvelope prebuilt)
        {
            return prebuilt with
            {
                Id = string.IsNullOrEmpty(prebuilt.Id) ? Guid.NewGuid().ToString() : prebuilt.Id,
                Source = string.IsNullOrEmpty(prebuilt.Source) ? options.CloudEventSource! : prebuilt.Source,
                Subject = prebuilt.Subject ?? request?.Subject,
                CorrelationId = prebuilt.CorrelationId ?? request?.CorrelationId ?? Guid.NewGuid().ToString(),
                CausationId = prebuilt.CausationId ?? request?.Id,
                Time = prebuilt.Time ?? now,
                RecordedTime = prebuilt.RecordedTime ?? now,
                TraceParent = prebuilt.TraceParent ?? traceParent,
                TraceState = prebuilt.TraceState ?? traceState,
            };
        }

        var descriptor = _registry.GetDescriptor(payload.GetType());

        var carried = new Dictionary<string, string>(StringComparer.Ordinal);
        var data = descriptor.Encode(payload, carried);

        // E-5: attributes the request carried are copied when the new type declares them and the handler left them unset.
        if (request is not null)
        {
            foreach (var name in descriptor.CarriedNames)
            {
                if (!carried.ContainsKey(name) && request.Extensions.TryGetValue(name, out var value))
                {
                    carried[name] = value;
                }
            }
        }

        return new EventEnvelope
        {
            Id = publish?.EventId ?? Guid.NewGuid().ToString(),
            Source = options.CloudEventSource!,
            Type = descriptor.TypeId,
            DataContentType = descriptor.ContentType,
            DataSchema = descriptor.Schema,
            Subject = publish?.Subject ?? request?.Subject,
            Time = now,
            RecordedTime = now,
            TraceParent = traceParent,
            TraceState = traceState,
            ExpiryInterval = publish?.ExpiryInterval ?? (options.MessageExpiry is { } expiry && expiry > TimeSpan.Zero ? expiry : null),
            CorrelationId = publish?.CorrelationId ?? request?.CorrelationId ?? Guid.NewGuid().ToString(),
            CausationId = publish?.CausationId ?? request?.Id,
            // A response names the action of its command, so the sender can match it by key (D-44).
            ActionKey = publish?.ActionKey ?? request?.ActionKey,
            Method = publish?.Method,
            Extensions = carried.ToImmutableDictionary(StringComparer.Ordinal),
            Data = data,
        };
    }

    private static (string? Parent, string? State) CurrentTrace()
    {
        var activity = Activity.Current;
        return activity is { IdFormat: ActivityIdFormat.W3C, Id: { } id } ? (id, activity.TraceStateString) : (null, null);
    }
}
