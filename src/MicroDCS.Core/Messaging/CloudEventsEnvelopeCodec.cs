using System.Collections.Frozen;
using System.Globalization;
using CloudNative.CloudEvents;

namespace MicroDCS.Core.Messaging;

/// <summary>
/// <see cref="IEnvelopeCodec"/> on the upstream CloudEvents SDK (D-14, D-36). The SDK parses and formats the
/// standard attributes; the MicroDCS attributes (<c>recordedtime</c>, <c>correlationid</c>, <c>mdcserrorkind</c>
/// and so on) are string extensions handled here.
/// </summary>
public sealed class CloudEventsEnvelopeCodec : IEnvelopeCodec
{
    private const string SpecVersionName = "specversion";
    private const string SupportedSpecVersion = "1.0";

    // Standard attributes the SDK knows. Extensions with these names are dropped on decode (E-3).
    private static readonly FrozenSet<string> _standardNames = new[]
    {
        SpecVersionName, "id", "source", "type", "datacontenttype", "dataschema", "subject", "time",
    }.ToFrozenSet(StringComparer.Ordinal);

    // MicroDCS model attributes, mapped to envelope properties (not extensions).
    private static readonly FrozenSet<string> _modelNames = new[]
    {
        "recordedtime", "traceparent", "tracestate", "expirytime", "expiryinterval", "correlationid",
        "causationid", "method", "mdcsactionkey", "mdcserrorkind", "mdcserrormessage", "mdcserrorcontext",
    }.ToFrozenSet(StringComparer.Ordinal);

    private const string DefaultErrorMessage = "Unknown error occurred.";

    /// <inheritdoc />
    public bool TryDecode(in EnvelopeCarrier carrier, out EventEnvelope envelope, out EnvelopeError? failure)
    {
        // The contract declares a non-null out value; callers must not read it when the method returns false.
        envelope = null!;
        var attributes = carrier.Attributes;

        if (attributes.TryGetValue(SpecVersionName, out var specVersion) && specVersion != SupportedSpecVersion)
        {
            failure = new EnvelopeError(ErrorKind.PropertyInvalid, $"Attribute '{SpecVersionName}' is not supported.");
            return false;
        }

        foreach (var required in new[] { "id", "source", "type" })
        {
            if (!attributes.TryGetValue(required, out var value) || string.IsNullOrEmpty(value))
            {
                failure = new EnvelopeError(ErrorKind.MissingProperty, $"Attribute '{required}' is missing.");
                return false;
            }
        }

        var cloudEvent = new CloudEvent();
        foreach (var name in _standardNames)
        {
            if (name != SpecVersionName && attributes.TryGetValue(name, out var value) && !TrySet(cloudEvent, name, value, out failure))
            {
                return false;
            }
        }

        if (!TryReadModel(attributes, out var model, out failure))
        {
            return false;
        }

        envelope = new EventEnvelope
        {
            Id = attributes["id"],
            Source = attributes["source"],
            Type = attributes["type"],
            DataContentType = cloudEvent.DataContentType ?? carrier.ContentType,
            DataSchema = attributes.GetValueOrDefault("dataschema"),
            Subject = cloudEvent.Subject,
            Time = cloudEvent.Time,
            RecordedTime = model.RecordedTime,
            TraceParent = attributes.GetValueOrDefault("traceparent"),
            TraceState = attributes.GetValueOrDefault("tracestate"),
            ExpiryTime = model.ExpiryTime,
            ExpiryInterval = model.ExpiryInterval,
            CorrelationId = attributes.GetValueOrDefault("correlationid"),
            CausationId = attributes.GetValueOrDefault("causationid"),
            Method = attributes.GetValueOrDefault("method"),
            ActionKey = attributes.GetValueOrDefault("mdcsactionkey"),
            Error = model.Error,
            Extensions = CollectExtensions(attributes),
            Data = carrier.Data,
        };
        failure = null;
        return true;
    }

    /// <inheritdoc />
    public EnvelopeCarrier Encode(EventEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var cloudEvent = new CloudEvent
        {
            Id = envelope.Id,
            Source = new Uri(envelope.Source, UriKind.RelativeOrAbsolute),
            Type = envelope.Type,
            DataContentType = envelope.DataContentType,
            DataSchema = envelope.DataSchema is null ? null : new Uri(envelope.DataSchema, UriKind.RelativeOrAbsolute),
            Subject = envelope.Subject,
            Time = envelope.Time,
        };

        var attributes = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [SpecVersionName] = SupportedSpecVersion,
        };
        foreach (var (attribute, value) in cloudEvent.GetPopulatedAttributes())
        {
            attributes[attribute.Name] = attribute.Type.Format(value);
        }

        AddModelAttributes(attributes, envelope);
        foreach (var (name, value) in envelope.Extensions)
        {
            if (!_standardNames.Contains(name) && !_modelNames.Contains(name))
            {
                attributes[name] = value;
            }
        }

        return new EnvelopeCarrier(attributes, envelope.DataContentType, envelope.Data);
    }

    private static bool TrySet(CloudEvent cloudEvent, string name, string value, out EnvelopeError? failure)
    {
        try
        {
            cloudEvent.SetAttributeFromString(name, value);
            failure = null;
            return true;
        }
        catch (Exception ex) when (ex is ArgumentException or FormatException or InvalidOperationException)
        {
            // Name the attribute, never the value.
            failure = new EnvelopeError(ErrorKind.PropertyInvalid, $"Attribute '{name}' is invalid.");
            return false;
        }
    }

    private static bool TryReadModel(IReadOnlyDictionary<string, string> attributes, out ModelValues model, out EnvelopeError? failure)
    {
        model = default;
        failure = null;

        if (!TryParseTime(attributes, "recordedtime", out var recorded, out failure)
            || !TryParseTime(attributes, "expirytime", out var expiry, out failure))
        {
            return false;
        }

        TimeSpan? interval = null;
        if (attributes.TryGetValue("expiryinterval", out var intervalText))
        {
            if (!int.TryParse(intervalText, NumberStyles.None, CultureInfo.InvariantCulture, out var seconds))
            {
                failure = new EnvelopeError(ErrorKind.PropertyInvalid, "Attribute 'expiryinterval' is invalid.");
                return false;
            }

            interval = TimeSpan.FromSeconds(seconds);
        }

        if (!TryReadError(attributes, out var error, out failure))
        {
            return false;
        }

        model = new ModelValues(recorded, expiry, interval, error);
        return true;
    }

    private static bool TryParseTime(IReadOnlyDictionary<string, string> attributes, string name, out DateTimeOffset? value, out EnvelopeError? failure)
    {
        value = null;
        failure = null;
        if (!attributes.TryGetValue(name, out var text))
        {
            return true;
        }

        if (!DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed))
        {
            failure = new EnvelopeError(ErrorKind.PropertyInvalid, $"Attribute '{name}' is invalid.");
            return false;
        }

        value = parsed;
        return true;
    }

    private static bool TryReadError(IReadOnlyDictionary<string, string> attributes, out EnvelopeError? error, out EnvelopeError? failure)
    {
        error = null;
        failure = null;
        if (!attributes.TryGetValue("mdcserrorkind", out var kindText))
        {
            return true;
        }

        if (!Enum.TryParse<ErrorKind>(kindText, ignoreCase: true, out var kind) || !Enum.IsDefined(kind))
        {
            failure = new EnvelopeError(ErrorKind.PropertyInvalid, "Attribute 'mdcserrorkind' is invalid.");
            return false;
        }

        var message = attributes.GetValueOrDefault("mdcserrormessage") ?? DefaultErrorMessage;
        error = new EnvelopeError(kind, message, ParseContext(attributes.GetValueOrDefault("mdcserrorcontext")));
        return true;
    }

    private static Dictionary<string, string>? ParseContext(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        var context = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in text.Split(','))
        {
            var separator = pair.IndexOf('=', StringComparison.Ordinal);
            if (separator > 0)
            {
                context[pair[..separator]] = pair[(separator + 1)..];
            }
        }

        return context;
    }

    private static FrozenDictionary<string, string> CollectExtensions(IReadOnlyDictionary<string, string> attributes)
    {
        var extensions = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, value) in attributes)
        {
            // Collision guard (E-3): a name that is standard or part of the model never becomes an extension.
            if (!_standardNames.Contains(name) && !_modelNames.Contains(name))
            {
                extensions[name] = value;
            }
        }

        return extensions.ToFrozenDictionary(StringComparer.Ordinal);
    }

    private static void AddModelAttributes(Dictionary<string, string> attributes, EventEnvelope envelope)
    {
        AddIfSet(attributes, "recordedtime", envelope.RecordedTime?.ToString("O", CultureInfo.InvariantCulture));
        AddIfSet(attributes, "traceparent", envelope.TraceParent);
        AddIfSet(attributes, "tracestate", envelope.TraceState);
        AddIfSet(attributes, "expirytime", envelope.ExpiryTime?.ToString("O", CultureInfo.InvariantCulture));
        AddIfSet(attributes, "expiryinterval", envelope.ExpiryInterval is { } interval
            ? ((long)interval.TotalSeconds).ToString(CultureInfo.InvariantCulture)
            : null);
        AddIfSet(attributes, "correlationid", envelope.CorrelationId);
        AddIfSet(attributes, "causationid", envelope.CausationId);
        AddIfSet(attributes, "method", envelope.Method);
        AddIfSet(attributes, "mdcsactionkey", envelope.ActionKey);

        if (envelope.Error is { } error)
        {
            attributes["mdcserrorkind"] = error.Kind.ToString();
            attributes["mdcserrormessage"] = string.IsNullOrEmpty(error.Message) ? DefaultErrorMessage : error.Message;
            AddIfSet(attributes, "mdcserrorcontext", FormatContext(error.Context));
        }
    }

    private static string? FormatContext(IReadOnlyDictionary<string, string>? context)
    {
        if (context is null || context.Count == 0)
        {
            return null;
        }

        var parts = new List<string>(context.Count);
        foreach (var (key, value) in context)
        {
            // D-30: no escaping, so separators in keys or values cannot be represented.
            if (key.Contains(',', StringComparison.Ordinal) || key.Contains('=', StringComparison.Ordinal)
                || value.Contains(',', StringComparison.Ordinal))
            {
                throw new ArgumentException("Error context keys and values must not contain ',' or '='.", nameof(context));
            }

            parts.Add($"{key}={value}");
        }

        return string.Join(',', parts);
    }

    private static void AddIfSet(Dictionary<string, string> attributes, string name, string? value)
    {
        if (value is not null)
        {
            attributes[name] = value;
        }
    }

    private readonly record struct ModelValues(DateTimeOffset? RecordedTime, DateTimeOffset? ExpiryTime, TimeSpan? ExpiryInterval, EnvelopeError? Error);
}
