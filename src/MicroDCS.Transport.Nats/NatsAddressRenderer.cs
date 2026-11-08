using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;

namespace MicroDCS.Transport.Nats;

/// <summary>
/// Renders addresses as NATS subjects: <c>prefix.scope.scope.discriminator.intent</c> (D-38). Subscriptions use <c>*</c> for the scope
/// levels. A binding prefix written with <c>/</c> (as for MQTT) is rendered with <c>.</c>, so one configuration serves both transports.
/// Segments never contain <c>.</c>, <c>*</c>, <c>&gt;</c> or spaces, so no escaping exists. The shared group is not part of the subject:
/// it names the JetStream consumer that the instances share (D-10).
/// </summary>
public sealed class NatsAddressRenderer : IAddressRenderer
{
    /// <summary>Converts a configured prefix or reply base to its subject form.</summary>
    /// <param name="value">The configured value; <c>/</c> and <c>.</c> separate segments.</param>
    /// <returns>The subject form.</returns>
    public static string NormalizePrefix(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return value.Replace('/', '.');
    }

    /// <summary>Tests whether a configured prefix or reply base has only valid segments.</summary>
    /// <param name="value">The configured value.</param>
    /// <returns><see langword="true"/> when every segment follows the segment rule.</returns>
    public static bool IsValidPrefix(string value) => NormalizePrefix(value).Split('.').All(LogicalAddress.IsValidSegment);

    /// <summary>Makes an instance id usable as a subject token and consumer name: other characters become <c>_</c> (a pod name can contain dots).</summary>
    /// <param name="instanceId">The instance id.</param>
    /// <returns>The token.</returns>
    public static string InstanceToken(string instanceId)
    {
        ArgumentNullException.ThrowIfNull(instanceId);
        return string.Create(instanceId.Length, instanceId, static (span, id) =>
        {
            for (var i = 0; i < id.Length; i++)
            {
                span[i] = char.IsAsciiLetterOrDigit(id[i]) || id[i] is '_' or '-' ? id[i] : '_';
            }
        });
    }

    /// <inheritdoc />
    public string RenderPublish(LogicalAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        var parts = new List<string> { NormalizePrefix(address.Prefix) };
        parts.AddRange(address.ScopePath);
        if (address.Discriminator is not null)
        {
            parts.Add(address.Discriminator);
        }

        parts.Add(IntentNames.ToSegment(address.Intent));
        return string.Join('.', parts);
    }

    /// <inheritdoc />
    public IReadOnlyList<string> RenderSubscriptions(AddressPattern pattern)
    {
        ArgumentNullException.ThrowIfNull(pattern);
        var result = new List<string>();
        for (var depth = 0; depth <= pattern.WildcardLevels; depth++)
        {
            var parts = new List<string> { NormalizePrefix(pattern.Prefix) };
            parts.AddRange(Enumerable.Repeat("*", depth));
            if (pattern.Discriminator is not null)
            {
                parts.Add(pattern.Discriminator);
            }

            parts.Add(IntentNames.ToSegment(pattern.Intent));
            result.Add(string.Join('.', parts));
        }

        return result;
    }

    /// <inheritdoc />
    public bool TryParse(string prefix, string rendered, out LogicalAddress address) => TryParse(prefix, null, rendered, out address);

    /// <summary>Parses a received subject; a binding with a discriminator expects it as the token before the intent.</summary>
    /// <param name="prefix">The binding prefix as configured.</param>
    /// <param name="discriminator">The discriminator of the binding, or <see langword="null"/>.</param>
    /// <param name="rendered">The received subject.</param>
    /// <param name="address">The logical address when parsing succeeded.</param>
    /// <returns><see langword="true"/> when the subject belongs to the prefix, ends in an intent and has valid segments.</returns>
    public static bool TryParse(string prefix, string? discriminator, string rendered, out LogicalAddress address)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(rendered);
        address = null!;
        var normalized = NormalizePrefix(prefix);
        if (!rendered.StartsWith(normalized + ".", StringComparison.Ordinal))
        {
            return false;
        }

        var rest = rendered[(normalized.Length + 1)..].Split('.');
        if (!IntentNames.TryParse(rest[^1], out var intent))
        {
            return false;
        }

        var scope = rest.SkipLast(1).ToList();
        if (discriminator is not null)
        {
            if (scope.Count == 0 || scope[^1] != discriminator)
            {
                return false;
            }

            scope.RemoveAt(scope.Count - 1);
        }

        if (!scope.All(LogicalAddress.IsValidSegment))
        {
            return false;
        }

        address = new LogicalAddress(prefix, [.. scope], discriminator, intent);
        return true;
    }

    /// <summary>Tests whether a subject matches a filter built by this renderer (only <c>*</c>).</summary>
    /// <param name="filter">The filter as subscribed.</param>
    /// <param name="subject">The subject of a received message.</param>
    /// <returns><see langword="true"/> when the subject matches.</returns>
    public static bool Matches(string filter, string subject)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentNullException.ThrowIfNull(subject);
        var f = filter.Split('.');
        var t = subject.Split('.');
        return f.Length == t.Length && f.Zip(t).All(static p => p.First == "*" || p.First == p.Second);
    }
}
