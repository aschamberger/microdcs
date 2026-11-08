using MicroDCS.Core.Messaging;
using MicroDCS.Core.Transport;

namespace MicroDCS.Transport.Mqtt;

/// <summary>
/// Renders addresses as MQTT topics: <c>prefix/scope/.../discriminator/intent</c> (D-38). Subscriptions use <c>+</c> for the scope
/// levels and, with a shared group, the native <c>$share/{group}/</c> form (D-10). Segments never contain <c>/</c>, <c>+</c>,
/// <c>#</c> or <c>$</c>, so no escaping exists.
/// </summary>
public sealed class MqttAddressRenderer : IAddressRenderer
{
    private const string SharePrefix = "$share/";

    private static readonly SlashAddressRenderer _slash = new();

    /// <inheritdoc />
    public string RenderPublish(LogicalAddress address) => _slash.RenderPublish(address);

    /// <inheritdoc />
    public IReadOnlyList<string> RenderSubscriptions(AddressPattern pattern)
    {
        ArgumentNullException.ThrowIfNull(pattern);
        var filters = _slash.RenderSubscriptions(pattern);
        if (pattern.SharedGroup is null)
        {
            return filters;
        }

        if (!LogicalAddress.IsValidSegment(pattern.SharedGroup))
        {
            throw new ArgumentException("The shared group may only use letters, digits, '_' and '-'.", nameof(pattern));
        }

        return [.. filters.Select(f => SharePrefix + pattern.SharedGroup + "/" + f)];
    }

    /// <inheritdoc />
    public bool TryParse(string prefix, string rendered, out LogicalAddress address) => TryParse(prefix, null, rendered, out address);

    /// <summary>Parses a received topic; a binding with a discriminator expects it as the segment before the intent.</summary>
    /// <param name="prefix">The binding prefix.</param>
    /// <param name="discriminator">The discriminator of the binding, or <see langword="null"/>.</param>
    /// <param name="rendered">The received topic.</param>
    /// <param name="address">The logical address when parsing succeeded.</param>
    /// <returns><see langword="true"/> when the topic belongs to the prefix, ends in an intent and has valid segments.</returns>
    public static bool TryParse(string prefix, string? discriminator, string rendered, out LogicalAddress address)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentNullException.ThrowIfNull(rendered);
        address = null!;
        if (!rendered.StartsWith(prefix + "/", StringComparison.Ordinal))
        {
            return false;
        }

        var rest = rendered[(prefix.Length + 1)..].Split('/');
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

    /// <summary>Tests whether a topic matches a subscription filter built by this renderer (only <c>+</c>, and a leading <c>$share/{group}/</c>, which the broker does not deliver).</summary>
    /// <param name="filter">The filter as subscribed.</param>
    /// <param name="topic">The topic of a received message.</param>
    /// <returns><see langword="true"/> when the topic matches.</returns>
    public static bool Matches(string filter, string topic)
    {
        ArgumentNullException.ThrowIfNull(filter);
        ArgumentNullException.ThrowIfNull(topic);
        if (filter.StartsWith(SharePrefix, StringComparison.Ordinal))
        {
            var slash = filter.IndexOf('/', SharePrefix.Length);
            filter = slash < 0 ? string.Empty : filter[(slash + 1)..];
        }

        var f = filter.Split('/');
        var t = topic.Split('/');
        return f.Length == t.Length && f.Zip(t).All(static p => p.First == "+" || p.First == p.Second);
    }
}
