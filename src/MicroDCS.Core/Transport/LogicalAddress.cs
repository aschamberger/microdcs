using System.Collections.Immutable;
using MicroDCS.Core.Messaging;

namespace MicroDCS.Core.Transport;

/// <summary>
/// The transport-neutral address of a message (A-3): binding prefix, scope path, optional discriminator and intent.
/// Each transport has a renderer; processors and the pipeline never see a rendered address.
/// </summary>
/// <param name="Prefix">The binding prefix, for example <c>dcs/jobs</c> (separators are the transport's business, see <see cref="IAddressRenderer"/>).</param>
/// <param name="ScopePath">The scope as path segments; empty for a level-0 address (R-6).</param>
/// <param name="Discriminator">Separates processors that share a prefix, or <see langword="null"/>.</param>
/// <param name="Intent">The message intent.</param>
public sealed record LogicalAddress(
    string Prefix,
    ImmutableArray<string> ScopePath,
    string? Discriminator,
    Intent Intent)
{
    /// <summary>The scope as the dotted form used in <c>subject</c>.</summary>
    public string Scope => string.Join('.', ScopePath);

    /// <summary>
    /// Segments must be non-empty and use only <c>A-Z a-z 0-9 _ -</c>. This is the one escaping rule for all
    /// transports: it excludes every separator and wildcard of MQTT, NATS, AMQP and Zenoh.
    /// </summary>
    /// <param name="segment">The segment to test.</param>
    /// <returns><see langword="true"/> when the segment is allowed.</returns>
    public static bool IsValidSegment(string segment) =>
        segment.Length > 0 && segment.All(static c => char.IsAsciiLetterOrDigit(c) || c is '_' or '-');

    /// <summary>Derives the scope path from the scope part of a subject (<c>scope[/resource]</c>, scope dotted).</summary>
    /// <param name="subject">The subject.</param>
    /// <param name="path">The path when the subject is valid.</param>
    /// <returns><see langword="true"/> when every scope segment is valid.</returns>
    public static bool TryGetScopePath(string subject, out ImmutableArray<string> path)
    {
        var slash = subject.IndexOf('/', StringComparison.Ordinal);
        var scope = slash < 0 ? subject : subject[..slash];
        var segments = scope.Split('.');
        path = segments.All(IsValidSegment) ? [.. segments] : default;
        return !path.IsDefault;
    }
}

/// <summary>A subscription pattern: the address with up to <see cref="WildcardLevels"/> single-level wildcards in the scope path.</summary>
/// <param name="Prefix">The binding prefix.</param>
/// <param name="WildcardLevels">Number of scope levels; a pattern is rendered for every depth from 0 to this value.</param>
/// <param name="Discriminator">The discriminator or <see langword="null"/>.</param>
/// <param name="Intent">The intent.</param>
/// <param name="SharedGroup">Competing-consumer group, or <see langword="null"/> for none. Rendered natively (D-10).</param>
public sealed record AddressPattern(
    string Prefix,
    int WildcardLevels,
    string? Discriminator,
    Intent Intent,
    string? SharedGroup);

/// <summary>Renders and parses addresses for one transport.</summary>
public interface IAddressRenderer
{
    /// <summary>Renders the address of a message to publish.</summary>
    /// <param name="address">The logical address.</param>
    /// <returns>The transport address.</returns>
    string RenderPublish(LogicalAddress address);

    /// <summary>Renders the subscriptions for a pattern (one per scope depth).</summary>
    /// <param name="pattern">The pattern.</param>
    /// <returns>The transport subscriptions.</returns>
    IReadOnlyList<string> RenderSubscriptions(AddressPattern pattern);

    /// <summary>Parses a received address back to a logical address, for the scope check (A-7).</summary>
    /// <param name="prefix">The binding prefix the address must start with.</param>
    /// <param name="rendered">The received transport address.</param>
    /// <param name="address">The logical address when parsing succeeded.</param>
    /// <returns><see langword="true"/> when the address belongs to the prefix and is well formed.</returns>
    bool TryParse(string prefix, string rendered, out LogicalAddress address);
}
