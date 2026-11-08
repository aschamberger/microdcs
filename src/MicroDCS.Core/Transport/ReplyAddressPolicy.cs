using MicroDCS.Core.Hosting;

namespace MicroDCS.Core.Transport;

/// <summary>
/// What a transport knows about its own address space, for <see cref="ReplyAddressPolicy"/>: the segment separator, the wildcard
/// characters, and the prefixes and reply bases of this instance's bindings in the transport's rendering.
/// </summary>
/// <param name="Separator">The segment separator, <c>/</c> for MQTT and <c>.</c> for NATS.</param>
/// <param name="Wildcards">Characters with a wildcard meaning, <c>+#</c> for MQTT and <c>*&gt;</c> for NATS.</param>
/// <param name="OwnPrefixes">The binding prefixes of this instance: its data, event, command and state addresses live below them.</param>
/// <param name="OwnReplyBases">The reply bases of this instance's bindings: instance-to-instance replies go there and stay allowed.</param>
public sealed record ReplyAddressRules(char Separator, string Wildcards, IReadOnlyList<string> OwnPrefixes, IReadOnlyList<string> OwnReplyBases);

/// <summary>
/// The check of a reply address a peer sends along (MQTT response topic, NATS <c>Mdcs-Reply-To</c>) before this instance publishes
/// to it with its own permissions (D-48). Rejected: system and server namespaces (<c>$</c>), wildcards, empty or white-space
/// segments, and addresses below one of this instance's binding prefixes (its own event and state topics, a confused deputy);
/// addresses below an own reply base stay allowed. A binding can further restrict to <c>AllowedReplyPrefixes</c>.
/// </summary>
public static class ReplyAddressPolicy
{
    /// <summary>Checks a reply address.</summary>
    /// <param name="reply">The address the peer named.</param>
    /// <param name="rules">The transport's rules.</param>
    /// <param name="binding">The binding the message arrived on.</param>
    /// <returns><see langword="null"/> when allowed, otherwise the reason (for the security log; never the address itself).</returns>
    public static string? Check(string reply, ReplyAddressRules rules, BindingOptions binding)
    {
        ArgumentNullException.ThrowIfNull(reply);
        ArgumentNullException.ThrowIfNull(rules);
        ArgumentNullException.ThrowIfNull(binding);
        if (reply.Length == 0 || reply.Any(static c => char.IsWhiteSpace(c) || char.IsControl(c)))
        {
            return "empty or white space";
        }

        if (reply[0] == '$')
        {
            return "system namespace";
        }

        if (reply.IndexOfAny(rules.Wildcards.ToCharArray()) >= 0)
        {
            return "wildcard";
        }

        if (reply.Split(rules.Separator).Any(static s => s.Length == 0))
        {
            return "empty segment";
        }

        var ownReply = rules.OwnReplyBases.Any(b => IsBelow(reply, b, rules.Separator));
        if (!ownReply && rules.OwnPrefixes.Any(p => IsBelow(reply, p, rules.Separator)))
        {
            return "below a binding prefix of this instance";
        }

        if (!ownReply && !string.IsNullOrWhiteSpace(binding.AllowedReplyPrefixes))
        {
            var allowed = binding.AllowedReplyPrefixes.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            if (!allowed.Any(a => reply.StartsWith(a, StringComparison.Ordinal)))
            {
                return "not in AllowedReplyPrefixes";
            }
        }

        return null;
    }

    private static bool IsBelow(string address, string prefix, char separator) =>
        prefix.Length > 0
        && address.StartsWith(prefix, StringComparison.Ordinal)
        && (address.Length == prefix.Length || address[prefix.Length] == separator);
}
