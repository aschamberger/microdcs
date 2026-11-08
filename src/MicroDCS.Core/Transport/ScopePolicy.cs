namespace MicroDCS.Core.Transport;

/// <summary>
/// Compares the scope of the address a message arrived on with the scope in its subject (A-7, T-4). Topic ACLs only
/// protect a scope if the scope the processor acts on is the one in the address.
/// </summary>
public enum ScopeCheck
{
    /// <summary>Reject a message whose subject scope differs from the address scope (default).</summary>
    Enforce,

    /// <summary>No check. The deployment relies on transport access control alone.</summary>
    Off,
}

/// <summary>What to do with a message that has a subject but arrived on a level-0 address (no scope path), R-6.</summary>
public enum Level0Subjects
{
    /// <summary>Reject (default): a scoped client must not be able to act on any scope through an unscoped address.</summary>
    Reject,

    /// <summary>Accept. The binding is documented as protected by transport ACLs on that address.</summary>
    Accept,
}

/// <summary>Scope decision for one inbound message. Pure, so it is unit-testable without a transport.</summary>
public static class ScopePolicy
{
    /// <summary>Applies the policy.</summary>
    /// <param name="check">Whether to enforce.</param>
    /// <param name="level0">What to do on an address without scope.</param>
    /// <param name="address">The parsed address the message arrived on.</param>
    /// <param name="subject">The envelope subject.</param>
    /// <returns><see langword="true"/> when the message may be processed.</returns>
    public static bool Allows(ScopeCheck check, Level0Subjects level0, LogicalAddress? address, string? subject)
    {
        if (check == ScopeCheck.Off || subject is null || address is null)
        {
            return true;
        }

        if (address.ScopePath.IsEmpty)
        {
            return level0 == Level0Subjects.Accept;
        }

        return LogicalAddress.TryGetScopePath(subject, out var path) && path.SequenceEqual(address.ScopePath);
    }
}
